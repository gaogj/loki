package memberlist

import (
	"bytes"
	"crypto/md5"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/hashicorp/go-sockaddr"
	"github.com/hashicorp/memberlist"
	"github.com/prometheus/client_golang/prometheus"
)

type messageType uint8

const (
	_ messageType = iota // don't use 0
	packet
	stream
)

type TcpTransportConfig struct {
	// BindAddrs is a list of addresses to bind to.
	BindAddrs []string

	// BindPort is the port to listen on, for each address above.
	BindPort int

	Logger log.Logger

	// Timeout used when making connections to other nodes to send packet.
	// Default is no timeout
	PacketDialTimeout time.Duration

	// Timeout for writing packet data. Default - no timeout.
	PacketWriteTimeout time.Duration

	// WriteTo is used to send "UDP" packets. Since we use TCP, we can detect more errors,
	// but memberlist doesn't seem to cope with that very well.
	ReportWriteToErrors bool

	// Where to put custom metrics. nil = don't register.
	MetricsRegisterer prometheus.Registerer
	MetricsNamespace  string
}

// TcpTransport is a Transport implementation that uses TCP for both packet and stream operations.
// It opens new TCP connection for each packet, and closes it after sending.
type TcpTransport struct {
	cfg          *TcpTransportConfig
	packetCh     chan *memberlist.Packet
	connCh       chan net.Conn
	logger       log.Logger
	wg           sync.WaitGroup
	tcpListeners []*net.TCPListener

	shutdown int32

	// metrics
	incomingStreams      prometheus.Counter
	outgoingStreams      prometheus.Counter
	outgoingStreamErrors prometheus.Counter

	receivedPackets       prometheus.Counter
	receivedPacketsBytes  prometheus.Counter
	receivedPacketsErrors prometheus.Counter
	sentPackets           prometheus.Counter
	sentPacketsBytes      prometheus.Counter
	sentPacketsErrors     prometheus.Counter

	unknownConnections prometheus.Counter
}

// NewNetTransport returns a net transport with the given configuration. On
// success all the network listeners will be created and listening.
func NewTcpTransport(config *TcpTransportConfig) (*TcpTransport, error) {
	if len(config.BindAddrs) == 0 {
		return nil, fmt.Errorf("at least one bind address is required")
	}

	// Build out the new transport.
	var ok bool
	t := TcpTransport{
		cfg:      config,
		packetCh: make(chan *memberlist.Packet),
		connCh:   make(chan net.Conn),
		logger:   config.Logger,
	}

	t.registerMetrics()

	// Clean up listeners if there's an error.
	defer func() {
		if !ok {
			_ = t.Shutdown()
		}
	}()

	// Build all the TCP and UDP listeners.
	port := config.BindPort
	for _, addr := range config.BindAddrs {
		ip := net.ParseIP(addr)

		tcpAddr := &net.TCPAddr{IP: ip, Port: port}
		tcpLn, err := net.ListenTCP("tcp", tcpAddr)
		if err != nil {
			return nil, fmt.Errorf("failed to start TCP listener on %q port %d: %v", addr, port, err)
		}
		t.tcpListeners = append(t.tcpListeners, tcpLn)

		// If the config port given was zero, use the first TCP listener
		// to pick an available port and then apply that to everything
		// else.
		if port == 0 {
			port = tcpLn.Addr().(*net.TCPAddr).Port
		}
	}

	// Fire them up now that we've been able to create them all.
	for i := 0; i < len(config.BindAddrs); i++ {
		t.wg.Add(1)
		go t.tcpListen(t.tcpListeners[i])
	}

	ok = true
	return &t, nil
}

// tcpListen is a long running goroutine that accepts incoming TCP connections
// and spawns new go routine to handle each connection. This transport uses TCP connections
// for both packet sending and streams.
// (copied from Memberlist net_transport.go)
func (t *TcpTransport) tcpListen(tcpLn *net.TCPListener) {
	defer t.wg.Done()

	// baseDelay is the initial delay after an AcceptTCP() error before attempting again
	const baseDelay = 5 * time.Millisecond

	// maxDelay is the maximum delay after an AcceptTCP() error before attempting again.
	// In the case that tcpListen() is error-looping, it will delay the shutdown check.
	// Therefore, changes to maxDelay may have an effect on the latency of shutdown.
	const maxDelay = 1 * time.Second

	var loopDelay time.Duration
	for {
		conn, err := tcpLn.AcceptTCP()
		if err != nil {
			if s := atomic.LoadInt32(&t.shutdown); s == 1 {
				break
			}

			if loopDelay == 0 {
				loopDelay = baseDelay
			} else {
				loopDelay *= 2
			}

			if loopDelay > maxDelay {
				loopDelay = maxDelay
			}

			level.Error(t.logger).Log("msg", "TcpTransport: Error accepting TCP connection", "err", err)
			time.Sleep(loopDelay)
			continue
		}
		// No error, reset loop delay
		loopDelay = 0

		go t.handleConnection(conn)
	}
}

func (t *TcpTransport) handleConnection(conn *net.TCPConn) {
	level.Debug(t.logger).Log("msg", "TcpTransport: New connection", "addr", conn.RemoteAddr())

	closeConn := true
	defer func() {
		if closeConn {
			_ = conn.Close()
		}
	}()

	// let's read first byte, and determine what to do about this connection
	msgType := []byte{0}
	_, err := io.ReadFull(conn, msgType)
	if err != nil {
		level.Error(t.logger).Log("msg", "TcpTransport: failed to read message type", "err", err)
		return
	}

	if messageType(msgType[0]) == stream {
		t.incomingStreams.Inc()

		// hand over this connection to memberlist
		closeConn = false
		t.connCh <- conn
	} else if messageType(msgType[0]) == packet {
		// it's a memberlist "packet", which contains an address and data.
		t.receivedPackets.Inc()

		// before reading packet, read the address
		b := []byte{0}
		_, err := io.ReadFull(conn, b)
		if err != nil {
			t.receivedPacketsErrors.Inc()
			level.Error(t.logger).Log("msg", "TcpTransport: error while reading address:", "err", err)
			return
		}

		addrBuf := make([]byte, b[0])
		_, err = io.ReadFull(conn, addrBuf)
		if err != nil {
			t.receivedPacketsErrors.Inc()
			level.Error(t.logger).Log("msg", "TcpTransport: error while reading address:", "err", err)
			return
		}

		// read the rest to buffer -- this is the "packet" itself
		buf, err := ioutil.ReadAll(conn)
		if err != nil {
			t.receivedPacketsErrors.Inc()
			level.Error(t.logger).Log("msg", "TcpTransport: error while reading packet data:", "err", err)
			return
		}

		if len(buf) < md5.Size {
			t.receivedPacketsErrors.Inc()
			level.Error(t.logger).Log("msg", "TcpTransport: not enough data received", "length", len(buf))
			return
		}

		receivedDigest := buf[len(buf)-md5.Size:]
		buf = buf[:len(buf)-md5.Size]

		expectedDigest := md5.Sum(buf)

		if bytes.Compare(receivedDigest, expectedDigest[:]) != 0 {
			t.receivedPacketsErrors.Inc()
			level.Warn(t.logger).Log("msg", "TcpTransport: packet digest mismatch", "expected", fmt.Sprintf("%x", expectedDigest), "received", fmt.Sprintf("%x", receivedDigest))
		}

		level.Debug(t.logger).Log("msg", "TcpTransport: Received packet", "addr", addr(addrBuf), "size", len(buf), "hash", fmt.Sprintf("%x", receivedDigest))

		t.receivedPacketsBytes.Add(float64(len(buf)))

		t.packetCh <- &memberlist.Packet{
			Buf:       buf,
			From:      addr(addrBuf),
			Timestamp: time.Now(),
		}
	} else {
		t.unknownConnections.Inc()
		level.Error(t.logger).Log("msg", "TcpTransport: unknown message type", "msgType", msgType)
	}
}

type addr string

func (a addr) Network() string {
	return "tcp"
}

func (a addr) String() string {
	return string(a)
}

// GetAutoBindPort returns the bind port that was automatically given by the
// kernel, if a bind port of 0 was given.
func (t *TcpTransport) GetAutoBindPort() int {
	// We made sure there's at least one TCP listener, and that one's
	// port was applied to all the others for the dynamic bind case.
	return t.tcpListeners[0].Addr().(*net.TCPAddr).Port
}

// See Transport. (Copied from memberlist' net_transport.go)
func (t *TcpTransport) FinalAdvertiseAddr(ip string, port int) (net.IP, int, error) {
	var advertiseAddr net.IP
	var advertisePort int
	if ip != "" {
		// If they've supplied an address, use that.
		advertiseAddr = net.ParseIP(ip)
		if advertiseAddr == nil {
			return nil, 0, fmt.Errorf("failed to parse advertise address %q", ip)
		}

		// Ensure IPv4 conversion if necessary.
		if ip4 := advertiseAddr.To4(); ip4 != nil {
			advertiseAddr = ip4
		}
		advertisePort = port
	} else {
		if t.cfg.BindAddrs[0] == "0.0.0.0" {
			// Otherwise, if we're not bound to a specific IP, let's
			// use a suitable private IP address.
			var err error
			ip, err = sockaddr.GetPrivateIP()
			if err != nil {
				return nil, 0, fmt.Errorf("failed to get interface addresses: %v", err)
			}
			if ip == "" {
				return nil, 0, fmt.Errorf("no private IP address found, and explicit IP not provided")
			}

			advertiseAddr = net.ParseIP(ip)
			if advertiseAddr == nil {
				return nil, 0, fmt.Errorf("failed to parse advertise address: %q", ip)
			}
		} else {
			// Use the IP that we're bound to, based on the first
			// TCP listener, which we already ensure is there.
			advertiseAddr = t.tcpListeners[0].Addr().(*net.TCPAddr).IP
		}

		// Use the port we are bound to.
		advertisePort = t.GetAutoBindPort()
	}

	return advertiseAddr, advertisePort, nil
}

func (t *TcpTransport) WriteTo(b []byte, addr string) (time.Time, error) {
	t.sentPackets.Inc()
	t.sentPacketsBytes.Add(float64(len(b)))

	err := t.writeTo(b, addr)
	if err != nil {
		t.sentPacketsErrors.Inc()

		if t.cfg.ReportWriteToErrors {
			return time.Time{}, fmt.Errorf("WriteTo %s: %w", addr, err)
		}

		level.Warn(t.logger).Log("msg", "TcpTransport: WriteTo failed", "addr", addr, "err", err)
		return time.Now(), nil
	}

	return time.Now(), nil
}

func (t *TcpTransport) writeTo(b []byte, addr string) error {
	// Open connection, write packet header and data, data hash, close. Simple.
	c, err := net.DialTimeout("tcp", addr, t.cfg.PacketDialTimeout)
	if err != nil {
		return nil
	}

	closed := false
	defer func() {
		if !closed {
			// If we still need to close, then there was another error. Ignore this one.
			_ = c.Close()
		}
	}()

	if t.cfg.PacketWriteTimeout > 0 {
		deadline := time.Now().Add(t.cfg.PacketWriteTimeout)
		err := c.SetDeadline(deadline)
		if err != nil {
			return fmt.Errorf("setting deadline: %v", err)
		}
	}

	buf := bytes.Buffer{}
	buf.WriteByte(byte(packet))

	// We need to send our address to the other side, otherwise other side can only see IP and port from TCP header.
	// But that doesn't match our node address (source port is assigned automatically), which confuses memberlist.
	// We will announce first listener's address as our address. This is what memberlist's net_transport.go does as well.
	ourAddr := t.tcpListeners[0].Addr().String()
	if len(ourAddr) > 255 {
		return fmt.Errorf("local address too long")
	}

	buf.WriteByte(byte(len(ourAddr)))
	buf.WriteString(ourAddr)

	_, err = c.Write(buf.Bytes())
	if err != nil {
		return fmt.Errorf("sending local address: %v", err)
	}

	n, err := c.Write(b)
	if err != nil {
		return fmt.Errorf("sending data: %v", err)
	}
	if n != len(b) {
		return fmt.Errorf("sending data: short write")
	}

	// Append digest. We use md5 as quick and relatively short hash, not in cryptographic context.
	// This helped to find some bugs, so let's keep it.
	digest := md5.Sum(b)
	n, err = c.Write(digest[:])
	if err != nil {
		return fmt.Errorf("digest: %v", err)
	}
	if n != len(digest) {
		return fmt.Errorf("digest: short write")
	}

	closed = true
	err = c.Close()
	if err != nil {
		return fmt.Errorf("close: %v", err)
	}

	level.Debug(t.logger).Log("msg", "WriteTo: packet sent", "addr", addr, "size", len(b), "hash", fmt.Sprintf("%x", digest))
	return nil
}

func (t *TcpTransport) PacketCh() <-chan *memberlist.Packet {
	return t.packetCh
}

func (t *TcpTransport) DialTimeout(addr string, timeout time.Duration) (net.Conn, error) {
	t.outgoingStreams.Inc()

	c, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		t.outgoingStreamErrors.Inc()
		return nil, err
	}

	_, err = c.Write([]byte{byte(stream)})
	if err != nil {
		t.outgoingStreamErrors.Inc()
		_ = c.Close()
		return nil, err
	}

	return c, nil
}

func (t *TcpTransport) StreamCh() <-chan net.Conn {
	return t.connCh
}

func (t *TcpTransport) Shutdown() error {
	// This will avoid log spam about errors when we shut down.
	atomic.StoreInt32(&t.shutdown, 1)

	// Rip through all the connections and shut them down.
	for _, conn := range t.tcpListeners {
		_ = conn.Close()
	}

	// Block until all the listener threads have died.
	t.wg.Wait()
	t.unregisterMetrics()
	return nil
}

func (t *TcpTransport) registerMetrics() {
	const subsystem = "tcp_transport"

	t.incomingStreams = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: t.cfg.MetricsNamespace,
		Subsystem: subsystem,
		Name:      "incoming_memberlist_streams",
		Help:      "Number of incoming memberlist streams",
	})

	t.outgoingStreams = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: t.cfg.MetricsNamespace,
		Subsystem: subsystem,
		Name:      "outgoing_memberlist_streams",
		Help:      "Number of outgoing streams",
	})

	t.outgoingStreamErrors = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: t.cfg.MetricsNamespace,
		Subsystem: subsystem,
		Name:      "outgoing_memberlist_stream_errors",
		Help:      "Number of errors when opening memberlist stream to another node",
	})

	t.receivedPackets = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: t.cfg.MetricsNamespace,
		Subsystem: subsystem,
		Name:      "memberlist_packets_received",
		Help:      "Number of received memberlist packets",
	})

	t.receivedPacketsBytes = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: t.cfg.MetricsNamespace,
		Subsystem: subsystem,
		Name:      "memberlist_packets_received_bytes",
		Help:      "Total bytes received as packets",
	})

	t.receivedPacketsErrors = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: t.cfg.MetricsNamespace,
		Subsystem: subsystem,
		Name:      "memberlist_packets_received_errors",
		Help:      "Number of errors when receiving memberlist packets",
	})

	t.sentPackets = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: t.cfg.MetricsNamespace,
		Subsystem: subsystem,
		Name:      "memberlist_packets_sent",
		Help:      "Number of memberlist packets sent",
	})

	t.sentPacketsBytes = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: t.cfg.MetricsNamespace,
		Subsystem: subsystem,
		Name:      "memberlist_packets_sent_bytes",
		Help:      "Total bytes sent as packets",
	})

	t.sentPacketsErrors = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: t.cfg.MetricsNamespace,
		Subsystem: subsystem,
		Name:      "memberlist_packets_sent_errors",
		Help:      "Number of errors when sending memberlist packets",
	})

	t.unknownConnections = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: t.cfg.MetricsNamespace,
		Subsystem: subsystem,
		Name:      "unknown_connections",
		Help:      "Number of unknown TCP connections (not a packet or stream)",
	})

	if t.cfg.MetricsRegisterer == nil {
		return
	}

	// if registration fails, that's too bad, but don't panic
	for _, c := range t.getAllMetrics() {
		err := t.cfg.MetricsRegisterer.Register(c.(prometheus.Collector))
		if err != nil {
			level.Error(t.logger).Log("msg", "failed to register metrics", "err", err, "metric", c.Desc().String())
		}
	}
}

func (t *TcpTransport) unregisterMetrics() {
	if t.cfg.MetricsRegisterer == nil {
		return
	}

	for _, c := range t.getAllMetrics() {
		t.cfg.MetricsRegisterer.Unregister(c.(prometheus.Collector))
	}
}

func (t *TcpTransport) getAllMetrics() []prometheus.Metric {
	return []prometheus.Metric{
		t.incomingStreams,
		t.outgoingStreams,
		t.outgoingStreamErrors,
		t.receivedPackets,
		t.receivedPacketsErrors,
		t.sentPackets,
		t.sentPacketsErrors,
		t.unknownConnections,
	}
}
