package memberlist

import (
	"bytes"
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/hashicorp/memberlist"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/cortexproject/cortex/pkg/ring/kv/codec"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/flagext"
)

const (
	MaxKeyLength = 255 // We encode key length as one byte

	NodesTimestamps = "mlclient/timestamps"
)

type Config struct {
	// Name of this node in the memberlist cluster.
	NodeName string `yaml:"node_name"`

	Logger log.Logger

	// Memberlist values
	BindAddr string `yaml:"bind_addr"`
	BindPort int    `yaml:"bind_port"`

	StreamTimeout time.Duration `yaml:"stream_timeout"`

	// Timeout used when making connections to other nodes to send packet.
	// Default is no timeout
	PacketDialTimeout time.Duration `yaml:"packet_dial_timeout"`

	// Timeout for writing packet data. Default - no timeout.
	PacketWriteTimeout time.Duration `yaml:"packet_write_timeout"`

	// Transport logs lot of messages at debug level, so it deserves an extra flag for turning it on
	TransportDebug bool

	RetransmitMult   int           `yaml:"retransmit_factor"`
	PushPullInterval time.Duration `yaml:"pull_push_interval"`
	GossipInterval   time.Duration `yaml:"gossip_interval"`
	GossipNodes      int           `yaml:"gossip_nodes"`

	// List of members to join
	JoinMembers      flagext.StringSlice `yaml:"join_members"`
	AbortIfJoinFails bool                `yaml:"abort_if_cluster_join_fails"`

	LeftIngestersTimeout time.Duration `yaml:"left_ingesters_timeout"`

	LeaveTimeout time.Duration `yaml:"leave_timeout"`

	// Where to put custom metrics. Metrics are not registered, if this is nil.
	MetricsRegisterer prometheus.Registerer
	MetricsNamespace  string

	TimestampInterval time.Duration
}

func (cfg *Config) RegisterFlags(f *flag.FlagSet, prefix string) {
	// "Defaults to hostname" -- memberlist sets it to hostname by default.
	f.StringVar(&cfg.NodeName, prefix+"mlclient.nodename", "", "Name of the node in memberlist cluster. Defaults to hostname.")
	f.StringVar(&cfg.BindAddr, prefix+"mlclient.bind-addr", "", "IP address to listen on for gossip messages")
	f.IntVar(&cfg.BindPort, prefix+"mlclient.bind-port", 0, "Port to listen on for gossip messages")
	f.IntVar(&cfg.RetransmitMult, prefix+"mlclient.retransmit-factor", 0, "Multiplication factor used when sending out messages (factor * log(N+1))")
	f.Var(&cfg.JoinMembers, prefix+"mlclient.join", "Other cluster members to join")
	f.BoolVar(&cfg.AbortIfJoinFails, prefix+"mlclient.abort-if-join-fails", true, "If this node fails to join memberlist cluster, abort")
	f.DurationVar(&cfg.LeftIngestersTimeout, prefix+"mlclient.left-ingesters-timeout", 5*time.Minute, "How long to keep LEFT ingesters in the ring")
	f.DurationVar(&cfg.LeaveTimeout, prefix+"mlclient.leave-timeout", 5*time.Second, "Timeout for leaving memberlist cluster")
	f.DurationVar(&cfg.GossipInterval, prefix+"mlclient.gossip-interval", 0, "How often to gossip. Uses memberlist LAN defaults if 0")
	f.IntVar(&cfg.GossipNodes, prefix+"mlclient.gossip-nodes", 0, "How many nodes to gossip to. Uses memberlist LAN defaults if 0")
	f.DurationVar(&cfg.PushPullInterval, prefix+"mlclient.pullpush-interval", 0, "How often to use pull/push mechanism. Uses memberlist LAN defaults if 0")
	f.DurationVar(&cfg.TimestampInterval, prefix+"mlclient.timestamp-interval", 5*time.Second, "If not 0, update and gossip internal timestamp using this interval. Used by metrics.")
	f.BoolVar(&cfg.TransportDebug, prefix+"mlclient.transport-debug", false, "Log debug transport messages. Note: global log.level must be at debug level as well.")
}

type MemberlistClient struct {
	logger       log.Logger
	cfg          Config
	memberlist   *memberlist.Memberlist
	broadcasts   *memberlist.TransmitLimitedQueue
	defaultCodec codec.Codec

	// KV Store.
	storeMu sync.Mutex
	store   map[string]valueDesc

	// Key watchers
	watchersMu     sync.Mutex
	watchers       map[string][]chan string
	prefixWatchers map[string][]chan string

	// closed on shutdown
	shutdown chan struct{}

	// metrics
	numberOfReceivedMessages            prometheus.Counter
	totalSizeOfReceivedMessages         prometheus.Counter
	numberOfInvalidReceivedMessages     prometheus.Counter
	numberOfPulls, numberOfPushes       prometheus.Counter
	totalSizeOfPulls                    prometheus.Counter
	totalSizeOfPushes                   prometheus.Counter
	numberOfBroadcastMessagesInQueue    prometheus.GaugeFunc
	totalSizeOfBroadcastMessagesInQueue prometheus.Gauge
	casAttempts                         prometheus.Counter
	casFailures                         prometheus.Counter
	casSuccesses                        prometheus.Counter
	watchPrefixDroppedNotifications     *prometheus.CounterVec

	storeValuesDesc *prometheus.Desc
	storeSizesDesc  *prometheus.Desc

	memberlistMembersCount    prometheus.GaugeFunc
	memberlistHealthScore     prometheus.GaugeFunc
	memberlistMembersInfoDesc *prometheus.Desc
}

type valueDesc struct {
	// We store bytes here. Reason is that clients calling CAS function will modify the object in place,
	// but unless CAS succeeds, we don't want those modifications to be visible.
	value []byte

	// version is used to detect changes when doing CAS
	version uint

	// Coded used to encode/decode the value
	codec codec.Codec
}

func NewMemberlistClient(cfg Config, codec codec.Codec) (*MemberlistClient, error) {
	logger := cfg.Logger
	if logger == nil {
		logger = util.Logger
	}

	transportLogger := logger
	if !cfg.TransportDebug {
		transportLogger = level.NewFilter(transportLogger, level.AllowInfo())
	}
	tr, err := NewTcpTransport(&TcpTransportConfig{
		BindAddrs: []string{cfg.BindAddr},
		BindPort:  cfg.BindPort,
		Logger:    transportLogger,

		PacketWriteTimeout: cfg.PacketWriteTimeout,
		PacketDialTimeout:  cfg.PacketDialTimeout,

		MetricsRegisterer: cfg.MetricsRegisterer,
		MetricsNamespace:  cfg.MetricsNamespace,
	})

	if err != nil {
		return nil, fmt.Errorf("failed to create transport: %v", err)
	}

	mlCfg := memberlist.DefaultLANConfig()

	if cfg.BindAddr != "" {
		mlCfg.BindAddr = cfg.BindAddr
	}
	if cfg.BindPort != 0 {
		mlCfg.BindPort = cfg.BindPort
	}
	if cfg.StreamTimeout != 0 {
		mlCfg.TCPTimeout = cfg.StreamTimeout
	}
	if cfg.RetransmitMult != 0 {
		mlCfg.RetransmitMult = cfg.RetransmitMult
	}
	if cfg.PushPullInterval != 0 {
		mlCfg.PushPullInterval = cfg.PushPullInterval
	}
	if cfg.GossipInterval != 0 {
		mlCfg.GossipInterval = cfg.GossipInterval
	}
	if cfg.GossipNodes != 0 {
		mlCfg.GossipNodes = cfg.GossipNodes
	}
	if cfg.NodeName != "" {
		mlCfg.Name = cfg.NodeName
	}

	mlCfg.LogOutput = NewMemberlistLoggerAdapter(logger, false)
	mlCfg.Transport = tr

	// Memberlist uses UDPBufferSize to figure out how many messages it can put into single packet.
	// As we don't use UDP for sending packets, we can use higher value here.
	if mlCfg.UDPBufferSize < 16384 {
		mlCfg.UDPBufferSize = 10 * 1024 * 1024
	}

	memberlistClient := &MemberlistClient{
		cfg:            cfg,
		logger:         logger,
		store:          make(map[string]valueDesc),
		watchers:       make(map[string][]chan string),
		prefixWatchers: make(map[string][]chan string),
		defaultCodec:   codec,
		shutdown:       make(chan struct{}),
	}

	// Setup custom codec for nodes timestamps
	memberlistClient.store[NodesTimestamps] = valueDesc{
		value:   nil,
		version: 0,
		codec:   timestampsCodec{},
	}

	mlCfg.Delegate = memberlistClient

	list, err := memberlist.Create(mlCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create memberlist: %v", err)
	}

	// finish delegate initialization
	memberlistClient.memberlist = list
	memberlistClient.broadcasts = &memberlist.TransmitLimitedQueue{
		NumNodes:       list.NumMembers,
		RetransmitMult: cfg.RetransmitMult,
	}

	// Almost ready...
	memberlistClient.createAndRegisterMetrics()

	// Join the cluster
	if len(cfg.JoinMembers) > 0 {
		reached, err := memberlistClient.JoinMembers(cfg.JoinMembers)
		if err != nil && cfg.AbortIfJoinFails {
			_ = memberlistClient.memberlist.Shutdown()
			return nil, err
		}

		if err != nil {
			level.Error(logger).Log("msg", "failed to join memberlist cluster", "err", err)
		} else {
			level.Info(logger).Log("msg", "Joined memberlist cluster", "reached_nodes", reached)
		}
	}

	go memberlistClient.updateNodeTimestamp()

	return memberlistClient, nil
}

func (m *MemberlistClient) GetListeningPort() int {
	return int(m.memberlist.LocalNode().Port)
}

// see https://godoc.org/github.com/hashicorp/memberlist#Memberlist.Join
func (m *MemberlistClient) JoinMembers(members []string) (int, error) {
	return m.memberlist.Join(members)
}

// Stop tries to leave memberlist cluster and then shutdown memberlist client.
// We do this in order to send out last messages, typically that ingester has LEFT the ring.
func (m *MemberlistClient) Stop() {
	level.Info(m.logger).Log("msg", "leaving memberlist cluster")

	// TODO: should we empty our broadcast queue before leaving? That would make sure that we have sent out everything we wanted.

	err := m.memberlist.Leave(m.cfg.LeaveTimeout)
	if err != nil {
		level.Error(m.logger).Log("msg", "error when leaving memberlist cluster", "err", err)
	}

	close(m.shutdown)

	err = m.memberlist.Shutdown()
	if err != nil {
		level.Error(m.logger).Log("msg", "error when shutting down memberlist client", "err", err)
	}

	m.unregisterMetrics()
}

// KV interface
func (m *MemberlistClient) Get(ctx context.Context, key string) (interface{}, error) {
	return m.get(key)
}

func (m *MemberlistClient) get(key string) (out interface{}, err error) {
	m.storeMu.Lock()
	v := m.store[key]
	m.storeMu.Unlock()

	out = nil
	if v.value != nil {
		c := v.codec
		if c == nil {
			c = m.defaultCodec
		}

		out, err = c.Decode(v.value)
		if err != nil {
			return nil, err
		}
	}

	return out, nil
}

func (m *MemberlistClient) WatchKey(ctx context.Context, key string, f func(interface{}) bool) {
	// keep one extra notification, to avoid missing notification if we're busy running the function
	w := make(chan string, 1)

	// register watcher
	m.watchersMu.Lock()
	m.watchers[key] = append(m.watchers[key], w)
	m.watchersMu.Unlock()

	defer func() {
		// unregister watcher on exit
		m.watchersMu.Lock()
		defer m.watchersMu.Unlock()

		removeWatcherChannel(key, w, m.watchers)
	}()

	for {
		select {
		case <-w:
			// value changed
			val, err := m.get(key)
			if err != nil {
				level.Warn(m.logger).Log("msg", "failed to decode value while watching for changes", "key", key, "err", err)
				continue
			}

			if !f(val) {
				return
			}

		case <-m.shutdown:
			// stop watching on shutdown
			return

		case <-ctx.Done():
			return
		}
	}
}

func (m *MemberlistClient) WatchPrefix(ctx context.Context, prefix string, f func(string, interface{}) bool) {
	// we use bigger buffer here, since keys are interesting and we don't want to lose them.
	w := make(chan string, 16)

	// register watcher
	m.watchersMu.Lock()
	m.prefixWatchers[prefix] = append(m.prefixWatchers[prefix], w)
	m.watchersMu.Unlock()

	defer func() {
		// unregister watcher on exit
		m.watchersMu.Lock()
		defer m.watchersMu.Unlock()

		removeWatcherChannel(prefix, w, m.prefixWatchers)
	}()

	for {
		select {
		case key := <-w:
			val, err := m.get(key)
			if err != nil {
				level.Warn(m.logger).Log("msg", "failed to decode value while watching for changes", "key", key, "err", err)
				continue
			}

			if !f(key, val) {
				return
			}

		case <-m.shutdown:
			// stop watching on shutdown
			return

		case <-ctx.Done():
			return
		}
	}
}

func removeWatcherChannel(k string, w chan string, watchers map[string][]chan string) {
	ws := watchers[k]
	for ix, kw := range ws {
		if kw == w {
			ws = append(ws[:ix], ws[ix+1:]...)
			break
		}
	}

	if len(ws) > 0 {
		watchers[k] = ws
	} else {
		delete(watchers, k)
	}
}

func (m *MemberlistClient) notifyWatchers(key string) {
	m.watchersMu.Lock()
	defer m.watchersMu.Unlock()

	for _, kw := range m.watchers[key] {
		select {
		case kw <- key:
			// notification sent.
		default:
			// cannot send notification to this watcher at the moment
			// but since this is a buffered channel, it means that
			// there is already a pending notification anyway
		}
	}

	for p, ws := range m.prefixWatchers {
		if strings.HasPrefix(key, p) {
			for _, pw := range ws {
				select {
				case pw <- key:
					// notification sent.
				default:
					c, _ := m.watchPrefixDroppedNotifications.GetMetricWithLabelValues(p)
					if c != nil {
						c.Inc()
					}

					level.Warn(m.logger).Log("msg", "failed to send notification to prefix watcher", "prefix", p)
				}
			}
		}
	}
}

func (m *MemberlistClient) CAS(ctx context.Context, key string, f func(in interface{}) (out interface{}, retry bool, err error)) error {
	if len(key) > MaxKeyLength {
		return fmt.Errorf("key too long: %d", len(key))
	}

	sleep := false
	for retries := 10; retries > 0; retries-- {
		m.casAttempts.Inc()

		if sleep {
			// wait for a while before retrying... typically merge fails because timestamp hasn't
			// changed. This is a problem with 1-second timestamp resolution. By waiting for one second,
			// the hope is that timestamp will be updated

			select {
			case <-time.After(1 * time.Second):
				// ok
			case <-ctx.Done():
				break
			}
		}

		val, err := m.get(key)
		if err != nil {
			return err
		}

		out, retry, err := f(val)
		if err != nil {
			return err
		}

		if out == nil {
			// no change to be done
			return nil
		}

		// Don't even try
		r, ok := out.(Mergeable)
		if !ok {
			return fmt.Errorf("invalid type: %T, expected Mergeable", out)
		}

		c := m.getCodecForKey(key)

		// just merge it into our state
		change, newver, err := m.mergeValueForKey(key, c, r)
		if err != nil {
			return err
		}

		if newver == 0 {
			// 'f' didn't do any update that we can merge. Let's give it a new chance?
			if !retry {
				break
			}

			sleep = true
			continue
		}

		m.casSuccesses.Inc()
		m.notifyWatchers(key)
		m.broadcastNewValue(key, c, change, newver)
		return nil
	}

	m.casFailures.Inc()
	return fmt.Errorf("failed to CAS-update key %s", key)
}

func (m *MemberlistClient) broadcastNewValue(key string, codec codec.Codec, change Mergeable, version uint) {
	data, err := codec.Encode(change)
	if err != nil {
		level.Error(m.logger).Log("msg", "failed to encode ring", "err", err)
		return
	}

	buf := bytes.Buffer{}
	buf.Write([]byte{byte(len(key))})
	buf.WriteString(key)
	buf.Write(data)

	if buf.Len() > 65535 {
		// Unfortunately, memberlist will happily let us send bigger messages via gossip,
		// but then it will fail to parse them properly, because its own size field is 2-bytes only.
		// (github.com/hashicorp/memberlist@v0.1.4/util.go:167, makeCompoundMessage function)
		//
		// Typically messages are smaller (when dealing with couple of updates only), but can get bigger
		// when broadcasting result of push/pull update.
		level.Debug(m.logger).Log("msg", "broadcast message too big, not broadcasting", "len", buf.Len())
		return
	}

	m.queueBroadcast(key, change.MergeContent(), version, buf.Bytes())
}

// Memberlist Delegate interface
func (m *MemberlistClient) NodeMeta(limit int) []byte {
	// we can send local state from here (512 bytes only)
	// if state is updated, we need to tell memberlist to distribute it.
	return nil
}

// Called when single message is received. (i.e. what our broadcastNewValue has sent)
func (m *MemberlistClient) NotifyMsg(msg []byte) {
	m.numberOfReceivedMessages.Inc()
	m.totalSizeOfReceivedMessages.Add(float64(len(msg)))

	if len(msg) == 0 {
		level.Warn(m.logger).Log("msg", "Empty message received")
		m.numberOfInvalidReceivedMessages.Inc()
		return
	}

	keyLen := int(msg[0])
	if len(msg) <= 1+keyLen {
		level.Warn(m.logger).Log("msg", "Too short message received", "length", len(msg))
		m.numberOfInvalidReceivedMessages.Inc()
		return
	}

	key := string(msg[1 : 1+keyLen])
	data := msg[1+keyLen:]

	c := m.getCodecForKey(key)

	// we have a ring update! Let's merge it with our version of the ring for given key
	mod, version, err := m.mergeBytesValueForKey(key, c, data)
	if err != nil {
		level.Error(m.logger).Log("msg", "failed to store received value", "key", key, "err", err)
	} else if version > 0 {
		m.notifyWatchers(key)

		// Forward this message
		// Memberlist will modify message once this function returns, so we need to make a copy
		msgCopy := append([]byte(nil), msg...)

		// forward this message further
		m.queueBroadcast(key, mod.MergeContent(), version, msgCopy)
	}
}

func (m *MemberlistClient) queueBroadcast(key string, content []string, version uint, message []byte) {
	l := len(message)

	b := ringBroadcast{
		logger:  m.logger,
		key:     key,
		content: content,
		version: version,
		msg:     message,
		finished: func(b ringBroadcast) {
			m.totalSizeOfBroadcastMessagesInQueue.Sub(float64(l))
		},
	}

	m.totalSizeOfBroadcastMessagesInQueue.Add(float64(l))
	m.broadcasts.QueueBroadcast(b)
}

func (m *MemberlistClient) GetBroadcasts(overhead, limit int) [][]byte {
	return m.broadcasts.GetBroadcasts(overhead, limit)
}

// Called when some other node joins the cluster or on periodic push/pull sync. Here we dump our entire state.
// There is no limit on message size here, as Memberlist uses 'stream' operations for transferring this state.
// This is 'pull' part of push/pull sync.
func (m *MemberlistClient) LocalState(join bool) []byte {
	m.numberOfPulls.Inc()

	m.storeMu.Lock()
	defer m.storeMu.Unlock()

	// For each Key/Value pair in our store, we write:
	// [1 byte key length] [key] [4-bytes value length] [value]

	buf := bytes.Buffer{}
	for key, val := range m.store {
		if val.value == nil {
			continue
		}

		if len(key) > MaxKeyLength {
			level.Error(m.logger).Log("msg", "key too long", "key", key)
			continue
		}
		if len(val.value) > math.MaxUint32 {
			level.Error(m.logger).Log("msg", "value too long", "key", key, "value_length", len(val.value))
			continue
		}

		buf.WriteByte(byte(len(key)))
		buf.WriteString(key)
		err := binary.Write(&buf, binary.BigEndian, uint32(len(val.value)))
		if err != nil {
			level.Error(m.logger).Log("msg", "failed to write uint32 to buffer?", "err", err)
			continue
		}
		buf.Write(val.value)
	}

	m.totalSizeOfPulls.Add(float64(buf.Len()))
	return buf.Bytes()
}

// Merge full state from other node (output from LocalState), that is complete KV store.
// This is 'push' part of push/pull sync.
func (m *MemberlistClient) MergeRemoteState(stateMsg []byte, join bool) {
	m.numberOfPushes.Inc()
	m.totalSizeOfPushes.Add(float64(len(stateMsg)))

	buf := bytes.NewBuffer(stateMsg)

	var err error
	for buf.Len() > 0 {
		keyLen := byte(0)
		keyLen, err = buf.ReadByte()
		if err != nil {
			break
		}

		keyBuf := make([]byte, keyLen)
		l := 0
		l, err = buf.Read(keyBuf)
		if err != nil {
			break
		}

		if l != len(keyBuf) {
			err = fmt.Errorf("cannot read key, expected %d, got %d bytes", keyLen, l)
			break
		}

		key := string(keyBuf)

		// next read the length of the data
		valueLength := uint32(0)
		err = binary.Read(buf, binary.BigEndian, &valueLength)
		if err != nil {
			break
		}

		if buf.Len() < int(valueLength) {
			err = fmt.Errorf("not enough data left for value in key %q, expected %d, remaining %d bytes", key, valueLength, buf.Len())
			break
		}

		valueBuf := make([]byte, valueLength)
		l, err = buf.Read(valueBuf)
		if err != nil {
			break
		}

		if l != len(valueBuf) {
			err = fmt.Errorf("cannot read value for key %q, expected %d, got %d bytes", key, valueLength, l)
			break
		}

		c := m.getCodecForKey(key)

		// we have both key and value, try to merge it with our state
		change, newver, err := m.mergeBytesValueForKey(key, c, valueBuf)
		if err != nil {
			level.Error(m.logger).Log("msg", "failed to store received value", "key", key, "err", err)
		} else if newver > 0 {
			m.notifyWatchers(key)
			m.broadcastNewValue(key, c, change, newver)
		}
	}

	if err != nil {
		level.Error(m.logger).Log("msg", "failed to parse remote state", "err", err)
	}
}

func (m *MemberlistClient) mergeBytesValueForKey(key string, codec codec.Codec, incomingData []byte) (Mergeable, uint, error) {
	decodedValue, err := codec.Decode(incomingData)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to decode value: %v", err)
	}

	incomingValue, ok := decodedValue.(Mergeable)
	if !ok {
		return nil, 0, fmt.Errorf("expected Mergeable, got: %T", decodedValue)
	}

	return m.mergeValueForKey(key, codec, incomingValue)
}

// Merges incoming value with value we have in our store. Returns "a change" that can be sent to other
// cluster members to update their state, and new version of the value.
// If no modification occurred, new version is 0.
func (m *MemberlistClient) mergeValueForKey(key string, codec codec.Codec, incomingValue Mergeable) (Mergeable, uint, error) {
	if codec == nil {
		panic("nil codec")
	}

	m.storeMu.Lock()
	defer m.storeMu.Unlock()

	curr := m.store[key]
	result, change, err := computeNewValue(incomingValue, curr.value, codec)
	if err != nil {
		return nil, 0, err
	}

	// No change, don't store it.
	if change == nil || len(change.MergeContent()) == 0 {
		return nil, 0, nil
	}

	if m.cfg.LeftIngestersTimeout > 0 {
		limit := time.Now().Add(-m.cfg.LeftIngestersTimeout)
		result.RemoveTombstones(limit)
	}

	encoded, err := codec.Encode(result)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to encode merged result: %v", err)
	}

	newVersion := curr.version + 1
	m.store[key] = valueDesc{
		value:   encoded,
		version: newVersion,
		codec:   codec,
	}

	return change, newVersion, nil
}

// returns [result, change, error]
func computeNewValue(incoming Mergeable, stored []byte, c codec.Codec) (Mergeable, Mergeable, error) {
	if len(stored) == 0 {
		return incoming, incoming, nil
	}

	old, err := c.Decode(stored)
	if err != nil {
		return incoming, incoming, fmt.Errorf("failed to decode stored value: %v", err)
	}

	if old == nil {
		return incoming, incoming, nil
	}

	oldVal, ok := old.(Mergeable)
	if !ok {
		return incoming, incoming, fmt.Errorf("stored value is not Mergeable, got %T", old)
	}

	if oldVal == nil {
		return incoming, incoming, nil
	}

	// otherwise we have two mergeables, so merge them
	change, err := oldVal.Merge(incoming)
	return oldVal, change, err
}

func (m *MemberlistClient) getCodecForKey(key string) codec.Codec {
	m.storeMu.Lock()
	defer m.storeMu.Unlock()

	c := m.store[key].codec
	if c == nil {
		c = m.defaultCodec
	}
	return c
}
