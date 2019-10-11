package memberlist

import (
	"context"
	"fmt"
	"time"

	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
)

func (m *MemberlistClient) createAndRegisterMetrics() {
	const subsystem = "memberlist_client"

	m.numberOfReceivedMessages = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: m.cfg.MetricsNamespace,
		Subsystem: subsystem,
		Name:      "received_broadcasts",
		Help:      "Number of received broadcast user messages",
	})

	m.totalSizeOfReceivedMessages = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: m.cfg.MetricsNamespace,
		Subsystem: subsystem,
		Name:      "received_broadcasts_size",
		Help:      "Total size of received broadcast user messages",
	})

	m.numberOfInvalidReceivedMessages = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: m.cfg.MetricsNamespace,
		Subsystem: subsystem,
		Name:      "received_broadcasts_invalid",
		Help:      "Number of received broadcast user messages that were invalid. Hopefully 0.",
	})

	m.numberOfPushes = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: m.cfg.MetricsNamespace,
		Subsystem: subsystem,
		Name:      "state_pushes_count",
		Help:      "How many times did this node push its full state to another node",
	})

	m.totalSizeOfPushes = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: m.cfg.MetricsNamespace,
		Subsystem: subsystem,
		Name:      "state_pushes_bytes",
		Help:      "Total size of pushed state",
	})

	m.numberOfPulls = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: m.cfg.MetricsNamespace,
		Subsystem: subsystem,
		Name:      "state_pulls_count",
		Help:      "How many times did this node pull full state from another node",
	})

	m.totalSizeOfPulls = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: m.cfg.MetricsNamespace,
		Subsystem: subsystem,
		Name:      "state_pulls_bytes",
		Help:      "Total size of pulled state",
	})

	m.numberOfBroadcastMessagesInQueue = prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: m.cfg.MetricsNamespace,
		Subsystem: subsystem,
		Name:      "messages_in_broadcast_queue",
		Help:      "Number of user messages in the broadcast queue",
	}, func() float64 {
		return float64(m.broadcasts.NumQueued())
	})

	m.totalSizeOfBroadcastMessagesInQueue = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: m.cfg.MetricsNamespace,
		Subsystem: subsystem,
		Name:      "messages_in_broadcast_size_total",
		Help:      "Total size of messages waiting in the broadcast queue",
	})

	m.casAttempts = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: m.cfg.MetricsNamespace,
		Subsystem: subsystem,
		Name:      "cas_attempt_count",
		Help:      "Attempted CAS operations",
	})

	m.casSuccesses = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: m.cfg.MetricsNamespace,
		Subsystem: subsystem,
		Name:      "cas_success_count",
		Help:      "Successful CAS operations",
	})

	m.casFailures = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: m.cfg.MetricsNamespace,
		Subsystem: subsystem,
		Name:      "cas_failure_count",
		Help:      "Failed CAS operations",
	})

	m.storeValuesDesc = prometheus.NewDesc(
		prometheus.BuildFQName(m.cfg.MetricsNamespace, subsystem, "kv_store_count"),
		"Number of values in KV Store",
		nil, nil,
	)

	m.storeSizesDesc = prometheus.NewDesc(
		prometheus.BuildFQName(m.cfg.MetricsNamespace, subsystem, "kv_store_value_size"),
		"Sizes of values in KV Store in bytes",
		[]string{"key"}, nil)

	m.memberlistMembersCount = prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: m.cfg.MetricsNamespace,
		Subsystem: subsystem,
		Name:      "cluster_members_count",
		Help:      "Number of members in memberlist cluster",
	}, func() float64 {
		return float64(m.memberlist.NumMembers())
	})

	m.memberlistHealthScore = prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: m.cfg.MetricsNamespace,
		Subsystem: subsystem,
		Name:      "cluster_node_health_score",
		Help:      "Health score of this cluster. Lower the better. 0 = totally health",
	}, func() float64 {
		return float64(m.memberlist.GetHealthScore())
	})

	m.memberlistMembersInfoDesc = prometheus.NewDesc(
		prometheus.BuildFQName(m.cfg.MetricsNamespace, subsystem, "cluster_members_last_timestamp"),
		"State information about memberlist cluster members",
		[]string{"name", "address"}, nil)

	m.watchPrefixDroppedNotifications = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: m.cfg.MetricsNamespace,
		Subsystem: subsystem,
		Name:      "watch_prefix_dropped_notifications",
		Help:      "Number of dropped notifications in WatchPrefix function",
	}, []string{"prefix"})

	if m.cfg.MetricsRegisterer == nil {
		return
	}

	// if registration fails, that's too bad, but don't panic
	for _, c := range m.getAllMetrics() {
		err := m.cfg.MetricsRegisterer.Register(c.(prometheus.Collector))
		if err != nil {
			level.Error(m.logger).Log("msg", "failed to register metrics", "err", err)
		}
	}

	err := m.cfg.MetricsRegisterer.Register(m)
	if err != nil {
		level.Error(m.logger).Log("msg", "failed to register metrics", "err", err)
	}
}

func (m *MemberlistClient) unregisterMetrics() {
	if m.cfg.MetricsRegisterer == nil {
		return
	}

	for _, c := range m.getAllMetrics() {
		m.cfg.MetricsRegisterer.Unregister(c.(prometheus.Collector))
	}
	m.cfg.MetricsRegisterer.Unregister(m)
}

func (m *MemberlistClient) getAllMetrics() []prometheus.Collector {
	return []prometheus.Collector{
		m.numberOfReceivedMessages,
		m.totalSizeOfReceivedMessages,
		m.numberOfInvalidReceivedMessages,
		m.numberOfBroadcastMessagesInQueue,
		m.numberOfPushes,
		m.numberOfPulls,
		m.totalSizeOfPushes,
		m.totalSizeOfPulls,
		m.totalSizeOfBroadcastMessagesInQueue,
		m.casAttempts,
		m.casFailures,
		m.casSuccesses,
		m.watchPrefixDroppedNotifications,
		m.memberlistMembersCount,
		m.memberlistHealthScore,
	}
}

func (m *MemberlistClient) Describe(ch chan<- *prometheus.Desc) {
	ch <- m.storeValuesDesc
	ch <- m.storeSizesDesc
	ch <- m.memberlistMembersInfoDesc
}

func (m *MemberlistClient) Collect(ch chan<- prometheus.Metric) {
	ts := (*timestamps)(nil)
	if obj, err := m.get(NodesTimestamps); err == nil {
		if tsp, ok := obj.(*timestamps); ok {
			ts = tsp
		}
	}

	for _, n := range m.memberlist.Members() {
		t := 0.0
		if ts != nil {
			ts, ok := ts.Timestamps[n.Name]
			if ok {
				t = float64(ts.UnixNano()) / float64(time.Second)
			}
		}

		ch <- prometheus.MustNewConstMetric(m.memberlistMembersInfoDesc, prometheus.GaugeValue, t, n.Name, n.Address())
	}

	// Everything that needs KV Store lock is below here.
	m.storeMu.Lock()
	defer m.storeMu.Unlock()

	ch <- prometheus.MustNewConstMetric(m.storeValuesDesc, prometheus.GaugeValue, float64(len(m.store)))

	for k, v := range m.store {
		ch <- prometheus.MustNewConstMetric(m.storeSizesDesc, prometheus.GaugeValue, float64(len(v.value)), k)
	}
}

// Runs a goroutine that updates timestamp for this node every couple of seconds
func (m *MemberlistClient) updateNodeTimestamp() {
	if m.cfg.TimestampInterval <= 0 {
		return
	}

	t := time.NewTicker(m.cfg.TimestampInterval)

	update := func() {
		err := m.CAS(context.Background(), NodesTimestamps, func(in interface{}) (out interface{}, retry bool, err error) {
			ts := (*timestamps)(nil)
			if in != nil {
				ok := false
				ts, ok = in.(*timestamps)
				if !ok {
					return nil, false, fmt.Errorf("invalid type: %T, expected *timestamps", in)
				}
			}

			if ts == nil {
				ts = newTimestamps()
			}

			node := m.memberlist.LocalNode()
			if node == nil {
				return nil, false, fmt.Errorf("nil node")
			}

			ts.Timestamps[node.Name] = time.Now()
			return ts, true, nil
		})

		if err != nil {
			level.Error(m.logger).Log("msg", "failed to update node timestamp", "err", err)
		}
	}

	update()
	for {
		select {
		case <-t.C:
			update()

		case <-m.shutdown:
			return
		}
	}
}
