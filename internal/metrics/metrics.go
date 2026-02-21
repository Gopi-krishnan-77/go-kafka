// Package metrics provides a lightweight, thread-safe metrics collector
// with counters, gauges, and histogram-style latency tracking.
package metrics

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Collector is a global registry of named int64 counters and gauges.
type Collector struct {
	mu       sync.RWMutex
	counters map[string]*atomic.Int64
	histMu   sync.Mutex
	latency  map[string][]int64 // ring buffer of recent latencies in µs
}

// New creates a Collector with pre-registered well-known metrics.
func New() *Collector {
	c := &Collector{
		counters: make(map[string]*atomic.Int64),
		latency:  make(map[string][]int64),
	}
	// Pre-register well-known counters so they always appear in snapshots.
	for _, name := range []string{
		"events.published",
		"events.processed",
		"events.failed",
		"events.expired",
		"broker.queue_depth",
		"store.total_events",
		"http.requests",
		"http.errors",
		"pipeline.runs",
		"pipeline.stage_errors",
		"scheduler.ticks",
	} {
		c.counters[name] = &atomic.Int64{}
	}
	return c
}

// Inc increments the named counter by 1.
func (c *Collector) Inc(name string) { c.Add(name, 1) }

// Add adds delta to the named counter (creates if missing).
func (c *Collector) Add(name string, delta int64) {
	c.mu.RLock()
	a, ok := c.counters[name]
	c.mu.RUnlock()
	if ok {
		a.Add(delta)
		return
	}
	c.mu.Lock()
	a, ok = c.counters[name]
	if !ok {
		a = &atomic.Int64{}
		c.counters[name] = a
	}
	c.mu.Unlock()
	a.Add(delta)
}

// Set sets the named gauge to an absolute value.
func (c *Collector) Set(name string, val int64) {
	c.mu.RLock()
	a, ok := c.counters[name]
	c.mu.RUnlock()
	if ok {
		a.Store(val)
		return
	}
	c.mu.Lock()
	a, ok = c.counters[name]
	if !ok {
		a = &atomic.Int64{}
		c.counters[name] = a
	}
	c.mu.Unlock()
	a.Store(val)
}

// RecordLatency stores a duration sample under the given name.
// Only the most recent 1000 samples are kept per name.
func (c *Collector) RecordLatency(name string, d time.Duration) {
	us := d.Microseconds()
	c.histMu.Lock()
	defer c.histMu.Unlock()
	buf := c.latency[name]
	if len(buf) >= 1000 {
		buf = buf[1:]
	}
	c.latency[name] = append(buf, us)
}

// Snapshot returns a point-in-time copy of all counters.
func (c *Collector) Snapshot() map[string]int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	snap := make(map[string]int64, len(c.counters))
	for k, v := range c.counters {
		snap[k] = v.Load()
	}
	return snap
}

// LatencySnapshot returns p50, p95, p99 in microseconds for the named histogram.
func (c *Collector) LatencySnapshot(name string) (p50, p95, p99 int64) {
	c.histMu.Lock()
	buf := make([]int64, len(c.latency[name]))
	copy(buf, c.latency[name])
	c.histMu.Unlock()
	if len(buf) == 0 {
		return 0, 0, 0
	}
	sort.Slice(buf, func(i, j int) bool { return buf[i] < buf[j] })
	percentile := func(p float64) int64 {
		idx := int(float64(len(buf)-1) * p)
		return buf[idx]
	}
	return percentile(0.50), percentile(0.95), percentile(0.99)
}
