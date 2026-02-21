// Package broker implements an in-memory message broker that simulates
// partitioned topics, consumer groups, offset management, dead-letter queues,
// and historical message replay — without any external dependencies.
package broker

import (
	"context"
	"fmt"
	"hash/fnv"
	"sync"
	"time"
)

// Message is the unit of data flowing through the broker.
type Message struct {
	Key       []byte
	Value     []byte
	Topic     string
	Partition int
	Offset    int64
	Timestamp time.Time
	Attempt   int
}

// Handler processes a single message. Return non-nil error to send the message to the DLQ.
type Handler func(ctx context.Context, msg Message) error

// partition is an append-only log of messages with a notification channel.
type partition struct {
	mu   sync.Mutex
	log  []Message
	subs []chan struct{} // notification channels for waiting consumers
}

func (p *partition) append(m Message) int64 {
	p.mu.Lock()
	m.Offset = int64(len(p.log))
	p.log = append(p.log, m)
	// wake all waiting consumers
	for _, ch := range p.subs {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
	p.mu.Unlock()
	return m.Offset
}

func (p *partition) subscribe() chan struct{} {
	p.mu.Lock()
	defer p.mu.Unlock()
	ch := make(chan struct{}, 1)
	p.subs = append(p.subs, ch)
	return ch
}

func (p *partition) get(offset int64) (Message, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if offset < 0 || int(offset) >= len(p.log) {
		return Message{}, false
	}
	return p.log[offset], true
}

func (p *partition) length() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return int64(len(p.log))
}

// topic holds N partitions and a dead-letter queue.
type topic struct {
	name       string
	partitions []*partition
	dlq        *partition
}

// consumerGroup tracks per-partition committed offsets for a named group.
type consumerGroup struct {
	mu      sync.Mutex
	offsets map[int]int64 // partition → committed offset
}

func (cg *consumerGroup) getOffset(partition int) int64 {
	cg.mu.Lock()
	defer cg.mu.Unlock()
	return cg.offsets[partition]
}

func (cg *consumerGroup) commit(partition int, offset int64) {
	cg.mu.Lock()
	defer cg.mu.Unlock()
	if offset > cg.offsets[partition] {
		cg.offsets[partition] = offset
	}
}

// Stats exposes broker-level counters.
type Stats struct {
	TopicStats map[string]TopicStats `json:"topics"`
}

// TopicStats holds per-topic statistics.
type TopicStats struct {
	Published  int64 `json:"published"`
	Partitions int   `json:"partitions"`
	DLQSize    int64 `json:"dlq_size"`
}

// Broker is the central message routing engine.
type Broker struct {
	mu     sync.RWMutex
	topics map[string]*topic
	groups map[string]map[string]*consumerGroup // topic → groupID → CG

	publishCount map[string]int64

	defaultPartitions int
	maxRetries        int
}

// Option configures broker behaviour.
type Option func(*Broker)

// WithPartitions sets the number of partitions for new topics.
func WithPartitions(n int) Option {
	return func(b *Broker) {
		if n > 0 {
			b.defaultPartitions = n
		}
	}
}

// WithMaxRetries sets the maximum delivery attempts before DLQ routing.
func WithMaxRetries(n int) Option {
	return func(b *Broker) {
		if n > 0 {
			b.maxRetries = n
		}
	}
}

// New creates a Broker with the given options.
func New(opts ...Option) *Broker {
	b := &Broker{
		topics:            make(map[string]*topic),
		groups:            make(map[string]map[string]*consumerGroup),
		publishCount:      make(map[string]int64),
		defaultPartitions: 4,
		maxRetries:        3,
	}
	for _, o := range opts {
		o(b)
	}
	return b
}

// ensureTopic creates a topic if it doesn't exist.
func (b *Broker) ensureTopic(name string) *topic {
	b.mu.Lock()
	defer b.mu.Unlock()
	t, ok := b.topics[name]
	if !ok {
		parts := make([]*partition, b.defaultPartitions)
		for i := range parts {
			parts[i] = &partition{}
		}
		t = &topic{
			name:       name,
			partitions: parts,
			dlq:        &partition{},
		}
		b.topics[name] = t
		b.groups[name] = make(map[string]*consumerGroup)
	}
	return t
}

// getTopic returns a topic or nil.
func (b *Broker) getTopic(name string) *topic {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.topics[name]
}

// Publish sends a message to the named topic, hash-partitioning by key.
func (b *Broker) Publish(topicName string, key, value []byte) (partition int, offset int64) {
	t := b.ensureTopic(topicName)
	p := b.partitionFor(key, len(t.partitions))
	msg := Message{
		Key:       key,
		Value:     value,
		Topic:     topicName,
		Partition: p,
		Timestamp: time.Now().UTC(),
		Attempt:   1,
	}
	offset = t.partitions[p].append(msg)

	b.mu.Lock()
	b.publishCount[topicName]++
	b.mu.Unlock()
	return p, offset
}

// Subscribe starts consuming from the named topic under the given consumer
// group. It spawns one goroutine per partition. The handler is called for
// each message; if it returns an error the message is retried up to
// maxRetries times before being routed to the dead-letter queue.
// Cancel the context to stop consumption.
func (b *Broker) Subscribe(ctx context.Context, topicName, groupID string, handler Handler) {
	t := b.ensureTopic(topicName)

	b.mu.Lock()
	cg, ok := b.groups[topicName][groupID]
	if !ok {
		cg = &consumerGroup{offsets: make(map[int]int64)}
		b.groups[topicName][groupID] = cg
	}
	b.mu.Unlock()

	for i := 0; i < len(t.partitions); i++ {
		go b.consumePartition(ctx, t, i, cg, handler)
	}
}

func (b *Broker) consumePartition(ctx context.Context, t *topic, pIdx int, cg *consumerGroup, handler Handler) {
	p := t.partitions[pIdx]
	notify := p.subscribe()

	for {
		offset := cg.getOffset(pIdx)
		msg, ok := p.get(offset)
		if !ok {
			// wait for new data or context cancellation
			select {
			case <-ctx.Done():
				return
			case <-notify:
				continue
			}
		}

		// attempt delivery
		var lastErr error
		for attempt := 1; attempt <= b.maxRetries; attempt++ {
			msg.Attempt = attempt
			if err := handler(ctx, msg); err != nil {
				lastErr = err
				time.Sleep(time.Duration(attempt*50) * time.Millisecond) // backoff
				continue
			}
			lastErr = nil
			break
		}
		if lastErr != nil {
			// route to DLQ
			t.dlq.append(msg)
		}
		cg.commit(pIdx, offset+1)
	}
}

// Replay returns all messages in [fromOffset, ...) across all partitions of a topic.
func (b *Broker) Replay(topicName string, fromOffset int64) ([]Message, error) {
	t := b.getTopic(topicName)
	if t == nil {
		return nil, fmt.Errorf("topic %q not found", topicName)
	}
	var msgs []Message
	for _, p := range t.partitions {
		p.mu.Lock()
		for _, m := range p.log {
			if m.Offset >= fromOffset {
				msgs = append(msgs, m)
			}
		}
		p.mu.Unlock()
	}
	return msgs, nil
}

// DLQ returns all dead-letter messages for the named topic.
func (b *Broker) DLQ(topicName string) ([]Message, error) {
	t := b.getTopic(topicName)
	if t == nil {
		return nil, fmt.Errorf("topic %q not found", topicName)
	}
	t.dlq.mu.Lock()
	defer t.dlq.mu.Unlock()
	out := make([]Message, len(t.dlq.log))
	copy(out, t.dlq.log)
	return out, nil
}

// TopicNames returns the list of known topic names.
func (b *Broker) TopicNames() []string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	names := make([]string, 0, len(b.topics))
	for n := range b.topics {
		names = append(names, n)
	}
	return names
}

// GetStats returns point-in-time broker statistics.
func (b *Broker) GetStats() Stats {
	b.mu.RLock()
	defer b.mu.RUnlock()
	s := Stats{TopicStats: make(map[string]TopicStats, len(b.topics))}
	for name, t := range b.topics {
		ts := TopicStats{
			Published:  b.publishCount[name],
			Partitions: len(t.partitions),
			DLQSize:    t.dlq.length(),
		}
		s.TopicStats[name] = ts
	}
	return s
}

// QueueDepth returns the total un-consumed messages across all topics.
func (b *Broker) QueueDepth() int64 {
	b.mu.RLock()
	defer b.mu.RUnlock()
	var depth int64
	for _, t := range b.topics {
		for _, p := range t.partitions {
			depth += p.length()
		}
	}
	return depth
}

// partitionFor maps a key to a partition index using FNV-1a hashing.
func (b *Broker) partitionFor(key []byte, numPartitions int) int {
	if len(key) == 0 || numPartitions <= 1 {
		return 0
	}
	h := fnv.New32a()
	h.Write(key)
	return int(h.Sum32()) % numPartitions
}
