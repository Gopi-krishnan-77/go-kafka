package kafka

import (
	"context"
	"errors"
	"sync"
)

type Message struct {
	Key    []byte
	Value  []byte
	Offset int64
}

type LeastBytes struct{}

type WriterConfig struct {
	Brokers  []string
	Topic    string
	Balancer interface{}
}

type Writer struct {
	topic string
}

func NewWriter(cfg WriterConfig) *Writer {
	return &Writer{topic: cfg.Topic}
}

func (w *Writer) WriteMessages(_ context.Context, msgs ...Message) error {
	if w.topic == "" {
		return errors.New("missing topic")
	}
	for _, msg := range msgs {
		enqueue(w.topic, msg)
	}
	return nil
}

func (w *Writer) Close() error { return nil }

type ReaderConfig struct {
	Brokers  []string
	GroupID  string
	Topic    string
	MinBytes int
	MaxBytes int
}

type Reader struct {
	topic string
}

func NewReader(cfg ReaderConfig) *Reader {
	return &Reader{topic: cfg.Topic}
}

func (r *Reader) FetchMessage(ctx context.Context) (Message, error) {
	if r.topic == "" {
		return Message{}, errors.New("missing topic")
	}
	return dequeue(ctx, r.topic)
}

func (r *Reader) CommitMessages(context.Context, ...Message) error { return nil }

func (r *Reader) Close() error { return nil }

var queues = struct {
	sync.Mutex
	byTopic map[string]chan Message
	offset  map[string]int64
}{
	byTopic: map[string]chan Message{},
	offset:  map[string]int64{},
}

func topicQueue(topic string) chan Message {
	queues.Lock()
	defer queues.Unlock()
	q, ok := queues.byTopic[topic]
	if !ok {
		q = make(chan Message, 1024)
		queues.byTopic[topic] = q
	}
	return q
}

func enqueue(topic string, m Message) {
	queues.Lock()
	m.Offset = queues.offset[topic]
	queues.offset[topic]++
	q, ok := queues.byTopic[topic]
	if !ok {
		q = make(chan Message, 1024)
		queues.byTopic[topic] = q
	}
	queues.Unlock()
	q <- m
}

func dequeue(ctx context.Context, topic string) (Message, error) {
	q := topicQueue(topic)
	select {
	case <-ctx.Done():
		return Message{}, ctx.Err()
	case m := <-q:
		return m, nil
	}
}
