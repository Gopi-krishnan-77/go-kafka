package broker

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestPublishAndSubscribe(t *testing.T) {
	b := New(WithPartitions(2))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var received atomic.Int32
	b.Subscribe(ctx, "test-topic", "g1", func(_ context.Context, msg Message) error {
		received.Add(1)
		return nil
	})

	for i := 0; i < 10; i++ {
		payload, _ := json.Marshal(map[string]int{"i": i})
		b.Publish("test-topic", []byte("key"), payload)
	}

	deadline := time.After(3 * time.Second)
	for received.Load() < 10 {
		select {
		case <-deadline:
			t.Fatalf("timed out: received %d/10", received.Load())
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	if got := received.Load(); got != 10 {
		t.Fatalf("expected 10, got %d", got)
	}
}

func TestPartitioning(t *testing.T) {
	b := New(WithPartitions(4))

	p1, _ := b.Publish("t", []byte("alpha"), []byte("v"))
	p2, _ := b.Publish("t", []byte("alpha"), []byte("v"))
	if p1 != p2 {
		t.Fatalf("same key should map to same partition, got %d and %d", p1, p2)
	}
}

func TestDLQOnPersistentError(t *testing.T) {
	b := New(WithPartitions(1), WithMaxRetries(2))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var attempts atomic.Int32
	b.Subscribe(ctx, "dlq-test", "g1", func(_ context.Context, msg Message) error {
		attempts.Add(1)
		return errors.New("always fail")
	})

	b.Publish("dlq-test", []byte("k"), []byte("bad-msg"))

	// wait for the message to be processed (retried and DLQ'd)
	deadline := time.After(3 * time.Second)
	for {
		dlq, _ := b.DLQ("dlq-test")
		if len(dlq) > 0 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("timed out waiting for DLQ entry")
		default:
			time.Sleep(50 * time.Millisecond)
		}
	}

	if got := attempts.Load(); got < 2 {
		t.Fatalf("expected at least 2 attempts, got %d", got)
	}
}

func TestReplay(t *testing.T) {
	b := New(WithPartitions(1))
	for i := 0; i < 5; i++ {
		b.Publish("replay", []byte("k"), []byte("msg"))
	}

	msgs, err := b.Replay("replay", 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 3 {
		t.Fatalf("expected 3 messages from offset 2, got %d", len(msgs))
	}
}

func TestStats(t *testing.T) {
	b := New(WithPartitions(2))
	b.Publish("s", []byte("k"), []byte("v"))
	b.Publish("s", []byte("k"), []byte("v"))

	stats := b.GetStats()
	ts, ok := stats.TopicStats["s"]
	if !ok {
		t.Fatal("missing topic stats")
	}
	if ts.Published != 2 {
		t.Fatalf("expected 2 published, got %d", ts.Published)
	}
	if ts.Partitions != 2 {
		t.Fatalf("expected 2 partitions, got %d", ts.Partitions)
	}
}
