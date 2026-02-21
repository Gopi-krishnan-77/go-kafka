// Weekend Engine — Standalone Worker
//
// This binary runs the processing pipeline as a standalone consumer.
// It subscribes to the in-memory broker and processes events through
// validate → enrich → score → classify → persist stages.
//
// Use this when you want to run the worker separately from the API.
// The API server already embeds workers, so this binary is for
// scale-out or isolated processing scenarios.
package main

import (
	"context"
	"encoding/json"
	"log"
	"os/signal"
	"syscall"
	"time"

	"github.com/weekend/go-kafka-fun/internal/broker"
	"github.com/weekend/go-kafka-fun/internal/config"
	"github.com/weekend/go-kafka-fun/internal/events"
	"github.com/weekend/go-kafka-fun/internal/metrics"
	"github.com/weekend/go-kafka-fun/internal/pipeline"
	"github.com/weekend/go-kafka-fun/internal/scheduler"
	"github.com/weekend/go-kafka-fun/internal/storage"
)

func main() {
	cfg := config.Load()

	mc := metrics.New()
	store := storage.New()
	brk := broker.New(
		broker.WithPartitions(cfg.BrokerPartitions),
		broker.WithMaxRetries(3),
	)
	pipe := pipeline.New(
		cfg.PipelineTimeout,
		pipeline.ValidateStage{},
		pipeline.EnrichStage{TTL: cfg.TTL},
		pipeline.ScoreStage{},
		pipeline.ClassifyStage{},
		pipeline.PersistStage{Store: store},
	)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Background scheduler
	sched := scheduler.New()
	sched.Add("ttl-eviction", 30*time.Second, func(_ context.Context) {
		n := store.EvictExpired()
		if n > 0 {
			mc.Add("events.expired", int64(n))
			log.Printf("evicted %d expired events", n)
		}
	})
	sched.Add("metrics-snapshot", 10*time.Second, func(_ context.Context) {
		mc.Set("store.total_events", int64(store.Size()))
		mc.Set("broker.queue_depth", brk.QueueDepth())
		mc.Inc("scheduler.ticks")
	})
	sched.Start(ctx)

	// Subscribe workers
	log.Printf("🔨 Weekend Engine Worker started  workers=%d  partitions=%d  ttl=%s",
		cfg.WorkerCount, cfg.BrokerPartitions, cfg.TTL)

	for i := 0; i < cfg.WorkerCount; i++ {
		brk.Subscribe(ctx, "weekend-events", "standalone-workers", func(bCtx context.Context, msg broker.Message) error {
			var evt events.WeekendEvent
			if err := json.Unmarshal(msg.Value, &evt); err != nil {
				mc.Inc("events.failed")
				log.Printf("✗ unmarshal error partition=%d offset=%d: %v", msg.Partition, msg.Offset, err)
				return err
			}

			mc.Inc("pipeline.runs")
			start := time.Now()
			if err := pipe.Run(bCtx, &evt); err != nil {
				mc.Inc("events.failed")
				mc.Inc("pipeline.stage_errors")
				log.Printf("✗ pipeline error id=%s: %v", evt.ID, err)
				return err
			}
			mc.RecordLatency("pipeline.latency", time.Since(start))
			mc.Inc("events.processed")

			log.Printf("✓ processed id=%s name=%q category=%s score=%.1f priority=%s day=%s season=%s",
				evt.ID, evt.Name, evt.Category, evt.FunScore, evt.Priority, evt.DayOfWeek, evt.Season)
			return nil
		})
	}

	<-ctx.Done()
	log.Println("worker shutting down…")
	sched.Wait()
	log.Println("goodbye 👋")
}
