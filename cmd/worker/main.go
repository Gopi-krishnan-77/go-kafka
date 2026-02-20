package main

import (
	"context"
	"encoding/json"
	"log"
	"os/signal"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/weekend/go-kafka-fun/internal/config"
	"github.com/weekend/go-kafka-fun/internal/events"
)

func main() {
	cfg := config.Load()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{cfg.Broker},
		GroupID:  cfg.Group,
		Topic:    cfg.Topic,
		MinBytes: 1,
		MaxBytes: 10e6,
	})
	defer reader.Close()

	log.Printf("worker started topic=%s broker=%s group=%s", cfg.Topic, cfg.Broker, cfg.Group)

	for {
		m, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				log.Println("worker shutting down")
				return
			}
			log.Printf("fetch error: %v", err)
			time.Sleep(2 * time.Second)
			continue
		}

		var event events.WeekendEvent
		if err := json.Unmarshal(m.Value, &event); err != nil {
			log.Printf("invalid event payload offset=%d err=%v", m.Offset, err)
			_ = reader.CommitMessages(ctx, m)
			continue
		}

		score := event.MoodBoost * len(event.Name)
		log.Printf("processed event category=%s name=%q mood_boost=%d fun_score=%d", event.Category, event.Name, event.MoodBoost, score)

		if err := reader.CommitMessages(ctx, m); err != nil {
			log.Printf("commit error offset=%d err=%v", m.Offset, err)
		}
	}
}
