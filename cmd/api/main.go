package main

import (
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/weekend/go-kafka-fun/internal/config"
	"github.com/weekend/go-kafka-fun/internal/events"
)

type createEventRequest struct {
	Name      string `json:"name"`
	Category  string `json:"category"`
	MoodBoost int    `json:"mood_boost"`
}

func main() {
	cfg := config.Load()

	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{cfg.Broker},
		Topic:    cfg.Topic,
		Balancer: &kafka.LeastBytes{},
	})
	defer writer.Close()

	http.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	http.HandleFunc("/events", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var req createEventRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid request payload", http.StatusBadRequest)
			return
		}

		event := events.WeekendEvent{
			Name:      req.Name,
			Category:  req.Category,
			MoodBoost: req.MoodBoost,
			CreatedAt: time.Now().UTC(),
		}

		payload, err := event.Bytes()
		if err != nil {
			http.Error(w, "could not encode event", http.StatusInternalServerError)
			return
		}

		message := kafka.Message{Key: event.Key(), Value: payload}
		if err := writer.WriteMessages(r.Context(), message); err != nil {
			http.Error(w, "failed to publish event", http.StatusBadGateway)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status": "queued",
			"event":  event,
		})
	})

	log.Printf("weekend API listening on %s topic=%s broker=%s", cfg.Addr(), cfg.Topic, cfg.Broker)
	if err := http.ListenAndServe(cfg.Addr(), nil); err != nil {
		log.Fatal(err)
	}
}
