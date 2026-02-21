// Weekend Engine — API Server
//
// Comprehensive REST API for the weekend event-driven processing system.
// All Kafka dependencies have been replaced by the in-memory broker.
package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/weekend/go-kafka-fun/internal/broker"
	"github.com/weekend/go-kafka-fun/internal/config"
	"github.com/weekend/go-kafka-fun/internal/events"
	"github.com/weekend/go-kafka-fun/internal/metrics"
	"github.com/weekend/go-kafka-fun/internal/middleware"
	"github.com/weekend/go-kafka-fun/internal/pipeline"
	"github.com/weekend/go-kafka-fun/internal/scheduler"
	"github.com/weekend/go-kafka-fun/internal/storage"
)

const topicName = "weekend-events"

func main() {
	cfg := config.Load()

	// ── Core components ──────────────────────────────────────
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

	// ── Background context ───────────────────────────────────
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// ── Scheduler: recurring background tasks ────────────────
	sched := scheduler.New()
	sched.Add("ttl-eviction", 30*time.Second, func(_ context.Context) {
		n := store.EvictExpired()
		if n > 0 {
			mc.Add("events.expired", int64(n))
			log.Printf("scheduler: evicted %d expired events", n)
		}
	})
	sched.Add("metrics-snapshot", 10*time.Second, func(_ context.Context) {
		mc.Set("store.total_events", int64(store.Size()))
		mc.Set("broker.queue_depth", brk.QueueDepth())
		mc.Inc("scheduler.ticks")
	})
	sched.Start(ctx)

	// ── Subscribe broker → pipeline ──────────────────────────
	brk.Subscribe(ctx, topicName, "pipeline-workers", func(bCtx context.Context, msg broker.Message) error {
		var evt events.WeekendEvent
		if err := json.Unmarshal(msg.Value, &evt); err != nil {
			mc.Inc("events.failed")
			return err
		}
		mc.Inc("pipeline.runs")
		start := time.Now()
		if err := pipe.Run(bCtx, &evt); err != nil {
			mc.Inc("events.failed")
			mc.Inc("pipeline.stage_errors")
			log.Printf("pipeline error: %v", err)
			return err
		}
		mc.RecordLatency("pipeline.latency", time.Since(start))
		mc.Inc("events.processed")
		log.Printf("✓ processed id=%s name=%q score=%.1f priority=%s", evt.ID, evt.Name, evt.FunScore, evt.Priority)
		return nil
	})

	// ── HTTP Router ──────────────────────────────────────────
	mux := http.NewServeMux()

	// Health
	mux.HandleFunc("/healthz", handleHealthz)

	// Events CRUD
	mux.HandleFunc("/events", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			handleCreateEvent(w, r, brk, mc)
		case http.MethodGet:
			handleListEvents(w, r, store)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	mux.HandleFunc("/events/search", func(w http.ResponseWriter, r *http.Request) {
		handleSearchEvents(w, r, store)
	})

	// Single event by ID — uses path prefix matching
	mux.HandleFunc("/events/", func(w http.ResponseWriter, r *http.Request) {
		id := strings.TrimPrefix(r.URL.Path, "/events/")
		if id == "" || strings.Contains(id, "/") {
			http.Error(w, "invalid event id", http.StatusBadRequest)
			return
		}
		switch r.Method {
		case http.MethodGet:
			handleGetEvent(w, id, store)
		case http.MethodDelete:
			handleDeleteEvent(w, id, store)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	// Stats & Metrics
	mux.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		handleStats(w, store)
	})
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		handleMetrics(w, mc)
	})

	// Broker introspection
	mux.HandleFunc("/broker/topics", func(w http.ResponseWriter, r *http.Request) {
		handleBrokerTopics(w, brk)
	})
	mux.HandleFunc("/broker/dlq/", func(w http.ResponseWriter, r *http.Request) {
		topicName := strings.TrimPrefix(r.URL.Path, "/broker/dlq/")
		handleBrokerDLQ(w, topicName, brk)
	})
	mux.HandleFunc("/broker/replay", func(w http.ResponseWriter, r *http.Request) {
		handleBrokerReplay(w, r, brk)
	})

	// Pipeline info
	mux.HandleFunc("/pipeline", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{
			"stages":  pipe.StageNames(),
			"timeout": cfg.PipelineTimeout.String(),
		})
	})

	// ── Middleware stack ──────────────────────────────────────
	handler := middleware.Chain(
		mux,
		middleware.SecurityHeaders,
		middleware.CORS,
		middleware.Recovery,
		middleware.RequestID,
		middleware.Logger(mc),
		middleware.RateLimiter(cfg.RateLimitRPS, cfg.RateLimitBurst),
		middleware.RequestSize(1<<20), // 1 MB max body
		middleware.Powered("1.0"),
	)

	// ── Start server ─────────────────────────────────────────
	srv := &http.Server{
		Addr:         cfg.Addr(),
		Handler:      handler,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		log.Printf("🚀 Weekend Engine API listening on %s", cfg.Addr())
		log.Printf("   partitions=%d  workers=%d  ttl=%s  rate_limit=%.0f rps",
			cfg.BrokerPartitions, cfg.WorkerCount, cfg.TTL, cfg.RateLimitRPS)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server error: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("shutting down…")

	shutCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutCtx); err != nil {
		log.Printf("shutdown error: %v", err)
	}
	sched.Wait()
	log.Println("goodbye 👋")
}

// ── Handlers ─────────────────────────────────────────────────

type createEventRequest struct {
	Name      string   `json:"name"`
	Category  string   `json:"category"`
	MoodBoost int      `json:"mood_boost"`
	Tags      []string `json:"tags,omitempty"`
}

func handleHealthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func handleCreateEvent(w http.ResponseWriter, r *http.Request, brk *broker.Broker, mc *metrics.Collector) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req createEventRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON payload", http.StatusBadRequest)
		return
	}

	evt := events.WeekendEvent{
		Name:      req.Name,
		Category:  req.Category,
		MoodBoost: req.MoodBoost,
		Tags:      req.Tags,
		CreatedAt: time.Now().UTC(),
	}

	if err := evt.Validate(); err != nil {
		writeJSON(w, http.StatusUnprocessableEntity, map[string]string{"error": err.Error()})
		return
	}

	payload, err := evt.Bytes()
	if err != nil {
		http.Error(w, "encoding error", http.StatusInternalServerError)
		return
	}

	partition, offset := brk.Publish(topicName, evt.Key(), payload)
	mc.Inc("events.published")

	writeJSON(w, http.StatusAccepted, map[string]any{
		"status":    "queued",
		"partition": partition,
		"offset":    offset,
		"event":     evt,
	})
}

func handleListEvents(w http.ResponseWriter, r *http.Request, store *storage.Store) {
	q := r.URL.Query()
	page, _ := strconv.Atoi(q.Get("page"))
	limit, _ := strconv.Atoi(q.Get("limit"))
	f := events.Filter{
		Category: q.Get("category"),
		Priority: q.Get("priority"),
	}
	result := store.List(f, page, limit)
	writeJSON(w, http.StatusOK, result)
}

func handleGetEvent(w http.ResponseWriter, id string, store *storage.Store) {
	evt, ok := store.Get(id)
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "event not found"})
		return
	}
	writeJSON(w, http.StatusOK, evt)
}

func handleDeleteEvent(w http.ResponseWriter, id string, store *storage.Store) {
	if !store.Delete(id) {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "event not found"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "deleted"})
}

func handleSearchEvents(w http.ResponseWriter, r *http.Request, store *storage.Store) {
	q := r.URL.Query().Get("q")
	if q == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "q parameter required"})
		return
	}
	results := store.Search(q)
	writeJSON(w, http.StatusOK, map[string]any{"query": q, "count": len(results), "results": results})
}

func handleStats(w http.ResponseWriter, store *storage.Store) {
	writeJSON(w, http.StatusOK, store.Stats())
}

func handleMetrics(w http.ResponseWriter, mc *metrics.Collector) {
	snap := mc.Snapshot()
	p50, p95, p99 := mc.LatencySnapshot("pipeline.latency")
	snap["pipeline.latency_p50_us"] = p50
	snap["pipeline.latency_p95_us"] = p95
	snap["pipeline.latency_p99_us"] = p99

	hp50, hp95, hp99 := mc.LatencySnapshot("http.latency")
	snap["http.latency_p50_us"] = hp50
	snap["http.latency_p95_us"] = hp95
	snap["http.latency_p99_us"] = hp99

	writeJSON(w, http.StatusOK, snap)
}

func handleBrokerTopics(w http.ResponseWriter, brk *broker.Broker) {
	writeJSON(w, http.StatusOK, brk.GetStats())
}

func handleBrokerDLQ(w http.ResponseWriter, topicName string, brk *broker.Broker) {
	msgs, err := brk.DLQ(topicName)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"topic": topicName, "count": len(msgs), "messages": msgs})
}

type replayRequest struct {
	Topic      string `json:"topic"`
	FromOffset int64  `json:"from_offset"`
}

func handleBrokerReplay(w http.ResponseWriter, r *http.Request, brk *broker.Broker) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req replayRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	if req.Topic == "" {
		req.Topic = topicName
	}
	msgs, err := brk.Replay(req.Topic, req.FromOffset)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"topic":       req.Topic,
		"from_offset": req.FromOffset,
		"count":       len(msgs),
		"messages":    msgs,
	})
}

// writeJSON is a helper that encodes v as JSON and writes it with the given status.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}
