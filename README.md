# Weekend Engine 🎉

A **zero-dependency event-driven processing engine** built in pure Go. No Kafka, no Docker, no external infrastructure — everything runs in-memory with production-grade patterns.

## What is Weekend Engine?

Weekend Engine is a full-stack project that demonstrates **real-world Go concurrency and event-driven architecture patterns** without requiring any external infrastructure. It simulates a "weekend activity planner" where users submit fun activities (like hiking, board games, cooking) and the system processes them through a sophisticated pipeline that scores, classifies, and stores each event.

The entire backend — message broker, processing pipeline, storage engine, scheduler, and HTTP server — runs **in a single Go binary** with zero dependencies beyond the standard library (plus `uuid` for ID generation). The frontend is a React TypeScript SPA that communicates with the backend via REST APIs.

**Why?** To showcase how you can build complex, production-grade systems using pure Go primitives (`goroutines`, `channels`, `sync.Mutex`, `context`) instead of relying on heavy infrastructure like Kafka, Redis, or PostgreSQL.

## How It Works

Here's the end-to-end flow when you create a weekend event:

1. **HTTP Request** — The React frontend sends a `POST /events` request with the event name, category, mood boost, and optional tags.

2. **Broker Publish** — The API handler validates the payload, serialises it to JSON, and publishes it to the **in-memory broker**. The broker uses FNV-1a hashing on the event's category to route it to one of 4 partitions (configurable), simulating Kafka-style partitioned topics.

3. **Consumer Goroutines** — On startup, the broker spawns one goroutine per partition. Each goroutine blocks until a new message arrives on its partition, then picks it up for processing. Consumer group offset tracking ensures each message is processed exactly once.

4. **5-Stage Pipeline** — The message is deserialized back into an event and run through 5 stages in sequence, each with its own timeout:
   - **Validate** — Checks name, category, and mood_boost are present and within range.
   - **Enrich** — Assigns a UUID, sets timestamps, derives day-of-week and season.
   - **Score** — Computes a weighted **Fun Score** (0–100) based on mood boost, name length, category multiplier, weekend bonus, and tag count.
   - **Classify** — Assigns a priority tier (critical/high/medium/low) based on the fun score.
   - **Persist** — Writes the fully enriched event to the storage engine.

5. **Error Handling** — If any pipeline stage fails, the broker retries up to 3 times with backoff. If all retries fail, the message is routed to a **dead-letter queue** (DLQ) for later inspection.

6. **Query** — The frontend polls the REST API every 5 seconds to fetch events, stats, metrics, and broker state, displaying them in a live dashboard.

## How Data is Stored (No Database!)

Weekend Engine has **no database** — all data lives in RAM inside Go data structures. Here's how:

| Component | Implementation | Details |
|-----------|----------------|---------|
| **Primary Store** | `map[string]Event` | Go map keyed by event UUID — O(1) reads, writes, deletes |
| **Category Index** | `map[string]Set[ID]` | Secondary index mapping each category to a set of event IDs for fast filtered queries |
| **Priority Index** | `map[string]Set[ID]` | Same pattern for priority-based filtering |
| **Full-Text Search** | Substring scan | Iterates all events, matching against lowercased name and category |
| **Pagination** | Sort + slice | Results sorted by `created_at` descending, then sliced by page/limit |
| **Thread Safety** | `sync.RWMutex` | Read-write lock allows concurrent reads with exclusive writes |
| **TTL Eviction** | Background scheduler | Every 30 seconds, the scheduler scans for events past their `expires_at` and removes them (default TTL: 24 hours) |
| **Broker Messages** | Append-only `[]Message` per partition | Messages are stored in partition logs for replay support |
| **Metrics** | `map[string]int64` + latency ring buffer | Counters, gauges, and percentile histograms (p50/p95/p99) |

> **⚠️ Important:** Since everything is in-memory, **all data is lost when the server restarts**. This is by design — the project focuses on demonstrating architecture patterns, not persistence. In a production system, you'd swap the storage layer for a real database.

## Architecture

```
┌──────────────┐     ┌────────────────┐     ┌──────────────────────────────────┐     ┌─────────────┐
│  HTTP API    │────▶│  In-Memory     │────▶│  Processing Pipeline             │────▶│  Storage    │
│  (11 routes) │     │  Broker        │     │  validate → enrich → score →     │     │  Engine     │
│              │     │  (partitioned) │     │  classify → persist              │     │  (indexed)  │
└──────────────┘     └────────────────┘     └──────────────────────────────────┘     └─────────────┘
       │                    │                          │                                    │
       │              ┌─────┴──────┐            ┌──────┴──────┐                      ┌──────┴──────┐
       │              │  DLQ       │            │  Metrics    │                      │  TTL        │
       │              │  (per-topic)│            │  Collector  │                      │  Eviction   │
       │              └────────────┘            └─────────────┘                      └─────────────┘
       │
  ┌────┴─────────────────────────────┐
  │  Middleware Stack                │
  │  rate-limit → CORS → recovery → │
  │  request-id → logging → security│
  └──────────────────────────────────┘
```

## What's Inside

| Package | Description |
|---------|-------------|
| `internal/broker` | In-memory message broker with partitioned topics, consumer groups, offset tracking, dead-letter queues, and message replay |
| `internal/pipeline` | Multi-stage processing pipeline: validate → enrich → score → classify → persist |
| `internal/storage` | Thread-safe indexed store with secondary indices, full-text search, pagination, and TTL-based eviction |
| `internal/middleware` | HTTP middleware stack: request ID, structured logging, token-bucket rate limiting, panic recovery, CORS, security headers |
| `internal/scheduler` | Recurring background job runner with jitter and panic recovery |
| `internal/metrics` | Lightweight metrics collector with counters, gauges, and latency histograms (p50/p95/p99) |
| `internal/events` | Enriched event model with UUID, priority tiers, tags, fun score, day-of-week, season |
| `internal/config` | Environment-based configuration with validation |

## Quick Start

```bash
# Run the API server (includes embedded workers and scheduler)
make run-api

# In another terminal, create some events
make demo

# Run the React frontend (in a separate terminal)
make frontend-install  # first time only
make frontend-dev      # starts on http://localhost:3000
```

That's it. No Docker. No Kafka. No setup.

## API Endpoints

### Events

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/events` | Create a new event (publishes to broker → pipeline) |
| `GET` | `/events` | List events with pagination and filtering |
| `GET` | `/events/{id}` | Get a single event by ID |
| `DELETE` | `/events/{id}` | Delete an event |
| `GET` | `/events/search?q=` | Full-text search across names and categories |

### Analytics

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/stats` | Aggregated stats: counts, averages, distributions |
| `GET` | `/metrics` | Raw counters and latency percentiles |

### Broker Introspection

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/broker/topics` | Topic metadata (partitions, publish count, DLQ size) |
| `GET` | `/broker/dlq/{topic}` | View dead-letter queue messages |
| `POST` | `/broker/replay` | Replay messages from a given offset |

### System

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/healthz` | Health check |
| `GET` | `/pipeline` | Pipeline stage names and timeout |

## Event Model

### Create Request

```json
{
  "name": "Sunrise Hike",
  "category": "outdoors",
  "mood_boost": 9,
  "tags": ["nature", "exercise"]
}
```

### Processed Event (after pipeline)

```json
{
  "id": "a1b2c3d4-...",
  "name": "Sunrise Hike",
  "category": "outdoors",
  "mood_boost": 9,
  "tags": ["nature", "exercise"],
  "fun_score": 82.5,
  "priority": "critical",
  "day_of_week": "Saturday",
  "season": "summer",
  "created_at": "2025-07-05T08:00:00Z",
  "processed_at": "2025-07-05T08:00:00.003Z",
  "expires_at": "2025-07-06T08:00:00Z"
}
```

## Fun Score Algorithm

The pipeline computes a composite **fun score** (0–100) from multiple weighted factors:

| Factor | Range | Weight |
|--------|-------|--------|
| Mood boost (1–10) | 0–40 | ×4.0 |
| Name length | 0–20 | log₂ scale |
| Category multiplier | ×1.0–×1.5 | `outdoors` = 1.5, `social` = 1.3, etc. |
| Weekend bonus | +10 | if Saturday or Sunday |
| Tag bonus | 0–10 | +2 per tag (max 5 tags) |

## Configuration

All settings are loaded from environment variables with sensible defaults:

| Variable | Default | Description |
|----------|---------|-------------|
| `PORT` | `8080` | HTTP listen port |
| `BROKER_PARTITIONS` | `4` | Number of partitions per topic |
| `PIPELINE_TIMEOUT` | `5s` | Max time per pipeline stage |
| `RATE_LIMIT_RPS` | `50` | Requests per second per IP |
| `RATE_LIMIT_BURST` | `100` | Token bucket burst size |
| `EVENT_TTL` | `24h` | Event time-to-live before eviction |
| `WORKER_COUNT` | `4` | Number of consumer goroutines |
| `METRICS_ENABLED` | `true` | Enable metrics collection |

## Frontend (React + TypeScript)

The `frontend/` directory contains a React TypeScript SPA built with Vite:

- **Landing page** with animated hero, feature cards, and architecture flow diagram
- **Dashboard** — live stats cards, events table with search/filter/paginate, score distribution & category bars, pipeline visualization, real-time metrics (auto-refresh 5s)
- **Event creation** — modal form with category picker, mood boost slider, and tag input
- **Broker introspection** — topic stats, DLQ viewer, and message replay controls
- **Dark theme** with glassmorphism, gradient animations, and full mobile responsiveness

```bash
make frontend-install   # install npm dependencies
make frontend-dev       # dev server on :3000 (proxies /api → :8080)
make frontend-build     # production build to frontend/dist/
```

## Development

```bash
make test       # Run all unit tests
make vet        # Static analysis
make build      # Build binaries to bin/
make tidy       # go mod tidy
make clean      # Remove build artifacts
```

## Project Structure

```
.
├── cmd/
│   ├── api/main.go          # REST API server + embedded workers
│   └── worker/main.go       # Standalone worker binary
├── internal/
│   ├── broker/              # In-memory message broker
│   ├── config/              # Environment configuration
│   ├── events/              # Event model
│   ├── metrics/             # Metrics collector
│   ├── middleware/          # HTTP middleware stack
│   ├── pipeline/            # Processing pipeline
│   ├── scheduler/           # Background job scheduler
│   └── storage/             # Indexed in-memory store
├── frontend/
│   ├── src/
│   │   ├── App.tsx          # SPA: Hero + Dashboard + Broker
│   │   ├── types.ts         # TypeScript interfaces
│   │   ├── services/api.ts  # Typed API client
│   │   └── index.css        # Full design system
│   ├── vite.config.ts       # Vite + proxy config
│   └── package.json
├── Makefile
├── go.mod
└── README.md
```
