# Weekend Engine — The Complete Guide 📖

A beginner-friendly explanation of **everything** in this project: what each part does, how they connect, and why they exist.

---

## Table of Contents

1. [The Big Picture](#the-big-picture)
2. [The Frontend (What You See)](#the-frontend-what-you-see)
3. [The Backend (What Runs Behind the Scenes)](#the-backend-what-runs-behind-the-scenes)
4. [The Journey of an Event (Step by Step)](#the-journey-of-an-event-step-by-step)
5. [Understanding Each Component](#understanding-each-component)
6. [The Dashboard Page](#the-dashboard-page)
7. [The Broker Page](#the-broker-page)
8. [How Data is Stored (No Database!)](#how-data-is-stored-no-database)
9. [Background Jobs (The Scheduler)](#background-jobs-the-scheduler)
10. [Middleware (The Security Guard)](#middleware-the-security-guard)
11. [Metrics (The Health Monitor)](#metrics-the-health-monitor)
12. [Configuration](#configuration)
13. [Common Questions](#common-questions)

---

## The Big Picture

Weekend Engine is a **weekend activity planner**. You submit fun activities (like "Sunrise Hike" or "Board Games Night") and the system:

1. **Receives** your activity via a REST API
2. **Routes** it through an in-memory message broker (like a mail sorting office)
3. **Processes** it through a 5-stage pipeline (validate → enrich → score → classify → store)
4. **Stores** the enriched result in memory
5. **Displays** everything on a live dashboard

The entire backend is a **single Go program** — no Kafka, no Redis, no PostgreSQL, no Docker. Everything runs in your computer's RAM.

```
 YOU (Browser)
   │
   ▼
┌──────────┐    ┌──────────┐    ┌──────────────────────────┐    ┌──────────┐
│  React   │───▶│  Go API  │───▶│  In-Memory Broker        │───▶│ Pipeline │
│ Frontend │◀───│  Server  │    │  (like a mini Kafka)     │    │ (5 steps)│
└──────────┘    └──────────┘    └──────────────────────────┘    └────┬─────┘
                     ▲                                               │
                     │          ┌──────────────────────────┐         │
                     └──────────│  In-Memory Storage       │◀────────┘
                                │  (Go maps = your "DB")   │
                                └──────────────────────────┘
```

---

## The Frontend (What You See)

The frontend is a **React + TypeScript** single-page app built with Vite. It has three pages:

### 🏠 Home / Landing Page
- A beautiful animated hero section introducing the project
- Feature cards explaining the architecture
- An "Event Processing Flow" diagram showing the 5 components

### 📊 Dashboard Page
- Where you interact with the system — create events, view them, search, filter
- More details in [The Dashboard Page](#the-dashboard-page) section below

### 📡 Broker Page
- A behind-the-scenes view of the message broker internals
- More details in [The Broker Page](#the-broker-page) section below

**How does the frontend talk to the backend?**

The frontend runs on `localhost:3000` and the backend on `localhost:8080`. Vite is configured with a **proxy** — any request to `/api/*` gets forwarded to `localhost:8080` with the `/api` prefix stripped off:

```
Frontend Request:  GET /api/events
       ↓ (Vite proxy strips /api)
Backend Receives:  GET /events
```

This is set up in `frontend/vite.config.ts`.

---

## The Backend (What Runs Behind the Scenes)

The backend is written in **Go** and lives in these folders:

| Folder | What it does |
|--------|-------------|
| `cmd/api/` | The main program — starts the HTTP server, broker, pipeline, and scheduler |
| `internal/broker/` | The in-memory message broker (simulates Kafka) |
| `internal/pipeline/` | The 5-stage event processing pipeline |
| `internal/storage/` | The in-memory data store (replaces a database) |
| `internal/events/` | The event data model (what a "weekend event" looks like) |
| `internal/middleware/` | HTTP middleware (rate limiting, CORS, logging, etc.) |
| `internal/metrics/` | Counters and timers to track system health |
| `internal/scheduler/` | Background job runner (TTL eviction, metrics snapshots) |
| `internal/config/` | Configuration loader (reads environment variables) |

When you run `go run ./cmd/api`, **all of these start together** in a single process.

---

## The Journey of an Event (Step by Step)

Let's trace exactly what happens when you click **"Create Event"** in the dashboard:

### Step 1: Frontend sends HTTP request
```
POST /api/events
Body: { "name": "Sunrise Hike", "category": "outdoors", "mood_boost": 9, "tags": ["nature"] }
```

### Step 2: API handler receives it
The Go server at `cmd/api/main.go` receives the request, validates the JSON, and creates a `WeekendEvent` struct.

### Step 3: Broker publishes the message
The event is serialized to JSON (bytes) and **published** to the broker's `"weekend-events"` topic.

The broker decides which **partition** to put it in using a hash of the category name. Think of partitions like lanes in a highway — they allow parallel processing.

```
Topic: "weekend-events"
├── Partition 0: [msg, msg, msg, ...]
├── Partition 1: [msg, msg, msg, ...]  ← "outdoors" events land here (based on hash)
├── Partition 2: [msg, msg, msg, ...]
└── Partition 3: [msg, msg, msg, ...]
```

### Step 4: Consumer goroutine picks it up
Each partition has a **goroutine** (a lightweight thread) constantly waiting for new messages. When a message arrives, the goroutine wakes up and processes it.

### Step 5: Pipeline processes the event
The event goes through 5 stages:

| Stage | What it does | Example |
|-------|-------------|---------|
| **Validate** | Checks the name isn't empty, category exists, mood_boost is 1-10 | Rejects `{ "name": "" }` |
| **Enrich** | Assigns a UUID, sets timestamps, figures out day-of-week and season | `id: "a1b2c3..."`, `day_of_week: "Friday"`, `season: "winter"` |
| **Score** | Calculates a **Fun Score** (0-100) from multiple factors | Mood 9 × 4 = 36, name bonus = 12, outdoor multiplier × 1.5 → **score: 82.5** |
| **Classify** | Assigns a priority tier based on the score | Score 82.5 → `"critical"` (≥80 = critical) |
| **Persist** | Saves the final event to the in-memory store | Stored in a Go map |

### Step 6: Event appears in the dashboard
The frontend auto-refreshes every 5 seconds. On the next poll, `GET /events` returns the newly processed event and the dashboard updates.

### What if something goes wrong?
If any pipeline stage fails (e.g., invalid data), the broker **retries** up to 3 times with increasing delays. If all retries fail, the message goes to the **Dead Letter Queue (DLQ)** — a holding area for failed messages that you can inspect on the Broker page.

---

## Understanding Each Component

### 📡 The Broker (In-Memory Message Broker)

**What is a message broker?**
Think of it as a **post office**. When you send a letter (event), the post office (broker) receives it, sorts it into the right mailbox (partition), and a mail carrier (consumer goroutine) delivers it to the recipient (pipeline).

**Why use a broker at all?**
- **Decoupling** — The API doesn't directly process events. It just drops them in the broker and responds immediately. This keeps the API fast.
- **Parallel processing** — Multiple goroutines can process messages from different partitions simultaneously.
- **Retry & DLQ** — Failed messages can be retried automatically, and permanently failed ones are saved for inspection.
- **Replay** — You can re-process old messages from any point in history.

**Key concepts:**
- **Topic** — A named stream of messages (this project uses one: `"weekend-events"`)
- **Partition** — A sub-stream within a topic. Messages are distributed across partitions using a hash of the key (category).
- **Consumer Group** — A group of goroutines that coordinate to avoid duplicate processing. Each partition is consumed by exactly one goroutine in the group.
- **Offset** — A counter tracking which messages have been processed. Like a bookmark in a book.
- **DLQ (Dead Letter Queue)** — Where messages go when they can't be processed after all retries.

### ⚡ The Pipeline

The pipeline is a chain of **stages** that process events one after another. Each stage has a timeout — if it takes too long, it's cancelled.

The pipeline pattern is common in real systems. Think of it like an assembly line in a factory — each station does one specific job before passing the product to the next station.

### 🗄️ The Storage Engine

A thread-safe in-memory store built with Go maps. It supports:
- **CRUD** — Create, Read, Update, Delete events by ID
- **Secondary indices** — Fast lookups by category or priority without scanning every event
- **Full-text search** — Search event names and categories by substring
- **Pagination** — Fetch results page by page (e.g., 10 events per page)
- **TTL eviction** — Events automatically expire after 24 hours

---

## The Dashboard Page

The dashboard is the main page where you interact with the system. Here's every section explained:

### ➕ "New Event" Button
Opens a modal form where you create a weekend activity:
- **Activity Name** — e.g., "Sunrise Hike", "Board Games Night"
- **Category** — dropdown (outdoors, social, music, creative, food, etc.)
- **Mood Boost** — slider from 1-10 (how much this activity lifts your mood)
- **Tags** — comma-separated labels like "nature, exercise"

### 📊 Stats Cards (top row)
Four cards showing aggregate statistics:
- **Total Events** — how many events are in the store right now
- **Avg Fun Score** — average fun score across all events
- **Top Category** — the most popular category and how many events it has
- **By Priority** — breakdown showing how many critical/high/medium/low events exist

### ⚡ Pipeline Stages
A visual representation of the 5-stage pipeline: `validate → enrich → score → classify → persist` with the configured timeout.

### 📋 Events Table
A paginated table showing all your events with:
- **Name** — the activity name
- **Category** — color-coded category label
- **Score** — a visual bar (0-100) showing the fun score
- **Priority** — badge (critical = red, high = yellow, medium = blue, low = grey)
- **Mood** — the original mood_boost value you set
- **Tags** — any tags you added
- **Day** — what day of the week it was created
- **Delete** — ✕ button to remove the event

**Filters:** Above the table, you can filter by category and/or priority using dropdowns.

**Search:** The search bar in the header performs live full-text search across event names and categories.

**Pagination:** "← Prev" and "Next →" buttons at the bottom for paging through results.

### 📈 Score Distribution
A bar chart showing how many events fall into each score range: 0-29, 30-54, 55-79, 80-100.

### 📂 By Category
A bar chart showing event counts per category (outdoors, social, music, etc.).

### 📊 Live Metrics
A grid of real-time system metrics that auto-refreshes every 5 seconds:
- `events.processed` — total events successfully processed
- `events.published` — total events published to the broker
- `events.failed` — total events that failed processing
- `pipeline.runs` — total pipeline executions
- `pipeline.latency_p50/p95/p99` — processing time percentiles in microseconds
- `http.latency_p50/p95/p99` — HTTP response time percentiles
- `store.total_events` — current events in store
- `broker.queue_depth` — unprocessed messages in broker
- `scheduler.ticks` — how many times the scheduler has run

---

## The Broker Page

The broker page lets you inspect the internals of the message broker. It's like looking at the engine of a car.

### 📌 Topic Cards
For each topic (typically `"weekend-events"`), you see:
- **Published** — total messages published to this topic
- **Partitions** — number of partitions (default: 4)
- **DLQ Size** — number of messages in the dead-letter queue (0 means all messages processed successfully)

### 🔍 View DLQ Button
Shows the contents of the dead-letter queue — messages that failed processing after all retries. If the DLQ is empty (it usually is), it shows "DLQ is empty — all messages processed successfully! 🎉"

### ↻ Replay Button
Lets you re-process messages from a specific offset. This is useful if you fix a bug and want to reprocess old messages. Enter an offset number and click "Replay" to replay all messages from that offset onwards.

---

## How Data is Stored (No Database!)

There is **no database** in this project. Everything lives in RAM (your computer's memory) as Go data structures.

### Where exactly?

```
Go Process Memory
│
├── storage.Store (your "database")
│   ├── primary:    map[string]Event      → All events, keyed by UUID
│   ├── byCategory: map[string]Set[ID]    → Index: category → event IDs
│   └── byPriority: map[string]Set[ID]    → Index: priority → event IDs
│
├── broker.Broker (the message system)
│   └── topics["weekend-events"]
│       ├── partition[0].log: []Message   → Append-only message log
│       ├── partition[1].log: []Message
│       ├── partition[2].log: []Message
│       ├── partition[3].log: []Message
│       └── dlq.log: []Message            → Failed messages
│
├── metrics.Collector (counters & timers)
│   ├── counters: map[string]int64        → Event counts, error counts, etc.
│   └── latencies: map[string][]Duration  → Ring buffers for percentile calculation
│
└── scheduler.Scheduler (background jobs)
    └── jobs: [ttl-eviction, metrics-snapshot]
```

### When does data disappear?

| Scenario | Data gone? | Why |
|----------|-----------|-----|
| Refresh browser | ❌ No | Data is on the server, not the browser |
| Close browser | ❌ No | Server keeps running |
| Restart frontend (`npm run dev`) | ❌ No | Frontend is stateless |
| **Stop the Go server** (`Ctrl+C`) | ✅ **Yes** | All RAM is freed |
| **Wait 24 hours** | ✅ **Yes** | TTL eviction deletes old events |
| **PC restart** | ✅ **Yes** | Server process dies |

---

## Background Jobs (The Scheduler)

The scheduler runs **recurring background tasks** on a timer, similar to cron jobs:

| Job | Interval | What it does |
|-----|----------|-------------|
| **TTL Eviction** | Every 30 seconds | Scans all events and deletes any whose `expires_at` timestamp is in the past (default TTL: 24 hours) |
| **Metrics Snapshot** | Every 10 seconds | Records the current store size and broker queue depth into the metrics collector |

The scheduler uses **jitter** (small random delays) to prevent all jobs from firing at the exact same moment, which is a pattern called "avoiding the thundering herd."

---

## Middleware (The Security Guard)

Every HTTP request passes through a **middleware stack** before reaching the actual handler. Think of it like airport security — you go through multiple checkpoints:

| Middleware | What it does |
|-----------|-------------|
| **Request Size** | Rejects requests larger than 1 MB |
| **Rate Limiter** | Limits to 50 requests/second per IP using a token bucket algorithm |
| **Powered** | Adds an `X-Powered-By: Weekend-Engine/1.0` header |
| **Request ID** | Assigns a unique ID to every request for tracing |
| **Logger** | Logs every request (method, path, status, duration) and records HTTP latency |
| **Recovery** | Catches panics (crashes) so the server doesn't die |
| **CORS** | Allows the frontend (different port) to talk to the backend |
| **Security Headers** | Adds headers to protect against common web attacks (XSS, clickjacking, etc.) |

The order matters — the request flows through them left-to-right, and the response flows back right-to-left.

---

## Metrics (The Health Monitor)

The metrics system tracks everything happening in the engine using three types of measurement:

- **Counters** — Numbers that only go up: `events.processed`, `events.failed`, `pipeline.runs`
- **Gauges** — Current values that go up and down: `store.total_events`, `broker.queue_depth`
- **Latency histograms** — Timing data stored in a ring buffer, used to calculate percentiles:
  - **p50** = median (half of requests are faster than this)
  - **p95** = 95th percentile (95% of requests are faster than this)
  - **p99** = 99th percentile (only 1% of requests are slower)

All metrics are available via `GET /metrics` and displayed in the dashboard's "Live Metrics" section.

---

## Configuration

All settings are loaded from **environment variables** when the server starts. If you don't set them, sensible defaults are used:

| Variable | Default | What it controls |
|----------|---------|-----------------|
| `PORT` | `8080` | What port the server listens on |
| `BROKER_PARTITIONS` | `4` | Number of parallel message lanes |
| `PIPELINE_TIMEOUT` | `5s` | Max time per pipeline stage before cancellation |
| `RATE_LIMIT_RPS` | `50` | Max requests per second allowed |
| `RATE_LIMIT_BURST` | `100` | Burst capacity for the rate limiter |
| `EVENT_TTL` | `24h` | How long events live before auto-deletion |
| `WORKER_COUNT` | `4` | (Now unused — broker handles concurrency internally) |
| `METRICS_ENABLED` | `true` | Whether to collect metrics |

Example: To change the TTL to 1 hour and use 8 partitions:
```bash
EVENT_TTL=1h BROKER_PARTITIONS=8 go run ./cmd/api
```

---

## Common Questions

### "Is this production-ready?"
It's production-**grade** in terms of patterns (rate limiting, graceful shutdown, error handling, retry logic) but not production-**ready** because all data is in memory. A real production system would need a database, persistent message broker, authentication, etc.

### "Why not just use Kafka?"
The point is to show you **don't need** Kafka for many use cases. The in-memory broker implements the same concepts (topics, partitions, consumer groups, offsets, DLQ, replay) in ~350 lines of Go.

### "Can I add a real database later?"
Yes! The storage layer (`internal/storage/`) has a clean interface (`Put`, `Get`, `Delete`, `List`, `Search`, `Stats`). You'd create a new implementation backed by PostgreSQL, SQLite, or any other database, and swap it in.

### "What happens if I create lots of events?"
They all live in RAM, so eventually you'd run out of memory. The TTL eviction (24h default) helps by cleaning up old events automatically. For a learning/demo project, this is perfectly fine.

### "Why does the fun score vary for similar events?"
The score algorithm considers multiple factors: mood boost (×4), name length (log scale), category multiplier (outdoors=1.5×, social=1.3×), weekend bonus (+10 on Sat/Sun), and tag count (+2 per tag). So "Sunrise Hike" on a Saturday with 3 tags scores very differently from "Nap" on a Tuesday with no tags.
