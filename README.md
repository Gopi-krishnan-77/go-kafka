# Weekend Fun Kafka Project (Go)

A tiny event-driven app for logging weekend activities and processing them asynchronously with Kafka.

## What this repo includes

- `cmd/api`: HTTP API that accepts events and publishes them to Kafka.
- `cmd/worker`: Consumer worker that reads events and prints a derived `fun_score`.
- `docker-compose.yml`: Single-node Kafka (KRaft mode) for local development.
- `Makefile`: Common project commands.

## Event model

```json
{
  "name": "Board games night",
  "category": "social",
  "mood_boost": 8
}
```

## Quick start

1. Start Kafka:
   ```bash
   make up
   ```
2. In terminal 1, run worker:
   ```bash
   make run-worker
   ```
3. In terminal 2, run API:
   ```bash
   make run-api
   ```
4. Publish an event:
   ```bash
   curl -X POST http://localhost:8080/events \
     -H 'content-type: application/json' \
     -d '{"name":"Sunrise hike","category":"outdoors","mood_boost":9}'
   ```

## Useful environment variables

- `KAFKA_BROKER` (default `localhost:9092`)
- `KAFKA_TOPIC` (default `weekend-events`)
- `KAFKA_GROUP` (default `weekend-workers`)
- `PORT` (default `8080`)

## Development

```bash
make tidy
make test
```
