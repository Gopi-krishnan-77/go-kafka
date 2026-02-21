.PHONY: run run-api run-worker test vet build clean demo frontend-install frontend-dev frontend-build

# Run the API server (includes embedded workers)
run: run-api

run-api:
	go run ./cmd/api

# Run the standalone worker (for isolated processing)
run-worker:
	go run ./cmd/worker

# Run all tests
test:
	go test ./... -v -count=1

# Static analysis
vet:
	go vet ./...

# Build both binaries
build:
	go build -o bin/api.exe ./cmd/api
	go build -o bin/worker.exe ./cmd/worker

# Clean build artifacts
clean:
	@if exist bin rmdir /s /q bin

# Fetch & tidy dependencies
tidy:
	go mod tidy

# Quick demo: create a few sample events
demo:
	@echo Creating sample events...
	@curl -s -X POST http://localhost:8080/events -H "Content-Type: application/json" -d "{\"name\":\"Sunrise Hike\",\"category\":\"outdoors\",\"mood_boost\":9,\"tags\":[\"nature\",\"exercise\"]}"
	@echo.
	@curl -s -X POST http://localhost:8080/events -H "Content-Type: application/json" -d "{\"name\":\"Board Games Night\",\"category\":\"social\",\"mood_boost\":8,\"tags\":[\"friends\",\"fun\"]}"
	@echo.
	@curl -s -X POST http://localhost:8080/events -H "Content-Type: application/json" -d "{\"name\":\"Guitar Practice\",\"category\":\"music\",\"mood_boost\":7,\"tags\":[\"creative\"]}"
	@echo.
	@curl -s -X POST http://localhost:8080/events -H "Content-Type: application/json" -d "{\"name\":\"Quick Nap\",\"category\":\"rest\",\"mood_boost\":3}"
	@echo.
	@echo.
	@echo Demo events created! Try:
	@echo   curl http://localhost:8080/events
	@echo   curl http://localhost:8080/stats
	@echo   curl http://localhost:8080/metrics
	@echo   curl "http://localhost:8080/events/search?q=hike"

# ─── Frontend (React + Vite) ──────────────────────

# Install frontend deps
frontend-install:
	cd frontend && npm install

# Start frontend dev server (proxies /api → :8080)
frontend-dev:
	cd frontend && npm run dev

# Production build
frontend-build:
	cd frontend && npm run build

