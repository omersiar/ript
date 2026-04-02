# Copilot Instructions

## Project Overview

RIPT (Reclaimer of Inactive Partitions and Topics) is a stateless Go daemon that monitors Apache Kafka topics to identify unused ones. It persists state in its own internal Kafka topic (`ript-state`) and exposes a REST API and web dashboard.

## Build, Test, and Run

```bash
make build        # Build binary to bin/ript
make run          # Build and run locally (requires .env or env vars)
make test         # go test -v ./...
make clean        # Remove build artifacts
make deps         # go mod download && go mod tidy

# Docker
make docker-up    # Start Zookeeper + Kafka + RIPT
make docker-down  # Tear down stack
make docker-logs  # Tail RIPT logs
```

**Single test:**
```bash
go test -v -run TestFunctionName ./internal/package_name
```

**Integration tests** (requires Docker):
```bash
./test-integration.sh
```

## Architecture

```
cmd/ript/           Entry point — wires up all components, handles OS signals, graceful shutdown
internal/config/    Loads and validates env vars with defaults (via godotenv)
internal/models/    Domain types: TopicStatus, PartitionInfo, ClusterSnapshot, Duration
internal/kafka/     Kafka client (topic metadata + offset fetching) and StateManager (snapshot persistence)
internal/tracker/   TopicTracker — core scan loop, staleness logic, in-memory snapshot
internal/api/       Gin HTTP server — routes and handlers read from TopicTracker snapshot
internal/logging/   Custom leveled logger (DEBUG/INFO/WARN/ERROR/FATAL)
web/templates/      dashboard.html — Bootstrap 5 frontend served by the API server
```

**Data flow:** `main` → initialize config/logging → connect Kafka (with retry) → `StateManager.Load()` → start `TopicTracker` scan loop → start `api.Server` → serve traffic from in-memory snapshot.

Each scan: fetch Kafka metadata + offsets → compute ages → update snapshot → persist to `ript-state` (log-compacted, one message per topic key).

**Staleness thresholds (configurable):**
- Partition "stale": no offset update in `RIPT_STALE_PARTITION_DAYS` days (default 7)
- Topic "unused": all partitions stale for `RIPT_UNUSED_TOPIC_DAYS` days (default 30)

## Key Conventions

**Concurrency:** Snapshot access uses `sync.RWMutex` (read-heavy). Goroutines are coordinated with context cancellation and `sync.WaitGroup` for shutdown.

**HTTP handlers (Gin):** All handlers are pointer receiver methods on `api.Server`. Return `503 ServiceUnavailable` if the tracker snapshot is not yet initialized. Use `gin.H` maps for all JSON responses.

**Error handling:** Wrap errors with `fmt.Errorf("context: %w", err)`. Log warnings and degrade gracefully for non-fatal errors; use `logger.Fatal()` only for initialization failures.

**State persistence:** `StateManager` writes one compacted Kafka message per topic (topic name as key). On restart, it reads back to EOF to restore the last snapshot. The app is stateless from infrastructure's perspective — Kafka is the durable store.

**Configuration:** All config via environment variables with `RIPT_` prefix. `.env` file auto-loaded if present. See `.env.example` for all supported variables. Key vars: `RIPT_KAFKA_BROKERS`, `RIPT_SCAN_INTERVAL_MINUTES`, `RIPT_HTTP_PORT`, `RIPT_LOG_LEVEL`.

**Models:** `Duration` is a custom type with `Days/Hours/Minutes/Seconds` fields and a `String()` method for human-readable output. Use `models.CalculateDuration(t time.Time)` to convert timestamps.

## Local Development Setup

```bash
cp .env.example .env   # Configure Kafka brokers and other settings
make docker-up         # Start Kafka stack (listens on localhost:9092)
make run               # Run RIPT against local Kafka
```

The RIPT dashboard is available at `http://localhost:8080` once running.
