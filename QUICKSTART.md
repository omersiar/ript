## Prerequisites

- Go 1.21+
- Docker & Docker Compose (for containerized deployment)
- A Kafka cluster to monitor
- A Compacted Topic for RIPT store its tracking information

### Option 1: Docker Compose (Recommended)
```bash
git clone https://github.com/omersiar/ript.git
cd ript
make docker-up
```
Visit http://localhost:8080 - dashboard is live in 30 seconds!

### Option 2: Binary + Local Kafka
```bash
# Copy and edit environment
cp .env.example .env
# Edit .env to point to your Kafka brokers
make run
```

## Configuration

For RIPT specific configuration: 

* Defaults should be good for most cases
* You need a compacted topic for RIPT's state storage, RIPT can create this topic if allowed, otherwise provision it with a lower segment.ms and set it to 86400000 (1 day)
* Decide on a good partition count for RIPT's state topic which will be used as sharding for large clusters
* Choose an interval for tracking the offsets - A daily scan should be okay for the most cases, for more dynamic environments choose a lower interval

Environment variables (see [.env.example](.env.example)) should be self explanatory

If your Kafka cluster enforce ACLs see [ACLs.md](ACLs.md).

Create/edit `.env`:
```bash
RIPT_KAFKA_BROKERS=localhost:9092          # Your Kafka brokers (comma-separated)
RIPT_SCAN_INTERVAL_MINUTES=5               # How often to scan (lower = faster updates)
RIPT_STATE_TOPIC=ript-state                # Internal state topic (auto-created if permitted)
RIPT_HTTP_PORT=8080                        # Dashboard port
RIPT_LOG_LEVEL=info                        # debug|info|warn|error
FRANZ_GO_DEBUG=1                           # Optional: Enable Kafka debug logging
```

## API

```bash
# Health check
curl http://localhost:8080/api/health

# Get all topics
curl http://localhost:8080/api/topics | jq

# Get unused topics (not updated in 30 days)
curl "http://localhost:8080/api/unused?threshold_days=30" | jq

# Get statistics
curl http://localhost:8080/api/stats | jq
```


## Building from Source

```bash
make build

./bin/ript
```
