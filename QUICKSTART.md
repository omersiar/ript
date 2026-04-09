## Configuration

For RIPT specific configuration: 

* Defaults should be good for most of the cases
* You need a compacted topic for RIPT's state storage, RIPT can create this topic if allowed, otherwise provision it with a lower segment.ms and set it to 86400000 (1 day)
* Decide on a good partition count for RIPT's state topic which will be used as sharding for large clusters
* Choose an interval for tracking the offsets - A daily scan should be okay for most of the cases, for more dynamic environments choose a lower interval

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