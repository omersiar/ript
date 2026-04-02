### Kafka ACLs (secured clusters)

If your Kafka cluster enforce ACLs, grant the RIPT principal permissions for:

- Cluster metadata discovery
- Reading monitored topics (offset and metadata inspection)
- Read/write access to the internal state topic
- Consumer group access for scan workload balancing (`RIPT_KAFKA_CONSUMER_GROUP_ID`)

Example (`User:ript`, Kafka ACL authorizer):

Adjust principal format as needed for your environment (for example, mTLS DNs or SASL usernames).

```bash
# 1) Cluster metadata
kafka-acls.sh --bootstrap-server localhost:9092 \
   --add --allow-principal User:ript \
   --operation Describe --cluster

# 2) Read monitored topics (use --topic '*' for all topics, or scope to prefixes)
kafka-acls.sh --bootstrap-server localhost:9092 \
   --add --allow-principal User:ript \
   --operation Describe --operation Read \
   --topic '*'

# 3) Internal state topic access
kafka-acls.sh --bootstrap-server localhost:9092 \
   --add --allow-principal User:ript \
   --operation Describe --operation Read --operation Write \
   --topic ript-state

# 4) Consumer group access used for balancing scans
kafka-acls.sh --bootstrap-server localhost:9092 \
   --add --allow-principal User:ript \
   --operation Describe --operation Read \
   --group ript-scan
```

Optional (only if RIPT should create `ript-state` itself):

```bash
kafka-acls.sh --bootstrap-server localhost:9092 \
   --add --allow-principal User:ript \
   --operation Create \
   --topic ript-state
```