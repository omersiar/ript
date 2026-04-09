### Kafka ACLs (secured clusters)

If your Kafka cluster enforce ACLs, grant the RIPT principal permissions for:

- Fetching offsets for the topics that RIPT track (reading messages not needed)
- Read/write access to the RIPT's internal state topic
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
   --operation Describe \
   --topic '*'

# 3) Internal state topic access
kafka-acls.sh --bootstrap-server localhost:9092 \
   --add --allow-principal User:ript \
   --operation All  \
   --topic ript-state

# 4) Consumer group access used for balancing scans
kafka-acls.sh --bootstrap-server localhost:9092 \
   --add --allow-principal User:ript \
   --operation All \
   --group ript-scan
```