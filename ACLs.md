### Kafka ACLs

If your Kafka cluster enforce ACLs, grant the RIPT principal permissions for:

- Fetching offsets for the topics that RIPT track (reading messages not needed)
- Describing topic configuration (in order to check topic's retention policy)
- Read/write access to the RIPT's internal state topic (`RIPT_STATE_TOPIC`)
- Consumer group access for scan workload balancing (`RIPT_KAFKA_CONSUMER_GROUP_ID`)

Example (`User:ript`, Kafka ACL authorizer):

Adjust principal format as needed for your environment (for example, mTLS DNs or SASL usernames).

```bash
# 1) Cluster metadata - Optional
kafka-acls.sh --bootstrap-server localhost:9092 \
   --add --allow-principal User:ript \
   --operation Describe --cluster

# 2) Need only to describe topics (use --topic '*' for all topics, or scope to prefixes)
kafka-acls.sh --bootstrap-server localhost:9092 \
   --add --allow-principal User:ript \
   --operation Describe \
   --topic '*'

# 3) Need only to describe topics configuration (use --topic '*' for all topics, or scope to prefixes)
kafka-acls.sh --bootstrap-server localhost:9092 \
   --add --allow-principal User:ript \
   --operation DescribeConfig \
   --topic '*'

# 4) Internal state topic access
kafka-acls.sh --bootstrap-server localhost:9092 \
   --add --allow-principal User:ript \
   --operation All  \
   --topic ript-state

# 5) Consumer group access used for sharding
kafka-acls.sh --bootstrap-server localhost:9092 \
   --add --allow-principal User:ript \
   --operation All \
   --group ript-scan
```