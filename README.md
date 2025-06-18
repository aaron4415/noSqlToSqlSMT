MongoDB to Oracle 10g Sync via Kafka & Debezium
Objective
Continuously capture data changes from MongoDB and apply them to Oracle 10g using Kafka Connect and Debezium. This includes:

Excluding unwanted fields

Flattening nested documents and arrays

Preserving change event semantics

1. Challenges
🔁 Data Model Mismatch
MongoDB: Document-oriented (nested objects, arrays)

Oracle: Relational schema (fixed columns)

🧹 Field Exclusion
Use field.exclude.list in connector config.

🧩 Handling Nested Objects & Arrays
Flatten nested documents

Normalize or flatten arrays

2. Debezium MongoDB Source Connector
A. Field Exclusion
properties

field.exclude.list=database.collection.field1,database.collection.field2
B. Extract & Flatten Documents
properties

transforms=unwrap
transforms.unwrap.type=io.debezium.connector.mongodb.transforms.ExtractNewDocumentState
transforms.unwrap.flatten.struct=true
transforms.unwrap.flatten.struct.delimiter=_
Example

Original:

json

{
  "_id": 1,
  "a": {"b": 1, "c": [1, 2, 3]},
  "d": 100
}
Flattened:

json

{
  "_id": 1,
  "a_b": 1,
  "a_c": [1, 2, 3],
  "d": 100
}
C. Handling Arrays
Option 1: array.encoding=document
properties

transforms.unwrap.array.encoding=document
Example Output:

json

{
  "_id": 1,
  "a1_0_a": 1,
  "a1_0_b": "none",
  "a1_1_a": "c",
  "a1_1_d": "something"
}
Drawback: Not suitable for large arrays.

Option 2: Change‑Stream Normalization
Emit each array element as a separate Kafka record and load into its own Oracle table.

Requires additional sink transformations or separate connectors per array field.

Option 3: Custom SMT in Go
Emits individual row events for array elements

Write a Go‑based SMT (avoiding Java) to transform arrays or nested docs into row‑based events.

Recommended Config:

capture.mode=change_streams_update_full_with_pre_image
transforms.unwrap.add.fields=op,before,after
snapshot.mode=when_needed
transforms.unwrap.delete.handling.mode=rewrite

Compare before and after states to add/remove array elements in Oracle.

Keeps Oracle schema clean

3. Recommended Approach
✅ Start Simple
If your data has no arrays or only flat documents, use Debezium’s built‑in ExtractNewDocumentState and flatten.struct.

📦 Array Handling
Small arrays → array.encoding=document

Large arrays → Normalize via custom SMT or change stream per array

⚙️ Custom Go SMT
Custom Go SMT (Schema Mapping Transform) benefits:

Recursive flattening of nested objects/arrays

Selective field mapping via field_mappings.yaml

Business logic (lookups, enrichments, filters)

Dynamic collection support

4. Connector Configurations
MongoDB Source Connector
json

{
  "connector.class": "io.debezium.connector.mongodb.MongoDbConnector",
  "mongodb.connection.string": "mongodb://user:pass@host:27017/?replicaSet=rs0",
  "database.include.list": "data-hub-stream,data-hub-source",
  "collection.include.list": "data-hub-stream.community_facility,data-hub-stream.bus",
  "topic.prefix": "mongo",
  "field.exclude.list": "orders.customField,users.internalNotes",

  "transforms": "unwrap",
  "transforms.unwrap.type": "io.debezium.connector.mongodb.transforms.ExtractNewDocumentState",
  "transforms.unwrap.flatten.struct": "true",
  "transforms.unwrap.flatten.struct.delimiter": "_",
  "transforms.unwrap.array.encoding": "document",
  "transforms.unwrap.add.fields": "op,before,after",
  "transforms.unwrap.delete.handling.mode": "rewrite",

  "capture.mode": "change_streams_update_full_with_pre_image",
  "snapshot.mode": "when_needed",
  "snapshot.fetch.size": "1024",

  "value.converter": "org.apache.kafka.connect.json.JsonConverter",
  "value.converter.schemas.enable": "true"
}
Oracle Sink Connector
json

{
  "name": "oracle-sink",
  "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
  "topics.regex": "mongo.*",
  "dialect.name": "OracleDatabaseDialect",
  "connection.url": "jdbc:oracle:thin:@//host:1521/ORCL",
  "connection.user": "dbuser",
  "connection.password": "dbpass",

  "auto.create": "true",
  "auto.evolve": "true",
  "insert.mode": "upsert",
  "pk.mode": "record_key",
  "pk.fields": "_id",
  "delete.enabled": "true",

  "value.converter": "org.apache.kafka.connect.json.JsonConverter",
  "value.converter.schemas.enable": "true",
  "key.converter": "org.apache.kafka.connect.storage.StringConverter",
  "key.converter.schemas.enable": "false"
}

5. Custom Go SMT: Pros & Cons

Pros

Flexible Array Flattening
Transforms arrays (and even arrays of objects) into separate Kafka topics or flattened field structures.

For deeply nested arrays or complex objects (e.g. route.station[].id), you can extend the SMT’s recursion logic to handle any nesting level.

Selective Field Inclusion
Define exactly which fields to ingest, rename, or exclude (even inside nested arrays/objects) via a simple field_mappings.yaml configuration.

Embedded Business Logic
Implement domain-specific transformations—data enrichment, lookups, conditional filtering—directly in Go, avoiding complex Oracle-side logic or extra connectors.

One Application for All Collections
A single Go SMT instance can monitor multiple collections dynamically.

When new collections are added, no additional connector or topic configuration is needed; the SMT auto-discovers and applies mapping rules.

Cons

Increased Source Load
Using change_streams_update_full_with_pre_image places extra I/O and storage demands on MongoDB.

Mitigation: scope each connector to a single database or shard to distribute load.

Operational Overhead
Hosting and maintaining a custom Go application (e.g., on EC2) adds infrastructure cost, deployment complexity, and the need for monitoring/alerts.

Development & Testing Effort
Custom SMT code requires thorough unit tests, integration tests, and maintenance—whereas built-in Debezium SMTs receive community support and frequent updates.

Choose the Go SMT when you need fine‑grained control over complex document schemas; for simpler or low‑volume use cases, consider Debezium’s native SMTs to reduce operational burden.

6. Kafka & Connector Deployment Guide
🧱 Prerequisites
Kafka (MSK or self-managed)

MongoDB with replica set

Oracle 10g with JDBC access

Kafka Connect & CLI tools

EC2 or VM to host Go SMT

Go toolchain (v1.18+)

Kafka Topic Operations
bash

# Set bootstrap server
export BS=yourBootstrapServer:9098

# List topics
kafka-topics.sh --bootstrap-server $BS --command-config client.properties --list

# Create a topic
kafka-topics.sh --bootstrap-server $BS --command-config client.properties \
  --create --topic my-topic --partitions 1 --replication-factor 3

# Delete a topic
kafka-topics.sh --bootstrap-server $BS --command-config client.properties \
  --delete --topic my-topic

# Consume messages
kafka-console-consumer.sh --bootstrap-server $BS \
  --consumer.config client.properties --topic my-topic --from-beginning \
  --property print.key=true
AWS MSK IAM Policy Example
json

{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "KafkaClusterConnect",
      "Effect": "Allow",
      "Action": ["kafka-cluster:Connect"],
      "Resource": ["arn:aws:kafka:ap-southeast-1:ACCOUNT_ID:cluster/Provisioned-Cluster-1/*"]
    },
    {
      "Sid": "KafkaTopicAccess",
      "Effect": "Allow",
      "Action": [
        "kafka-cluster:DescribeTopic",
        "kafka-cluster:ReadData",
        "kafka-cluster:WriteData",
        "kafka-cluster:CreateTopic",
        "kafka-cluster:DeleteTopic"
      ],
      "Resource": ["arn:aws:kafka:ap-southeast-1:ACCOUNT_ID:topic/Provisioned-Cluster-1/*"]
    },
    {
      "Sid": "KafkaGroupAccess",
      "Effect": "Allow",
      "Action": [
        "kafka-cluster:AlterGroup",
        "kafka-cluster:DescribeGroup"
      ],
      "Resource": ["arn:aws:kafka:ap-southeast-1:ACCOUNT_ID:group/Provisioned-Cluster-1/*"]
    }
  ]
}

7. Visuals
SMT Architecture
![image](https://raw.githubusercontent.com/aaron4415/noSqlToSqlSMT/main/go_smt.png)

SMT Processing Flow
![image](https://raw.githubusercontent.com/aaron4415/noSqlToSqlSMT/main/smt_flow_2.png)

