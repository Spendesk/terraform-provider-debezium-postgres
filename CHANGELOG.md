## 0.1.0 (Unreleased)

FEATURES:

* **New Resource:** `debezium_postgres_connector` manages a Debezium PostgreSQL outbox connector
  on a Kafka Connect cluster (create/read/update/delete/import), with `sasl_scram` and
  `aws_msk_iam` authentication for the schema-history Kafka client.
* **New Data Source:** `debezium_postgres_connector` looks up an existing connector by name.
