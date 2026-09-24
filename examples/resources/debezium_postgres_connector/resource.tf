# Connector authenticating the schema-history Kafka client with SASL/SCRAM.
resource "debezium_postgres_connector" "sasl_scram_example" {
  name                 = "example-service-event-outbox"
  database_hostname    = "postgres"
  database_user        = "debezium_example_ro"
  database_password    = var.example_database_password
  database_dbname      = "example_db"
  database_server_name = "example-service"
  slot_name            = "example_service_debezium"
  table_include_list   = "public.outbox_events"

  schema_history_kafka_topic             = "example-service-dbhistory"
  schema_history_kafka_bootstrap_servers = "kafka-b1:9096,kafka-b2:9096,kafka-b3:9096"

  outbox_route_topic_replacement = "example.outbox.event.$${routedByValue}"

  # Optional tuning knobs; shown here with non-default values, omit to use the defaults.
  snapshot_mode         = "initial"
  heartbeat_interval_ms = 5000

  sasl_scram = {
    username = "debezium"
    password = var.example_kafka_password
  }
}

# Connector authenticating the schema-history Kafka client with AWS MSK IAM.
resource "debezium_postgres_connector" "aws_msk_iam_example" {
  name                 = "example-other-service-event-outbox"
  database_hostname    = "postgres-other"
  database_user        = "debezium_example_other_ro"
  database_password    = var.example_other_database_password
  database_dbname      = "example_other_db"
  database_server_name = "example-other-service"
  slot_name            = "example_other_service_debezium"
  table_include_list   = "public.outbox_events"

  schema_history_kafka_topic             = "example-other-service-dbhistory"
  schema_history_kafka_bootstrap_servers = "kafka-b1:9096,kafka-b2:9096,kafka-b3:9096"

  outbox_route_topic_replacement = "example-other.outbox.event.$${routedByValue}"

  aws_msk_iam = true

  # Escape hatch: any Kafka Connect/Debezium config key not exposed as an attribute above can be
  # set directly here. Merged in last, so it can also override attribute-derived values.
  extra_config = {
    "publication.name" = "example_other_service_publication"
  }
}
