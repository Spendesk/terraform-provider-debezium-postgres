terraform {
  required_providers {
    debezium = {
      source = "spendesk/debezium-postgres"
    }
  }
}

provider "debezium" {
  endpoint = "http://localhost:8083"
}

data "debezium_postgres_connector" "example" {
  name = "example-service-event-outbox"
}
