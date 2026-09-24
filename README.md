# terraform-provider-debezium-postgres

A Terraform provider, built on the [Terraform Plugin Framework](https://github.com/hashicorp/terraform-plugin-framework),
for managing Debezium PostgreSQL outbox connectors on a Kafka Connect cluster (Debezium 1.9).

It exists so that Postgres connectors used for the event-outbox pattern can be declared as code
against the Kafka Connect REST API, instead of being created as a side effect of another process.
It supports the two authentication modes used for the schema-history Kafka client: SASL/SCRAM and
AWS MSK IAM.

## Provider and resources

- Provider: `debezium` (published as `spendesk/debezium-postgres`) — configured with a single
  `endpoint` (the Kafka Connect REST API base URL),
  which can also be set via the `DEBEZIUM_CONNECT_ENDPOINT` environment variable.
- Resource: `debezium_postgres_connector` — full create/read/update/delete/import of a connector.
- Data source: `debezium_postgres_connector` — read-only lookup of an existing connector by name.

See [`docs/`](docs/) for the full schema reference and [`examples/resources/debezium_postgres_connector/resource.tf`](examples/resources/debezium_postgres_connector/resource.tf)
for working examples of both authentication modes.

## Requirements

- [Terraform](https://developer.hashicorp.com/terraform/downloads) >= 1.0
- [Go](https://golang.org/doc/install) >= 1.22
- [Docker](https://docs.docker.com/get-docker/) (only for acceptance tests)

## Building

```shell
go install -v ./...
```

## Developing

```shell
make fmt      # gofmt -s -w -e .
make lint     # golangci-lint run
make test     # unit tests: go test -v -cover -timeout=120s -parallel=10 ./...
make generate # regenerate docs/ from schema + examples/ (runs tfplugindocs)
```

Acceptance tests exercise a real Kafka Connect REST API and skip automatically if none is
reachable:

```shell
docker compose -f docker/docker-compose.yml up -d   # local Kafka + Zookeeper + Debezium Connect + PostgreSQL
make testacc                                        # TF_ACC=1 go test ...
docker compose -f docker/docker-compose.yml down
```

`DEBEZIUM_CONNECT_ENDPOINT` overrides the default `http://localhost:8083` used by acceptance tests.

See [`CLAUDE.md`](CLAUDE.md) for a deeper architecture walkthrough.
