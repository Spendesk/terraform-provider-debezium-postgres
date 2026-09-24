# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A Terraform provider (local name `debezium`, published as `spendesk/debezium-postgres`) built on
the Terraform Plugin Framework whose sole purpose is
managing **Debezium 1.9 PostgreSQL outbox connectors** on a **Kafka Connect** cluster, via Kafka
Connect's REST API (`PUT/GET/DELETE /connectors/{name}/config`) — not the Debezium or Kafka wire
protocols themselves. It exists so Postgres connectors used for the event-outbox pattern can be
declared as Terraform code instead of being created as a side effect of another process
(INFRA-2838). It supports exactly the two authentication modes those connectors actually use for
the schema-history Kafka client: SASL/SCRAM and AWS MSK IAM — not the full Debezium/Kafka Connect
config surface (~121 fields across `CommonConnectorConfig`/`RelationalDatabaseConnectorConfig`/
`PostgresConnectorConfig`/the outbox `EventRouter` transform/`KafkaDatabaseHistory`, verified
against the actual 1.9.0-SNAPSHOT Debezium source). The ~15 fields people actually tune are
first-class attributes; everything else is reachable through `extra_config` (see below) rather
than as speculative dedicated attributes.

It started from HashiCorp's scaffolding template — all of that boilerplate (example
resource/data source/function/ephemeral resource, and their docs/examples/tests) has been deleted;
what remains is a single resource (`debezium_postgres_connector`) and matching data source
(`debezium_postgres_connector`). The resource/data-source names deliberately don't carry the
`spendesk` name — the provider's registry *namespace* is `spendesk` (who publishes it), but the
resource itself is just a Debezium Postgres connector. The provider's local/type name
(`Metadata.TypeName` in `provider.go`) is `debezium`, not `debezium_postgres`: Terraform provider
local names may only contain letters, digits, and dashes (no underscores — `provider
"debezium_postgres" {}` is a hard `terraform init` error), so the resource/data source
`Metadata.TypeName` is set explicitly to `req.ProviderTypeName + "_postgres_connector"` rather than
derived generically as `+ "_connector"`. Terraform's implied-provider matching only requires the
local name to be a prefix of the resource type up to the first underscore (same pattern as
`docker` → `docker_container`, `confluent` → `confluent_kafka_topic`), so `debezium` +
`_postgres_connector` resolves automatically with no explicit `provider = ` argument needed.

Built and reviewed against
[HashiCorp's Plugin Framework provider lab](https://developer.hashicorp.com/terraform/tutorials/community-providers/providers-plugin-framework-lab):
file-per-resource layout, `Optional`/`Required`/`Sensitive`/`UseStateForUnknown()` schema
attributes, the standard `Configure` nil-check-and-type-assert pattern, environment variable
fallback for provider config (`endpoint` / `DEBEZIUM_CONNECT_ENDPOINT`, explicit attribute always
wins — see `resolveEndpoint` in `provider.go`), and `tflog`-based logging (see below). It goes
beyond that lab's scope in a few places: `Read` reconciles state from the live API instead of
trusting prior state, there's a real acceptance-test suite (not just unit tests), and docs are
generated via `tfplugindocs` rather than hand-written.

## Commands

```shell
go install -v ./...                          # build + install
make build                                    # go build ./...
make fmt                                      # gofmt -s -w -e .
make lint                                     # golangci-lint run
make test                                     # go test -v -cover -timeout=120s -parallel=10 ./...
go test -v ./internal/provider/... -run TestBuildConnectorConfig   # single test
make generate                                 # regenerate docs/ from schema + examples/ (runs tfplugindocs via tools/)
```

Acceptance tests (`TestAccConnectorResource`) exercise the real Kafka Connect REST API and are
skipped automatically if it isn't reachable:

```shell
docker compose -f docker/docker-compose.yml up -d   # local Kafka + Zookeeper + Debezium Connect + PostgreSQL
make testacc                                        # TF_ACC=1 go test ... (creates/updates/imports/deletes a real connector)
```

`DEBEZIUM_CONNECT_ENDPOINT` overrides the default `http://localhost:8083` used by acceptance tests.

## Architecture

- `internal/provider/provider.go` — provider `debezium`; its only config is `endpoint` (Kafka
  Connect REST API base URL), resolvable from the `DEBEZIUM_CONNECT_ENDPOINT` environment variable
  when the attribute is omitted (`resolveEndpoint`; explicit attribute takes precedence). `Configure`
  builds one `*DebeziumConnectClient` shared by the resource and data source via
  `resp.ResourceData`/`resp.DataSourceData`.
- `internal/provider/client.go` — `DebeziumConnectClient`, a thin HTTP client over the Kafka
  Connect REST API (`PUT/GET/DELETE /connectors/{name}/config` and `/connectors/{name}`). Logs
  method/URL/status at `Debug` via `tflog` for every call; the connector config body itself is
  never logged since it can carry credentials.
- `internal/provider/connector_resource.go` — the `debezium_postgres_connector` resource. This is where
  almost all the domain logic lives:
  - `buildConnectorConfig` renders the Terraform model into the flat `map[string]string` Kafka
    Connect expects (dotted keys like `database.hostname`, `transforms.outbox.*`,
    `schema.history.internal.*`). Both the legacy `database.history.*` and current
    `schema.history.internal.*` topic/bootstrap-servers keys are set, matching connectors already
    running in production. Verified against the actual 1.9.0-SNAPSHOT source
    (`DatabaseHistory.java`/`KafkaDatabaseHistory.java`): Debezium 1.9 only recognizes the
    `database.history.*` prefix — `schema.history.internal.*` is a later rename and is a no-op on
    1.9 — so `database.history.store.only.captured.tables.ddl` (not just the
    `schema.history.internal.*` sibling) is set to limit what gets written to the history topic.
  - `parseConnectorConfig` is the inverse: reconstructs the full model from the API's config
    response, so `Read`/`Import` detect drift and don't just trust prior state.
  - `extra_config` is a passthrough map merged in *last* (can override any typed attribute) and is
    explicitly **not** reconstructed on Read/Import — there's no way to know which keys were meant
    to be managed through it, so it's left as-is from state (or null on import).
  - Auth for the schema-history Kafka client is exactly one of `sasl_scram` or `aws_msk_iam = true`,
    enforced in `ValidateConfig`. SASL/SCRAM credentials round-trip through a JAAS config string
    (`scramJaasConfig`/`parseScramJaasConfig`); AWS MSK IAM has no reversible secret so it's just a
    boolean flag.
  - `name` has `RequiresReplace`; changing it means a new connector.
- `internal/provider/connector_data_source.go` — read-only lookup of an existing connector by
  `name`; shares the same schema shape and reuses `parseConnectorConfig`. Always returns
  `extra_config` as empty for the same reason described above.
- Every enum-like attribute (`snapshot_mode`, `decimal_handling_mode`,
  `publication_autocreate_mode`, `security_protocol`) is validated against a `valid*` allow-list in
  `ValidateConfig` rather than at the schema level, so defaults/unknowns don't get rejected.
- Logging follows [HashiCorp's `tflog` guidance](https://developer.hashicorp.com/terraform/tutorials/providers-plugin-framework/providers-plugin-framework-logging):
  `Info` for lifecycle events (Create/Update/Delete/Import, provider/resource/data-source
  `Configure`), `Debug` for Read and every Kafka Connect API call, `Warn` when a connector's gone
  missing. `tflog.SetField` attaches `connector_name`/`debezium_connect_endpoint`/
  `kafka_connect_method`/`kafka_connect_url`/`status_code`. `withConnectorLogContext` in
  `connector_resource.go` also calls `tflog.MaskFieldValuesWithFieldKeys(ctx, "database_password",
  "password")` defensively, even though nothing currently logs the config body those keys live in.
  Verified live: `TF_LOG=DEBUG TF_LOG_PATH=... go test -run TestAccConnectorResource` against the
  docker-compose stack, then grepped the log for `password` to confirm nothing leaked.

## Tests

- `connector_resource_test.go` — unit tests for `buildConnectorConfig`/`parseConnectorConfig`
  (both directions, both auth modes, `extra_config` override behavior). Fixtures mirror the real
  connector configs under `.ai-docs/connectors-examples/`.
- `connector_acc_test.go` — the acceptance test; covers create, import (with `ImportStateVerify`,
  ignoring `extra_config`), update, a data source lookup, and delete (`CheckDestroy` asserts
  against the live API, not just Terraform state).
- `provider_test.go` — `testAccProtoV6ProviderFactories` (shared by acceptance tests) and
  `TestResolveEndpoint` (the `endpoint`/`DEBEZIUM_CONNECT_ENDPOINT` precedence logic).

Every acceptance-tested change in this repo has actually been run against a live stack
(`docker compose -f docker/docker-compose.yml up -d`, `TF_ACC=1 make testacc`, confirm
`curl localhost:8083/connectors` is empty after, then `docker compose down`) rather than left as
"should work" — do the same before calling provider-level work done.

## Docs and examples

`docs/` and `examples/` are generated/consumed by `tfplugindocs` (`make generate`) — hand-edit
`examples/resources/debezium_postgres_connector/resource.tf` and schema `MarkdownDescription`s, not
`docs/resources/connector.md` directly.
