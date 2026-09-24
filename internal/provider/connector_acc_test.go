// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package provider

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/hashicorp/terraform-plugin-testing/knownvalue"
	"github.com/hashicorp/terraform-plugin-testing/statecheck"
	"github.com/hashicorp/terraform-plugin-testing/terraform"
	"github.com/hashicorp/terraform-plugin-testing/tfjsonpath"
)

// testAccConnectEndpoint returns the Kafka Connect REST API base URL used by acceptance tests.
// Run `docker compose -f docker/docker-compose.yml up -d` to stand up a local Kafka Connect +
// Debezium + PostgreSQL stack that satisfies this, then run `make testacc`.
func testAccConnectEndpoint() string {
	if v := os.Getenv("DEBEZIUM_CONNECT_ENDPOINT"); v != "" {
		return v
	}
	return "http://localhost:8083"
}

// testAccConnectPreCheck skips the test when no Kafka Connect REST API is reachable, so `make
// testacc` stays green in environments (like CI) where the docker-compose stack isn't running.
func testAccConnectPreCheck(t *testing.T) {
	t.Helper()

	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(testAccConnectEndpoint() + "/connectors")
	if err != nil {
		t.Skipf("Kafka Connect REST API not reachable at %s: %s (run `docker compose -f docker/docker-compose.yml up -d` to enable this test)", testAccConnectEndpoint(), err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Skipf("Kafka Connect REST API at %s returned status %d", testAccConnectEndpoint(), resp.StatusCode)
	}
}

// testAccCheckConnectorDestroy confirms, directly against the Kafka Connect REST API, that every
// debezium_postgres_connector in the test's final state was actually deleted, per INFRA-2838's acceptance
// criterion that tests cover connector delete, not just create.
func testAccCheckConnectorDestroy(s *terraform.State) error {
	client := &DebeziumConnectClient{Endpoint: testAccConnectEndpoint(), HTTPClient: &http.Client{Timeout: 5 * time.Second}}

	for _, rs := range s.RootModule().Resources {
		if rs.Type != "debezium_postgres_connector" {
			continue
		}

		_, found, err := client.GetConnectorConfig(context.Background(), rs.Primary.ID)
		if err != nil {
			return fmt.Errorf("checking connector %q was destroyed: %w", rs.Primary.ID, err)
		}
		if found {
			return fmt.Errorf("connector %q still exists on Kafka Connect after destroy", rs.Primary.ID)
		}
	}

	return nil
}

func TestAccConnectorResource(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccConnectPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		CheckDestroy:             testAccCheckConnectorDestroy,
		Steps: []resource.TestStep{
			// Create and Read testing.
			{
				Config: testAccConnectorResourceConfig("public.outbox_events"),
				ConfigStateChecks: []statecheck.StateCheck{
					statecheck.ExpectKnownValue(
						"debezium_postgres_connector.test",
						tfjsonpath.New("id"),
						knownvalue.StringExact("tf-acc-test-connector"),
					),
					statecheck.ExpectKnownValue(
						"debezium_postgres_connector.test",
						tfjsonpath.New("table_include_list"),
						knownvalue.StringExact("public.outbox_events"),
					),
					statecheck.ExpectKnownValue(
						"debezium_postgres_connector.test",
						tfjsonpath.New("plugin_name"),
						knownvalue.StringExact("pgoutput"),
					),
					statecheck.ExpectKnownValue(
						"debezium_postgres_connector.test",
						tfjsonpath.New("aws_msk_iam"),
						knownvalue.Bool(false),
					),
					statecheck.ExpectKnownValue(
						"debezium_postgres_connector.test",
						tfjsonpath.New("snapshot_mode"),
						knownvalue.StringExact("never"),
					),
					statecheck.ExpectKnownValue(
						"debezium_postgres_connector.test",
						tfjsonpath.New("message_key_columns"),
						knownvalue.StringExact("public.outbox_events:id"),
					),
					statecheck.ExpectKnownValue(
						"debezium_postgres_connector.test",
						tfjsonpath.New("extra_config"),
						knownvalue.MapExact(map[string]knownvalue.Check{
							"slot.drop.on.stop": knownvalue.StringExact("false"),
						}),
					),
				},
			},
			// ImportState testing: Read must reconstruct every attribute from the connector's
			// live config, so a plain passthrough import should match the prior state exactly.
			// extra_config is the one documented exception - it isn't reversible, so it's left
			// null on import rather than reconstructed.
			{
				ResourceName:            "debezium_postgres_connector.test",
				ImportState:             true,
				ImportStateVerify:       true,
				ImportStateVerifyIgnore: []string{"extra_config"},
			},
			// Update and Read testing, plus a data source lookup of the same connector.
			{
				Config: testAccConnectorResourceConfig("public.other_events") + testAccConnectorDataSourceConfig(),
				ConfigStateChecks: []statecheck.StateCheck{
					statecheck.ExpectKnownValue(
						"debezium_postgres_connector.test",
						tfjsonpath.New("table_include_list"),
						knownvalue.StringExact("public.other_events"),
					),
					statecheck.ExpectKnownValue(
						"data.debezium_postgres_connector.test",
						tfjsonpath.New("table_include_list"),
						knownvalue.StringExact("public.other_events"),
					),
					statecheck.ExpectKnownValue(
						"data.debezium_postgres_connector.test",
						tfjsonpath.New("sasl_scram").AtMapKey("username"),
						knownvalue.StringExact("debezium"),
					),
				},
			},
			// Delete testing automatically occurs in TestCase.
		},
	})
}

func testAccConnectorResourceConfig(tableIncludeList string) string {
	return fmt.Sprintf(`
provider "debezium" {
  endpoint = %[1]q
}

resource "debezium_postgres_connector" "test" {
  name                  = "tf-acc-test-connector"
  database_hostname     = "dbzui-db-pg"
  database_user         = "postgres"
  database_password     = "postgres"
  database_dbname       = "postgres"
  database_server_name  = "tf-acc-test"
  slot_name              = "tf_acc_test_slot"
  table_include_list    = %[2]q

  schema_history_kafka_topic             = "tf-acc-test-dbhistory"
  schema_history_kafka_bootstrap_servers = "dbzui-kafka:9092"
  security_protocol                      = "PLAINTEXT"

  outbox_route_topic_replacement = "tf-acc-test.outbox.event.$${routedByValue}"

  snapshot_mode       = "never"
  message_key_columns = "public.outbox_events:id"
  extra_config = {
    "slot.drop.on.stop" = "false"
  }

  sasl_scram = {
    username = "debezium"
    password = "debezium"
  }
}
`, testAccConnectEndpoint(), tableIncludeList)
}

func testAccConnectorDataSourceConfig() string {
	return `
data "debezium_postgres_connector" "test" {
  name = debezium_postgres_connector.test.name
}
`
}
