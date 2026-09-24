// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package provider

import (
	"reflect"
	"testing"

	"github.com/hashicorp/terraform-plugin-framework/attr"
	"github.com/hashicorp/terraform-plugin-framework/types"
)

// TestBuildConnectorConfig checks that both authentication modes render the Kafka Connect config
// keys observed in the real connectors under .ai-docs/connectors-examples (anonymized here).
func TestBuildConnectorConfig(t *testing.T) {
	base := ConnectorModel{
		Name:                                 types.StringValue("example-service-event-outbox"),
		DatabaseHostname:                     types.StringValue("postgres"),
		DatabasePort:                         types.Int64Value(5432),
		DatabaseUser:                         types.StringValue("debezium_example_ro"),
		DatabasePassword:                     types.StringValue("s3cr3t"),
		DatabaseDbname:                       types.StringValue("example_db"),
		DatabaseServerName:                   types.StringValue("example-service"),
		SlotName:                             types.StringValue("example_service_debezium"),
		SlotDropOnStop:                       types.BoolValue(false),
		PluginName:                           types.StringValue("pgoutput"),
		TableIncludeList:                     types.StringValue("public.outbox_events"),
		SnapshotMode:                         types.StringValue("initial"),
		HeartbeatIntervalMs:                  types.Int64Value(0),
		MessageKeyColumns:                    types.StringNull(),
		DecimalHandlingMode:                  types.StringValue("precise"),
		PublicationAutocreateMode:            types.StringValue("all_tables"),
		SchemaHistoryKafkaTopic:              types.StringValue("example-service-dbhistory"),
		SchemaHistoryKafkaBootstrapServers:   types.StringValue("b-1:9096,b-2:9096,b-3:9096"),
		OutboxRouteTopicReplacement:          types.StringValue("example.outbox.event.${routedByValue}"),
		OutboxRouteByField:                   types.StringValue("queue_scope_type"),
		OutboxTableFieldEventKey:             types.StringValue("queue_scope_id"),
		OutboxTableFieldEventTimestamp:       types.StringValue("created_at"),
		OutboxTableFieldsAdditionalPlacement: types.StringValue("event:header:event,created_at:header:created_at,serial:header:serial,database_name:header:database_name"),
		MaxQueueSize:                         types.Int64Value(131072),
		MaxBatchSize:                         types.Int64Value(32768),
		TombstonesOnDelete:                   types.BoolValue(false),
		SecurityProtocol:                     types.StringValue("PLAINTEXT"),
		ExtraConfig:                          types.MapNull(types.StringType),
	}

	t.Run("sasl_scram", func(t *testing.T) {
		data := base
		data.SaslScram = &SaslScramModel{
			Username: types.StringValue("myuser"),
			Password: types.StringValue("mypass"),
		}
		data.AwsMskIam = types.BoolValue(false)

		cfg := buildConnectorConfig(data)

		want := map[string]string{
			"connector.class": connectorClassPostgres,
			"database.history.consumer.security.protocol":     "PLAINTEXT",
			"database.history.producer.security.protocol":     "PLAINTEXT",
			"database.history.consumer.sasl.mechanism":        saslMechanismScram,
			"database.history.producer.sasl.mechanism":        saslMechanismScram,
			"database.history.kafka.topic":                    "example-service-dbhistory",
			"schema.history.internal.kafka.topic":             "example-service-dbhistory",
			"database.history.store.only.captured.tables.ddl": "true",
			"transforms": "outbox",
			"transforms.outbox.route.topic.replacement": "example.outbox.event.${routedByValue}",
			"snapshot.mode":               "initial",
			"heartbeat.interval.ms":       "0",
			"decimal.handling.mode":       "precise",
			"publication.autocreate.mode": "all_tables",
			"slot.drop.on.stop":           "false",
		}
		for k, v := range want {
			if cfg[k] != v {
				t.Errorf("cfg[%q] = %q, want %q", k, cfg[k], v)
			}
		}
		if _, ok := cfg["message.key.columns"]; ok {
			t.Error("did not expect message.key.columns to be set when message_key_columns is null")
		}

		wantJaas := `org.apache.kafka.common.security.scram.ScramLoginModule required username="myuser" password="mypass";`
		if got := cfg["database.history.consumer.sasl.jaas.config"]; got != wantJaas {
			t.Errorf("consumer sasl.jaas.config = %q, want %q", got, wantJaas)
		}
		if got := cfg["database.history.producer.sasl.jaas.config"]; got != wantJaas {
			t.Errorf("producer sasl.jaas.config = %q, want %q", got, wantJaas)
		}
		if _, ok := cfg["database.history.consumer.sasl.client.callback.handler.class"]; ok {
			t.Error("did not expect an AWS MSK IAM callback handler class for sasl_scram auth")
		}
	})

	t.Run("message_key_columns and extra_config override", func(t *testing.T) {
		data := base
		data.SaslScram = &SaslScramModel{Username: types.StringValue("myuser"), Password: types.StringValue("mypass")}
		data.MessageKeyColumns = types.StringValue("public.outbox_events:id")
		data.ExtraConfig = types.MapValueMust(types.StringType, map[string]attr.Value{
			"snapshot.mode":         types.StringValue("never"),
			"topic.creation.enable": types.StringValue("true"),
		})

		cfg := buildConnectorConfig(data)

		if got := cfg["message.key.columns"]; got != "public.outbox_events:id" {
			t.Errorf("message.key.columns = %q, want %q", got, "public.outbox_events:id")
		}
		if got := cfg["snapshot.mode"]; got != "never" {
			t.Errorf("extra_config should override typed attributes: snapshot.mode = %q, want %q", got, "never")
		}
		if got := cfg["topic.creation.enable"]; got != "true" {
			t.Errorf("topic.creation.enable = %q, want %q", got, "true")
		}
	})

	t.Run("aws_msk_iam", func(t *testing.T) {
		data := base
		data.SaslScram = nil
		data.AwsMskIam = types.BoolValue(true)
		data.SecurityProtocol = types.StringValue("SASL_SSL")

		cfg := buildConnectorConfig(data)

		want := map[string]string{
			"database.history.consumer.security.protocol":                  "SASL_SSL",
			"database.history.producer.security.protocol":                  "SASL_SSL",
			"database.history.consumer.sasl.mechanism":                     saslMechanismAwsMskIam,
			"database.history.producer.sasl.mechanism":                     saslMechanismAwsMskIam,
			"database.history.consumer.sasl.jaas.config":                   awsMskIamJaasConfig,
			"database.history.producer.sasl.jaas.config":                   awsMskIamJaasConfig,
			"database.history.consumer.sasl.client.callback.handler.class": awsMskIamCallbackHandlerClass,
			"database.history.producer.sasl.client.callback.handler.class": awsMskIamCallbackHandlerClass,
			"database.history.sasl.client.callback.handler.class":          awsMskIamCallbackHandlerClass,
		}
		for k, v := range want {
			if cfg[k] != v {
				t.Errorf("cfg[%q] = %q, want %q", k, cfg[k], v)
			}
		}
	})
}

// TestConnectorConfigRoundTrip checks that parseConnectorConfig fully reverses buildConnectorConfig,
// which is what makes Read/Import able to reconstruct state from Kafka Connect's response.
func TestConnectorConfigRoundTrip(t *testing.T) {
	base := ConnectorModel{
		Name:                                 types.StringValue("example-service-event-outbox"),
		DatabaseHostname:                     types.StringValue("postgres"),
		DatabasePort:                         types.Int64Value(5432),
		DatabaseUser:                         types.StringValue("debezium_example_ro"),
		DatabasePassword:                     types.StringValue(`s3cr3t"with"quotes`),
		DatabaseDbname:                       types.StringValue("example_db"),
		DatabaseServerName:                   types.StringValue("example-service"),
		SlotName:                             types.StringValue("example_service_debezium"),
		SlotDropOnStop:                       types.BoolValue(true),
		PluginName:                           types.StringValue("pgoutput"),
		TableIncludeList:                     types.StringValue("public.outbox_events"),
		SnapshotMode:                         types.StringValue("always"),
		HeartbeatIntervalMs:                  types.Int64Value(5000),
		MessageKeyColumns:                    types.StringValue("public.outbox_events:id"),
		DecimalHandlingMode:                  types.StringValue("string"),
		PublicationAutocreateMode:            types.StringValue("filtered"),
		SchemaHistoryKafkaTopic:              types.StringValue("example-service-dbhistory"),
		SchemaHistoryKafkaBootstrapServers:   types.StringValue("b-1:9096,b-2:9096,b-3:9096"),
		OutboxRouteTopicReplacement:          types.StringValue("example.outbox.event.${routedByValue}"),
		OutboxRouteByField:                   types.StringValue("queue_scope_type"),
		OutboxTableFieldEventKey:             types.StringValue("queue_scope_id"),
		OutboxTableFieldEventTimestamp:       types.StringValue("created_at"),
		OutboxTableFieldsAdditionalPlacement: types.StringValue("event:header:event,created_at:header:created_at,serial:header:serial,database_name:header:database_name"),
		MaxQueueSize:                         types.Int64Value(131072),
		MaxBatchSize:                         types.Int64Value(32768),
		TombstonesOnDelete:                   types.BoolValue(true),
		SecurityProtocol:                     types.StringValue("PLAINTEXT"),
		// extra_config never round-trips through Read (see TestBuildConnectorConfig for its
		// write-side merge behavior), so parseConnectorConfig always yields a null map here.
		ExtraConfig: types.MapNull(types.StringType),
	}

	t.Run("sasl_scram", func(t *testing.T) {
		want := base
		want.SaslScram = &SaslScramModel{
			Username: types.StringValue(`my"user`),
			Password: types.StringValue(`s3cr3t"with"quotes`),
		}
		want.AwsMskIam = types.BoolValue(false)

		got, err := parseConnectorConfig(buildConnectorConfig(want))
		if err != nil {
			t.Fatalf("parseConnectorConfig() error = %v", err)
		}

		if !reflect.DeepEqual(got, want) {
			t.Errorf("round trip mismatch:\n got  = %+v\n want = %+v", got, want)
		}
	})

	t.Run("aws_msk_iam", func(t *testing.T) {
		want := base
		want.SaslScram = nil
		want.AwsMskIam = types.BoolValue(true)
		want.SecurityProtocol = types.StringValue("SASL_SSL")

		got, err := parseConnectorConfig(buildConnectorConfig(want))
		if err != nil {
			t.Fatalf("parseConnectorConfig() error = %v", err)
		}

		if !reflect.DeepEqual(got, want) {
			t.Errorf("round trip mismatch:\n got  = %+v\n want = %+v", got, want)
		}
	})
}
