// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package provider

import (
	"context"
	"fmt"

	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/datasource/schema"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/hashicorp/terraform-plugin-log/tflog"
)

var (
	_ datasource.DataSource              = &ConnectorDataSource{}
	_ datasource.DataSourceWithConfigure = &ConnectorDataSource{}
)

func NewDebeziumPgDataSource() datasource.DataSource {
	return &ConnectorDataSource{}
}

// ConnectorDataSource looks up an existing Debezium connector already registered on Kafka Connect.
type ConnectorDataSource struct {
	client *DebeziumConnectClient
}

func (d *ConnectorDataSource) Metadata(ctx context.Context, req datasource.MetadataRequest, resp *datasource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_postgres_connector"
}

func (d *ConnectorDataSource) Schema(ctx context.Context, req datasource.SchemaRequest, resp *datasource.SchemaResponse) {
	resp.Schema = schema.Schema{
		MarkdownDescription: "Looks up an existing Debezium PostgreSQL outbox connector by name.",

		Attributes: map[string]schema.Attribute{
			"id": schema.StringAttribute{
				Computed:            true,
				MarkdownDescription: "Connector identifier (same value as `name`).",
			},
			"name": schema.StringAttribute{
				Required:            true,
				MarkdownDescription: "Name of the connector in Kafka Connect.",
			},
			"database_hostname": schema.StringAttribute{
				Computed:            true,
				MarkdownDescription: "PostgreSQL server hostname.",
			},
			"database_port": schema.Int64Attribute{
				Computed:            true,
				MarkdownDescription: "PostgreSQL server port.",
			},
			"database_user": schema.StringAttribute{
				Computed:            true,
				MarkdownDescription: "PostgreSQL replication user.",
			},
			"database_password": schema.StringAttribute{
				Computed:            true,
				Sensitive:           true,
				MarkdownDescription: "PostgreSQL replication user password.",
			},
			"database_dbname": schema.StringAttribute{
				Computed:            true,
				MarkdownDescription: "PostgreSQL database name being captured.",
			},
			"database_server_name": schema.StringAttribute{
				Computed:            true,
				MarkdownDescription: "Logical name identifying the PostgreSQL server/cluster, used as a topic prefix.",
			},
			"slot_name": schema.StringAttribute{
				Computed:            true,
				MarkdownDescription: "Name of the PostgreSQL logical replication slot.",
			},
			"slot_drop_on_stop": schema.BoolAttribute{
				Computed:            true,
				MarkdownDescription: "Whether the logical replication slot is dropped when the connector stops.",
			},
			"plugin_name": schema.StringAttribute{
				Computed:            true,
				MarkdownDescription: "PostgreSQL logical decoding plug-in.",
			},
			"table_include_list": schema.StringAttribute{
				Computed:            true,
				MarkdownDescription: "Comma-separated list of `schema.table` regexes being captured.",
			},
			"snapshot_mode": schema.StringAttribute{
				Computed:            true,
				MarkdownDescription: "Snapshotting behavior on first startup.",
			},
			"heartbeat_interval_ms": schema.Int64Attribute{
				Computed:            true,
				MarkdownDescription: "Interval in milliseconds at which the connector sends heartbeat messages.",
			},
			"message_key_columns": schema.StringAttribute{
				Computed:            true,
				MarkdownDescription: "Semicolon-separated list of `schema.table:column1,column2` expressions overriding the message key for specific tables.",
			},
			"decimal_handling_mode": schema.StringAttribute{
				Computed:            true,
				MarkdownDescription: "How `DECIMAL`/`NUMERIC` columns are represented in change events.",
			},
			"publication_autocreate_mode": schema.StringAttribute{
				Computed:            true,
				MarkdownDescription: "How the PostgreSQL publication is created when using `pgoutput`.",
			},
			"schema_history_kafka_topic": schema.StringAttribute{
				Computed:            true,
				MarkdownDescription: "Kafka topic used to store the database schema history.",
			},
			"schema_history_kafka_bootstrap_servers": schema.StringAttribute{
				Computed:            true,
				MarkdownDescription: "Comma-separated Kafka bootstrap servers used by the schema-history client.",
			},
			"outbox_route_topic_replacement": schema.StringAttribute{
				Computed:            true,
				MarkdownDescription: "Topic name replacement pattern for the outbox event router.",
			},
			"outbox_route_by_field": schema.StringAttribute{
				Computed:            true,
				MarkdownDescription: "Outbox table column used to route events to a topic.",
			},
			"outbox_table_field_event_key": schema.StringAttribute{
				Computed:            true,
				MarkdownDescription: "Outbox table column used as the outgoing record key.",
			},
			"outbox_table_field_event_timestamp": schema.StringAttribute{
				Computed:            true,
				MarkdownDescription: "Outbox table column used as the outgoing record timestamp.",
			},
			"outbox_table_fields_additional_placement": schema.StringAttribute{
				Computed:            true,
				MarkdownDescription: "Additional outbox column-to-header/envelope placement mappings.",
			},
			"max_queue_size": schema.Int64Attribute{
				Computed:            true,
				MarkdownDescription: "Maximum size of the connector's internal change event queue.",
			},
			"max_batch_size": schema.Int64Attribute{
				Computed:            true,
				MarkdownDescription: "Maximum size of a batch processed by the connector.",
			},
			"tombstones_on_delete": schema.BoolAttribute{
				Computed:            true,
				MarkdownDescription: "Whether a tombstone event is emitted after a delete event.",
			},
			"security_protocol": schema.StringAttribute{
				Computed:            true,
				MarkdownDescription: "Security protocol used by the schema-history Kafka client.",
			},
			"sasl_scram": schema.SingleNestedAttribute{
				Computed:            true,
				MarkdownDescription: "Set when the connector authenticates the schema-history Kafka client with SASL/SCRAM-SHA-512.",
				Attributes: map[string]schema.Attribute{
					"username": schema.StringAttribute{
						Computed:            true,
						MarkdownDescription: "SASL/SCRAM username.",
					},
					"password": schema.StringAttribute{
						Computed:            true,
						Sensitive:           true,
						MarkdownDescription: "SASL/SCRAM password.",
					},
				},
			},
			"aws_msk_iam": schema.BoolAttribute{
				Computed:            true,
				MarkdownDescription: "True when the connector authenticates the schema-history Kafka client using AWS MSK IAM.",
			},
			"extra_config": schema.MapAttribute{
				Computed:            true,
				ElementType:         types.StringType,
				MarkdownDescription: "Always empty for looked-up connectors: there is no way to tell which config keys, if any, were originally set through a resource's `extra_config` escape hatch.",
			},
		},
	}
}

func (d *ConnectorDataSource) Configure(ctx context.Context, req datasource.ConfigureRequest, resp *datasource.ConfigureResponse) {
	if req.ProviderData == nil {
		return
	}

	client, ok := req.ProviderData.(*DebeziumConnectClient)
	if !ok {
		resp.Diagnostics.AddError(
			"Unexpected Data Source Configure Type",
			fmt.Sprintf("Expected *DebeziumConnectClient, got: %T. Please report this issue to the provider developers.", req.ProviderData),
		)
		return
	}

	d.client = client
	tflog.Info(ctx, "Configured Debezium connector data source client")
}

func (d *ConnectorDataSource) Read(ctx context.Context, req datasource.ReadRequest, resp *datasource.ReadResponse) {
	var config ConnectorModel
	resp.Diagnostics.Append(req.Config.Get(ctx, &config)...)
	if resp.Diagnostics.HasError() {
		return
	}

	name := config.Name.ValueString()
	ctx = tflog.SetField(ctx, "connector_name", name)
	ctx = tflog.MaskFieldValuesWithFieldKeys(ctx, "database_password", "password")
	tflog.Debug(ctx, "Looking up Debezium connector")

	cfg, found, err := d.client.GetConnectorConfig(ctx, name)
	if err != nil {
		resp.Diagnostics.AddError("Error reading connector", err.Error())
		return
	}
	if !found {
		tflog.Warn(ctx, "Debezium connector not found")
		resp.Diagnostics.AddError(
			"Connector not found",
			fmt.Sprintf("No connector named %q was found on the Kafka Connect cluster.", name),
		)
		return
	}

	data, err := parseConnectorConfig(cfg)
	if err != nil {
		resp.Diagnostics.AddError("Error parsing connector config", err.Error())
		return
	}
	data.Id = data.Name

	tflog.Debug(ctx, "Found Debezium connector")
	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
}
