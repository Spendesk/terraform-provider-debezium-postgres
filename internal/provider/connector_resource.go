// Copyright (c) HashiCorp, Inc.

// SPDX-License-Identifier: MPL-2.0

package provider

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/booldefault"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/int64default"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringdefault"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/hashicorp/terraform-plugin-log/tflog"
)

// Debezium PostgreSQL connector config values that never vary for this provider's outbox-pattern
// connectors, so they aren't exposed as attributes.
const (
	connectorClassPostgres = "io.debezium.connector.postgresql.PostgresConnector"
	outboxTransformType    = "io.debezium.transforms.outbox.EventRouter"

	saslMechanismScram            = "SCRAM-SHA-512"
	saslMechanismAwsMskIam        = "AWS_MSK_IAM"
	awsMskIamJaasConfig           = "software.amazon.msk.auth.iam.IAMLoginModule required;"
	awsMskIamCallbackHandlerClass = "software.amazon.msk.auth.iam.IAMClientCallbackHandler"
)

// Allowed values for the enum-like Debezium fields exposed as attributes, per
// io.debezium.connector.postgresql.PostgresConnectorConfig.SnapshotMode/AutoCreateMode and
// io.debezium.relational.RelationalDatabaseConnectorConfig.DecimalHandlingMode (Debezium 1.9).
var (
	validSnapshotModes              = []string{"always", "initial", "never", "initial_only", "exported", "custom"}
	validDecimalHandlingModes       = []string{"precise", "double", "string"}
	validPublicationAutocreateModes = []string{"all_tables", "disabled", "filtered"}
	validSecurityProtocols          = []string{"PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"}
)

var (
	_ resource.Resource                   = &ConnectorResource{}
	_ resource.ResourceWithConfigure      = &ConnectorResource{}
	_ resource.ResourceWithValidateConfig = &ConnectorResource{}
	_ resource.ResourceWithImportState    = &ConnectorResource{}
)

func NewDebeziumPgConnector() resource.Resource {
	return &ConnectorResource{}
}

// ConnectorResource manages a Debezium PostgreSQL connector deployed to a Kafka Connect cluster.
type ConnectorResource struct {
	client *DebeziumConnectClient
}

// ConnectorModel describes the debezium connector resource data model.
type ConnectorModel struct {
	Id types.String `tfsdk:"id"`

	Name               types.String `tfsdk:"name"`
	DatabaseHostname   types.String `tfsdk:"database_hostname"`
	DatabasePort       types.Int64  `tfsdk:"database_port"`
	DatabaseUser       types.String `tfsdk:"database_user"`
	DatabasePassword   types.String `tfsdk:"database_password"`
	DatabaseDbname     types.String `tfsdk:"database_dbname"`
	DatabaseServerName types.String `tfsdk:"database_server_name"`
	SlotName           types.String `tfsdk:"slot_name"`
	SlotDropOnStop     types.Bool   `tfsdk:"slot_drop_on_stop"`
	PluginName         types.String `tfsdk:"plugin_name"`
	TableIncludeList   types.String `tfsdk:"table_include_list"`

	SnapshotMode              types.String `tfsdk:"snapshot_mode"`
	HeartbeatIntervalMs       types.Int64  `tfsdk:"heartbeat_interval_ms"`
	MessageKeyColumns         types.String `tfsdk:"message_key_columns"`
	DecimalHandlingMode       types.String `tfsdk:"decimal_handling_mode"`
	PublicationAutocreateMode types.String `tfsdk:"publication_autocreate_mode"`

	SchemaHistoryKafkaTopic            types.String `tfsdk:"schema_history_kafka_topic"`
	SchemaHistoryKafkaBootstrapServers types.String `tfsdk:"schema_history_kafka_bootstrap_servers"`

	OutboxRouteTopicReplacement          types.String `tfsdk:"outbox_route_topic_replacement"`
	OutboxRouteByField                   types.String `tfsdk:"outbox_route_by_field"`
	OutboxTableFieldEventKey             types.String `tfsdk:"outbox_table_field_event_key"`
	OutboxTableFieldEventTimestamp       types.String `tfsdk:"outbox_table_field_event_timestamp"`
	OutboxTableFieldsAdditionalPlacement types.String `tfsdk:"outbox_table_fields_additional_placement"`

	MaxQueueSize       types.Int64  `tfsdk:"max_queue_size"`
	MaxBatchSize       types.Int64  `tfsdk:"max_batch_size"`
	TombstonesOnDelete types.Bool   `tfsdk:"tombstones_on_delete"`
	SecurityProtocol   types.String `tfsdk:"security_protocol"`

	SaslScram *SaslScramModel `tfsdk:"sasl_scram"`
	AwsMskIam types.Bool      `tfsdk:"aws_msk_iam"`

	// ExtraConfig is a passthrough escape hatch for any Kafka Connect/Debezium config key not
	// otherwise exposed as an attribute. It is merged in last (so it can override any other
	// attribute) and, since it isn't reversible, is left untouched by Read/Import rather than
	// reconstructed from the API response.
	ExtraConfig types.Map `tfsdk:"extra_config"`
}

// SaslScramModel describes the sasl_scram authentication block.
type SaslScramModel struct {
	Username types.String `tfsdk:"username"`
	Password types.String `tfsdk:"password"`
}

func (r *ConnectorResource) Metadata(ctx context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_postgres_connector"
}

func (r *ConnectorResource) Schema(ctx context.Context, req resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		MarkdownDescription: "Manages a Debezium PostgreSQL outbox connector on a Kafka Connect cluster. " +
			"Authenticate the schema-history Kafka client with either `sasl_scram` or `aws_msk_iam` (exactly one is required).",

		Attributes: map[string]schema.Attribute{
			"id": schema.StringAttribute{
				Computed:            true,
				MarkdownDescription: "Connector identifier (same value as `name`).",
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.UseStateForUnknown(),
				},
			},
			"name": schema.StringAttribute{
				Required:            true,
				MarkdownDescription: "Name of the connector in Kafka Connect.",
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"database_hostname": schema.StringAttribute{
				Required:            true,
				MarkdownDescription: "PostgreSQL server hostname.",
			},
			"database_port": schema.Int64Attribute{
				Optional:            true,
				Computed:            true,
				Default:             int64default.StaticInt64(5432),
				MarkdownDescription: "PostgreSQL server port. Defaults to `5432`.",
			},
			"database_user": schema.StringAttribute{
				Required:            true,
				MarkdownDescription: "PostgreSQL replication user.",
			},
			"database_password": schema.StringAttribute{
				Required:            true,
				Sensitive:           true,
				MarkdownDescription: "PostgreSQL replication user password.",
			},
			"database_dbname": schema.StringAttribute{
				Required:            true,
				MarkdownDescription: "PostgreSQL database name to capture.",
			},
			"database_server_name": schema.StringAttribute{
				Required:            true,
				MarkdownDescription: "Logical name identifying the PostgreSQL server/cluster, used as a topic prefix.",
			},
			"slot_name": schema.StringAttribute{
				Required:            true,
				MarkdownDescription: "Name of the PostgreSQL logical replication slot.",
			},
			"slot_drop_on_stop": schema.BoolAttribute{
				Optional:            true,
				Computed:            true,
				Default:             booldefault.StaticBool(false),
				MarkdownDescription: "Whether to drop the logical replication slot when the connector stops. Defaults to `false`.",
			},
			"plugin_name": schema.StringAttribute{
				Optional:            true,
				Computed:            true,
				Default:             stringdefault.StaticString("pgoutput"),
				MarkdownDescription: "PostgreSQL logical decoding plug-in. Defaults to `pgoutput`.",
			},
			"table_include_list": schema.StringAttribute{
				Required:            true,
				MarkdownDescription: "Comma-separated list of `schema.table` regexes to capture, e.g. `public.outbox_events`.",
			},
			"snapshot_mode": schema.StringAttribute{
				Optional:            true,
				Computed:            true,
				Default:             stringdefault.StaticString("initial"),
				MarkdownDescription: "Snapshotting behavior on first startup. One of " + oneOfMarkdown(validSnapshotModes) + ". Defaults to `initial`.",
			},
			"heartbeat_interval_ms": schema.Int64Attribute{
				Optional:            true,
				Computed:            true,
				Default:             int64default.StaticInt64(0),
				MarkdownDescription: "Interval in milliseconds at which the connector sends heartbeat messages. `0` disables heartbeats. Defaults to `0`.",
			},
			"message_key_columns": schema.StringAttribute{
				Optional:            true,
				MarkdownDescription: "Semicolon-separated list of `schema.table:column1,column2` expressions overriding the message key for specific tables.",
			},
			"decimal_handling_mode": schema.StringAttribute{
				Optional:            true,
				Computed:            true,
				Default:             stringdefault.StaticString("precise"),
				MarkdownDescription: "How `DECIMAL`/`NUMERIC` columns are represented in change events. One of " + oneOfMarkdown(validDecimalHandlingModes) + ". Defaults to `precise`.",
			},
			"publication_autocreate_mode": schema.StringAttribute{
				Optional:            true,
				Computed:            true,
				Default:             stringdefault.StaticString("all_tables"),
				MarkdownDescription: "How the PostgreSQL publication is created when using `pgoutput`. One of " + oneOfMarkdown(validPublicationAutocreateModes) + ". Defaults to `all_tables`.",
			},
			"schema_history_kafka_topic": schema.StringAttribute{
				Required:            true,
				MarkdownDescription: "Kafka topic used to store the database schema history.",
			},
			"schema_history_kafka_bootstrap_servers": schema.StringAttribute{
				Required:            true,
				MarkdownDescription: "Comma-separated Kafka bootstrap servers used by the schema-history client.",
			},
			"outbox_route_topic_replacement": schema.StringAttribute{
				Required:            true,
				MarkdownDescription: "Topic name replacement pattern for the outbox event router, e.g. `trunk.outbox.event.${routedByValue}`.",
			},
			"outbox_route_by_field": schema.StringAttribute{
				Optional:            true,
				Computed:            true,
				Default:             stringdefault.StaticString("queue_scope_type"),
				MarkdownDescription: "Outbox table column used to route events to a topic. Defaults to `queue_scope_type`.",
			},
			"outbox_table_field_event_key": schema.StringAttribute{
				Optional:            true,
				Computed:            true,
				Default:             stringdefault.StaticString("queue_scope_id"),
				MarkdownDescription: "Outbox table column used as the outgoing record key. Defaults to `queue_scope_id`.",
			},
			"outbox_table_field_event_timestamp": schema.StringAttribute{
				Optional:            true,
				Computed:            true,
				Default:             stringdefault.StaticString("created_at"),
				MarkdownDescription: "Outbox table column used as the outgoing record timestamp. Defaults to `created_at`.",
			},
			"outbox_table_fields_additional_placement": schema.StringAttribute{
				Optional:            true,
				Computed:            true,
				Default:             stringdefault.StaticString("event:header:event,created_at:header:created_at,serial:header:serial,database_name:header:database_name"),
				MarkdownDescription: "Additional outbox column-to-header/envelope placement mappings.",
			},
			"max_queue_size": schema.Int64Attribute{
				Optional:            true,
				Computed:            true,
				Default:             int64default.StaticInt64(131072),
				MarkdownDescription: "Maximum size of the connector's internal change event queue. Defaults to `131072`.",
			},
			"max_batch_size": schema.Int64Attribute{
				Optional:            true,
				Computed:            true,
				Default:             int64default.StaticInt64(32768),
				MarkdownDescription: "Maximum size of a batch processed by the connector. Defaults to `32768`.",
			},
			"tombstones_on_delete": schema.BoolAttribute{
				Optional:            true,
				Computed:            true,
				Default:             booldefault.StaticBool(false),
				MarkdownDescription: "Whether a tombstone event is emitted after a delete event. Defaults to `false`.",
			},
			"security_protocol": schema.StringAttribute{
				Optional:            true,
				Computed:            true,
				Default:             stringdefault.StaticString("SASL_SSL"),
				MarkdownDescription: "Security protocol used by the schema-history Kafka client. One of " + oneOfMarkdown(validSecurityProtocols) + ". Defaults to `SASL_SSL`.",
			},
			"sasl_scram": schema.SingleNestedAttribute{
				Optional:            true,
				MarkdownDescription: "Authenticate the schema-history Kafka client with SASL/SCRAM-SHA-512. Mutually exclusive with `aws_msk_iam`.",
				Attributes: map[string]schema.Attribute{
					"username": schema.StringAttribute{
						Required:            true,
						MarkdownDescription: "SASL/SCRAM username.",
					},
					"password": schema.StringAttribute{
						Required:            true,
						Sensitive:           true,
						MarkdownDescription: "SASL/SCRAM password.",
					},
				},
			},
			"aws_msk_iam": schema.BoolAttribute{
				Optional:            true,
				Computed:            true,
				Default:             booldefault.StaticBool(false),
				MarkdownDescription: "Authenticate the schema-history Kafka client using AWS MSK IAM. Mutually exclusive with `sasl_scram`.",
			},
			"extra_config": schema.MapAttribute{
				Optional:            true,
				ElementType:         types.StringType,
				MarkdownDescription: "Escape hatch for any Kafka Connect/Debezium config key not otherwise exposed as an attribute. Merged in last, so it can override any other attribute's value. Not reconstructed on read/import.",
			},
		},
	}
}

// oneOfMarkdown renders a list of allowed values as a Markdown-formatted, backtick-quoted list for
// use in attribute descriptions.
func oneOfMarkdown(values []string) string {
	quoted := make([]string, len(values))
	for i, v := range values {
		quoted[i] = "`" + v + "`"
	}
	return strings.Join(quoted, ", ")
}

// requireOneOf returns an error if value is set but isn't one of allowed. A null/unknown value
// (not yet set/defaulted) is always considered valid here.
func requireOneOf(value types.String, allowed []string) error {
	if value.IsNull() || value.IsUnknown() {
		return nil
	}

	v := value.ValueString()
	for _, a := range allowed {
		if v == a {
			return nil
		}
	}

	return fmt.Errorf("must be one of %s, got %q", strings.Join(allowed, ", "), v)
}

func (r *ConnectorResource) ValidateConfig(ctx context.Context, req resource.ValidateConfigRequest, resp *resource.ValidateConfigResponse) {
	var data ConnectorModel
	resp.Diagnostics.Append(req.Config.Get(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}

	hasScram := data.SaslScram != nil
	hasAwsIam := data.AwsMskIam.ValueBool()

	if hasScram == hasAwsIam {
		resp.Diagnostics.AddError(
			"Invalid authentication configuration",
			"Exactly one of \"sasl_scram\" or \"aws_msk_iam = true\" must be set.",
		)
	}

	enumChecks := []struct {
		attribute string
		value     types.String
		allowed   []string
	}{
		{"snapshot_mode", data.SnapshotMode, validSnapshotModes},
		{"decimal_handling_mode", data.DecimalHandlingMode, validDecimalHandlingModes},
		{"publication_autocreate_mode", data.PublicationAutocreateMode, validPublicationAutocreateModes},
		{"security_protocol", data.SecurityProtocol, validSecurityProtocols},
	}
	for _, c := range enumChecks {
		if err := requireOneOf(c.value, c.allowed); err != nil {
			resp.Diagnostics.AddAttributeError(path.Root(c.attribute), "Invalid value", err.Error())
		}
	}
}

func (r *ConnectorResource) Configure(ctx context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
	if req.ProviderData == nil {
		return
	}

	client, ok := req.ProviderData.(*DebeziumConnectClient)
	if !ok {
		resp.Diagnostics.AddError(
			"Unexpected Resource Configure Type",
			fmt.Sprintf("Expected *DebeziumConnectClient, got: %T. Please report this issue to the provider developers.", req.ProviderData),
		)
		return
	}

	r.client = client
	tflog.Info(ctx, "Configured Debezium connector resource client")
}

// withConnectorLogContext attaches the connector name to the log context and masks fields that
// could otherwise leak credentials (the database password and any nested "password", such as
// sasl_scram.password) if ever included in a future log call.
func withConnectorLogContext(ctx context.Context, name string) context.Context {
	ctx = tflog.SetField(ctx, "connector_name", name)
	ctx = tflog.MaskFieldValuesWithFieldKeys(ctx, "database_password", "password")
	return ctx
}

func (r *ConnectorResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var data ConnectorModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}

	ctx = withConnectorLogContext(ctx, data.Name.ValueString())
	tflog.Info(ctx, "Creating Debezium connector")

	if err := r.client.PutConnectorConfig(ctx, data.Name.ValueString(), buildConnectorConfig(data)); err != nil {
		resp.Diagnostics.AddError("Error creating connector", err.Error())
		return
	}

	data.Id = data.Name

	tflog.Info(ctx, "Created Debezium connector")
	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
}

// Read fetches the connector's live config from Kafka Connect and reconstructs the full resource
// state from it, so that drift made outside Terraform (or via `terraform import`) is detected.
func (r *ConnectorResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var state ConnectorModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	ctx = withConnectorLogContext(ctx, state.Name.ValueString())
	tflog.Debug(ctx, "Reading Debezium connector")

	cfg, found, err := r.client.GetConnectorConfig(ctx, state.Name.ValueString())
	if err != nil {
		resp.Diagnostics.AddError("Error reading connector", err.Error())
		return
	}
	if !found {
		tflog.Warn(ctx, "Debezium connector no longer exists, removing from state")
		resp.State.RemoveResource(ctx)
		return
	}

	data, err := parseConnectorConfig(cfg)
	if err != nil {
		resp.Diagnostics.AddError("Error parsing connector config", err.Error())
		return
	}
	data.Id = data.Name
	data.ExtraConfig = state.ExtraConfig

	tflog.Debug(ctx, "Read Debezium connector")
	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
}

// ImportState only needs to seed the lookup key; Read reconstructs every other attribute from the
// connector's live config. extra_config is left null since there's no way to know which keys, if
// any, were meant to be managed through it.
func (r *ConnectorResource) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	tflog.Info(withConnectorLogContext(ctx, req.ID), "Importing Debezium connector")
	resp.Diagnostics.Append(resp.State.SetAttribute(ctx, path.Root("id"), req.ID)...)
	resp.Diagnostics.Append(resp.State.SetAttribute(ctx, path.Root("name"), req.ID)...)
}

func (r *ConnectorResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var data ConnectorModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}

	ctx = withConnectorLogContext(ctx, data.Name.ValueString())
	tflog.Info(ctx, "Updating Debezium connector")

	if err := r.client.PutConnectorConfig(ctx, data.Name.ValueString(), buildConnectorConfig(data)); err != nil {
		resp.Diagnostics.AddError("Error updating connector", err.Error())
		return
	}

	data.Id = data.Name

	tflog.Info(ctx, "Updated Debezium connector")
	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
}

func (r *ConnectorResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var data ConnectorModel
	resp.Diagnostics.Append(req.State.Get(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}

	ctx = withConnectorLogContext(ctx, data.Name.ValueString())
	tflog.Info(ctx, "Deleting Debezium connector")

	if err := r.client.DeleteConnector(ctx, data.Name.ValueString()); err != nil {
		resp.Diagnostics.AddError("Error deleting connector", err.Error())
		return
	}

	tflog.Info(ctx, "Deleted Debezium connector")
}

// buildConnectorConfig renders the flat Kafka Connect config map for a Debezium PostgreSQL outbox connector.
func buildConnectorConfig(data ConnectorModel) map[string]string {
	name := data.Name.ValueString()
	historyTopic := data.SchemaHistoryKafkaTopic.ValueString()
	bootstrapServers := data.SchemaHistoryKafkaBootstrapServers.ValueString()

	cfg := map[string]string{
		"name":                  name,
		"connector.class":       connectorClassPostgres,
		"connector.displayName": name,

		"database.hostname":    data.DatabaseHostname.ValueString(),
		"database.port":        fmt.Sprintf("%d", data.DatabasePort.ValueInt64()),
		"database.user":        data.DatabaseUser.ValueString(),
		"database.password":    data.DatabasePassword.ValueString(),
		"database.dbname":      data.DatabaseDbname.ValueString(),
		"database.server.name": data.DatabaseServerName.ValueString(),
		"slot.name":            data.SlotName.ValueString(),
		"slot.drop.on.stop":    fmt.Sprintf("%t", data.SlotDropOnStop.ValueBool()),
		"plugin.name":          data.PluginName.ValueString(),
		"table.include.list":   data.TableIncludeList.ValueString(),

		"snapshot.mode":               data.SnapshotMode.ValueString(),
		"heartbeat.interval.ms":       fmt.Sprintf("%d", data.HeartbeatIntervalMs.ValueInt64()),
		"decimal.handling.mode":       data.DecimalHandlingMode.ValueString(),
		"publication.autocreate.mode": data.PublicationAutocreateMode.ValueString(),

		"tombstones.on.delete": fmt.Sprintf("%t", data.TombstonesOnDelete.ValueBool()),
		"max.queue.size":       fmt.Sprintf("%d", data.MaxQueueSize.ValueInt64()),
		"max.batch.size":       fmt.Sprintf("%d", data.MaxBatchSize.ValueInt64()),

		// Both the legacy "database.history.*" and current "schema.history.internal.*" topic
		// coordinates are set, matching the connectors already running in production. Debezium 1.9
		// only recognizes "database.history.*" (verified against the 1.9.0-SNAPSHOT source: there is
		// no "schema.history.internal." prefix in DatabaseHistory.java/KafkaDatabaseHistory.java), so
		// "database.history.store.only.captured.tables.ddl" is the one that actually takes effect.
		"schema.history.internal.kafka.topic":                    historyTopic,
		"schema.history.internal.kafka.bootstrap.servers":        bootstrapServers,
		"schema.history.internal.store.only.captured.tables.ddl": "true",
		"database.history.kafka.topic":                           historyTopic,
		"database.history.kafka.bootstrap.servers":               bootstrapServers,
		"database.history.store.only.captured.tables.ddl":        "true",

		"transforms":             "outbox",
		"transforms.outbox.type": outboxTransformType,
		"transforms.outbox.table.expand.json.payload":         "false",
		"transforms.outbox.route.by.field":                    data.OutboxRouteByField.ValueString(),
		"transforms.outbox.route.topic.replacement":           data.OutboxRouteTopicReplacement.ValueString(),
		"transforms.outbox.table.field.event.key":             data.OutboxTableFieldEventKey.ValueString(),
		"transforms.outbox.table.field.event.timestamp":       data.OutboxTableFieldEventTimestamp.ValueString(),
		"transforms.outbox.table.fields.additional.placement": data.OutboxTableFieldsAdditionalPlacement.ValueString(),
	}

	if !data.MessageKeyColumns.IsNull() {
		cfg["message.key.columns"] = data.MessageKeyColumns.ValueString()
	}

	for k, v := range historyClientAuthConfig(data) {
		cfg[k] = v
	}

	// extra_config is merged last so it can override any attribute-derived value above.
	for k, v := range data.ExtraConfig.Elements() {
		if s, ok := v.(types.String); ok {
			cfg[k] = s.ValueString()
		}
	}

	return cfg
}

// historyClientAuthConfig renders the auth config for the schema-history Kafka producer/consumer clients.
func historyClientAuthConfig(data ConnectorModel) map[string]string {
	securityProtocol := data.SecurityProtocol.ValueString()

	cfg := make(map[string]string, 8)
	for _, role := range []string{"consumer", "producer"} {
		cfg[fmt.Sprintf("database.history.%s.security.protocol", role)] = securityProtocol
	}

	if data.SaslScram != nil {
		jaas := scramJaasConfig(data.SaslScram.Username.ValueString(), data.SaslScram.Password.ValueString())
		for _, role := range []string{"consumer", "producer"} {
			cfg[fmt.Sprintf("database.history.%s.sasl.mechanism", role)] = saslMechanismScram
			cfg[fmt.Sprintf("database.history.%s.sasl.jaas.config", role)] = jaas
		}
		return cfg
	}

	for _, role := range []string{"consumer", "producer"} {
		cfg[fmt.Sprintf("database.history.%s.sasl.mechanism", role)] = saslMechanismAwsMskIam
		cfg[fmt.Sprintf("database.history.%s.sasl.jaas.config", role)] = awsMskIamJaasConfig
		cfg[fmt.Sprintf("database.history.%s.sasl.client.callback.handler.class", role)] = awsMskIamCallbackHandlerClass
	}
	cfg["database.history.sasl.client.callback.handler.class"] = awsMskIamCallbackHandlerClass

	return cfg
}

func scramJaasConfig(username, password string) string {
	escape := func(s string) string { return strings.ReplaceAll(s, `"`, `\"`) }
	return fmt.Sprintf(
		`org.apache.kafka.common.security.scram.ScramLoginModule required username="%s" password="%s";`,
		escape(username), escape(password),
	)
}

var scramJaasConfigPattern = regexp.MustCompile(`username="((?:[^"\\]|\\.)*)"\s+password="((?:[^"\\]|\\.)*)"`)

func parseScramJaasConfig(jaas string) (username, password string, err error) {
	m := scramJaasConfigPattern.FindStringSubmatch(jaas)
	if m == nil {
		return "", "", fmt.Errorf("could not parse SASL/SCRAM JAAS config %q", jaas)
	}

	unescape := func(s string) string { return strings.ReplaceAll(s, `\"`, `"`) }
	return unescape(m[1]), unescape(m[2]), nil
}

// messageKeyColumnsValue returns null when the key is absent (attribute left unset), rather than
// conflating that with an explicitly configured empty string.
func messageKeyColumnsValue(cfg map[string]string) types.String {
	v, ok := cfg["message.key.columns"]
	if !ok {
		return types.StringNull()
	}
	return types.StringValue(v)
}

// parseConnectorConfig reverses buildConnectorConfig, reconstructing a ConnectorModel from the flat
// config map returned by the Kafka Connect API.
func parseConnectorConfig(cfg map[string]string) (ConnectorModel, error) {
	databasePort, err := strconv.ParseInt(cfg["database.port"], 10, 64)
	if err != nil {
		return ConnectorModel{}, fmt.Errorf("parsing database.port: %w", err)
	}

	maxQueueSize, err := strconv.ParseInt(cfg["max.queue.size"], 10, 64)
	if err != nil {
		return ConnectorModel{}, fmt.Errorf("parsing max.queue.size: %w", err)
	}

	maxBatchSize, err := strconv.ParseInt(cfg["max.batch.size"], 10, 64)
	if err != nil {
		return ConnectorModel{}, fmt.Errorf("parsing max.batch.size: %w", err)
	}

	tombstonesOnDelete, err := strconv.ParseBool(cfg["tombstones.on.delete"])
	if err != nil {
		return ConnectorModel{}, fmt.Errorf("parsing tombstones.on.delete: %w", err)
	}

	slotDropOnStop, err := strconv.ParseBool(cfg["slot.drop.on.stop"])
	if err != nil {
		return ConnectorModel{}, fmt.Errorf("parsing slot.drop.on.stop: %w", err)
	}

	heartbeatIntervalMs, err := strconv.ParseInt(cfg["heartbeat.interval.ms"], 10, 64)
	if err != nil {
		return ConnectorModel{}, fmt.Errorf("parsing heartbeat.interval.ms: %w", err)
	}

	data := ConnectorModel{
		Name:               types.StringValue(cfg["name"]),
		DatabaseHostname:   types.StringValue(cfg["database.hostname"]),
		DatabasePort:       types.Int64Value(databasePort),
		DatabaseUser:       types.StringValue(cfg["database.user"]),
		DatabasePassword:   types.StringValue(cfg["database.password"]),
		DatabaseDbname:     types.StringValue(cfg["database.dbname"]),
		DatabaseServerName: types.StringValue(cfg["database.server.name"]),
		SlotName:           types.StringValue(cfg["slot.name"]),
		SlotDropOnStop:     types.BoolValue(slotDropOnStop),
		PluginName:         types.StringValue(cfg["plugin.name"]),
		TableIncludeList:   types.StringValue(cfg["table.include.list"]),

		SnapshotMode:              types.StringValue(cfg["snapshot.mode"]),
		HeartbeatIntervalMs:       types.Int64Value(heartbeatIntervalMs),
		MessageKeyColumns:         messageKeyColumnsValue(cfg),
		DecimalHandlingMode:       types.StringValue(cfg["decimal.handling.mode"]),
		PublicationAutocreateMode: types.StringValue(cfg["publication.autocreate.mode"]),

		// extra_config isn't reconstructed from the API response; Read/Import set it separately.
		ExtraConfig: types.MapNull(types.StringType),

		SchemaHistoryKafkaTopic:            types.StringValue(cfg["schema.history.internal.kafka.topic"]),
		SchemaHistoryKafkaBootstrapServers: types.StringValue(cfg["schema.history.internal.kafka.bootstrap.servers"]),

		OutboxRouteTopicReplacement:          types.StringValue(cfg["transforms.outbox.route.topic.replacement"]),
		OutboxRouteByField:                   types.StringValue(cfg["transforms.outbox.route.by.field"]),
		OutboxTableFieldEventKey:             types.StringValue(cfg["transforms.outbox.table.field.event.key"]),
		OutboxTableFieldEventTimestamp:       types.StringValue(cfg["transforms.outbox.table.field.event.timestamp"]),
		OutboxTableFieldsAdditionalPlacement: types.StringValue(cfg["transforms.outbox.table.fields.additional.placement"]),

		MaxQueueSize:       types.Int64Value(maxQueueSize),
		MaxBatchSize:       types.Int64Value(maxBatchSize),
		TombstonesOnDelete: types.BoolValue(tombstonesOnDelete),
		SecurityProtocol:   types.StringValue(cfg["database.history.consumer.security.protocol"]),
	}

	switch mechanism := cfg["database.history.consumer.sasl.mechanism"]; mechanism {
	case saslMechanismAwsMskIam:
		data.AwsMskIam = types.BoolValue(true)
	case saslMechanismScram:
		username, password, err := parseScramJaasConfig(cfg["database.history.consumer.sasl.jaas.config"])
		if err != nil {
			return ConnectorModel{}, err
		}
		data.SaslScram = &SaslScramModel{
			Username: types.StringValue(username),
			Password: types.StringValue(password),
		}
		data.AwsMskIam = types.BoolValue(false)
	default:
		return ConnectorModel{}, fmt.Errorf("unrecognized schema-history sasl.mechanism %q", mechanism)
	}

	return data, nil
}
