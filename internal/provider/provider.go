// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package provider

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/provider"
	"github.com/hashicorp/terraform-plugin-framework/provider/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/hashicorp/terraform-plugin-log/tflog"
)

// debeziumConnectEndpointEnvVar is the environment variable fallback for the "endpoint" provider
// attribute, per https://developer.hashicorp.com/terraform/tutorials/community-providers/providers-plugin-framework-lab.
const debeziumConnectEndpointEnvVar = "DEBEZIUM_CONNECT_ENDPOINT"

// Ensure DebeziumPostgres satisfies the provider interface.
var _ provider.Provider = &DebeziumPostgres{}

// DebeziumPostgres defines the provider implementation.
type DebeziumPostgres struct {
	// version is set to the provider version on release, "dev" when the
	// provider is built and ran locally, and "test" when running acceptance
	// testing.
	version string
}

// DebeziumPostgresModel describes the provider data model.
type DebeziumPostgresModel struct {
	Endpoint types.String `tfsdk:"endpoint"`
}

func (p *DebeziumPostgres) Metadata(ctx context.Context, req provider.MetadataRequest, resp *provider.MetadataResponse) {
	resp.TypeName = "debezium"
	resp.Version = p.version
}

func (p *DebeziumPostgres) Schema(ctx context.Context, req provider.SchemaRequest, resp *provider.SchemaResponse) {
	resp.Schema = schema.Schema{
		Attributes: map[string]schema.Attribute{
			"endpoint": schema.StringAttribute{
				MarkdownDescription: "Base URL of the Kafka Connect REST API used to manage Debezium connectors (e.g. `https://connect.example.com:8083`). " +
					"Can also be set via the `" + debeziumConnectEndpointEnvVar + "` environment variable; the attribute takes precedence when both are set.",
				Optional: true,
			},
		},
	}
}

// resolveEndpoint applies the DEBEZIUM_CONNECT_ENDPOINT environment variable fallback: an explicit
// "endpoint" attribute always wins, otherwise the environment variable is used, per
// https://developer.hashicorp.com/terraform/tutorials/community-providers/providers-plugin-framework-lab.
func resolveEndpoint(configEndpoint types.String, getenv func(string) string) (string, error) {
	endpoint := getenv(debeziumConnectEndpointEnvVar)
	if !configEndpoint.IsNull() {
		endpoint = configEndpoint.ValueString()
	}

	if endpoint == "" {
		return "", fmt.Errorf("set the \"endpoint\" attribute in the provider configuration, or the %s environment variable", debeziumConnectEndpointEnvVar)
	}

	return endpoint, nil
}

func (p *DebeziumPostgres) Configure(ctx context.Context, req provider.ConfigureRequest, resp *provider.ConfigureResponse) {
	var data DebeziumPostgresModel

	resp.Diagnostics.Append(req.Config.Get(ctx, &data)...)

	if resp.Diagnostics.HasError() {
		return
	}

	endpoint, err := resolveEndpoint(data.Endpoint, os.Getenv)
	if err != nil {
		resp.Diagnostics.AddAttributeError(path.Root("endpoint"), "Missing Kafka Connect endpoint", err.Error())
		return
	}

	ctx = tflog.SetField(ctx, "debezium_connect_endpoint", endpoint)
	tflog.Info(ctx, "Configuring Debezium Kafka Connect client")

	client := &DebeziumConnectClient{
		Endpoint:   strings.TrimRight(endpoint, "/"),
		HTTPClient: http.DefaultClient,
	}

	resp.DataSourceData = client
	resp.ResourceData = client

	tflog.Info(ctx, "Configured Debezium Kafka Connect client")
}

func (p *DebeziumPostgres) Resources(ctx context.Context) []func() resource.Resource {
	return []func() resource.Resource{
		NewDebeziumPgConnector,
	}
}

func (p *DebeziumPostgres) DataSources(ctx context.Context) []func() datasource.DataSource {
	return []func() datasource.DataSource{
		NewDebeziumPgDataSource,
	}
}

func New(version string) func() provider.Provider {
	return func() provider.Provider {
		return &DebeziumPostgres{
			version: version,
		}
	}
}
