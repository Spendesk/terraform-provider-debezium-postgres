// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package provider

import (
	"testing"

	"github.com/hashicorp/terraform-plugin-framework/providerserver"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/hashicorp/terraform-plugin-go/tfprotov6"
)

// testAccProtoV6ProviderFactories is used to instantiate a provider during acceptance testing.
// The factory function is called for each Terraform CLI command to create a provider
// server that the CLI can connect to and interact with.
var testAccProtoV6ProviderFactories = map[string]func() (tfprotov6.ProviderServer, error){
	"debezium": providerserver.NewProtocol6WithError(New("test")()),
}

func TestResolveEndpoint(t *testing.T) {
	fakeEnv := func(value string) func(string) string {
		return func(key string) string {
			if key == debeziumConnectEndpointEnvVar {
				return value
			}
			return ""
		}
	}

	tests := []struct {
		name           string
		configEndpoint types.String
		env            string
		want           string
		wantErr        bool
	}{
		{"explicit attribute wins", types.StringValue("https://from-config:8083"), "https://from-env:8083", "https://from-config:8083", false},
		{"env var fallback", types.StringNull(), "https://from-env:8083", "https://from-env:8083", false},
		{"neither set is an error", types.StringNull(), "", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := resolveEndpoint(tt.configEndpoint, fakeEnv(tt.env))
			if (err != nil) != tt.wantErr {
				t.Fatalf("resolveEndpoint() error = %v, wantErr %v", err, tt.wantErr)
			}
			if got != tt.want {
				t.Errorf("resolveEndpoint() = %q, want %q", got, tt.want)
			}
		})
	}
}
