// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/hashicorp/terraform-plugin-log/tflog"
)

// DebeziumConnectClient talks to the Kafka Connect REST API that hosts Debezium connectors.
type DebeziumConnectClient struct {
	Endpoint   string
	HTTPClient *http.Client
}

func (c *DebeziumConnectClient) connectorConfigURL(name string) string {
	return fmt.Sprintf("%s/connectors/%s/config", c.Endpoint, url.PathEscape(name))
}

// logAPICall attaches the request method/URL to the log context and emits a Debug line before the
// call. The connector config body is deliberately never logged, since it can carry credentials
// (database.password, SASL JAAS strings).
func logAPICall(ctx context.Context, method, requestURL string) context.Context {
	ctx = tflog.SetField(ctx, "kafka_connect_method", method)
	ctx = tflog.SetField(ctx, "kafka_connect_url", requestURL)
	tflog.Debug(ctx, "Calling Kafka Connect API")
	return ctx
}

func logAPIResponse(ctx context.Context, statusCode int) {
	tflog.Debug(ctx, "Received Kafka Connect API response", map[string]interface{}{"status_code": statusCode})
}

// PutConnectorConfig creates or updates a connector, since Kafka Connect's config endpoint is an idempotent upsert.
func (c *DebeziumConnectClient) PutConnectorConfig(ctx context.Context, name string, config map[string]string) error {
	body, err := json.Marshal(config)
	if err != nil {
		return fmt.Errorf("encoding connector config: %w", err)
	}

	requestURL := c.connectorConfigURL(name)
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, requestURL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	ctx = logAPICall(ctx, http.MethodPut, requestURL)
	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		tflog.Error(ctx, "Kafka Connect API call failed", map[string]interface{}{"error": err.Error()})
		return fmt.Errorf("calling Kafka Connect API: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	logAPIResponse(ctx, resp.StatusCode)

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("unexpected status %d from Kafka Connect API: %s", resp.StatusCode, readBody(resp.Body))
	}

	return nil
}

// GetConnectorConfig fetches the current config of a connector. The second return value is false
// if no connector with that name is registered.
func (c *DebeziumConnectClient) GetConnectorConfig(ctx context.Context, name string) (map[string]string, bool, error) {
	requestURL := c.connectorConfigURL(name)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL, nil)
	if err != nil {
		return nil, false, err
	}

	ctx = logAPICall(ctx, http.MethodGet, requestURL)
	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		tflog.Error(ctx, "Kafka Connect API call failed", map[string]interface{}{"error": err.Error()})
		return nil, false, fmt.Errorf("calling Kafka Connect API: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	logAPIResponse(ctx, resp.StatusCode)

	switch resp.StatusCode {
	case http.StatusOK:
		var cfg map[string]string
		if err := json.NewDecoder(resp.Body).Decode(&cfg); err != nil {
			return nil, false, fmt.Errorf("decoding connector config: %w", err)
		}
		return cfg, true, nil
	case http.StatusNotFound:
		return nil, false, nil
	default:
		return nil, false, fmt.Errorf("unexpected status %d from Kafka Connect API: %s", resp.StatusCode, readBody(resp.Body))
	}
}

// DeleteConnector removes a connector. A missing connector is treated as already deleted.
func (c *DebeziumConnectClient) DeleteConnector(ctx context.Context, name string) error {
	deleteURL := fmt.Sprintf("%s/connectors/%s", c.Endpoint, url.PathEscape(name))

	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, deleteURL, nil)
	if err != nil {
		return err
	}

	ctx = logAPICall(ctx, http.MethodDelete, deleteURL)
	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		tflog.Error(ctx, "Kafka Connect API call failed", map[string]interface{}{"error": err.Error()})
		return fmt.Errorf("calling Kafka Connect API: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	logAPIResponse(ctx, resp.StatusCode)

	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusNotFound {
		return fmt.Errorf("unexpected status %d from Kafka Connect API: %s", resp.StatusCode, readBody(resp.Body))
	}

	return nil
}

func readBody(r io.Reader) string {
	b, _ := io.ReadAll(r)
	return string(b)
}
