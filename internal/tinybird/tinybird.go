// Package tinybird provides the managed hot-analytics integration.
package tinybird

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/log-analytics-platform/internal/config"
	"github.com/log-analytics-platform/internal/models"
	"go.uber.org/zap"
)

type Client struct {
	baseURL, appendToken, readToken, datasource string
	http                                        *http.Client
	logger                                      *zap.Logger
}

func NewClient(cfg config.TinybirdConfig, logger *zap.Logger) (*Client, error) {
	if cfg.Datasource == "" {
		return nil, fmt.Errorf("TINYBIRD_DATASOURCE must be configured")
	}
	// Size the keep-alive pool for concurrent appends: several batches per
	// worker may be in flight at once, and each should reuse a warm TLS
	// connection instead of re-handshaking against the regional API host.
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConns = 64
	transport.MaxIdleConnsPerHost = 16
	transport.IdleConnTimeout = 90 * time.Second
	return &Client{baseURL: strings.TrimRight(cfg.APIURL, "/"), appendToken: cfg.AppendToken, readToken: cfg.ReadToken, datasource: cfg.Datasource, http: &http.Client{Timeout: 30 * time.Second, Transport: transport}, logger: logger}, nil
}

// AppendEvents writes a micro-batch and waits for Tinybird to acknowledge it
// before the Kafka consumer commits its offsets.
func (c *Client) AppendEvents(ctx context.Context, events []models.LogEvent) error {
	if len(events) == 0 {
		return nil
	}
	if c.appendToken == "" {
		return fmt.Errorf("TINYBIRD_APPEND_TOKEN must be configured")
	}
	var body bytes.Buffer
	body.Grow(len(events) * 512)
	for _, e := range events {
		// Keep arbitrary attributes as a JSON string. It gives the managed
		// datasource a stable schema while preserving the complete payload.
		attrs, err := json.Marshal(e.Attributes)
		if err != nil {
			return err
		}
		row := map[string]interface{}{"event_id": e.EventID, "timestamp": e.Timestamp, "service": e.Service, "severity": e.Severity, "message": e.Message, "attributes": string(attrs), "trace_id": e.TraceID, "source": e.Source, "region": e.Region, "version": e.Version}
		b, err := json.Marshal(row)
		if err != nil {
			return err
		}
		body.Write(b)
		body.WriteByte('\n')
	}
	// Gzip the NDJSON batch: log JSON compresses ~10x, which keeps the cloud
	// append path from becoming network-bound at high event rates.
	var gzipped bytes.Buffer
	gzipped.Grow(body.Len() / 4)
	gz, err := gzip.NewWriterLevel(&gzipped, gzip.BestSpeed)
	if err != nil {
		return err
	}
	if _, err := gz.Write(body.Bytes()); err != nil {
		return err
	}
	if err := gz.Close(); err != nil {
		return err
	}

	u := c.baseURL + "/v0/events?name=" + url.QueryEscape(c.datasource) + "&wait=true"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, &gzipped)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.appendToken)
	req.Header.Set("Content-Type", "application/x-ndjson")
	req.Header.Set("Content-Encoding", "gzip")
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("events api: %w", err)
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	if err != nil {
		return fmt.Errorf("events api: read response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("events api returned %s: %s", resp.Status, strings.TrimSpace(string(respBody)))
	}
	// wait=true only guarantees an acknowledgement. The payload reports how
	// many rows were actually committed, so verify it before the caller
	// commits Kafka offsets for this batch.
	var ack struct {
		SuccessfulRows  int `json:"successful_rows"`
		QuarantinedRows int `json:"quarantined_rows"`
	}
	if err := json.Unmarshal(respBody, &ack); err != nil {
		c.logger.Warn("events api returned an unexpected acknowledgement payload",
			zap.String("payload", strings.TrimSpace(string(respBody))))
		return nil
	}
	if ack.QuarantinedRows > 0 {
		return fmt.Errorf("events api quarantined %d of %d rows", ack.QuarantinedRows, len(events))
	}
	if ack.SuccessfulRows < len(events) {
		return fmt.Errorf("events api committed %d of %d rows", ack.SuccessfulRows, len(events))
	}
	return nil
}

// Query executes a read-only SQL query against Tinybird.
func (c *Client) Query(ctx context.Context, sql string) ([]map[string]json.RawMessage, error) {
	if c.readToken == "" {
		return nil, fmt.Errorf("TINYBIRD_READ_TOKEN must be configured")
	}
	payload, _ := json.Marshal(map[string]string{"q": sql + " FORMAT JSON"})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/v0/sql", bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.readToken)
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("query api: %w", err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("query api returned %s: %s", resp.Status, strings.TrimSpace(string(b)))
	}
	var result struct {
		Data []map[string]json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(b, &result); err != nil {
		return nil, fmt.Errorf("decode query response: %w", err)
	}
	return result.Data, nil
}

func (c *Client) Datasource() string { return c.datasource }
