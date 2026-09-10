package tinybird

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/log-analytics-platform/internal/config"
	"github.com/log-analytics-platform/internal/models"
	"go.uber.org/zap"
)

type Client struct {
	baseURL     string
	appendToken string
	readToken   string
	Datasource  string
	http        *http.Client
	logger      *zap.Logger
}

type StatusError struct {
	Status      int
	Body        string
	Quarantined int
	Committed   int
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("events api returned %d: %s", e.Status, e.Body)
}

func IsRetryable(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var se *StatusError
	if errors.As(err, &se) {
		return se.Status == 429 || se.Status >= 500
	}
	var ne net.Error
	if errors.As(err, &ne) {
		return true
	}
	return true
}

var (
	bufPool = sync.Pool{New: func() any { return new(bytes.Buffer) }}
	gzPool  = sync.Pool{New: func() any {
		w, _ := gzip.NewWriterLevel(io.Discard, gzip.BestSpeed)
		return w
	}}
)

func NewClient(cfg config.TinybirdConfig, logger *zap.Logger) (*Client, error) {
	if cfg.Datasource == "" {
		return nil, fmt.Errorf("TINYBIRD_DATASOURCE must be configured")
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConns = 128
	transport.MaxIdleConnsPerHost = 32
	transport.IdleConnTimeout = 90 * time.Second
	transport.ResponseHeaderTimeout = 35 * time.Second
	return &Client{
		baseURL:     strings.TrimRight(cfg.APIURL, "/"),
		appendToken: cfg.AppendToken,
		readToken:   cfg.ReadToken,
		Datasource:  cfg.Datasource,
		http:        &http.Client{Timeout: 35 * time.Second, Transport: transport},
		logger:      logger,
	}, nil
}

type tinybirdRow struct {
	EventID    string    `json:"event_id"`
	Timestamp  time.Time `json:"timestamp"`
	Service    string    `json:"service"`
	Severity   string    `json:"severity"`
	Message    string    `json:"message"`
	Attributes string    `json:"attributes"`
	TraceID    string    `json:"trace_id"`
	Source     string    `json:"source"`
	Region     string    `json:"region"`
	Version    string    `json:"version"`
}

func (c *Client) AppendEvents(ctx context.Context, events []models.LogEvent) error {
	if len(events) == 0 {
		return nil
	}
	if c.appendToken == "" {
		return fmt.Errorf("TINYBIRD_APPEND_TOKEN must be configured")
	}

	ndjson := bufPool.Get().(*bytes.Buffer)
	ndjson.Reset()
	defer bufPool.Put(ndjson)

	enc := json.NewEncoder(ndjson)
	enc.SetEscapeHTML(false)
	for _, e := range events {
		attrs := "{}"
		if e.Attributes != nil {
			b, err := json.Marshal(e.Attributes)
			if err != nil {
				return err
			}
			attrs = string(b)
		}
		row := tinybirdRow{
			EventID:    e.EventID,
			Timestamp:  e.Timestamp,
			Service:    e.Service,
			Severity:   e.Severity,
			Message:    e.Message,
			Attributes: attrs,
			TraceID:    e.TraceID,
			Source:     e.Source,
			Region:     e.Region,
			Version:    e.Version,
		}
		if err := enc.Encode(row); err != nil { // Encode already writes '\n'
			return err
		}
	}

	compressed := bufPool.Get().(*bytes.Buffer)
	compressed.Reset()
	defer bufPool.Put(compressed)

	gz := gzPool.Get().(*gzip.Writer)
	gz.Reset(compressed)
	if _, err := gz.Write(ndjson.Bytes()); err != nil {
		gz.Close()
		gzPool.Put(gz)
		return err
	}
	if err := gz.Close(); err != nil {
		gzPool.Put(gz)
		return err
	}
	gzPool.Put(gz)

	u := c.baseURL + "/v0/events?name=" + url.QueryEscape(c.Datasource) + "&wait=true"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(compressed.Bytes()))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.appendToken)
	req.Header.Set("Content-Type", "application/x-ndjson")
	req.Header.Set("Content-Encoding", "gzip")

	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	if err != nil {
		return fmt.Errorf("events api: read response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return &StatusError{Status: resp.StatusCode, Body: strings.TrimSpace(string(respBody))}
	}

	var ack struct {
		SuccessfulRows  int `json:"successful_rows"`
		QuarantinedRows int `json:"quarantined_rows"`
	}
	if err := json.Unmarshal(respBody, &ack); err != nil {
		return fmt.Errorf("events api returned unexpected payload: %s", bytes.TrimSpace(respBody))
	}
	if ack.QuarantinedRows > 0 {
		return &StatusError{
			Status:      200,
			Quarantined: ack.QuarantinedRows,
			Committed:   ack.SuccessfulRows,
			Body:        fmt.Sprintf("quarantined %d of %d rows", ack.QuarantinedRows, len(events)),
		}
	}
	if ack.SuccessfulRows < len(events) {
		return &StatusError{
			Status:    200,
			Committed: ack.SuccessfulRows,
			Body:      fmt.Sprintf("committed %d of %d rows", ack.SuccessfulRows, len(events)),
		}
	}
	return nil
}

// sqlAPIResponse is Tinybird's /v0/sql FORMAT JSON payload.
type sqlAPIResponse struct {
	Data    []map[string]json.RawMessage `json:"data"`
	Error   string                       `json:"error"`
	Message string                       `json:"message"`
}

// Query runs a SQL statement against Tinybird using the read token.
// Returns one map per row; values are raw JSON so callers can unmarshal
// into strings, numbers, or nested objects (as query-coordinator does).
func (c *Client) Query(ctx context.Context, sql string) ([]map[string]json.RawMessage, error) {
	if c.readToken == "" {
		return nil, fmt.Errorf("TINYBIRD_READ_TOKEN must be configured")
	}
	if strings.TrimSpace(sql) == "" {
		return nil, fmt.Errorf("empty SQL")
	}

	q := strings.TrimSpace(sql)
	if !strings.Contains(strings.ToUpper(q), " FORMAT ") {
		q += " FORMAT JSON"
	}

	form := url.Values{}
	form.Set("q", q)

	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		c.baseURL+"/v0/sql",
		strings.NewReader(form.Encode()),
	)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.readToken)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 64<<20)) // 64 MiB cap
	if err != nil {
		return nil, fmt.Errorf("sql api: read response: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("sql api returned %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var parsed sqlAPIResponse
	if err := json.Unmarshal(body, &parsed); err != nil {
		return nil, fmt.Errorf("sql api: decode: %w", err)
	}
	if parsed.Error != "" {
		return nil, fmt.Errorf("sql api error: %s", parsed.Error)
	}
	if parsed.Message != "" && parsed.Data == nil {
		return nil, fmt.Errorf("sql api error: %s", parsed.Message)
	}
	if parsed.Data == nil {
		return []map[string]json.RawMessage{}, nil
	}
	return parsed.Data, nil
}
