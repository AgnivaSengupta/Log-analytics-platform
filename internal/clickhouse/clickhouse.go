package clickhouse

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
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/log-analytics-platform/internal/config"
	"github.com/log-analytics-platform/internal/models"
	"go.uber.org/zap"
)

type Client struct {
	baseURL  string
	user     string
	password string
	Database string
	Table    string
	http     *http.Client
	logger   *zap.Logger
}

type StatusError struct {
	Status      int
	Body        string
	Quarantined int
	Committed   int
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("clickhouse http returned %d: %s", e.Status, e.Body)
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

func NewClient(cfg config.ClickHouseConfig, logger *zap.Logger) (*Client, error) {
	if cfg.URL == "" {
		return nil, fmt.Errorf("CLICKHOUSE_URL must be configured")
	}
	if err := validateIdent("CLICKHOUSE_DATABASE", cfg.Database); err != nil {
		return nil, err
	}
	if err := validateIdent("CLICKHOUSE_TABLE", cfg.Table); err != nil {
		return nil, err
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConns = 128
	transport.MaxIdleConnsPerHost = 32
	transport.IdleConnTimeout = 90 * time.Second
	transport.ResponseHeaderTimeout = 35 * time.Second
	return &Client{
		baseURL:  strings.TrimRight(cfg.URL, "/"),
		user:     cfg.User,
		password: cfg.Password,
		Database: cfg.Database,
		Table:    cfg.Table,
		http:     &http.Client{Timeout: 35 * time.Second, Transport: transport},
		logger:   logger,
	}, nil
}

func validateIdent(name, value string) error {
	if value == "" {
		return fmt.Errorf("%s must be configured", name)
	}
	for _, r := range value {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' {
			continue
		}
		return fmt.Errorf("%s contains invalid characters", name)
	}
	return nil
}

func (c *Client) TableRef() string {
	if c.Database == "" || c.Database == "default" {
		return c.Table
	}
	return c.Database + "." + c.Table
}

type logRow struct {
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
	if c.password == "" {
		return fmt.Errorf("CLICKHOUSE_PASSWORD must be configured")
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
		row := logRow{
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
		if err := enc.Encode(row); err != nil {
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

	query := "INSERT INTO " + c.TableRef() + " FORMAT JSONEachRow"
	u := c.baseURL + "/?" + url.Values{
		"query":                                 {query},
		"wait_end_of_query":                     {"1"},
		"send_progress_in_http_headers":         {"1"},
		"input_format_skip_unknown_fields":     {"1"},
	}.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(compressed.Bytes()))
	if err != nil {
		return err
	}
	c.setAuth(req)
	req.Header.Set("Content-Type", "application/x-ndjson")
	req.Header.Set("Content-Encoding", "gzip")

	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	if err != nil {
		return fmt.Errorf("clickhouse insert: read response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return &StatusError{Status: resp.StatusCode, Body: strings.TrimSpace(string(respBody))}
	}

	written, ok := writtenRows(resp.Header.Get("X-ClickHouse-Summary"))
	if ok && written < len(events) {
		return &StatusError{
			Status:     200,
			Committed: written,
			Body:       fmt.Sprintf("committed %d of %d rows", written, len(events)),
		}
	}
	return nil
}

func writtenRows(summary string) (int, bool) {
	if strings.TrimSpace(summary) == "" {
		return 0, false
	}
	var parsed struct {
		WrittenRows json.RawMessage `json:"written_rows"`
	}
	if err := json.Unmarshal([]byte(summary), &parsed); err != nil {
		return 0, false
	}
	var n int
	if json.Unmarshal(parsed.WrittenRows, &n) == nil {
		return n, true
	}
	var s string
	if json.Unmarshal(parsed.WrittenRows, &s) == nil {
		n, err := strconv.Atoi(s)
		return n, err == nil
	}
	return 0, false
}

type sqlAPIResponse struct {
	Data      []map[string]json.RawMessage `json:"data"`
	Exception string                       `json:"exception"`
}

func (c *Client) Query(ctx context.Context, sql string) ([]map[string]json.RawMessage, error) {
	if c.password == "" {
		return nil, fmt.Errorf("CLICKHOUSE_PASSWORD must be configured")
	}
	if strings.TrimSpace(sql) == "" {
		return nil, fmt.Errorf("empty SQL")
	}

	q := strings.TrimSpace(sql)
	if !strings.Contains(strings.ToUpper(q), " FORMAT ") {
		q += " FORMAT JSON"
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/", strings.NewReader(q))
	if err != nil {
		return nil, err
	}
	c.setAuth(req)
	req.Header.Set("Content-Type", "text/plain")

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
	if err != nil {
		return nil, fmt.Errorf("clickhouse sql: read response: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("clickhouse sql returned %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var parsed sqlAPIResponse
	if err := json.Unmarshal(body, &parsed); err != nil {
		return nil, fmt.Errorf("clickhouse sql: decode: %w", err)
	}
	if parsed.Exception != "" {
		return nil, fmt.Errorf("clickhouse sql error: %s", parsed.Exception)
	}
	if parsed.Data == nil {
		return []map[string]json.RawMessage{}, nil
	}
	return parsed.Data, nil
}

func (c *Client) setAuth(req *http.Request) {
	if c.user != "" {
		req.Header.Set("X-ClickHouse-User", c.user)
	}
	if c.password != "" {
		req.Header.Set("X-ClickHouse-Key", c.password)
	}
}