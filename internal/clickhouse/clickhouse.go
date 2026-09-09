package clickhouse

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"go.uber.org/zap"

	"github.com/log-analytics-platform/internal/config"
	"github.com/log-analytics-platform/internal/models"
)

// Client wraps the ClickHouse connection.
type Client struct {
	db     *sql.DB
	logger *zap.Logger
	cfg    config.ClickHouseConfig
}

// NewClient creates a new ClickHouse client.
func NewClient(cfg config.ClickHouseConfig, logger *zap.Logger) (*Client, error) {
	conn := clickhouse.OpenDB(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)},
		Auth: clickhouse.Auth{
			Database: cfg.Database,
			Username: cfg.User,
			Password: cfg.Password,
		},
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
		DialTimeout:     5 * time.Second,
		MaxOpenConns:    10,
		MaxIdleConns:    5,
		ConnMaxLifetime: time.Hour,
	})

	if err := conn.Ping(); err != nil {
		return nil, fmt.Errorf("clickhouse ping failed: %w", err)
	}

	return &Client{db: conn, logger: logger, cfg: cfg}, nil
}

// InitializeSchema creates the required tables.
func (c *Client) InitializeSchema(ctx context.Context) error {
	schema := []string{
		// Main events table - optimized for time-series analytics
		`CREATE TABLE IF NOT EXISTS logs (
			event_id String,
			timestamp DateTime64(3),
			service LowCardinality(String),
			severity LowCardinality(String),
			message String,
			attributes String,
			trace_id String,
			source LowCardinality(String),
			region LowCardinality(String),
			version LowCardinality(String)
		) ENGINE = MergeTree()
		PARTITION BY toYYYYMMDD(timestamp)
		ORDER BY (service, severity, timestamp)
		TTL timestamp + INTERVAL 30 DAY`,

		// Aggregated metrics table for fast dashboards
		`CREATE TABLE IF NOT EXISTS log_metrics (
			timestamp DateTime,
			service LowCardinality(String),
			severity LowCardinality(String),
			count UInt64,
			avg_message_length Float64
		) ENGINE = SummingMergeTree()
		PARTITION BY toYYYYMMDD(timestamp)
		ORDER BY (timestamp, service, severity)
		TTL timestamp + INTERVAL 90 DAY`,

		// Deduplication table to track processed event IDs
		`CREATE TABLE IF NOT EXISTS processed_events (
			event_id String,
			processed_at DateTime
		) ENGINE = ReplacingMergeTree(processed_at)
		ORDER BY event_id
		TTL processed_at + INTERVAL 7 DAY`,
	}

	for _, stmt := range schema {
		if _, err := c.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("schema creation failed: %w", err)
		}
	}

	c.logger.Info("ClickHouse schema initialized")
	return nil
}

// BatchInsert inserts a batch of log events into ClickHouse.
func (c *Client) BatchInsert(ctx context.Context, events []models.LogEvent) error {
	if len(events) == 0 {
		return nil
	}

	tx, err := c.db.Begin()
	if err != nil {
		return fmt.Errorf("begin tx failed: %w", err)
	}

	stmt, err := tx.Prepare(`
		INSERT INTO logs (event_id, timestamp, service, severity, message, attributes, trace_id, source, region, version)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("prepare failed: %w", err)
	}
	defer stmt.Close()

	for _, ev := range events {
		attrs := "{}"
		if len(ev.Attributes) > 0 {
			// Simple JSON serialization
			attrs = fmt.Sprintf("%v", ev.Attributes)
		}

		_, err := stmt.ExecContext(ctx,
			ev.EventID,
			ev.Timestamp,
			ev.Service,
			ev.Severity,
			ev.Message,
			attrs,
			ev.TraceID,
			ev.Source,
			ev.Region,
			ev.Version,
		)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("insert failed: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit failed: %w", err)
	}

	return nil
}

// Query retrieves log events based on filters.
func (c *Client) Query(ctx context.Context, req models.QueryRequest) ([]models.LogEvent, int64, error) {
	where := "WHERE timestamp >= ? AND timestamp <= ?"
	args := []interface{}{req.StartTime, req.EndTime}

	if req.Service != "" {
		where += " AND service = ?"
		args = append(args, req.Service)
	}
	if req.Severity != "" {
		where += " AND severity = ?"
		args = append(args, req.Severity)
	}
	if req.Search != "" {
		where += " AND message LIKE ?"
		args = append(args, "%"+req.Search+"%")
	}

	// Get total count
	var total int64
	countQuery := fmt.Sprintf("SELECT count() FROM logs %s", where)
	if err := c.db.QueryRowContext(ctx, countQuery, args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count query failed: %w", err)
	}

	// Get results
	limit := req.Limit
	if limit <= 0 || limit > 1000 {
		limit = 100
	}

	query := fmt.Sprintf(`
		SELECT event_id, timestamp, service, severity, message, attributes, trace_id, source, region, version
		FROM logs %s
		ORDER BY timestamp DESC
		LIMIT ? OFFSET ?
	`, where)
	args = append(args, limit, req.Offset)

	rows, err := c.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, 0, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	var results []models.LogEvent
	for rows.Next() {
		var ev models.LogEvent
		var attrs string
		if err := rows.Scan(
			&ev.EventID,
			&ev.Timestamp,
			&ev.Service,
			&ev.Severity,
			&ev.Message,
			&attrs,
			&ev.TraceID,
			&ev.Source,
			&ev.Region,
			&ev.Version,
		); err != nil {
			return nil, 0, fmt.Errorf("scan failed: %w", err)
		}
		results = append(results, ev)
	}

	return results, total, nil
}

// AggregateCount returns event counts grouped by service and severity.
func (c *Client) AggregateCount(ctx context.Context, startTime, endTime time.Time) (map[string]map[string]int64, error) {
	query := `
		SELECT service, severity, count()
		FROM logs
		WHERE timestamp >= ? AND timestamp <= ?
		GROUP BY service, severity
	`

	rows, err := c.db.QueryContext(ctx, query, startTime, endTime)
	if err != nil {
		return nil, fmt.Errorf("aggregate query failed: %w", err)
	}
	defer rows.Close()

	results := make(map[string]map[string]int64)
	for rows.Next() {
		var service, severity string
		var count int64
		if err := rows.Scan(&service, &severity, &count); err != nil {
			return nil, fmt.Errorf("scan failed: %w", err)
		}
		if results[service] == nil {
			results[service] = make(map[string]int64)
		}
		results[service][severity] = count
	}

	return results, nil
}

// Timeline returns event counts over time buckets.
func (c *Client) Timeline(ctx context.Context, service string, startTime, endTime time.Time, bucketSeconds int) ([]map[string]interface{}, error) {
	query := `
		SELECT
			toStartOfInterval(timestamp, INTERVAL ? second) as bucket,
			count() as count,
			countIf(severity = 'ERROR') as error_count
		FROM logs
		WHERE timestamp >= ? AND timestamp <= ?
	`
	args := []interface{}{bucketSeconds, startTime, endTime}

	if service != "" {
		query += " AND service = ?"
		args = append(args, service)
	}

	query += " GROUP BY bucket ORDER BY bucket"

	rows, err := c.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("timeline query failed: %w", err)
	}
	defer rows.Close()

	var results []map[string]interface{}
	for rows.Next() {
		var bucket time.Time
		var count, errorCount int64
		if err := rows.Scan(&bucket, &count, &errorCount); err != nil {
			return nil, fmt.Errorf("scan failed: %w", err)
		}
		results = append(results, map[string]interface{}{
			"bucket":      bucket,
			"count":       count,
			"error_count": errorCount,
		})
	}

	return results, nil
}

// IsDuplicate checks if an event has already been processed.
func (c *Client) IsDuplicate(ctx context.Context, eventID string) (bool, error) {
	var count int64
	query := "SELECT count() FROM processed_events WHERE event_id = ?"
	if err := c.db.QueryRowContext(ctx, query, eventID).Scan(&count); err != nil {
		return false, err
	}
	return count > 0, nil
}

// MarkProcessed marks an event as processed.
func (c *Client) MarkProcessed(ctx context.Context, eventID string) error {
	query := "INSERT INTO processed_events (event_id, processed_at) VALUES (?, ?)"
	_, err := c.db.ExecContext(ctx, query, eventID, time.Now())
	return err
}

// Close closes the database connection.
func (c *Client) Close() error {
	return c.db.Close()
}
