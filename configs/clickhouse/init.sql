CREATE TABLE IF NOT EXISTS logs
(
    event_id String,
    timestamp DateTime64(3),
    service LowCardinality(String),
    severity LowCardinality(String),
    message String,
    attributes String,
    trace_id String,
    source LowCardinality(String),
    region LowCardinality(String),
    version LowCardinality(String),

    INDEX idx_msg_ngram message TYPE ngrambf_v1(4, 32768, 2, 0) GRANULARITY 1,
    INDEX idx_trace_id trace_id TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_event_id event_id TYPE bloom_filter(0.01) GRANULARITY 1
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (service, timestamp, severity, event_id)
TTL toDateTime(timestamp) + INTERVAL 30 DAY;

-- Summing Materialized View table for pre-aggregated UI charts (Timeline & Aggregates)
CREATE TABLE IF NOT EXISTS logs_metrics_mv
(
    bucket DateTime,
    service LowCardinality(String),
    severity LowCardinality(String),
    count UInt64,
    error_count UInt64
)
ENGINE = SummingMergeTree
PARTITION BY toYYYYMMDD(bucket)
ORDER BY (service, severity, bucket)
TTL bucket + INTERVAL 30 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS logs_metrics_mv_trigger TO logs_metrics_mv AS
SELECT
    toStartOfMinute(timestamp) AS bucket,
    service,
    severity,
    count() AS count,
    countIf(severity = 'ERROR') AS error_count
FROM logs
GROUP BY bucket, service, severity;

CREATE USER IF NOT EXISTS logs_append IDENTIFIED WITH sha256_password BY 'append';
CREATE USER IF NOT EXISTS logs_read IDENTIFIED WITH sha256_password BY 'read';

GRANT INSERT, SELECT ON default.logs TO logs_append;
GRANT INSERT, SELECT ON default.logs_metrics_mv TO logs_append;
GRANT SELECT ON default.logs TO logs_read;
GRANT SELECT ON default.logs_metrics_mv TO logs_read;