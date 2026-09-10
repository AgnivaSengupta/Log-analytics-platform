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
    version LowCardinality(String)
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (service, severity, timestamp)
TTL timestamp + INTERVAL 30 DAY;

CREATE USER IF NOT EXISTS logs_append IDENTIFIED WITH sha256_password BY 'append';
CREATE USER IF NOT EXISTS logs_read IDENTIFIED WITH sha256_password BY 'read';

GRANT INSERT ON default.logs TO logs_append;
GRANT SELECT ON default.logs TO logs_read;