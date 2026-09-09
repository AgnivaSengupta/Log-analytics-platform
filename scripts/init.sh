#!/bin/bash
# Initialize the platform: create topics, ensure buckets, verify connectivity

set -e

echo "═══════════════════════════════════════════"
echo "  Platform Initialization"
echo "═══════════════════════════════════════════"
echo ""

# Wait for Kafka
echo "Waiting for Kafka..."
until docker compose exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092 > /dev/null 2>&1; do
    echo "  Kafka not ready, waiting..."
    sleep 5
done
echo "✅ Kafka is ready"

# Wait for ClickHouse
echo "Waiting for ClickHouse..."
until docker compose exec clickhouse clickhouse-client --query "SELECT 1" > /dev/null 2>&1; do
    echo "  ClickHouse not ready, waiting..."
    sleep 5
done
echo "✅ ClickHouse is ready"

# Wait for MinIO
echo "Waiting for MinIO..."
until curl -sf http://localhost:9001/minio/health/live > /dev/null 2>&1; do
    echo "  MinIO not ready, waiting..."
    sleep 5
done
echo "✅ MinIO is ready"

# Create Kafka topics
echo ""
echo "Creating Kafka topics..."
docker compose exec kafka kafka-topics --create --bootstrap-server localhost:9092 \
    --topic logs --partitions 12 --replication-factor 1 --if-not-exists
docker compose exec kafka kafka-topics --create --bootstrap-server localhost:9092 \
    --topic logs-dlq --partitions 6 --replication-factor 1 --if-not-exists
docker compose exec kafka kafka-topics --create --bootstrap-server localhost:9092 \
    --topic alerts --partitions 6 --replication-factor 1 --if-not-exists
echo "✅ Topics created"

# Create MinIO bucket
echo ""
echo "Creating MinIO bucket..."
docker compose exec minio mc alias set local http://localhost:9000 PLACEHOLDER_S3_ACCESS_KEY PLACEHOLDER_S3_SECRET_KEY 2>/dev/null || true
docker compose exec minio mc mb local/log-archive --ignore-existing 2>/dev/null || true
echo "✅ Bucket created"

# Initialize ClickHouse schema
echo ""
echo "Initializing ClickHouse schema..."
docker compose exec clickhouse clickhouse-client --query "
    CREATE DATABASE IF NOT EXISTS log_analytics;

    CREATE TABLE IF NOT EXISTS log_analytics.logs (
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
    TTL timestamp + INTERVAL 30 DAY;

    CREATE TABLE IF NOT EXISTS log_analytics.processed_events (
        event_id String,
        processed_at DateTime
    ) ENGINE = ReplacingMergeTree(processed_at)
    ORDER BY event_id
    TTL processed_at + INTERVAL 7 DAY;
"
echo "✅ Schema initialized"

# Verify services
echo ""
echo "Verifying services..."
echo ""
docker compose ps

echo ""
echo "═══════════════════════════════════════════"
echo "  Platform ready!"
echo "═══════════════════════════════════════════"
echo ""
echo "  UI:         http://localhost:3000"
echo "  Gateway:    http://localhost:8080"
echo "  Query API:  http://localhost:8081"
echo "  Grafana:    http://localhost:3001"
echo "  Prometheus: http://localhost:9090"
echo ""
