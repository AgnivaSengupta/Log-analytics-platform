# Distributed Log Analytics Platform

A production-grade distributed log analytics system built with Go, Kafka, ClickHouse, and S3/MinIO. Designed to demonstrate distributed-system fundamentals: partitioning, replay, horizontal scaling, backpressure, and failure recovery.

## Architecture

```
Producers → OpenTelemetry Collector → Load Balancer → Gateways → Kafka
                                                              │
                          ┌──────────────────────────────────────┤
                          │                                      │
                    Processing Workers              Raw Archive Writer
                          │                                      │
                     ClickHouse                              S3 / MinIO
                          │                                      │
                          └──────── Query Coordinator ───────────┘
                                              │
                                          Search API → React UI

Kafka → Detection Service → Alert Service → Webhook/Pager
```

## Quick Start

### Prerequisites
- Docker and Docker Compose
- 8GB+ RAM recommended

### Start the Platform

```bash
# Start all infrastructure and application services
docker compose up -d

# Wait for services to be healthy (~60 seconds)
docker compose ps

# Check gateway health
curl http://localhost:8080/health
```

### Access the UI

| Service | URL |
|---------|-----|
| Search UI | http://localhost:3000 |
| Query API | http://localhost:8081 |
| Gateway | http://localhost:8080 |
| Grafana | http://localhost:3001 (admin/admin) |
| Prometheus | http://localhost:9090 |
| MinIO Console | http://localhost:9002 |

### Run Benchmarks

```bash
# Full benchmark suite (throughput + detection + sustained load)
chmod +x scripts/benchmark.sh
./scripts/benchmark.sh

# Failure recovery test
chmod +x scripts/failure-test.sh
./scripts/failure-test.sh

# Manual load test
docker compose run --rm load-generator /bin/service \
    -gateway http://gateway:8080 \
    -rate 50000 \
    -duration 60s \
    -workers 15 \
    -batch 200
```

## Components

### Gateway (`cmd/gateway`)
Stateless HTTP ingestion gateway. Authenticates producers, validates events, enforces rate quotas, and publishes to Kafka. Scales horizontally behind a load balancer.

- **Port:** 8080
- **Metrics:** `/metrics`
- **Health:** `/health`

### Processing Workers (`cmd/worker`)
Kafka consumer group that normalizes, enriches, and redacts events before batch-inserting into ClickHouse. Implements idempotent processing via event ID deduplication.

- **Batch size:** 1000 events or 2-second flush interval
- **Dead letter queue:** Unparseable events go to `logs-dlq` topic

### Archive Writer (`cmd/archive-writer`)
Independent Kafka consumer that writes raw events to S3/MinIO for long-term retention. Preserves original events before normalization for compliance and reprocessing.

- **Partitioning:** `raw/YYYY/MM/DD/HH/service/`
- **Format:** Newline-delimited JSON (JSONL)
- **Batch size:** 500 events or 10-second flush interval

### Detection Service (`cmd/detection`)
Real-time stream processor that evaluates sliding-window rules:
- Error rate threshold (>5% triggers alert)
- Recurring error fingerprinting
- Deduplicated alerting with 15-minute cooldown

### Query Coordinator (`cmd/query-coordinator`)
Unified search API that routes queries to the appropriate storage tier:
- **Hot (0-7 days):** ClickHouse for fast interactive queries
- **Cold (7+ days):** S3/MinIO for historical analysis
- **Merged:** Spans both tiers with result merging

### Alert Service (`cmd/alert-service`)
Consumes from the `alerts` Kafka topic, deduplicates, and dispatches notifications to configured webhooks.

### React UI (`ui/`)
Search and analytics dashboard with:
- Full-text search with filters (service, severity, time range)
- Event timeline visualization
- Expandable log detail view
- Service breakdown and severity distribution
- Pagination for large result sets

## Event Contract

```json
{
  "event_id": "uuid-or-ulid",
  "timestamp": "2026-09-09T08:42:17.123Z",
  "service": "payment",
  "severity": "ERROR",
  "message": "payment authorization timed out",
  "attributes": { "http.status_code": 504 },
  "trace_id": "7f3a9c2d...",
  "source": "otel-collector",
  "region": "us-east-1",
  "version": "1.2.3"
}
```

### Delivery Semantics
- **At-least-once** delivery with idempotent processing
- Gateway ACKs only after Kafka replication succeeds
- Workers deduplicate via `event_id` within retention window
- Consumer offsets committed after durable downstream write

## Configuration

All configuration is via environment variables (see `.env`):

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BROKERS` | `kafka:9092` | Kafka bootstrap servers |
| `KAFKA_TOPIC_LOGS` | `logs` | Main log topic |
| `CLICKHOUSE_HOST` | `clickhouse` | ClickHouse server |
| `CLICKHOUSE_PASSWORD` | (placeholder) | ClickHouse password |
| `S3_ENDPOINT` | `http://minio:9000` | S3/MinIO endpoint |
| `S3_ACCESS_KEY` | (placeholder) | S3 access key |
| `S3_SECRET_KEY` | (placeholder) | S3 secret key |
| `GATEWAY_INGEST_QUOTA_PER_SEC` | `100000` | Per-gateway rate limit |
| `DETECTION_ERROR_RATE_THRESHOLD` | `0.05` | Error rate alert threshold |
| `QUERY_HOT_RETENTION_DAYS` | `7` | Days in ClickHouse before cold |

## Scaling

### Horizontal Scaling
```bash
# Scale gateways (stateless, behind load balancer)
docker compose up -d --scale gateway=4

# Scale processing workers (consumer group auto-balances)
docker compose up -d --scale worker=4

# Scale archive writers
docker compose up -d --scale archive-writer=2
```

### Kafka Partitioning
- **12 partitions** by default (configurable)
- Partition key: `service:source` for per-service ordering
- Max parallel consumers = partition count

## Monitoring

### Platform Metrics (Prometheus)
- `gateway_events_ingested_total` — Ingest rate by service
- `gateway_events_rejected_total` — Quota rejections
- `worker_events_processed_total` — Processing throughput
- `worker_events_dead_lettered_total` — DLQ count
- `archive_events_total` — Archive throughput
- `detection_events_processed_total` — Detection throughput
- `detection_alerts_fired_total` — Alerts triggered
- `query_coordinator_latency_seconds` — Query latency by tier

### Key Dashboards to Create in Grafana
1. **Ingestion Pipeline:** Ingest rate, rejection rate, gateway latency
2. **Processing Pipeline:** Worker throughput, batch size, processing latency
3. **Kafka Health:** Consumer lag, partition distribution
4. **Storage:** ClickHouse insert latency, query P50/P95/P99, S3 write rate
5. **Detection:** Alert rate, detection latency, window states

## Failure Scenarios

| Scenario | Expected Behavior |
|----------|-------------------|
| Worker crash | Consumer group rebalances; another worker resumes from last committed offset |
| ClickHouse down | Kafka backlog grows; archive/detection remain independent; backlog drains on recovery |
| Traffic spike | Kafka buffers excess; gateways issue 429 above quota |
| Gateway crash | LB routes to healthy replicas; producers retry with idempotency |
| Bad event format | Processor sends to dead-letter topic for inspection |
| S3 unavailable | Archive writer retries; main processing pipeline unaffected |

## Project Structure

```
log-analytics-platform/
├── cmd/
│   ├── gateway/              # Ingestion gateway
│   ├── worker/               # Processing workers
│   ├── archive-writer/       # S3 archiver
│   ├── detection/            # Stream detection
│   ├── query-coordinator/    # Unified query API
│   ├── alert-service/        # Alert dispatcher
│   └── load-generator/       # Benchmark tool
├── internal/
│   ├── models/               # Shared data models
│   ├── config/               # Configuration loading
│   ├── kafka/                # Kafka producer/consumer
│   ├── clickhouse/           # ClickHouse client
│   ├── storage/              # S3/MinIO client
│   └── detection/            # Detection engine
├── ui/                       # React frontend
├── configs/                  # Prometheus, Grafana, OTEL configs
├── scripts/                  # Benchmark and test scripts
├── docker-compose.yml        # Full stack orchestration
└── .env                      # Environment configuration
```

## License

MIT
