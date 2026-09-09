# Distributed Log Analytics Platform

A production-grade distributed log analytics system built with Go, Kafka, Tinybird, and Cloudflare R2. Designed to demonstrate distributed-system fundamentals: partitioning, replay, horizontal scaling, backpressure, and failure recovery.

Kafka, the gateways, workers, detection, alerting, and the UI run in Docker. Tinybird is the managed hot analytics store and Cloudflare R2 is the managed raw archive. No local database or object-store containers are used.

## Architecture

```
Producers → OpenTelemetry Collector → Load Balancer → Gateways → Kafka
                                                              │
                          ┌──────────────────────────────────────┤
                          │                                      │
                    Processing Workers              Raw Archive Writer
                          │                                      │
                       Tinybird                           Cloudflare R2
                       (managed)                            (managed)
                          │                                      │
                          └──────── Query Coordinator ───────────┘
                                              │
                                          Search API → React UI

Kafka → Detection Service → Alert Service → Webhook/Pager
```

## Quick Start

### Prerequisites
- Docker and Docker Compose
- A Tinybird workspace with a `logs` datasource (see `tinybird/datasources/logs.datasource`)
- A Cloudflare R2 bucket with an API token (object read + write)
- 4GB+ RAM recommended

> **First-time setup:** copy `.env.example` to `.env`, provision the managed
> services and fill it in by following
> [MANAGED_SERVICES_SETUP.md](MANAGED_SERVICES_SETUP.md) before starting the
> stack.

### Start the Platform

```bash
# Start Kafka and all application services
docker compose up -d --build

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
| Tinybird dashboard | Your Tinybird workspace (managed) |
| R2 bucket browser | Cloudflare dashboard -> R2 (managed) |

### Run Benchmarks

```bash
# Full benchmark suite (throughput + detection + sustained load)
chmod +x scripts/benchmark.sh
./scripts/benchmark.sh
# Windows PowerShell: powershell -ExecutionPolicy Bypass -File .\scripts\benchmark.ps1

# Failure recovery test
chmod +x scripts/failure-test.sh
./scripts/failure-test.sh
# Windows PowerShell: powershell -ExecutionPolicy Bypass -File .\scripts\failure-test.ps1

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
Kafka consumer group that normalizes, enriches, and redacts events before appending them to Tinybird via the Events API (`wait=true`). Commits Kafka offsets only after Tinybird acknowledges the batch.

- **Batch size:** 1000 events or 2-second flush interval
- **Dead letter queue:** Unparseable events go to `logs-dlq` topic

### Archive Writer (`cmd/archive-writer`)
Independent Kafka consumer that writes raw events to Cloudflare R2 for long-term retention. Preserves original events before normalization for compliance and reprocessing. Commits offsets only after R2 confirms the object write.

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
- **Hot (0-7 days):** Tinybird for fast interactive queries
- **Cold (7+ days):** Cloudflare R2 for historical analysis
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
- **At-least-once** delivery end to end
- Gateway ACKs only after Kafka replication succeeds
- Worker offsets committed only after Tinybird acknowledges the batch
- Archive offsets committed only after R2 confirms the object write
- Unparseable events are diverted to the `logs-dlq` topic, never silently dropped

## Configuration

All configuration is via environment variables (see `.env`):

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BROKERS` | `kafka:29092` | Kafka bootstrap servers (internal listener) |
| `KAFKA_TOPIC_LOGS` | `logs` | Main log topic |
| `TINYBIRD_API_URL` | `https://api.tinybird.co` | Tinybird API host for your region |
| `TINYBIRD_DATASOURCE` | `logs` | Tinybird datasource name |
| `TINYBIRD_APPEND_TOKEN` | (required) | Token with `DATASOURCE:APPEND` on the datasource |
| `TINYBIRD_READ_TOKEN` | (required) | Token with read access to the datasource |
| `S3_ENDPOINT` | (required) | R2 S3 endpoint: `https://<account-id>.r2.cloudflarestorage.com` |
| `S3_REGION` | `auto` | R2 region (always `auto`) |
| `S3_BUCKET` | `log-archive` | R2 bucket (created in Cloudflare) |
| `S3_ACCESS_KEY` | (required) | R2 API token access key ID |
| `S3_SECRET_KEY` | (required) | R2 API token secret |
| `GATEWAY_INGEST_QUOTA_PER_SEC` | `100000` | Per-gateway rate limit |
| `DETECTION_ERROR_RATE_THRESHOLD` | `0.05` | Error rate alert threshold |
| `QUERY_HOT_RETENTION_DAYS` | `7` | Days served from Tinybird before cold |

The `S3_*` variables configure R2 over its S3-compatible API; no AWS or
local object store is involved.

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
4. **Storage:** Tinybird append latency, query P50/P95/P99, R2 write rate
5. **Detection:** Alert rate, detection latency, window states

## Failure Scenarios

| Scenario | Expected Behavior |
|----------|-------------------|
| Worker crash | Consumer group rebalances; another worker resumes from last committed offset |
| Tinybird unavailable | Kafka backlog grows; archive/detection remain independent; backlog drains on recovery |
| Traffic spike | Kafka buffers excess; gateways issue 429 above quota |
| Gateway crash | LB routes to healthy replicas; producers retry with idempotency |
| Bad event format | Processor sends to dead-letter topic for inspection |
| R2 unavailable | Archive writer retries; main processing pipeline unaffected |

## Project Structure

```
log-analytics-platform/
├── cmd/
│   ├── gateway/              # Ingestion gateway
│   ├── worker/               # Processing workers
│   ├── archive-writer/       # R2 archiver
│   ├── detection/            # Stream detection
│   ├── query-coordinator/    # Unified query API
│   ├── alert-service/        # Alert dispatcher
│   └── load-generator/       # Benchmark tool
├── internal/
│   ├── models/               # Shared data models
│   ├── config/               # Configuration loading
│   ├── kafka/                # Kafka producer/consumer
│   ├── tinybird/             # Tinybird client
│   ├── storage/              # Cloudflare R2 client (S3 API)
│   └── detection/            # Detection engine
├── ui/                       # React frontend
├── configs/                  # Prometheus, Grafana, OTEL configs
├── scripts/                  # Benchmark and test scripts
├── docker-compose.yml        # Full stack orchestration
└── .env                      # Environment configuration
```

## License

MIT
