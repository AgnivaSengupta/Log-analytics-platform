# Quick Start Guide

## 1. Start the Platform

```bash
cd log-analytics-platform

# Start all services
docker compose up -d

# Wait for services to be healthy (~60 seconds)
docker compose ps

# Initialize topics and schema (optional, auto-created by services)
./scripts/init.sh
```

## 2. Access the UI

| Service | URL | Credentials |
|---------|-----|-------------|
| **Search UI** | http://localhost:3000 | - |
| **Gateway API** | http://localhost:8080 | - |
| **Query API** | http://localhost:8081 | - |
| **Grafana** | http://localhost:3001 | admin / admin |
| **Prometheus** | http://localhost:9090 | - |
| **MinIO Console** | http://localhost:9002 | PLACEHOLDER_S3_ACCESS_KEY / PLACEHOLDER_S3_SECRET_KEY |

## 3. Ingest Sample Logs

```bash
# Send a test event
curl -X POST http://localhost:8080/v1/ingest \
  -H "Content-Type: application/json" \
  -d '{
    "events": [{
      "event_id": "test-001",
      "timestamp": "'$(date -u +%Y-%m-%dT%H:%M:%S.000Z)'",
      "service": "payment",
      "severity": "ERROR",
      "message": "payment authorization timed out",
      "attributes": {"http.status_code": 504},
      "trace_id": "abc123"
    }]
  }'

# Verify it was accepted
curl http://localhost:8081/v1/search \
  -H "Content-Type: application/json" \
  -d '{
    "service": "payment",
    "start_time": "'$(date -u -d '1 hour ago' +%Y-%m-%dT%H:%M:%S.000Z 2>/dev/null || date -u -v-1H +%Y-%m-%dT%H:%M:%S.000Z)'",
    "end_time": "'$(date -u +%Y-%m-%dT%H:%M:%S.000Z)'",
    "limit": 10
  }'
```

## 4. Run Benchmarks

```bash
# Full benchmark suite (throughput + detection tests)
./scripts/benchmark.sh

# Or run individual tests:

# Throughput test: 10K → 25K → 50K → 100K events/sec
docker compose run --rm load-generator /bin/service \
  -gateway http://gateway:8080 \
  -step \
  -workers 20 \
  -batch 200

# Sustained load test: 50K events/sec for 5 minutes
docker compose run --rm load-generator /bin/service \
  -gateway http://gateway:8080 \
  -rate 50000 \
  -duration 300s \
  -workers 20

# Error rate spike test (triggers detection alerts)
docker compose run --rm load-generator /bin/service \
  -gateway http://gateway:8080 \
  -rate 25000 \
  -duration 120s \
  -error-rate 0.08
```

## 5. Test Failure Recovery

```bash
# Run the failure recovery test
./scripts/failure-test.sh

# This will:
# 1. Start sustained load
# 2. Kill a worker
# 3. Show consumer lag increase
# 4. Restart worker
# 5. Show recovery
```

## 6. Scale the Platform

```bash
# Scale gateways (stateless)
docker compose up -d --scale gateway=4

# Scale processing workers (consumer group auto-balances)
docker compose up -d --scale worker=4

# Scale archive writers
docker compose up -d --scale archive-writer=2
```

## 7. Monitor with Grafana

1. Open http://localhost:3001
2. Login: `admin` / `admin`
3. Prometheus datasource is pre-configured
4. Create dashboards using these metrics:
   - `gateway_events_ingested_total`
   - `worker_events_processed_total`
   - `archive_events_total`
   - `detection_alerts_fired_total`
   - `query_coordinator_latency_seconds`

## 8. View Logs

```bash
# All services
docker compose logs -f

# Specific service
docker compose logs -f gateway
docker compose logs -f worker
docker compose logs -f detection

# Tail Kafka topic
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic logs \
  --from-beginning

# Check ClickHouse
docker compose exec clickhouse clickhouse-client
# Then run: SELECT count() FROM log_analytics.logs;
```

## 9. Common Issues

### Services not starting
```bash
# Check logs
docker compose logs <service-name>

# Restart specific service
docker compose restart <service-name>

# Full rebuild
docker compose down
docker compose build --no-cache
docker compose up -d
```

### Kafka connection issues
```bash
# Check Kafka is healthy
docker compose exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092

# List topics
docker compose exec kafka kafka-topics --list --bootstrap-server localhost:9092
```

### ClickHouse connection issues
```bash
# Check ClickHouse is healthy
docker compose exec clickhouse clickhouse-client --query "SELECT 1"

# View tables
docker compose exec clickhouse clickhouse-client --query "SHOW TABLES FROM log_analytics"
```

## 10. Stop the Platform

```bash
# Stop all services (data preserved)
docker compose stop

# Stop and remove containers (data preserved in volumes)
docker compose down

# Stop and remove everything including data
docker compose down -v
```

## Architecture Quick Reference

```
Gateway (8080) → Kafka → Worker → ClickHouse
                       → Archive Writer → S3/MinIO
                       → Detection → Alert Service
                       
Query Coordinator (8081) → ClickHouse (hot) + S3 (cold)
```

## Key Files

- `docker-compose.yml` - Full stack orchestration
- `.env` - Environment configuration
- `cmd/` - All Go service implementations
- `ui/` - React frontend
- `scripts/` - Benchmark and test scripts
- `configs/` - Prometheus, Grafana, OpenTelemetry configs

## Next Steps

1. **Explore the UI** at http://localhost:3000
2. **Run benchmarks** to measure throughput
3. **Test failure scenarios** to verify resilience
4. **Create Grafana dashboards** for monitoring
5. **Review the code** in `cmd/` to understand the implementation
6. **Read the full README.md** for detailed documentation
