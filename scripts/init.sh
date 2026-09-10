#!/bin/bash
# Initialize the platform: validate config, create Kafka topics, probe ClickHouse + R2.
#
# Hot storage is self-hosted ClickHouse in docker-compose. The cold archive
# (Cloudflare R2) is provisioned outside Docker (see MANAGED_SERVICES_SETUP.md).

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.."

echo "==========================================="
echo "  Platform Initialization"
echo "==========================================="
echo ""

# 1. Load and validate .env
if [ ! -f .env ]; then
    echo "ERROR: .env not found. Copy .env.example to .env and fill in R2 credentials (see MANAGED_SERVICES_SETUP.md)."
    exit 1
fi

set -a
source .env
set +a

echo "Checking configuration..."
missing=0
for var in S3_ENDPOINT S3_BUCKET S3_ACCESS_KEY S3_SECRET_KEY; do
    value="$(printenv "$var")"
    if [ -z "$value" ]; then
        echo "  MISSING: $var is empty"
        missing=1
    elif [[ "$value" == REPLACE_WITH_* ]]; then
        echo "  PLACEHOLDER: $var still has its template value"
        missing=1
    fi
done
if [ "$missing" -ne 0 ]; then
    echo ""
    echo "Fill in .env first - see MANAGED_SERVICES_SETUP.md."
    exit 1
fi

if [[ "$S3_ENDPOINT" != https://*.r2.cloudflarestorage.com ]]; then
    echo "  WARNING: S3_ENDPOINT does not look like an R2 endpoint: $S3_ENDPOINT"
fi
if [ "$S3_REGION" != "auto" ]; then
    echo "  WARNING: S3_REGION should be 'auto' for R2 (got '$S3_REGION')"
fi
echo "OK: configuration looks valid"
echo ""

# 2. Wait for Kafka
echo "Waiting for Kafka..."
until docker compose exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092 > /dev/null 2>&1; do
    echo "  Kafka not ready, waiting..."
    sleep 5
done
echo "OK: Kafka is ready"
echo ""

# 3. Create Kafka topics
echo "Creating Kafka topics..."
docker compose exec kafka kafka-topics --create --bootstrap-server localhost:9092 \
    --topic logs --partitions 12 --replication-factor 1 --if-not-exists
docker compose exec kafka kafka-topics --create --bootstrap-server localhost:9092 \
    --topic logs-dlq --partitions 6 --replication-factor 1 --if-not-exists
docker compose exec kafka kafka-topics --create --bootstrap-server localhost:9092 \
    --topic alerts --partitions 6 --replication-factor 1 --if-not-exists
echo "OK: Topics created"
echo ""

# 4. Verify ClickHouse table
CH_URL="${CLICKHOUSE_URL:-http://localhost:8123}"
# Host-side probe uses the published HTTP port.
if [[ "$CH_URL" == *clickhouse:8123* ]]; then
    CH_URL="http://localhost:8123"
fi
CH_USER="${CLICKHOUSE_READ_USER:-logs_read}"
CH_PASS="${CLICKHOUSE_READ_PASSWORD:-read}"
CH_TABLE="${CLICKHOUSE_TABLE:-logs}"
echo "Verifying ClickHouse ($CH_URL)..."
if ! curl -sf -u "$CH_USER:$CH_PASS" --data-binary "SELECT count() AS total FROM ${CH_TABLE} LIMIT 1 FORMAT JSON" "$CH_URL/" > /dev/null; then
    echo ""
    echo "ERROR: ClickHouse query failed. Check that:"
    echo "  - the clickhouse service is healthy (docker compose ps)"
    echo "  - configs/clickhouse/init.sql created table '$CH_TABLE'"
    echo "  - CLICKHOUSE_READ_USER / CLICKHOUSE_READ_PASSWORD can SELECT"
    exit 1
fi
echo "OK: ClickHouse table '$CH_TABLE' is queryable"
echo ""

# 5. Verify R2 endpoint reachability. An unsigned request cannot authenticate,
# so any HTTP response (typically 403) only proves DNS/TLS work; bucket
# access itself is verified by the archive writer at runtime.
echo "Verifying R2 endpoint ($S3_ENDPOINT)..."
http_code="$(curl -s -o /dev/null -w '%{http_code}' --max-time 15 "$S3_ENDPOINT/" || echo 000)"
if [ "$http_code" = "000" ]; then
    echo "ERROR: cannot reach $S3_ENDPOINT - check the endpoint and network."
    exit 1
fi
echo "OK: R2 endpoint reachable (HTTP $http_code on unsigned request)"
echo ""

# 6. Show services
echo "Verifying services..."
echo ""
docker compose ps

echo ""
echo "==========================================="
echo "  Platform ready!"
echo "==========================================="
echo ""
echo "  UI:         http://localhost:3000"
echo "  Gateway:    http://localhost:8080"
echo "  Query API:  http://localhost:8081"
echo "  ClickHouse: http://localhost:8123"
echo "  Grafana:    http://localhost:3001"
echo "  Prometheus: http://localhost:9090"
echo ""
echo "Ingest the smoke-test event from MANAGED_SERVICES_SETUP.md,"
echo "then search for it at http://localhost:3000"
