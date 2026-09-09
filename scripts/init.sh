#!/bin/bash
# Initialize the platform: validate managed-service config, create Kafka topics.
#
# Hot storage (Tinybird) and the cold archive (Cloudflare R2) are managed
# services provisioned outside Docker (see MANAGED_SERVICES_SETUP.md). This
# script checks that .env points at them correctly and prepares Kafka.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.."

echo "==========================================="
echo "  Platform Initialization"
echo "==========================================="
echo ""

# 1. Load and validate .env
if [ ! -f .env ]; then
    echo "ERROR: .env not found. Copy .env.example to .env and fill it in (see MANAGED_SERVICES_SETUP.md)."
    exit 1
fi

set -a
source .env
set +a

echo "Checking managed-service configuration..."
missing=0
for var in TINYBIRD_API_URL TINYBIRD_APPEND_TOKEN TINYBIRD_READ_TOKEN TINYBIRD_DATASOURCE S3_ENDPOINT S3_BUCKET S3_ACCESS_KEY S3_SECRET_KEY; do
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

# 4. Verify Tinybird auth + datasource
echo "Verifying Tinybird ($TINYBIRD_API_URL)..."
query="$(printf '{"q":"SELECT count() AS total FROM %s LIMIT 1 FORMAT JSON"}' "$TINYBIRD_DATASOURCE")"
if ! curl -sf -X POST "$TINYBIRD_API_URL/v0/sql" \
    -H "Authorization: Bearer $TINYBIRD_READ_TOKEN" \
    -H "Content-Type: application/json" \
    -d "$query" > /dev/null; then
    echo ""
    echo "ERROR: Tinybird query failed. Check that:"
    echo "  - TINYBIRD_API_URL matches your workspace region"
    echo "  - the datasource exists (push tinybird/datasources/logs.datasource)"
    echo "  - TINYBIRD_READ_TOKEN has read access to the datasource"
    exit 1
fi
echo "OK: Tinybird datasource '$TINYBIRD_DATASOURCE' is queryable"
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
echo "  Grafana:    http://localhost:3001"
echo "  Prometheus: http://localhost:9090"
echo ""
echo "Ingest the smoke-test event from MANAGED_SERVICES_SETUP.md,"
echo "then search for it at http://localhost:3000"
