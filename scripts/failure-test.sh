#!/bin/bash
# Failure recovery test: kill a worker during sustained load and observe recovery

set -e

GATEWAY_URL="${GATEWAY_URL:-http://localhost:8080}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "╔══════════════════════════════════════════════════╗"
echo "║   Failure Recovery Test                          ║"
echo "╚══════════════════════════════════════════════════╝"
echo ""

# Start sustained load in background
echo "Starting sustained load (25K events/sec)..."
docker compose run -d --name load-test --rm load-generator /bin/service \
    -gateway "$GATEWAY_URL" \
    -rate 25000 \
    -duration 180s \
    -workers 10 \
    -batch 100 \
    -error-rate 0.002 &
LOAD_PID=$!

echo "Waiting 30 seconds for steady state..."
sleep 30

# Show initial consumer lag
echo ""
echo "=== Before failure ==="
echo "Worker containers running:"
docker compose ps worker
echo ""

# Kill a worker
echo "💥 Killing a processing worker..."
WORKER_CONTAINER=$(docker compose ps -q worker | head -1)
if [ -n "$WORKER_CONTAINER" ]; then
    docker kill "$WORKER_CONTAINER"
    echo "Worker killed: $WORKER_CONTAINER"
else
    echo "No worker container found, starting one to kill..."
    docker compose up -d worker
    sleep 5
    WORKER_CONTAINER=$(docker compose ps -q worker | head -1)
    docker kill "$WORKER_CONTAINER"
    echo "Worker killed: $WORKER_CONTAINER"
fi

echo ""
echo "Waiting 15 seconds for rebalance..."
sleep 15

echo ""
echo "=== After failure (rebalance in progress) ==="
docker compose ps worker
echo ""

# Restart the worker
echo "🔄 Restarting worker..."
docker compose up -d worker
sleep 10

echo ""
echo "=== After recovery ==="
docker compose ps worker
echo ""

echo "Waiting for load test to complete..."
wait $LOAD_PID 2>/dev/null || true

echo ""
echo "═══════════════════════════════════════════"
echo "  FAILURE RECOVERY TEST COMPLETE"
echo "═══════════════════════════════════════════"
echo ""
echo "Check Grafana for consumer lag metrics:"
echo "  http://localhost:3001"
echo ""
echo "Expected observations:"
echo "  1. Consumer lag increased when worker was killed"
echo "  2. Consumer group rebalanced partitions"
echo "  3. Lag recovered after worker restart"
echo "  4. No acknowledged events were lost"
