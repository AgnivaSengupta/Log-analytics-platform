#!/bin/bash
# Benchmark script for the distributed log analytics platform
# Tests: throughput, failure recovery, and real-time detection

set -e

GATEWAY_URL="${GATEWAY_URL:-http://localhost:8080}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RESULTS_DIR="${SCRIPT_DIR}/../benchmark-results"

mkdir -p "$RESULTS_DIR"

echo "╔══════════════════════════════════════════════════╗"
echo "║   Distributed Log Analytics - Benchmark Suite    ║"
echo "╚══════════════════════════════════════════════════╝"
echo ""
echo "Gateway: $GATEWAY_URL"
echo "Results: $RESULTS_DIR"
echo ""

# Check gateway is available
if ! curl -sf "$GATEWAY_URL/health" > /dev/null 2>&1; then
    echo "❌ Gateway not available at $GATEWAY_URL"
    echo "   Start with: docker compose up -d"
    exit 1
fi

echo "✅ Gateway is healthy"
echo ""

# ==========================================
# Test 1: Throughput Stepping
# ==========================================
echo "═══════════════════════════════════════════"
echo "  TEST 1: THROUGHPUT STEPPING"
echo "  10K → 25K → 50K → 100K events/sec"
echo "═══════════════════════════════════════════"
echo ""

docker compose run --rm load-generator /bin/service \
    -gateway "$GATEWAY_URL" \
    -step \
    -workers 20 \
    -batch 200 \
    -error-rate 0.002 \
    2>&1 | tee "$RESULTS_DIR/throughput-test.log"

echo ""

# ==========================================
# Test 2: Sustained 50K for 5 minutes
# ==========================================
echo "═══════════════════════════════════════════"
echo "  TEST 2: SUSTAINED LOAD (50K/sec, 5min)"
echo "═══════════════════════════════════════════"
echo ""

docker compose run --rm load-generator /bin/service \
    -gateway "$GATEWAY_URL" \
    -rate 50000 \
    -duration 300s \
    -workers 20 \
    -batch 200 \
    -error-rate 0.002 \
    2>&1 | tee "$RESULTS_DIR/sustained-test.log"

echo ""

# ==========================================
# Test 3: Error Rate Spike (Detection)
# ==========================================
echo "═══════════════════════════════════════════"
echo "  TEST 3: ERROR RATE SPIKE (DETECTION)"
echo "  Normal → 8% errors to trigger alert"
echo "═══════════════════════════════════════════"
echo ""

# Phase 1: Normal error rate
echo "Phase 1: Normal traffic (0.2% errors, 60s)..."
docker compose run --rm load-generator /bin/service \
    -gateway "$GATEWAY_URL" \
    -rate 25000 \
    -duration 60s \
    -workers 10 \
    -batch 100 \
    -error-rate 0.002 \
    2>&1 | tee "$RESULTS_DIR/detection-phase1.log"

echo ""

# Phase 2: Spike errors
echo "Phase 2: Error spike (8% errors, 120s)..."
docker compose run --rm load-generator /bin/service \
    -gateway "$GATEWAY_URL" \
    -rate 25000 \
    -duration 120s \
    -workers 10 \
    -batch 100 \
    -error-rate 0.08 \
    2>&1 | tee "$RESULTS_DIR/detection-phase2.log"

echo ""

# ==========================================
# Summary
# ==========================================
echo "═══════════════════════════════════════════"
echo "  BENCHMARK COMPLETE"
echo "═══════════════════════════════════════════"
echo ""
echo "Results saved to: $RESULTS_DIR/"
ls -la "$RESULTS_DIR/"
echo ""
echo "View Grafana dashboards at: http://localhost:3001"
echo "View UI at: http://localhost:3000"
