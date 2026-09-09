#!/bin/bash
# Benchmark script for the distributed log analytics platform
# Tests: throughput, failure recovery, and real-time detection

set -e

GATEWAY_URL="${GATEWAY_URL:-http://localhost:8080}"
# The load-generator runs inside the compose network, where the gateway
# is reachable as http://gateway:8080 (localhost would be itself).
LOAD_GATEWAY_URL="${LOAD_GATEWAY_URL:-http://gateway:8080}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RESULTS_DIR="${SCRIPT_DIR}/../benchmark-results"

# Quick mode is the default (~5 minutes total). Set BENCHMARK_MODE=full
# for the original ~15-minute suite.
BENCHMARK_MODE="${BENCHMARK_MODE:-quick}"
if [ "$BENCHMARK_MODE" = "full" ]; then
    QUICK_FLAG=""
    SUSTAINED_SECS=300
    DETECT_P1_SECS=60
    DETECT_P2_SECS=120
else
    QUICK_FLAG="-quick"
    SUSTAINED_SECS=120
    DETECT_P1_SECS=30
    DETECT_P2_SECS=60
fi

mkdir -p "$RESULTS_DIR"

echo "╔══════════════════════════════════════════════════╗"
echo "║   Distributed Log Analytics - Benchmark Suite    ║"
echo "╚══════════════════════════════════════════════════╝"
echo ""
echo "Gateway: $GATEWAY_URL"
echo "Load target: $LOAD_GATEWAY_URL"
echo "Results: $RESULTS_DIR"
echo "Mode: $BENCHMARK_MODE (BENCHMARK_MODE=full for the full suite)"
echo ""

# Check gateway is available
if ! curl -sf "$GATEWAY_URL/health" > /dev/null 2>&1; then
    echo "❌ Gateway not available at $GATEWAY_URL"
    echo "   Start with: docker compose up -d"
    exit 1
fi

echo "✅ Gateway is healthy"
echo ""

# Rebuild the load-generator so -report and the latest flags exist
# (profile-gated services are skipped by a plain 'up --build').
docker compose --profile benchmark build load-generator

# ==========================================
# Test 1: Throughput Stepping
# ==========================================
echo "═══════════════════════════════════════════"
echo "  TEST 1: THROUGHPUT STEPPING"
echo "  10K → 25K → 50K → 100K events/sec"
echo "═══════════════════════════════════════════"
echo ""

docker compose run --rm -v "$RESULTS_DIR:/reports" load-generator /bin/service \
    -gateway "$LOAD_GATEWAY_URL" \
    -step \
    $QUICK_FLAG \
    -workers 20 \
    -batch 200 \
    -error-rate 0.002 \
    -report /reports/throughput-report.json \
    2>&1 | tee "$RESULTS_DIR/throughput-test.log"

echo ""

# ==========================================
# Test 2: Sustained 50K (duration depends on BENCHMARK_MODE)
# ==========================================
echo "═══════════════════════════════════════════"
echo "  TEST 2: SUSTAINED LOAD (50K/sec, ${SUSTAINED_SECS}s)"
echo "═══════════════════════════════════════════"
echo ""

docker compose run --rm -v "$RESULTS_DIR:/reports" load-generator /bin/service \
    -gateway "$LOAD_GATEWAY_URL" \
    -rate 50000 \
    -duration ${SUSTAINED_SECS}s \
    -workers 20 \
    -batch 200 \
    -error-rate 0.002 \
    -report /reports/sustained-report.json \
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
echo "Phase 1: Normal traffic (0.2% errors, ${DETECT_P1_SECS}s)..."
docker compose run --rm -v "$RESULTS_DIR:/reports" load-generator /bin/service \
    -gateway "$LOAD_GATEWAY_URL" \
    -rate 25000 \
    -duration ${DETECT_P1_SECS}s \
    -workers 10 \
    -batch 100 \
    -error-rate 0.002 \
    -report /reports/detection-phase1-report.json \
    2>&1 | tee "$RESULTS_DIR/detection-phase1.log"

echo ""

# Phase 2: Spike errors
echo "Phase 2: Error spike (8% errors, ${DETECT_P2_SECS}s)..."
docker compose run --rm -v "$RESULTS_DIR:/reports" load-generator /bin/service \
    -gateway "$LOAD_GATEWAY_URL" \
    -rate 25000 \
    -duration ${DETECT_P2_SECS}s \
    -workers 10 \
    -batch 100 \
    -error-rate 0.08 \
    -report /reports/detection-phase2-report.json \
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
echo "(per-step JSON reports are the *-report.json files)"
ls -la "$RESULTS_DIR/"
echo ""
echo "View Grafana dashboards at: http://localhost:3001"
echo "View UI at: http://localhost:3000"
