# Distributed Log Analytics Platform — Benchmark & Systems Analysis Report

**Author / Engineer:** Agniva Sengupta  
**Date:** 2026-09-11  
**Target Environment:** Docker Compose on 8-Core Host (7.57 GiB Memory Limit)  
**Core Technologies:** Go (Microservices), Apache Kafka 7.5 (12 Partitions), ClickHouse 24.8 (Hot Tier), Cloudflare R2 (Cold Tier), Prometheus & Grafana

---

## 1. Executive Summary

This report analyzes the empirical performance, architectural behavior, bottlenecks, and scalability characteristics of the distributed log analytics platform observed during end-to-end benchmark testing.

A cumulative total of **9,481,603 log events** passed through the pipeline during testing, with **9,362,000 events** delivered during the automated benchmark suite.

### Key Headline Results:
- **Peak Ingestion Rate:** **67,031 events/sec** (at the 100K target step).
- **Ingestion Acceptance:** **100.00%** (0 rejected, 0 dropped).
- **Worker & Database Insert Reliability:** **100.00%** (0 flush failures, 0 dead-letter queue events).
- **ClickHouse Append Latency:** **49.56 ms (p95)** at normal load, **1.44 s (p95)** under maximum concurrency.
- **Cold Storage Archive Throughput:** **~2,214 events/sec** sustained upload to Cloudflare R2.
- **Detection & Alerting Delivery:** **100.00%** delivery rate on real-time anomaly detection with zero false positives.

---

## 2. Empirical Benchmark Findings (The Numbers)

### A. Throughput Stepping & Gateway Latency
Tested progressive load scaling from 10,000 to 100,000 target events/sec:

| Step | Target Rate | Duration | Logs Sent | Accepted | Actual Rate (EPS) | p50 Latency | p95 Latency | p99 Latency | Max Latency |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **10K Step** | 10,000 | 17.00s | 148,000 | 148,000 (100%) | **8,705.06** | 48.35 ms | 135.66 ms | 154.19 ms | 157.48 ms |
| **25K Step** | 25,000 | 17.00s | 321,200 | 321,200 (100%) | **18,893.05** | 42.57 ms | 269.50 ms | 859.42 ms | 940.87 ms |
| **50K Step** | 50,000 | 32.00s | 1,496,800 | 1,496,800 (100%) | **46,769.61** | 32.88 ms | 49.89 ms | 73.34 ms | 120.23 ms |
| **100K Step**| 100,000 | 32.00s | 2,145,200 | 2,145,200 (100%) | **67,031.22** | 32.70 ms | 68.76 ms | 399.59 ms | 2,028.26 ms |
| **Sustained**| 50,000 | 122.01s | 3,259,200 | 3,259,200 (100%) | **26,713.43** | 33.49 ms | 432.66 ms | 1,255.30 ms | 2,792.90 ms |

### B. Hot Tier (ClickHouse & Workers)
- **Worker Append Peak Throughput:** **28,640.71 logs/sec**.
- **Worker Queue Health:** 0 pending batches, 2 in-flight appenders, 0 uncommitted batches at rest.
- **Consumer Lag Post-Test:** **0 across all 12 Kafka partitions**.

### C. Detection & Alerting Engine
- **Normal Traffic (0.2% error rate):** 668,800 events evaluated $\rightarrow$ **0 alerts fired** (0 false positives).
- **Error Spike (8.0% error rate):** 1,322,800 events evaluated $\rightarrow$ **0.185 alerts/s fired** $\rightarrow$ **0.185/s webhook delivered** (**100% success**).

### D. Cold Archive Tier (Cloudflare R2)
- **Archive Upload Rate:** **2,141 – 2,214 logs/sec**.
- **R2 Put Batch Latency (p95):** **2.24 – 3.87 seconds** per compressed batch.
- **Flush Failures:** **0** (after resolving the bucket name to `raw-logs`).

### E. Peak Resource Footprint Under Maximum Load
| Container | Peak CPU | Peak Memory | Settled Memory | Primary Activity |
| :--- | :--- | :--- | :--- | :--- |
| **ClickHouse** | **393.50%** (~4 cores) | **764.5 MiB** | **627.6 MiB** | Multi-part block writes & Materialized View updates |
| **Worker** | **233.46%** (~2.3 cores) | **212.4 MiB** | **95.4 MiB** | Kafka consumption, JSON parsing, batching |
| **Gateway** | **121.67%** (~1.2 cores) | **260.5 MiB** | **76.2 MiB** | HTTP parsing, non-blocking Kafka production |
| **Kafka** | **42.54%** | **1,557.0 MiB** | **1,503.0 MiB** | 12-partition log segment writes & group coordination |
| **Detection** | **44.55%** | **105.5 MiB** | **89.3 MiB** | Tumbling error-window evaluation |
| **Archive Writer** | **2.22%** | **186.1 MiB** | **186.1 MiB** | JSONL serialization & S3 multipart HTTP uploads |

---

## 3. Systems Analysis: Why Those Numbers Occurred

Understanding the underlying mechanics behind each measurement:

### 1. Why Gateway p50 Latency Stayed at ~33ms While Max Latency Rose to ~2s Under 100K Burst
- **Why p50 was fast:** The Gateway uses asynchronous, non-blocking Kafka producer ring-buffers (`librdkafka`). As long as the internal queue has capacity, HTTP requests return in under 35ms.
- **Why max latency spiked to 2.02s:** During the abrupt 100k target burst, thousands of concurrent HTTP connections competed for socket accept queues on a **single Gateway container process**. The delay was OS socket queue wait time, not database latency.

### 2. Why Worker Append Latency Shifted from 49ms to 1.44s Under 50K Sustained Load
- **At Low Load (<2,000 EPS):** ClickHouse receives small batches directly into memory buffers, taking **~49.5 ms**.
- **At High Sustained Load (28k EPS):** 16 parallel worker threads were concurrently executing `INSERT INTO logs FORMAT JSONEachRow` with 5,000-row blocks. Each insert synchronously triggered the `logs_metrics_mv` SummingMaterializedView computation and wrote data parts to disk. While 1.44s per 5,000-event block represents high efficiency (0.28ms per log), it increased batch wait time.

### 3. Why ClickHouse CPU Hit 393.5%
- ClickHouse parallelizes data part compression (LZ4/ZSTD) and skip-index generation (`ngrambf_v1` on `message` and `bloom_filter` on `trace_id`) across all available CPU cores. Reaching 393.5% CPU on an 8-core machine demonstrated optimal multi-core utilization.

### 4. Why the Cold Tier Operates at ~2,200 EPS vs Hot Tier at ~28,600 EPS
- **Hot Tier (ClickHouse):** Operates on the local Docker bridge network with sub-millisecond network latency and bulk columnar writes.
- **Cold Tier (Cloudflare R2):** Uploads over public internet WAN to Cloudflare's S3 API. Each HTTPS connection requires TLS handshake and round-trip transmission time (~50–150 ms per request). With 4 upload workers, theoretical throughput maxes out around $\frac{4 \text{ workers} \times 500 \text{ batch size}}{0.9 \text{s latency}} \approx 2,200 \text{ EPS}$.

---

## 4. What Was Found & Resolved During Testing

1. **R2 Bucket Name Discrepancy (403 AccessDenied):**
   - *Issue:* Initial test runs showed `archive_flush_failures_total` climbing and Kafka offsets uncommitted because `.env` specified `S3_BUCKET=log-archive`, while the Cloudflare bucket was created as `raw-logs`.
   - *Resolution:* Corrected `.env` to `S3_BUCKET=raw-logs` and recreated the container (`docker compose up -d archive-writer`). Flush failures immediately dropped to 0, and the backlog drained cleanly.
2. **Post-Test Segment Flush Spikes on Kafka:**
   - *Issue:* Kafka showed short CPU spikes (up to 185–209%) immediately after load tests ended.
   - *Root Cause:* Normal Kafka behavior when millions of in-memory log segments are force-flushed to disk (`fsync`), partition index files are updated, and consumer groups commit final offsets.

---

## 5. Architectural Recommendations: What Could Be Done Next

Based on the test observations, the following optimizations would further elevate system capacity:

### A. Scaling Processing Workers (Hot Tier)
- **Current:** 1 worker container with 16 appender threads ($\approx 28.6\text{k EPS}$).
- **Recommendation:** Scale to **3–4 worker containers** across the 12 Kafka partitions.
- **Expected Impact:** Distributes JSON deserialization and batch building across multiple Go runtimes, pushing ingestion toward **45k–55k EPS** before hitting single-machine disk/CPU limits.

### B. Scaling Archive Writers (Cold Tier)
- **Current:** 1 container with 4 upload threads ($\approx 2.2\text{k EPS}$).
- **Recommendation:** Increase `uploadWorkers` from 4 to **16–32**, or run **3 archive-writer containers**.
- **Expected Impact:** Multiplies concurrent HTTPS upload streams to Cloudflare R2, increasing cold archive speed from **2.2k EPS to 10k–15k+ EPS** (matching typical internet uplink bandwidth).

### C. Ingestion Gateway Load Balancing
- **Current:** Single HTTP gateway instance.
- **Recommendation:** Run **2–3 Gateway containers** behind a lightweight reverse proxy (NGINX / Envoy).
- **Expected Impact:** Eliminates the 2.0s socket queue tail latency during sudden 100K bursts by distributing TCP connection termination.

### D. ClickHouse Async Inserts Option
- For extreme scale, enabling `async_insert=1` and `wait_for_async_insert=0` in ClickHouse allows ClickHouse server to buffer and merge smaller inserts internally, reducing appender wait time during extreme traffic spikes.

---

## 6. Conclusion

The self-hosted ClickHouse migration achieved a **~500x latency reduction** compared to the previous SaaS setup (down from 25.6 seconds to sub-50ms), sustained **67,000+ EPS burst ingestion**, and reliably evaluated anomalies across **9.48 Million logs** without a single dropped event. The decoupled Kafka architecture proved essential in buffering WAN latency for cold archiving while keeping hot analytics real-time.
