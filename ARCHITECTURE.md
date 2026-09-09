# Distributed Log Analytics Platform — Design & Architecture

| | |
|---|---|
| **Document** | System design / architecture reference |
| **Component** | `github.com/log-analytics-platform` |
| **Status** | Living document. Describes the system **as built** in this branch, including rationale, invariants, and known gaps. |
| **Verified against** | `0d7fe8e6b948abe3b6f829b46f132a780144414c` (2026-09-09) |
| **Audience** | Engineers operating, extending, or reviewing the platform |
| **Companion docs** | [`README.md`](README.md) (overview), [`QUICKSTART.md`](QUICKSTART.md) (run it), [`MANAGED_SERVICES_SETUP.md`](MANAGED_SERVICES_SETUP.md) (provision it) |

> **Reading convention.** Sections 2–15 describe the intended design and the
> behavior actually implemented. Section 16 collects *deviations, limitations
> and defects* with `file:line` references so that nothing in this document is
> aspirational by accident — every claim about a gap points at code.

---

## Table of contents

1. [Purpose & scope](#1-purpose--scope)
2. [System overview](#2-system-overview)
3. [Requirements](#3-requirements)
4. [High-level architecture](#4-high-level-architecture)
5. [Data model & contracts](#5-data-model--contracts)
6. [Component design](#6-component-design)
7. [Key design decisions](#7-key-design-decisions)
8. [Consistency, ordering & delivery semantics](#8-consistency-ordering--delivery-semantics)
9. [Backpressure & overload policy](#9-backpressure--overload-policy)
10. [Partitioning, scaling & capacity model](#10-partitioning-scaling--capacity-model)
11. [Reliability: failure modes & recovery](#11-reliability-failure-modes--recovery)
12. [Performance & cost characteristics](#12-performance--cost-characteristics)
13. [Security & compliance design](#13-security--compliance-design)
14. [Observability design](#14-observability-design)
15. [Deployment topology & operations](#15-deployment-topology--operations)
16. [Tradeoffs, limitations & known defects](#16-tradeoffs-limitations--known-defects)
17. [Roadmap](#17-roadmap)
18. [Appendices](#18-appendices)

---

## 1. Purpose & scope

### 1.1 What the platform does

The platform ingests high-volume structured log events from many producers, and
serves two distinct workloads from the same event stream:

* **Interactive investigation** — full-text search, filtering and time-bucketed
  aggregation over recent data at sub-second latency.
* **Durable retention & replay** — an immutable raw archive of everything that
  was accepted, queryable for compliance and reprocessing.

It also derives a third, dependent workload: **real-time detection** — sliding
window evaluation over the same stream that produces deduplicated alerts.

### 1.2 Why it is built this way

The system exists to demonstrate and validate distributed-system fundamentals
on a workload where those fundamentals are unavoidable:

| Fundamental | Where it is exercised |
|---|---|
| Partitioning | Kafka key `service:source`, per-partition consumer groups, date/hour object prefixes |
| Replay | 7-day Kafka retention + verbatim raw archive → reprocess with a new parser |
| Horizontal scaling | Stateless gateways, two independently scalable consumer groups |
| Backpressure | HTTP 429 quota, Kafka buffering, commit-after-durable-write |
| Failure recovery | Consumer-group rebalance, buffer restoration, DLQ diversion, at-least-once semantics |
| Tiered storage | Hot columnar store for the interactive window, cheap object store for the tail |

### 1.3 In scope / out of scope

**In scope:** ingest, normalization/enrichment/redaction, hot analytics
storage, cold raw archive, tier-aware query routing, stream detection, alert
dispatch, a search UI, container orchestration, and the monitoring stack.

**Out of scope (by design, not by omission):**

* Multi-tenancy, per-tenant billing, and quota management beyond a
  per-process ingest rate limit.
* Long-term *indexed* cold search (the cold tier is scan-based, see [§5.4](#54-cold-tier-r2-object-layout)).
* Metrics/traces as first-class signals. `trace_id` is carried as an opaque
  correlation field, not a trace-queryable index.
* Log *collection* on the host side. Producers push to the platform; the
  platform does not tail files (a reference OpenTelemetry Collector config is
  shipped but not deployed — see [D14](#d14-reference-otel-config-is-shipped-but-not-deployed)).
* Alert *management* (scheduling, escalation policies, ack/resolve workflow).
  The alert service dispatches; it does not manage incidents.

---

## 2. System overview

### 2.1 One-paragraph summary

Producers `POST` JSON batches to a stateless **gateway**, which validates,
stamps, rate-limits, and publishes each event to a partitioned **Kafka** topic
and only acknowledges once Kafka has replicated the record. Three independent
consumer groups read that same topic: **processing workers** normalize, redact
and bulk-append to **Tinybird** (hot, columnar, 30-day TTL); the **archive
writer** copies the *original bytes* into **Cloudflare R2** (cold, JSONL,
partitioned by day/hour/service); and the **detection service** evaluates
error-rate and error-fingerprint rules, publishing alerts to an `alerts` topic
that the **alert service** deduplicates and forwards to a webhook. A **query
coordinator** presents one search API, routing by time range to Tinybird, to R2,
or merging both, and a **React UI** consumes it. Prometheus scrapes every
service; a provisioned Grafana dashboard visualizes the pipeline.

### 2.2 Design targets

Targets are engineering commitments derived from configuration defaults
(`.env.example`, `docker-compose.yml`) — they are **not** measured results of
this repository's CI (see [§12.4](#124-measurement-method)).

| # | Target | Basis |
|---|---|---|
| T1 | ≥ 50,000 events/s sustained per host in the reference deployment | `GATEWAY_INGEST_QUOTA_PER_SEC=100000` per gateway; benchmark suite steps 10K→100K |
| T2 | ≥ 10,000 events/s ingest accepted with p95 gateway latency < 250 ms | Gateway does validate + produce + await delivery report; `gateway_ingest_latency_seconds` buckets 1 ms…~0.5 s |
| T3 | End-to-end visibility of an event < 10 s (ingest → searchable) | Worker flush interval 2 s + Tinybird `wait=true` synchronous append; query path is read-through |
| T4 | Zero acknowledged-event loss across a single component crash | Commit-after-durable-write in both consumers; Kafka replication |
| T5 | Interactive query p95 < 2 s over the 7-day hot window | Tinybird point/aggregate queries with partition pruning |
| T6 | 100% of accepted events recoverable from the raw archive | Archive is an independent consumer group on the same topic |
| T7 | Detection alert within one window of the triggering event | 5-minute window, evaluated per event |

### 2.3 Guiding principles

1. **Kafka is the single source of truth for in-flight data.** Every
   downstream is a subscriber, never a fan-out target of a hand-written
   dispatcher. This makes new consumers a config change, not a pipeline change.
2. **Commit only after durability.** No consumer advances an offset until the
   storage system it writes to has acknowledged the write ([D3](#d3-commit-only-after-the-downstream-store-has-acknowledged)).
3. **Preserve the raw event forever.** Normalization is a *derived view*, so
   the archive stores bytes verbatim ([D5](#d5-the-archive-stores-original-bytes-not-normalized-ones)).
4. **At-least-once + idempotent-friendly keys, never exactly-once.** Duplicates
   are tolerated and are the explicit price of not using transactions;
   `event_id` makes dedup possible at any layer.
5. **Managed where it is commodity.** Columnar hot storage and object storage
   are bought, not run; orchestration, stream semantics, and query routing are
   built.
6. **Fail-stop beats fail-silent.** A consumer that cannot persist exits and is
   restarted with its offsets uncommitted rather than skipping data
   ([D12](#d12-fail-stop-consumer-loops)).

---

## 3. Requirements

### 3.1 Functional

| ID | Requirement | Realized by |
|---|---|---|
| F1 | Accept batches of JSON log events over HTTP | `POST /v1/ingest`, `cmd/gateway/main.go` |
| F2 | Reject structurally invalid events without dropping them invisibly | Gateway field validation + `logs-dlq` diversion in the worker |
| F3 | Normalize severity vocabulary and redact secrets | `normalizeSeverity`, `redactSecrets`, `redactAttributes` in `cmd/worker/main.go` |
| F4 | Store events so they can be searched within seconds | Tinybird `logs` datasource |
| F5 | Store raw events for long-term retention and replay | R2 `raw/YYYY/MM/DD/HH/<service>/batch-*.jsonl` |
| F6 | Search by service, severity, free text, and time range with paging | `POST /v1/search` |
| F7 | Answer queries that span the hot/cold boundary as one result set | Merged tier + global sort |
| F8 | Aggregate counts by time bucket and by service×severity | `GET /v1/timeline`, `GET /v1/aggregates` |
| F9 | Detect elevated error rates and recurring error signatures | `internal/detection/engine.go` |
| F10 | Suppress duplicate alerts within a cooldown | `cmd/alert-service/main.go` dedup map |
| F11 | Deliver alerts to an HTTP webhook | `sendNotification` |
| F12 | Provide a search UI with timeline, table, and breakdown panels | `ui/src/App.js` + 4 components |
| F13 | Generate load and produce machine-readable benchmark reports | `cmd/load-generator/main.go` (`-report`) |

### 3.2 Non-functional

| ID | Requirement | Design response | Residual risk |
|---|---|---|---|
| N1 | Scalability | Gateways and both consumer groups scale horizontally; scaling unit is the Kafka partition count | 12 partitions caps hot-path consumers at 12 |
| N2 | Availability | Stateless HTTP tier + `restart: unless-stopped` + consumer-group failover | Kafka is a single broker (RF=1) in the reference deployment |
| N3 | Durability | `acks=all`, commit-after-ack, 7-day Kafka retention, R2 archive | Kafka-side durability = broker-local disks in compose |
| N4 | Latency | 5 ms producer linger, gzip'd 5 000-row appends, read-through queries | Worker flush adds up to 2 s tail latency by design |
| N5 | Fault tolerance | Buffer restore on failure, DLQ for poison events, independent consumer groups | Poison event that fails *repeatedly* re-enters the buffer only as DLQ'd; store faults stall the group |
| N6 | Security | Scoped Tinybird append vs read tokens; R2 token scoped to one bucket; `.env` gitignored | No ingest authentication, no TLS, no query RBAC ([§13](#13-security--compliance-design)) |
| N7 | Compliance / privacy | Redaction of secrets, e-mail, card numbers before hot storage | **Redaction does not apply to the raw archive** ([L13](#162-reliability--scalability-limits)) |
| N8 | Observability | Per-service Prometheus counters/histograms + provisioned Grafana dashboard | Scaled replicas are not individually scraped |
| N9 | Deployability | One `docker compose up -d --build`; managed services provisioned by script-checked `.env` | Requires external Tinybird + R2 accounts |
| N10 | Testability | Deterministic load generator, benchmark + failure-recovery scripts, JSON reports | No unit/integration tests in-repo ([L21](#163-engineering--hygiene)) |
| N11 | Operability | `scripts/init.sh` validates config and provisions topics; documented runbook | No K8s manifests; compose is single-host |

### 3.3 Constraints & assumptions

* **Runtime.** Go 1.21 with `CGO_ENABLED=1` for the Kafka client
  (`confluent-kafka-go` → librdkafka). `librdkafka-dev` must be present at
  build time; `librdkafka1`/`librdkafka` at run time. Only the load generator
  builds with `CGO_ENABLED=0`.
* **Managed dependencies must exist before boot.** The workers fail fast without
  `TINYBIRD_APPEND_TOKEN`; the archive writer and query coordinator fail fast
  without R2 credentials. There is no "run without storage" mode.
* **Producers own the event contract.** The gateway does not transform payloads
  beyond filling `event_id`/`timestamp`; anything a producer sends outside
  `LogEvent` is silently dropped at marshal time.
* **Ordering is per-key, not per-stream.** Consumers must not assume global
  time ordering; the query tier sorts explicitly.
* **Single-region.** `region` is a data field, not a topology concept; Kafka,
  Tinybird and R2 are assumed reachable from the same VPC/network.

---

## 4. High-level architecture

### 4.1 Context diagram

```
                         ┌────────────────────────────────────────────────────────────┐
                         │                       PRODUCERS                              │
                         │  app SDKs · OTel Collector (ref. config) · load-generator    │
                         └───────────────────────────┬────────────────────────────────┘
                                                     │ POST /v1/ingest {events:[…]}
                                                     │ (batched JSON, at-least-once)
                         ┌───────────────────────────▼────────────────────────────────┐
                         │        INGESTION TIER — stateless, horizontally scaled       │
                         │  gateway#1 … gateway#N   :8080                              │
                         │  validate → stamp id/ts → rate-limit → partition-key         │
                         │  → ProduceBatch → WAIT for delivery reports                  │
                         └───────────────────────────┬────────────────────────────────┘
                                                     │ acks=all, snappy, linger 5ms
        ┌────────────────────────────────────────────▼────────────────────────────────────────────┐
        │                        KAFKA  (topic `logs`, 12 partitions, 168 h retention)             │
        │                        key = service[:source]      topics: logs, logs-dlq, alerts        │
        └───────────┬──────────────────────────────┬──────────────────────────────┬───────────────┘
                    │ group: processing-workers    │ group: archive-writers       │ group: detection-service
        ┌───────────▼─────────────┐    ┌───────────▼─────────────┐    ┌───────────▼─────────────┐
        │ PROCESSING WORKERS ×M   │    │ ARCHIVE WRITER ×K       │    │ DETECTION ×P            │
        │ normalize · redact      │    │ raw bytes, verbatim     │    │ tumbling windows        │
        │ buffer 5000 / 2 s       │    │ buffer 500 / 10 s       │    │ per-service fingerprints│
        └───────────┬─────────────┘    └───────────┬─────────────┘    └───────────┬─────────────┘
                    │ gzip NDJSON, wait=true       │ PutObject (JSONL)            │ produce to `alerts`
        ┌───────────▼─────────────┐    ┌───────────▼─────────────┐    ┌───────────▼─────────────┐
        │ TINYBIRD (managed)      │    │ CLOUDFLARE R2 (managed) │    │ ALERT SERVICE           │
        │ HOT tier · 30 d TTL     │    │ COLD tier · raw archive │    │ dedup 15 min · webhook  │
        │ MergeTree · daily parts │    │ raw/Y/M/D/HH/service/   │    └─────────────────────────┘
        └───────────┬─────────────┘    └───────────┬─────────────┘
                    └──────────────┬───────────────┘
                       ┌───────────▼───────────────┐        ┌──────────────────────┐
                       │ QUERY COORDINATOR  :8081  │        │ SIDE CHANNELS        │
                       │ hot | cold | merged tiers │        │ DLQ topic · logs-dlq │
                       └───────────┬───────────────┘        │ Prometheus · Grafana │
                                   │ JSON API (CORS *)      └──────────────────────┘
                       ┌───────────▼───────────────┐
                       │ REACT UI  :3000           │
                       │ search · timeline · table │
                       └───────────────────────────┘
```

The essential shape: **one topic, three independent subscriber groups, two
storage tiers behind one query API.** Nothing writes to two stores
transactionally; each consumer owns its own offset and its own durability
boundary.

### 4.2 Ingest path (happy path, with the durability handshake)

```
Producer          Gateway                Kafka              Worker              Tinybird
   │                 │                     │                   │                    │
   │ POST /v1/ingest │                     │                   │                    │
   ├────────────────►│                     │                   │                    │
   │                 │ decode + validate   │                   │                    │
   │                 │ fill event_id / ts  │                   │                    │
   │                 │ quota check ──►429  │                   │                    │
   │                 │ ProduceBatch()      │                   │                    │
   │                 ├────────────────────►│                   │                    │
   │                 │  (enqueue, returns quickly, linger 5 ms)│                    │
   │                 │ ◄── delivery reports per message (≤10 s)                    │
   │                 │   accepted = # of acked reports        │                    │
   │ ◄── {accepted:N, failed:M} ──────────┤                   │                    │
   │                 │                     │  Poll(100 ms)     │                    │
   │                 │                     │◄──────────────────┤                    │
   │                 │                     │   normalize/redact│                    │
   │                 │                     │   buffer++        │                    │
   │                 │                     │   at 5000 rows or 2 s:                │
   │                 │                     │                   ├─gzip NDJSON───────►│
   │                 │                     │                   │  POST /v0/events   │
   │                 │                     │                   │   ?wait=true       │
   │                 │                     │                   │◄─successful_rows / quarantined_rows
   │                 │                     │  CommitOffsets(max+1 per partition)   │
   │                 │                     │◄──────────────────┤                    │
   │                 │                     │   buffer restored, NO commit if the   │
   │                 │                     │   append fails → redelivery           │
```

Two properties matter and they are the whole reason the pipeline is shaped this
way: the client is not told "accepted" until Kafka has the record, and Kafka is
not told "consumed" until Tinybird has the rows.

### 4.3 Query path (tier routing)

```
POST /v1/search { service, severity, search, start_time, end_time, limit, offset }
        │
        ▼
hotCutoff = now − QUERY_HOT_RETENTION_DAYS (7 d)
        │
        ├── end_time <  hotCutoff ──────────────► COLD ONLY
        │                                          R2: list day prefixes in range
        │                                          → GET each object → filter in memory
        │                                          → sort desc → paginate
        │                                          source:"cold",  total = len(scanned matches)
        │
        ├── start_time >  hotCutoff ────────────► HOT ONLY
        │                                          Tinybird SQL: count() + row page
        │                                          ORDER BY timestamp DESC LIMIT/OFFSET
        │                                          source:"hot",   total = count()
        │
        └── straddles ──────────────────────────► MERGED
                                                   hot:  [hotCutoff, end], fetchLimit = offset+limit
                                                   cold: [start, hotCutoff], full scan
                                                   concat → global sort desc → paginate
                                                   source:"merged", total = hotTotal + len(cold)
```

### 4.4 Network / port map

| Port | Exposed to host | Owner | Purpose |
|---|---|---|---|
| 8080 | yes | gateway | `POST /v1/ingest`, `GET /health`, `GET /metrics` |
| 8081 | yes | query coordinator | `/v1/search`, `/v1/timeline`, `/v1/aggregates`, `/health`, `/metrics` |
| 8082 | no | — | `ALERT_SERVICE_PORT` — declared in config, **not bound** ([L22](#163-engineering--hygiene)) |
| 9091–9094 | no (in-network) | worker / archive / detection / alert | Prometheus `/metrics` only (detection also `/health`) |
| 29092 / 9092 | internal / host | Kafka | `PLAINTEXT` internal listener / `PLAINTEXT_HOST` advertised for host tooling |
| 2181, 9997 | yes | ZooKeeper, Kafka JMX | dev diagnostics |
| 9090, 3001, 3000 | yes | Prometheus, Grafana, UI | operator surfaces |

Docker's embedded DNS (`gateway`, `kafka`, `worker`, …) is the service
registry. There is no service mesh, no sidecar proxy, and no TLS anywhere in
the reference deployment.

### 4.5 Trust boundaries

```
[untrusted producers] ──HTTP:8080──► [platform network] ──► [Kafka, plaintext, no authN]
      │                                    │
      │ no token/key required today        └──► [Tinybird: scoped append token]  (per-service credential)
      │                                    └──► [R2: bucket-scoped API token]    (per-service credential)
      └──► [query API:8081, CORS *]  ◄── browser  ← no auth, no row-level policy
```

Credential isolation is real (each service gets only the scope it needs — see
[§13.2](#132-credential-topology)); network-level isolation is not.

---

## 5. Data model & contracts

### 5.1 Canonical event: `models.LogEvent`

The single schema shared by every component (`internal/models/models.go:9`):

| Field | Type | Required | Who fills it | Notes |
|---|---|---|---|---|
| `event_id` | string | yes (auto) | producer or gateway (`uuid`) | dedup/idempotency key; never enforced |
| `timestamp` | RFC3339 time | yes (auto) | producer or gateway (`time.Now()`) | drives partitioning, tier routing, TTL |
| `service` | string | **yes** | producer | validation gate at the gateway; partition-key component |
| `severity` | string | no | producer, normalized by worker | `DEBUG…FATAL`; unknown values pass through upper-cased |
| `message` | string | **yes** | producer | validation gate; redacted before hot storage; free-text index |
| `attributes` | object | no | producer | stored as an opaque JSON **string** in the hot tier |
| `trace_id` | string | no | producer | correlation only; indexed neither in hot nor cold |
| `source` | string | no | producer | partition-key component |
| `region`, `version` | string | no | producer | pass-through labels |

**Validation rule (deliberately minimal):** an event is accepted if `service`
and `message` are non-empty. Everything else is defaulted, never rejected — a
missing severity becomes `INFO` downstream, a missing timestamp becomes ingest
time. The rationale: at log volumes, silent defaulting is preferable to
rejecting telemetry that an operator will want later.

### 5.2 Three representations of one event

This is the most important thing to understand about the data model. The same
logical event exists in three shapes, and each is used for a reason:

| Representation | Where | Content | Redaction | Normalization |
|---|---|---|---|---|
| **Wire event** | Kafka `logs` payload | gateway-marshaled `LogEvent` (with defaults filled) | none | none |
| **Hot row** | Tinybird `logs` | 10 flat columns, `attributes` as JSON text | **yes** | **yes** (severity canonicalized) |
| **Cold object** | R2 JSONL | original Kafka bytes, byte-for-byte | none | none |

Consequences that callers must know:

* The hot tier answers `severity = 'ERROR'` exactly; the cold tier matches
  case-insensitively (`strings.EqualFold`) against the producer's original
  string. `severity: "error"` is therefore findable in cold and in hot (because
  the worker rewrote it to `ERROR`), but a query for `severity=err` behaves
  differently per tier.
* The archive writer is a *wire-format* consumer. Cold queries can only project
  fields that producers actually named as in `LogEvent`; a producer sending
  `{"body": …}` is archived faithfully but returns nothing from `search`.
* Detection consumes the **wire** event, so `severity: "crit"` is *not* counted
  as an error by the engine (it matches `ERROR|FATAL|CRITICAL` only) even though
  the hot copy is normalized to `FATAL`.

### 5.3 Hot tier: Tinybird schema

`tinybird/datasources/logs.datasource`, pushed with
`tb push tinybird/datasources/logs.datasource --force`:

```
event_id    String                timestamp   DateTime64(3)   service  LowCardinality(String)
severity    LowCardinality(String) message    String          attributes String
trace_id    String                source      LowCardinality(String)
region      LowCardinality(String) version     LowCardinality(String)

ENGINE MergeTree
SORTING KEY      service, severity, timestamp
PARTITION KEY    toYYYYMMDD(timestamp)
TTL              timestamp + INTERVAL 30 DAY
```

Design notes:

* **`attributes` as JSON text** keeps the ingest schema stable — arbitrary
  producer fields never require a datasource migration. The cost is that
  attribute predicates need `JSONExtract*` and cannot use the sort key.
* **Daily partitions + `(service, severity, timestamp)` sort key** match the
  two access patterns the coordinator actually issues: time-boxed service
  filters, and time-bucketed aggregates.
* **`LowCardinality` on every label column** is what makes the
  `GROUP BY service, severity` aggregate cheap at tens of millions of rows.
* **Physical TTL (30 d) ≠ routing window (7 d)** — see [D8](#d8-hot-window-and-hot-ttl-are-separate-knobs).

### 5.4 Cold tier: R2 object layout

```
s3://<S3_BUCKET>/
  raw/
    2026/09/09/               ← day prefix (partition pruning unit for queries)
      08/                     ← hour prefix (write locality only, not used by query)
        payment/              ← service prefix
          batch-1757429332000000000.jsonl     ← one PUT per flush
      09/
        inventory/
          batch-1757429399000000000.jsonl
```

* **Format:** newline-delimited JSON, uncompressed, one line per event, exactly
  the Kafka payload (`internal/storage/s3.go:142`).
* **Object granularity:** one object per (flush, service), 500 events max, so
  ~200 KB at 400 B/event. See [§12.3](#123-object-store-write-amplification) for the sizing
  consequences.
* **Key uniqueness:** `batch-<unix-ns>.jsonl` — unique per writer process in
  practice, *not* deterministic, so a retried flush creates a second object
  containing duplicate events ([L9](#161-confirmed-defects)).
* **No manifest, no footer, no columnar format.** Query cost is linear in bytes
  stored inside the requested day range ([§12.2](#122-cold-path-scan-cost)).

### 5.5 Stream contracts: topics, keys, partitions

| Topic | Partitions | Produced by | Consumed by (group) | Retention | Payload |
|---|---|---|---|---|---|
| `logs` | 12 | gateway | `processing-workers`, `archive-writers`, `detection-service` | `KAFKA_LOG_RETENTION_HOURS=168` | `LogEvent` JSON |
| `logs-dlq` | 12 auto-created (`gateway/main.go` CreateTopics) / 6 via `scripts/init.sh` — first writer wins | worker | operator tooling (no consumer in-repo) | broker default | `DeadLetterEvent` JSON |
| `alerts` | 6 | detection | `alert-service` | broker default | `Alert` JSON |

* **Partition key = `service` + optional `:source`** (`cmd/gateway/main.go:123`).
  This gives per-(service,source) ordering, keeps one producer's stream
  contiguous, and spreads the default 10-service load across 12 partitions.
  The documented risk is skew: `payment` at 20× the volume of the other services
  is bound to whatever partitions `payment:*` hashes to.
* **Topic creation is redundant on purpose** — the gateway creates
  `logs`/`logs-dlq`, detection creates `alerts`, and `scripts/init.sh` creates
  all three idempotently (`--if-not-exists`). Any single one suffices; the
  duplication exists so that no service requires an out-of-band bootstrap step.
* **Replication factor 1** everywhere in compose, including
  `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1`. This is a dev-availability
  tradeoff, not a production topology ([L14](#162-reliability--scalability-limits)).

### 5.6 Auxiliary payloads

```jsonc
// logs-dlq — produced by cmd/worker/main.go:sendToDLQ
{ "event_id": "", "raw": "<original bytes as a JSON string>",
  "error": "unmarshal: unexpected end of JSON input",
  "failed_at": "2026-09-09T08:42:17Z", "component": "processing-worker" }

// alerts — produced by cmd/detection/main.go:handleAlert, keyed by fingerprint
{ "id": "alert-payment-1757429337000000000", "rule_id": "error-rate-threshold",
  "service": "payment", "fingerprint": "error_rate_payment", "severity": "WARNING",
  "value": 0.0821, "message": "Error rate 8.21% exceeds threshold 5.00% for service payment",
  "triggered_at": "2026-09-09T08:42:17Z", "status": "firing" }
```

Note the DLQ payload carries an **unparseable** original as a string, not as
nested JSON — that is what makes the record itself always serializable, which
matters because a DLQ write that throws would lose the evidence of the failure.

### 5.7 HTTP API surface

**Ingest**

```
POST /v1/ingest          (gateway :8080, CORS: *, no auth)
  body: {"events":[LogEvent,…]}
  200 → {"accepted":998,"failed":2}          // accepted = Kafka-acked count
  400 → {"error":"invalid request body"}      // un-decodable payload
  429 → {"error":"quota exceeded","retry_after":1}   + Retry-After: 1
```

Contract detail: `accepted` counts **only** messages whose delivery report came
back without error. A partial success is representable and is the expected
behavior during a broker leader election; producers should retry the batch using
the same `event_id`s.

**Query**

```
POST /v1/search          (query coordinator :8081)
  body: {"service":"payment","severity":"ERROR","search":"timeout",
         "start_time":"2026-09-08T00:00:00Z","end_time":"2026-09-09T00:00:00Z",
         "limit":100,"offset":0}
  200 → {"results":[LogEvent,…],"total":48213,"source":"hot"}
  defaults: limit 100 (max 10000), end=now, start=end−24h
  500 → {"error":"hot storage query failed"|"cold storage query failed"}

GET /v1/timeline?service=&hours=24      → {"timeline":[{"bucket":…,"count":…,"error_count":…},…]}
GET /v1/aggregates?hours=24             → {"aggregates":{"payment":{"ERROR":12,"INFO":940},…}}
GET /health                             → {"status":"healthy"}
GET /metrics                            → Prometheus text format
```

`start_time`/`end_time` are `snake_case` (`internal/models/models.go:48`). The
React UI currently sends `startTime`/`endTime`, so the range is silently ignored
and every search defaults to the last 24 hours ([L1](#161-confirmed-defects)).

### 5.8 Compatibility rules

Because normalization and schema are split across three layers, contract
changes have specific safe ordering:

1. **Adding an optional `LogEvent` field** is safe: old producers omit it,
   `encoding/json` zeroes it, Tinybird rows it as empty, R2 archives it as
   absent. Roll out the model first, the datasource column second.
2. **Changing a type or renaming a field is a breaking change for the cold
   tier** — historical objects are never rewritten, so any reader of R2 must
   tolerate every past shape simultaneously. That is exactly why replay is
   specified as "reprocess the archive into a new datasource" rather than
   "migrate the archive in place".
3. **Severity vocabulary** may only be extended in `normalizeSeverity`'s
   default branch (unknown values pass through upper-cased); collapsing two
   existing levels would silently rewrite history for hot queries only.
4. **`version`** is a producer-supplied string, not a schema discriminator. The
   platform does not branch on it today; if the contract hardens, the field to
   key on is already archived in both tiers.

---

## 6. Component design

### 6.1 Gateway (`cmd/gateway`, port 8080)

**Responsibility.** The only write path into the platform: authenticate (not
yet implemented — see [L2](#161-confirmed-defects)), validate, default,
rate-limit, partition, and durably enqueue.

**Request pipeline** (`handleIngest`):

```
decode → quota gate → per-event: stamp id/ts, validate, marshal, key = service[:source]
       → ProduceBatch(…, 10 s) → count Kafka-acked reports only → respond
```

**Key implementation choices**

| Choice | Detail | Why |
|---|---|---|
| Synchronous batch acknowledgement | `internal/kafka.Producer.ProduceBatch` enqueues every message with a per-message delivery channel, then blocks until all reports arrive or a 10 s deadline; unreported messages are counted as failed | An HTTP 200 that promises durability requires it; the alternative (fire-and-forget + `Flush`) would report "accepted" for messages Kafka never took |
| Synthetic error events for enqueue failures | On a `Produce()` error, a fake report with that error is pushed into the channel | Keeps the "exactly one report per message" invariant simple, so the receive loop is `len(messages)` iterations with no special cases |
| Metric attribution by position | Accepted/failed counts are attributed to services in batch order | `BatchResult` has no per-index identity; totals are what dashboards use |
| Process-local rate limit | Package-level `atomic.Int64` incremented by accepted events, zeroed by a 1 s ticker | One-line backpressure; not a token bucket ([§9.2](#92-quota-implementation-and-its-limits)) |
| `http.Server` with read 10 s / write 30 s | | Bounds slow-client occupancy |

**Scaling unit.** Stateless. `docker compose up -d --scale gateway=N` behind a
load balancer. No shared state, no leader, no warm-up.

**Failure mode.** Crash mid-request → the client sees a connection error and
retries; already-acked Kafka messages may duplicate, which is why `event_id`
exists. There is no graceful drain: `SIGTERM` cancels a context that is not
wired to `server.Shutdown` (`cmd/gateway/main.go:248-255`), so in-flight requests
are cut off — safe for the ingest path (client retries), not free for large
batches.

**Does not do:** transform payloads, deduplicate, guarantee schema evolution, or
persist anything itself.

### 6.2 Processing workers (`cmd/worker`, consumer group `processing-workers`)

**Responsibility.** Turn wire events into queryable hot rows: normalize,
redact, micro-batch, append, and only then commit.

**Core loop** — one goroutine, a select over `{ctx.Done, flushTimer, default}`:

```
loop:
  flushTimer(2 s) fires        → Flush() → commitOffsets(msgs)
  else Poll(100 ms)            → addToBuffer(msg)
        unmarshal fails         → produce to logs-dlq, buffer a sentinel {msg only}
        ok                      → buffer {event, msg}
        len(buffer) ≥ 5000      → Flush() → commitOffsets(msgs); reset timer
  ctx cancelled (SIGTERM)      → final Flush() → commit → exit
```

**`Flush()` invariants** (`cmd/worker/main.go:182`) — the interesting part of
the system:

1. Swap the buffer out under a mutex (so the poll loop never blocks on the
   network).
2. Split items into *real* events and *sentinels* (DLQ'd messages that still
   need their offset advanced).
3. If there are real events: one gzipped NDJSON `POST /v0/events?wait=true`;
   verify `quarantined_rows == 0` and `successful_rows ≥ len(events)`.
4. On any error: **restore the whole batch at the head of the buffer** and
   return an error, so the caller commits nothing → Kafka redelivers.
5. On success: return all messages (including sentinels) for commit.

**Offset math** (`commitOffsets`): max offset per topic-partition across the
batch, `+1`, committed with `CommitOffsets`. Correct because the buffer is
fed by a single poll loop in arrival order; with multi-partition batches each
partition advances independently, which is exactly what the API expects.

**Transformation rules**

* Severity: `FATAL|CRITICAL|EMERGENCY|EMERG→FATAL`, `ERROR|ERR→ERROR`,
  `WARNING|WARN→WARNING`, `INFO|INFORMATION→INFO`, `DEBUG|TRACE→DEBUG`,
  empty→`INFO`, anything else upper-cased and kept.
* Redaction (3 compiled regexes, `cmd/worker/main.go:81`): `password|passwd|
  secret|token|api_key|apikey|authorization` assignments; e-mail addresses;
  16-digit card groups → `[REDACTED]`. Plus key-name matching over `attributes`
  (`password, secret, token, api_key, apikey, authorization, ssn`).
* Regexes and secret patterns are compiled **once at package init** — this runs
  on every event and would otherwise dominate CPU at 50 K events/s.

**Batching rationale.** `appendBatchSize = 5000` with a 2 s floor. The Tinybird
round trip is synchronous (`wait=true`) and is the dominant cost in the loop;
the batch size amortizes it. 2 s is the latency ceiling for small traffic.
The pair is the classic latency/throughput knob of this design.

**Metrics.** `worker_events_processed_total`,
`worker_events_dead_lettered_total`, `worker_flush_failures_total`,
`worker_offsets_committed_total`, `worker_batch_size`,
`worker_processing_latency_seconds`, `worker_append_latency_seconds` on `:9091`.

**Failure modes.** Store unreachable → buffer grows (unbounded — see
[L6](#161-confirmed-defects)); offsets stall; lag grows; drains on
recovery. Poison event → DLQ, offset advances, no hot row. Process killed
between flush and commit → duplicate rows on restart (accepted: at-least-once).

### 6.3 Archive writer (`cmd/archive-writer`, group `archive-writers`)

**Responsibility.** Faithful, independent copy of every event to object storage
— the durability guarantee that lets the pipeline be reinterpreted later.

**Design points**

* **Per-service buffers** (`map[service][]bufferedArchiveItem`), flush at
  500 items for a service or a 10 s timer, whichever first. Object keys inherit
  the service prefix, so a compactor/reader can prune by service.
* **Metadata is extracted, not decoded.** `extractMetadata` unmarshals only
  `service` and `timestamp` into an envelope struct — cheaper than a full model
  decode and, more importantly, it guarantees the writer never mutates or
  re-serializes the payload. Unparseable events are archived under `_unknown`
  rather than dropped.
* **Raw bytes are defensively copied** (`copy(raw, msg.Value)`), because
  librdkafka may reuse the message backing array after the poll returns.
* **`FlushAll` is all-or-nothing per service**: any service whose PUT fails
  aborts the whole commit, and the failed service's items are pushed back onto
  the head of its buffer. Partial progress is therefore possible in R2 but not
  in the committed offsets — duplicates, never gaps.
* **Replay contract.** Because the object is the original Kafka payload, a new
  parser can be run over `raw/**` and re-appended to a fresh datasource; that is
  the designed response to a schema or redaction-policy change.

**Interaction with the hot tier.** None. The two consumers never talk; they
share only the topic. Consequently a Tinybird outage does not stop the archive
(and vice versa) — the property that makes tiered storage safe to operate.

### 6.4 Detection service (`cmd/detection`, group `detection-service`)

**Responsibility.** Real-time rules over the event stream, with cheap state.

**Two rules, both evaluated per event** (`internal/detection/engine.go`):

1. **Error-rate threshold.** Per-service window; window starts at the first
   event after the previous window expired (a *tumbling* window, not sliding —
   see [L3](#161-confirmed-defects)). If `totalEvents ≥ min_events` (100) and
   `errors/total ≥ threshold` (0.05), fire an `error-rate-threshold` alert
   with fingerprint `error_rate_<service>`.
2. **Recurring-error fingerprint.** `normalizeMessage` strips volatility —
   UUIDs → `<UUID>`, IPv4 → `<IP>`, long hex → `<HEX>`, integers → `<N>` —
   then MD5s the template. A `(service, template)` that exceeds 100 occurrences
   and has not alerted in the last 15 minutes fires a `recurring-error` alert.
   Precompiled regexes are deliberate (per-message compilation would dominate
   CPU during exactly the spike that must be caught).

**State and its consequences.** `windows` and `fingerprints` are in-process maps
behind an `RWMutex`, pruned every 30 s (windows older than 2× the duration,
fingerprints idle for 4× the duration). This yields:

* **No persistence** → a restart loses partial-window state.
* **No global view** → each instance only sees its assigned partitions, so
  `min_events=100` and the 100-occurrence fingerprint trigger are *per-instance*
  counts. Running `detection` at 2 replicas roughly halves the effective volume
  per window. Scaling detection is therefore a *semantic* change, not just a
  capacity change ([§10.3](#103-services-that-do-not-scale-by-replica-count)).
* **Alert storm at the source**: rule 1 re-fires on every event while the rate
  is high; the engine has no cooldown for it. Suppression is deliberately
  deferred to the alert service ([D10](#d10-alert-deduplication-lives-downstream-of-detection)),
  at the cost of `alerts`-topic amplification during incidents.

**Delivery.** `fireAlert` spawns `go cb(alert)` per alert; the callback produces
to `alerts` keyed by fingerprint (so duplicates for one problem land on one
partition and are deduplicated in order by the consumer). Offsets for `logs` are
committed per message by the shared `PollLoop` — the detector never blocks on
dispatch, so a slow alert path cannot stall detection itself.

### 6.5 Alert service (`cmd/alert-service`)

**Responsibility.** Turn alert records into human notifications without paging
the same problem twenty times.

**Algorithm** (`Process`, via `Consumer.PollLoop`):

```
unmarshal → RLock: is fingerprint within 15 min cooldown? → yes: count dedup, ack
                                                          → no:  POST webhook (10 s)
     success → Lock: record dedup[fingerprint] = now → commit offset (PollLoop)
     failure → return error → PollLoop stops WITHOUT committing → Fatal → container restart
```

Two decisions worth preserving:

* **Dedup is recorded only after successful delivery.** Recording before send
  would suppress all retries of a failed notification for 15 minutes — the alert
  would simply vanish. This ordering converts "webhook flapped" into "alert
  re-fires", which is the correct failure direction.
* **Return-the-error to stop the commit** rather than retrying in-process:
  Kafka becomes the retry queue, with backoff provided by container restart.
  See [D12](#d12-fail-stop-consumer-loops).

State (the dedup map) is in-memory per replica, so a restart can re-notify.
`cleanupDedup` drops entries older than 2× cooldown every 5 minutes, bounding
the map. `ALERT_SERVICE_PORT` is loaded but unused; only `:9094/metrics` binds.

### 6.6 Query coordinator (`cmd/query-coordinator`, port 8081)

**Responsibility.** One API, two storage engines, consistent result shape.

**Tier selection:** `hotCutoff = now − HotRetentionDays` and a three-way branch
on the requested `[start,end]` ([§4.3](#43-query-path-tier-routing)).

**Hot path.** Two SQL statements per search (count, then page) against the
datasource. Predicates are built by `hotWhere` with `sqlLiteral` (single quotes
doubled) — parameter-style escaping without a prepared-statement API, which is
the right call for an HTTP-fronted OLAP endpoint but is still the security-
sensitive line in this service; free text becomes
`positionCaseInsensitive(message, '…') > 0`.

**Cold path.** Delegate to `storage.QueryEvents`: enumerate **one prefix per day**
in the range, list objects, `GET` each, unmarshal, apply service/severity/
free-text/time filters in memory, capped at 10 000 *scanned* events (then a hard
error, not truncation).

**Merged path.** Hot page (`offset+limit` rows since `hotCutoff`) + all cold
matches → concat → global sort desc → paginate. `total` mixes an exact hot count
with a cold scan count, so paging beyond `total` on a merged query can under- or
over-report. A tier that fails is logged and the request continues with partial
results and HTTP 200 ([L11](#161-confirmed-defects)).

**What it deliberately does not do:** cache, queue, budget per user, or write
anything back. It is a stateless router — the reason it can scale by `--scale`
with no coordination (and is currently the only service with no `depends_on`
on Kafka, since it never touches the log).

### 6.7 React UI (`ui/`)

Create React App, four presentational components, inline style objects, no
state library and no router.

| Component | Responsibility |
|---|---|
| `App.js` | Owns the query object; 3 parallel fetches (`/v1/search`, `/v1/timeline`, `/v1/aggregates`) on every query change |
| `SearchBar` | service, severity, free text, relative time range (1 h…30 d) → `onSearch` |
| `LogTable` | severity-colored rows, expandable detail (full message + pretty-printed attributes), client-paging via `offset` |
| `Timeline` | Inline SVG bars: total vs. error count per bucket |
| `MetricsPanel` | Totals, overall error rate, per-service breakdown from `aggregates` |

Design stance: the UI is a thin, dependency-light client that assumes the
coordinator already merged tiers — the UI never knows whether a row came from
Tinybird or R2 (`source` is displayed but not used for behavior). `REACT_APP_API_URL`
is the only configuration, and the compose container runs the dev server
(`npm start`), which is a convenience for a demo stack, not a serving strategy.

### 6.8 Shared internal packages

| Package | Public surface | Contract |
|---|---|---|
| `internal/config` | `Load() *Config` | Env-only, typed sub-structs, defaults for everything except secrets; malformed numeric values silently fall back to defaults |
| `internal/models` | 8 shared structs | The wire/schema contract; no validation methods by design (validation lives with each stage's responsibility) |
| `internal/kafka` | `Producer` (`Produce`, `ProduceSync`, `ProduceJSON`, `ProduceBatch`, `Flush`, `Close`), `Consumer` (`Poll`, `CommitMessage`, `CommitOffsets`, `PollLoop`), `CreateTopics` | Producer tuned for durability (`acks=all`, snappy, linger 5 ms, 1000-msg batches); consumer tuned for manual commits (`enable.auto.commit=false`, `earliest`) |
| `internal/tinybird` | `NewClient`, `AppendEvents`, `Query`, `Datasource` | Append is *verified*: `wait=true` plus row-count assertions; query response capped at 16 MiB |
| `internal/storage` | `S3Client`: `ArchiveRawBatch`, `QueryEvents`, `ListPartitions`, (+ unused `ArchiveEvents`/`ArchiveRaw`) | R2 via the S3 API dialect: path-style, `region=auto`, static credentials; never creates buckets unless `S3_ENSURE_BUCKET=true` |
| `internal/detection` | `Engine`: `NewEngine`, `ProcessEvent`, `OnAlert`, `GetWindows` | Pure in-memory state machine, no Kafka/HTTP knowledge → unit-testable in isolation (and currently untested, [L21](#163-engineering--hygiene)) |

The rule these packages encode: **`cmd/*` owns orchestration and transport;
`internal/*` owns protocol adapters.** No service imports another service.

### 6.9 Load generator (`cmd/load-generator`)

A closed-loop, rate-shaped producer used as the benchmark harness. Per worker:
`interval = batch / (target_rate / workers)` via a `time.Ticker`; each tick sends
one `/v1/ingest` batch and records wall-clock request latency, keeping the last
1 000 samples per worker. Reports sent/accepted/rejected/failed plus
min/avg/p50/p95/p99/max, in a `-step` mode (10 K → 25 K → 50 K → 100 K) and
with `-error-rate` to exercise detection. `-report` emits JSON so
`scripts/benchmark.sh` can archive machine-readable results. It links no Kafka
or cgo dependency, so it builds statically (`CGO_ENABLED=0`) and runs in the
`benchmark` compose profile (`docker compose run --rm load-generator …`).

Because it is closed-loop with a *fixed* target rate, it measures "did the
platform keep up" rather than "how fast can it go" — the right instrument for
T1/T2, and the reason `-rate` (not `-concurrency`) is the primary dial.

---

## 7. Key design decisions

Each decision lists alternatives considered and the accepted cost.

### D1. Kafka as the fan-out bus instead of a dispatcher that writes to two stores

*Considered:* (a) gateway writes Tinybird and R2 directly; (b) a "pipeline"
service multi-writes; (c) publish once to Kafka, let each sink subscribe.
*Chosen:* (c).
*Rationale:* (a) and (b) couple availability — a managed-store outage becomes
an ingest outage — and adding a third sink means editing a write path with
two durability regimes. With a bus, each sink has its own offset, its own batch
policy, its own restart semantics, and lag is observable per group.
*Cost:* an extra broker component to run, and no cross-sink transactionality
(duplicates on partial failure) — accepted, since the tiers serve different SLAs.

### D2. Consumers write to exactly one store each

*Chosen* so that the "commit ⟺ that store has the data" invariant is
unambiguously true (`processing-workers` ↔ Tinybird, `archive-writers` ↔ R2).
*Considered:* one worker writing both; it needs two-phase commit or a
per-store offset map, and the failure matrix grows combinatorially.

### D3. Commit only after the downstream store has acknowledged

The load-bearing rule of the whole pipeline. Both batch consumers implement the
same shape: buffer with retained `*kafka.Message`s → write → on success commit
`max offset + 1` per partition → on failure restore the buffer and commit
nothing. *Cost:* duplicate writes on crash-after-write-before-commit. *Chosen
because* the alternative — commit-then-write — has an equivalent code shape and
loses data on the same crash window. At-least-once with an idempotency key
strictly dominates at-most-once for telemetry.

### D4. Manual offset management, no auto-commit

`enable.auto.commit=false` in the shared consumer factory (`internal/kafka/kafka.go:207`)
even though auto-commit with `commit.interval` would be simpler and faster.
Auto-commit cannot express "commit *after* my write succeeded", and periodic
auto-commit while the buffer is un-flushed is exactly the loss window D3
forbids. *Cost:* commit-on-every-message in `PollLoop` (detection, alert
service) is chatty — fine for those volumes, and the batch consumers use
`CommitOffsets` instead.

### D5. The archive stores original bytes, not normalized ones

*Chosen:* verbatim `msg.Value` to R2, with only `service`/`timestamp` peeked for
routing. *Considered:* writing the normalized/redacted `LogEvent` (one shared
representation, and PII-safe everywhere — the code path `ArchiveEvents` still
exists for it). *Rationale:* the archive's purpose is re-interpretation — new
parsers, new redaction policy, new severity vocabulary, replay into a new
datasource. A normalized archive can only ever be as good as the normalizer
that produced it, which makes the archive a second copy of the hot tier instead
of a historical record. *Cost:* two representations to reason about, and
un-redacted content at rest (see N7 / [L13](#162-reliability--scalability-limits)).

### D6. One gzipped, `wait=true` append per batch to the hot tier

*Considered:* streaming NDJSON without `wait` (higher throughput, unverified);
per-event inserts (durable, ~100× the request count); async append plus a
reconciliation job (complex). *Chosen:* `POST /v0/events?name=logs&wait=true`
with `Content-Encoding: gzip`, and *assert* the ack (`successful_rows`,
`quarantined_rows == 0`) before committing. Gzip matters because log JSON
compresses ~10×, which keeps the cloud append from being network-bound. Asserting
the ack matters because `wait=true` acknowledges receipt, not a row count.

### D7. Partition key `service[:source]`

*Considered:* round-robin (perfect balance, no per-key ordering); event ID
(ordering only for identical events — useless); timestamp (hot-partition skew).
*Chosen:* service-scoped key, which preserves per-service ordering for
replay/dedup and lets a "reprocess one service" job consume one key family.
*Cost:* hot-service skew, bounded by `service:source` spreading large services
over several partitions.

### D8. Hot window and hot TTL are separate knobs

Routing (`QUERY_HOT_RETENTION_DAYS=7`) and physical retention (datasource
`TTL timestamp + INTERVAL 30 DAY`) are decoupled so that the interactive
experience is priced in the cheapest-to-serve slice while 30 days stay
*available* in the fast engine for ad-hoc SQL from the Tinybird console.
*Cost:* days 8–30 are answered from R2 (slow) even though a copy still sits in
Tinybird — an operator-visible inconsistency worth fixing by aligning the two
([roadmap R6](#17-roadmap)).

### D9. Detection state in process memory

*Considered:* RocksDB-backed state, a Kafka `__control` topic, or pushing
detection into Tinybird SQL. *Chosen:* per-service maps in RAM, with a 30 s
prune. *Rationale:* the state is tiny (one window per service), the recovery
story is "rebuild from Kafka at `earliest`", and correctness of *alerting* does
not need to survive a restart for a demo-grade SLO. *Cost:* partial-window loss
on restart and non-global thresholds under scale ([D11](#d11-one-detection-replica-is-a-correctness-assumption)).

### D10. Alert deduplication lives downstream of detection

Detection fires greedily; the alert service owns the 15-minute cooldown.
*Rationale:* the suppression policy (who gets paged, how often) is an
operational concern that changes often and applies per channel; keeping it out
of the stream processor means it can be reconfigured without touching — or
replaying — the detection pipeline. *Cost:* the `alerts` topic absorbs the storm
(≈ one message per event above threshold), and `handleAlert` spawns one
goroutine per alert.

### D11. One detection replica is a correctness assumption

With per-instance state, N replicas compute N partial error rates. The design
therefore assumes `detection: 1` and treats capacity as "one broker's worth of
partitions". *Considered:* key-by-service so a service's stream lands entirely
on one instance — better, but still breaks on rebalance. *Alternative for real
scale:* move rules into periodic Tinybird SQL ([roadmap R5](#17-roadmap)).

### D12. Fail-stop consumer loops

`PollLoop` returns on handler error instead of continuing (`internal/kafka/kafka.go:263`
— the comment documents why: continuing and committing a later offset would skip
the failed record). The process exits, `restart: unless-stopped` restarts it,
and the group reassigns from the last committed offset. *Rationale:* a stuck
partition with a lagging offset is recoverable and *observable*; a silently
skipped event is neither. *Cost:* restart-loop amplification when the failure is
permanent (e.g. a webhook endpoint that always 500s) — mitigated in practice by
the alert path's own cooldown.

### D13. Managed hot and cold storage, self-hosted streaming

*Considered:* ClickHouse + MinIO in compose (the repo still carries the S3 API
dialect and `S3_*` names from that design). *Chosen:* Tinybird + R2. *Win:* no
storage capacity management, no replication or compaction surgery, no
object-store disk failure domain; append and PUT are the whole integration
surface. *Cost:* the stack no longer boots offline; `scripts/init.sh` and
`MANAGED_SERVICES_SETUP.md` exist precisely to make that prerequisite explicit;
and correctness now depends on third-party availability windows, which is why
every consumer's failure mode is "buffer in Kafka", not "fail the client".

### D14. Reference OTel config is shipped but not deployed

`configs/otel-collector-config.yaml` (OTLP gRPC 4317 / HTTP 4318, `filelog`,
batch/memory-limiter/resource processors) documents the intended producer-side
collection story. The collector is **not** a compose service, and its
`otlphttp` exporter pointed at `http://gateway:8080/v1/ingest` is not
protocol-compatible: that endpoint takes raw `{"events":[…]}` JSON, while
OTLP/HTTP sends protobuf. A gateway OTLP receiver/adapter is roadmap work
([R7](#17-roadmap)); until then the config is documentation, not a deployable
artifact.

---

## 8. Consistency, ordering & delivery semantics

### 8.1 Guarantee per hop

| Hop | Guarantee | Mechanism | Duplicate window |
|---|---|---|---|
| Producer → gateway | at-most-once (client-driven) | HTTP 200 ⟺ Kafka acked | producer retries ⇒ duplicates |
| Gateway → Kafka | at-least-once | `acks=all`, `retries=3`, `ProduceBatch` awaits reports | no `enable.idempotence` ⇒ in-batch dupes possible on retry |
| Kafka → each consumer | at-least-once, ordered per partition | manual commit after durable write; `earliest` reset | write-succeeded/commit-failed, or buffer restored |
| Worker → Tinybird | at-least-once, batch-verified | `wait=true` + row-count assertion | full re-append of an acked-but-uncommitted batch |
| Archive → R2 | at-least-once, immutable append | `PutObject` before commit; non-deterministic key | duplicate **objects**, not just rows |
| Detection → alerts | at-most-once per event | best-effort produce, errors logged | lost alert if produce fails (offset still commits) |
| Alert → webhook | at-least-once | error ⇒ no commit ⇒ restart ⇒ redeliver | re-notify after cooldown expiry or restart |

**Overall: at-least-once end to end, with duplicates the only deviation from
exactly-once, and one at-most-once hop (detection → `alerts`).**

### 8.2 Where duplicates land and what to do about them

| Symptom | Cause | Intended response |
|---|---|---|
| Same `event_id` twice in a hot search page | worker flushed, crashed before commit | dedup on `event_id` at read time, or tolerate (analytics view) |
| A day's cold objects overlap | `FlushAll` retried a partially written service batch | dedup on `event_id`; or make the key deterministic (see below) |
| Producer sees `accepted < batch size` | Kafka leader election mid-batch | retry the whole batch with the same `event_id`s |
| One event missing from hot but present in cold | row quarantined on append → error → buffer restored → retried until it succeeds; or the event was DLQ'd and its commit advanced | inspect `logs-dlq` |

No deduplication layer exists today. `event_id` is the designed key: because
Tinybird is a MergeTree-family engine with no primary-key enforcement in this
datasource, dedup must be applied by the reader (`GROUP BY event_id`) or by a
replacing/aggregating merge datasource if the platform decides duplicates hurt
count accuracy more than they hurt availability.

### 8.3 Ordering

* **Within a partition:** offset order. Since the key is `service[:source]`, the
  platform preserves ordering *per producing source*.
* **Across partitions:** none. Every read path therefore sorts explicitly
  (`ORDER BY timestamp DESC` hot, `sort.Slice` desc for cold/merged).
* **Event-time vs processing-time:** Kafka record timestamps are set to
  enqueue time (`time.Now()` in `ProduceBatch`), while all filtering, bucketing,
  TTL and partitioning use the **event's own `timestamp`** (gateway-defaulted to
  ingest time when absent). Late arrival therefore never loses data, but it does
  land in the partition/segment of its event time while being *readable*
  immediately — and a sufficiently old `timestamp` will route a search to the
  cold tier even though the event is fresh in the hot tier ([L7](#161-confirmed-defects)).
* **Detection windows** use wall-clock window starts, not event-time watermarks;
  out-of-order events count in the window they arrive in. Acceptable for
  rate alerting, wrong for exactly-once sessionization — the reason detection is
  not a general stream processor.

---

## 9. Backpressure & overload policy

### 9.1 The chain, end to end

```
producer ──HTTP 200/429──► gateway ──ProduceBatch──► Kafka (7 d buffer) ──► consumers
   ▲                            │                                                  │
   │ retry w/ backoff           └─ 429 + Retry-After: 1 when                       ▼
   │                              per-process accepted > quota              downstream slow?
   └──────────────────────────────────────────────────────────────────────────────┘
     Nothing is dropped to relieve pressure: lag grows, and drains on recovery.
```

Three distinct buffering stages, each with its own policy:

| Stage | Buffer | Bound | When full |
|---|---|---|---|
| Ingress | none (synchronous to Kafka) | `GATEWAY_INGEST_QUOTA_PER_SEC` = 100 000/s/gateway, `ProduceBatch` 10 s deadline | 429 to client; or 500 if Kafka can't ack in 10 s |
| Broker | segment logs on disk | 168 h retention, 1 GiB segments | old data ages out (data already in both tiers is unaffected) |
| Consumers | in-memory batch buffers | worker 5 000 items; archive 500/service | flush is triggered, not blocked; on store failure the buffer *grows* |

### 9.2 Quota implementation and its limits

`currentRate` (`cmd/gateway/main.go:43,84,167,215`) counts **accepted** events
in a wall-clock second, reset by a 1 s ticker. Consequences an operator should
know:

* Rejected/invalid events do not consume quota.
* The window is per-process: scaling to 4 gateways multiplies the platform-wide
  allowance to ~400 K/s (per-gateway limit, not global). It is a self-protection
  mechanism, not a tenant policy.
* It is not a token bucket: within one second an arbitrarily large batch can be
  admitted as long as the counter is below quota at check time, and a request
  straddling the reset boundary is counted in whichever second it completes.
  The limit is therefore an approximate ceiling for load shedding, and precise
  only in the aggregate.

#### 9.2.1 What downstream pressure looks like

Tinybird and R2 apply pressure *only through Kafka lag* — this is the design's
main protection. A slow append means: fewer flushes, larger buffers, committed
offsets frozen, lag rising, gateways unaffected. Nothing in the pipeline blocks
the producer because a managed store is slow (only the `ProduceBatch` deadline
does), and nothing shrinks retention to make room.

Two known pressure-relief gaps, both in the same direction (memory growth rather
than shedding):

* Worker: on repeated append failure the buffer is prepended back
  (`cmd/worker/main.go:219`) with no ceiling → a multi-minute outage at 50 K/s
  is a multi-GB heap per worker. A capped buffer + shed-to-DLQ (or explicit
  "block the poll loop and let lag absorb it") is the intended fix
  ([R3](#17-roadmap)).
* Archive: `FlushAll` retries every service buffer each cycle; the same growth
  applies at 500-per-service granularity, and R2 rejects are usually
  retry-friendly (throttling), which the current loop handles by simply not
  advancing offsets.

### 9.3 Overload policy summary

**Prefer: shed at the edge (429) → buffer in Kafka → delay visibility.
Never: silently drop an event that was acknowledged.** The only sanctioned drop
path is DLQ diversion for structurally invalid payloads, which preserves the
bytes for inspection.

---

## 10. Partitioning, scaling & capacity model

### 10.1 Scaling units

| Component | Unit of parallelism | State | Scale signal | Command |
|---|---|---|---|---|
| gateway | request | none | ingest latency p95, 429 rate | `--scale gateway=4` |
| worker | Kafka partition (max 12/group) | buffer + offset position | `worker_append_latency_seconds`, group lag | `--scale worker=4` |
| archive-writer | partition (max 12) + service buffers | buffer | `archive_batch_latency_seconds`, lag | `--scale archive-writer=2` |
| detection | **1 instance by design** | in-process windows | events/s vs. CPU | do not scale without [D11](#d11-one-detection-replica-is-a-correctness-assumption) |
| alert-service | `alerts` partition (max 6) | dedup map | webhook failures | scale allowed; dedup weakens per replica |
| query-coordinator | request | none | query p95 per tier | `--scale query-coordinator=3` |
| Kafka | partition count | data | throughput/segment | increase `KAFKA_NUM_PARTITIONS` + `CreateTopics` (partitions are additive; per-key ordering across a repartition is not preserved) |

The **12-partition ceiling is the platform's real scaling wall for freshness**:
more workers than partitions sit idle, so ingest capacity for the hot tier is
`12 × (events per worker per second)`. Raising it is a partition-count change,
not a replica-count change.

### 10.2 Capacity model

Let `R` = events/s, `E` = mean event size (~400 B wire JSON), `B_w` = 5 000
(worker batch), `L_a` = Tinybird append round trip, `B_a` = 500 (archive batch),
`N` = partitions.

```
Ingress bytes/s          = R × E
Kafka bytes/s            = R × E × RF                       RF = 1 in compose
per-worker events/s      = B_w / (L_a + B_w × c_norm)       c_norm = normalize+redact cost/event
Workers needed           = ceil(R / per-worker events/s)    useful max = N (partitions)
Tinybird requests/s      = R / B_w                          1 request per flush
R2 requests/s            = R / B_a                          set by batch size, not by R
R2 objects/day           = R × 86400 / B_a
R2 bytes/day             = R × 86400 × E
Kafka bytes retained     = R × 86400 × 7 × E                168 h, no compaction
Cold query bytes read    ≈ Σ bytes in day-prefixes          capped at 10 000 scanned events
```

Worked example at **T1 (50 000 events/s, 400 B, 10 services, 12 partitions)**:

| Quantity | Value | Read |
|---|---|---|
| Bytes through the system | 20 MB/s ≈ 1.73 TB/day | each sink sees the full firehose; 7-day Kafka retention ⇒ ~12 TB of *broker* disk, which the reference host does **not** have — T1 is a throughput target, not a full-retention capacity target |
| Workers needed (≈28 K events/s each: 5 000 ÷ (150 ms append + ~25 ms normalize)) | **2 minimum, 4 recommended** | 4 leaves headroom to *drain* lag, not just track it; ≤12 is useful (§10.1) |
| Tinybird requests | 10 req/s, ~1.5 MB gzipped each | well inside managed-append limits |
| R2 PUTs | **100/s → 8.6 M objects/day** | object-count pathology, see §12.3 |
| Cold-scan cap | 10 000 events = 20 objects ≈ 0.2 s of data | a cold search at this volume is unusable beyond minutes |

The two non-obvious lines are the last two: at the platform's own throughput
target, **the archive's object size and the cold query's scan cap are the
binding constraints, not CPU or Kafka** ([R2](#17-roadmap), [R4](#17-roadmap)).

### 10.3 Services that do not scale by replica count

* **detection** — thresholds are per-instance (D11).
* **alert-service** — correctness (dedup) degrades linearly with replicas, since
  the cooldown map is local. Safe to scale for *throughput*, unsafe for *noise
  suppression*: a 2-replica alert service can double pages for one incident.
* **Kafka consumers of `logs-dlq`** — none exist; scaling is moot until a
  replay tool is added.
* Everything else scales linearly with partitions or request count.

### 10.4 Partitioning strategy summary

| Layer | Partition/bucket unit | Rationale |
|---|---|---|
| Kafka | `service[:source]` hash → 12 | per-key ordering, replayable per service, bounded hot partitions |
| Tinybird | `toYYYYMMDD(timestamp)` | drop-a-day TTL semantics, time-range pruning |
| R2 key | `raw/YYYY/MM/DD/HH/<service>/` | day for query pruning, hour for write locality, service for targeted replay |
| Detection | `map[service]` | per-service alert identity, so one bad service can't mask another |
| Alert dedup | `fingerprint` (= `error_rate_<service>` or `<service>:md5(template)`) | the natural identity of "the same problem" |

---

## 11. Reliability: failure modes & recovery

### 11.1 Behavior matrix

| # | Scenario | Implemented behavior | Recovery | Observable via |
|---|---|---|---|---|
| 1 | Gateway pod dies | LB/producer sees connection error; already-acked records are safe in Kafka | producer retries; `event_id` dedups | 429/5xx at LB, `gateway_events_ingested_total` drop |
| 2 | Worker dies mid-batch | uncommitted offsets; buffered items lost from RAM | rebalance after `session.timeout.ms=6000` → peer resumes from committed offset → re-reads and re-normalizes (duplicates in hot) | group lag, `worker_offsets_committed_total` stall |
| 3 | Worker dies between append and commit | rows already in Tinybird | redelivery → duplicate rows | hot `count()` vs. `count(DISTINCT event_id)` |
| 4 | Tinybird unavailable / slow | append errors → **buffer restored, nothing committed**; gateways unaffected | Kafka holds ≤7 d; drains at consumer throughput on recovery | `worker_flush_failures_total`, `worker_append_latency_seconds`, lag |
| 5 | R2 unavailable | PUT errors → `FlushAll` aborts; other services' objects written but not committed | redelivery → duplicate objects; hot tier unaffected | `archive_batch_latency_seconds`, archive-group lag |
| 6 | Kafka broker down | `ProduceBatch` errors/timeout → 500 to producers; consumers poll-error loop with 1 s sleep | clients retry; producers get 500 (not a silent success) | gateway 5xx, broker health |
| 7 | Kafka full / retention expiry | oldest segments deleted | **already in Tinybird + R2** — this is why both tiers subscribe independently | disk, per-group lag |
| 8 | Poison / malformed event | worker → `logs-dlq`, offset advances (no infinite loop); archive → `_unknown` partition; detection → warn + skip | reprocess from DLQ after fixing the producer | `worker_events_dead_lettered_total` |
| 9 | Traffic spike | 429 above quota per gateway; below quota the excess queues in Kafka | producers back off ≥1 s (`Retry-After`) | `gateway_events_rejected_total` |
| 10 | Consumer rebalance storm | each rebalance resets the worker buffer (in-RAM items are simply re-consumed by the new owner) | self-heals; extra duplicates possible | `kafka` JMX, `worker_batch_size` dips |
| 11 | Detection restart | in-memory windows + fingerprints lost; partial window re-accumulates from committed offset | alerting resumes within one window | `detection_events_processed_total` gap |
| 12 | Webhook down | `Process` errors → `PollLoop` returns → `logger.Fatal` → restart → redeliver | retry forever via Kafka; **restart-loop** while endpoint is down | `alert_service_sent_total{status="failed"}`, container restarts |
| 13 | Slow query / huge cold range | `maxScanItems` → explicit error "narrow the time range or filters" | caller narrows range | `query_coordinator_latency_seconds{source="cold"}`, 500s |
| 14 | Hot+cold partial failure (merged) | tier error logged, 200 with the surviving tier's rows | caller cannot tell from status code | coordinator error logs; `source:"merged"` count |

### 11.2 RPO / RTO by path

| Path | RPO (data) | RTO (function) |
|---|---|---|
| Ingest → Kafka | ~0 (client is not acked otherwise) | gateway: seconds (restart / LB failover) |
| Kafka → Tinybird | 0 (lag = freshness debt, no loss) | seconds-to-minutes = rebalance + drain at consumer throughput ÷ lag |
| Kafka → R2 | 0 | same, with a 10 s flush quantum |
| Detection → alert | an *unproduced* alert is lost (at-most-once hop) | one window |
| Whole stack (host loss) | ≤ what was in Kafka and not yet archived — bounded by the 2 s / 10 s commit quantum, and unrecoverable only if the broker's disk is lost | minutes: `docker compose up -d` + auto rebalance + drain |

There is no multi-host durability in the reference deployment: RPO ≈ 0 assumes
the Kafka volume survives. Production requires RF ≥ 3 brokers and rack-aware
placement ([R1](#17-roadmap)).

### 11.3 Replay (the designed recovery for "we mis-processed history")

```
# 1. Choose scope: R2 for > 7 d or hot-corrupted data; Kafka for anything in retention.
# 2. Add a second consumer group (no code change to producers):
KAFKA_GROUP_WORKERS=replay-<date>  →  auto.offset.reset=earliest replays the topic
# 3. Point TINYBIRD_DATASOURCE at a freshly pushed datasource, scale workers up.
# 4. Monitor lag; when it drains, cut TINYBIRD_DATASOURCE for the query
#    coordinator back to the good datasource and drop the old one.
```

`auto.offset.reset=earliest` + a **new group id** is the entire replay
mechanism: a brand-new group consumes the full 7-day topic from the beginning.
For anything older, read `raw/**` from R2 with a new parser (`ArchiveRawBatch`'s
inverse) and append. The archive exists so this procedure always works, which is
the payoff of [D5](#d5-the-archive-stores-original-bytes-not-normalized-ones).

### 11.4 Validated by

`scripts/failure-test.sh` — sustained 25 K/s load, `docker kill` a worker, wait
through rebalance, restart it, and assert lag recovers with no acknowledged
events lost. `scripts/benchmark.sh` — throughput stepping, 5-minute sustained
load, and a two-phase 0.2 %→8 % error-rate test that must produce alerts.
These are the executable reliability spec; neither is wired into CI ([L21](#163-engineering--hygiene)).

---

## 12. Performance & cost characteristics

### 12.1 Per-event cost by stage

| Stage | Work per event | Amortized over | Hot spot |
|---|---|---|---|
| gateway | decode, 2 field checks, marshal, key hash, enqueue | batch of up to 1 000 (producer `linger.ms=5`) | JSON decode/encode; per-message channel bookkeeping in `ProduceBatch` |
| worker | unmarshal, severity map, 3 regexes + attribute scan, marshal row, gzip | 5 000-row batch | regex redaction, then the network round trip |
| worker (per batch) | 1 gzipped HTTPS POST + ack parse | — | **`L_a`**: managed-store RTT, ~10× the CPU cost per event at 400 B |
| archive | 1 partial unmarshal (`extractMetadata`) + memcpy | 500 per service | 1 PUT / 500 events; copy dominates |
| detection | unmarshal, map ops, regexes **only for ERROR/FATAL/CRITICAL** | n/a | fingerprint normalization, proportional to error volume — spikes exactly when load is high |
| query (hot) | SQL parse, `positionCaseInsensitive` scan | per query | free-text scan of `message` in range (no inverted index) |
| query (cold) | list, GET, unmarshal, filter every line | per day prefix | O(bytes in range) with a 10 000-event cap |

The shape to remember: **the pipeline is network-latency-bound at the Tinybird
append and CPU-bound in the redaction/fingerprint regexes.** Both were addressed
the same way — batch and compile once — which is why the two `// compiled once`
comments in the source are load-bearing and should not be "simplified".

### 12.2 Cold path scan cost

`QueryEvents` has no pushdown beyond the *day* prefix: listing every object in
each day in the range, downloading and unmarshaling all of them, then filtering.
At 1.73 TB/day that is a 1.73 TB read for a one-day query — hence the cap
(`internal/storage/s3.go:201`), which converts "unbounded bill" into "explicit,
retryable error". Ranked fixes: service+hour prefix pruning when the filter is
known, gzip + `Content-Encoding`, a per-partition manifest (event-time min/max +
service set per object) so objects can be skipped without download, then a
columnar format.

### 12.3 Object-store write amplification

At the T1 target, 500-event batches produce ~100 PUTs/s and 8.6 M objects/day.
Beyond request cost, small objects are what makes cold queries expensive
(per-object list+GET overhead is paid 8.6 M times) and makes the bucket hard to
inspect. Target state: flush on **max(size, time)** — e.g. 64–256 MB or 5 min —
and add a compaction pass. `QUERY_MAX_SCAN_BYTES` was clearly added for the
"bound cold reads by bytes" idea and never wired up ([L7](#161-confirmed-defects)).

### 12.4 Measurement method

Targets in [§2.2](#22-design-targets) are validated by running, not by unit
tests: `docker compose up -d --build` → `./scripts/benchmark.sh` → read
`benchmark-results/*-report.json` (`accepted_rate`, `p95_ms`, `p99_ms`) and
Grafana for pipeline-side effects (`worker_append_latency_seconds` for
freshness, `query_coordinator_latency_seconds` for interactivity). Expected
signatures of a healthy run: acceptance ≈ 100 %, ingest p95 well under the 10 s
produce deadline, batch-size histogram pinned at 5 000 in the stepping test, and
per-group lag returning to ~0 within seconds of the load stopping.

### 12.5 Cost drivers & levers

| Driver | Scale | Main lever |
|---|---|---|
| Kafka disk | 1.73 TB/day × 7 d | shorten retention once both tiers are healthy, or add brokers |
| Tinybird ingest + storage | rows, then compressed bytes | gzip (done), batch size, TTL 30 d → match routing window |
| R2 storage | 1.73 TB/day | lifecycle policy (not configured in-repo), gzip/Parquet |
| R2 requests | 100 PUT/s at T1 | bigger batches + compaction |
| Egress | cold scans, UI result pages | global `LimitReader` caps (64 KiB ack, 16 MiB query) bound single responses |

---

## 13. Security & compliance design

### 13.1 Attack surface as deployed

| Surface | Exposure today | Risk |
|---|---|---|
| `POST :8080/v1/ingest` | host-published, **no authentication**, CORS `*` | anyone who can reach the port can inject, forge, or flood logs; quota is the only brake. Documented as intended for producers (`README`) but unimplemented ([L2](#161-confirmed-defects)) |
| `:8081` query API | host-published, no auth, CORS `*` | full-text read access to every service's logs — the highest-value surface, since archived messages may contain un-redacted secrets |
| Kafka `:9092` | host-published, `PLAINTEXT` | unauthenticated produce/consume; a consumer can also *commit offsets* for other groups |
| ZooKeeper `:2181`, JMX `:9997` | host-published | dev-only admin surfaces |
| Prometheus/Grafana | `:9090`, `:3001` `admin/admin` | default credentials |
| Metrics `:9091-9094` | in-network only | low risk, but `/health` on detection leaks window internals |

Everything above is a *reference-deployment* property: ports published to the
host, no TLS, no SASL. It is acceptable for a local demonstration and is exactly
the list to close before a real deployment.

### 13.2 Credential topology

The part that is designed deliberately, per least privilege:

| Secret | Held by | Scope |
|---|---|---|
| `TINYBIRD_APPEND_TOKEN` | workers only | `DATASOURCE:APPEND` on `logs` — cannot read |
| `TINYBIRD_READ_TOKEN` | query coordinator only | read on `logs` — cannot write |
| `S3_ACCESS_KEY`/`SECRET` | archive writer **and** query coordinator | R2 token scoped to `S3_BUCKET`, Object Read & Write |
| Kafka | — | no credentials (open) |
| `ALERT_WEBHOOK_URL` | alert service | bearer-less webhook |

Append and read tokens live on *different services and different containers*, so
compromise of the public query API cannot write, and compromise of an ingest
worker cannot read. `.env` is gitignored (`.env.*` except `.env.example`);
`MANAGED_SERVICES_SETUP.md` is the provisioning procedure; `scripts/init.sh`
refuses to start with `REPLACE_WITH_*` placeholders still in place. R2 bucket
creation is explicitly out of application scope (`S3_ENSURE_BUCKET=false`), so
no service carries bucket-admin rights.

### 13.3 Data protection & privacy

* **Redaction at the hot boundary** (worker) — secret-looking assignments,
  e-mails, card-number groups, and sensitive attribute keys are replaced with
  `[REDACTED]` before the row reaches Tinybird. This is *policy-in-code*, not
  configuration, and it is best-effort pattern matching: it catches common
  shapes, not every secret, and it does not touch `event_id`/`trace_id`.
* **The archive is intentionally unredacted** ([D5](#d5-the-archive-stores-original-bytes-not-normalized-ones)).
  That is a compliance *tension*, not an oversight: "immutable raw retention for
  replay" and "erase/PURGE requests" cannot both hold without an encryption /
  re-encryption story. Sign-off required, and the cheap interim answers are (a)
  redact-then-archive behind a flag, or (b) treat the bucket as a restricted
  system-of-record with its own access review ([R8](#17-roadmap)).
* **Retention:** Kafka 7 d, Tinybird 30 d, R2 unbounded (no lifecycle rule in
  this repo). `ListPartitions` exists to support day-level expiry tooling but no
  expiry job is implemented.
* **Injection:** hot-tier SQL is built by string composition with `sqlLiteral`
  quote-doubling and `fmt.Sprintf` for `LIMIT`/`OFFSET`/`INTERVAL` from
  validated ints. There is no prepared-statement API upstream, so escaping is
  the control — the single most sensitive code path in the platform, and the one
  that most needs a test suite.
* **Response-size hard limits** (`64<<10`, `16<<20` `io.LimitReader`) bound
  uncontrolled memory growth from a misbehaving upstream.

### 13.4 Production hardening checklist

Ordered by risk reduction per unit of effort:

1. Authenticate ingest (`Authorization` → per-tenant key/rotatable token,
   validated at the gateway) and authorize the query API (JWT/OIDC + service
   allowlist per caller).
2. TLS + SASL_SSL on Kafka; remove host-published `9092`/`2181`/`9997`.
3. Restrict CORS to the UI origin; drop `AllowCredentials` with `*`.
4. Secrets from a vault/K8s `Secret`, not `.env`; rotate Tinybird/R2 tokens.
5. Redaction decision for the cold tier, documented and enforced.
6. Audit log for queries (who searched what) — cheap in the coordinator.
7. Grafana/Prometheus credentials and network policy.

---

## 14. Observability design

### 14.1 Metrics inventory (all implemented and registered)

| Service | Port | Metric | Type | Use |
|---|---|---|---|---|
| gateway | 8080 | `gateway_events_ingested_total{service,status}` | counter | ingest rate; `status` ∈ accepted/rejected/kafka_failed |
| | | `gateway_events_rejected_total` | counter | quota shedding |
| | | `gateway_ingest_latency_seconds` | histogram | client-visible ack latency |
| worker | 9091 | `worker_events_processed_total` | counter | processing throughput |
| | | `worker_events_dead_lettered_total` | counter | payload-quality drift |
| | | `worker_flush_failures_total` | counter | **store-health signal** |
| | | `worker_offsets_committed_total` | counter | commit liveness (stall ⇒ freshness debt) |
| | | `worker_batch_size` | histogram | whether 5 000 batching is working |
| | | `worker_processing_latency_seconds`, `worker_append_latency_seconds` | histograms | CPU vs. network split |
| archive | 9092 | `archive_events_total`, `archive_batch_latency_seconds` | counter, histogram | archive lag & RTT |
| detection | 9093 | `detection_events_processed_total`, `detection_alerts_fired_total`, `detection_processing_latency_seconds` | counters, histogram | coverage + alert rate |
| alert | 9094 | `alert_service_received_total`, `alert_service_sent_total{channel,status}`, `alert_service_dedup_total` | counters | delivery + suppression |
| query | 8081 | `query_coordinator_queries_total{source}`, `query_coordinator_latency_seconds{source}` | counter, histogram | per-tier latency; `source` ∈ hot/cold/merged |

**Cross-check invariant that should hold in dashboards:**
`sum(rate(gateway_events_ingested_total{status="accepted"}[5m]))` ≈
`rate(worker_events_processed_total[5m]) + rate(worker_events_dead_lettered_total[5m])`
≈ `rate(archive_events_total[5m])`. Divergence localizes the fault: gateway→
consumer = ingestion/queueing; worker vs. archive = the hot path only.

### 14.2 SLIs / SLOs (proposed; the metric for each already exists)

| SLI | SLO | Query sketch |
|---|---|---|
| Ingest availability | 99.9 % non-5xx on `/v1/ingest` | `1 − (5xx + timeouts)/requests` (needs a request counter — today only *event* counters exist) |
| Freshness (p95) | < 10 s ingest→searchable | `(timestamp_commit − event.timestamp)`; not exported → **needs a new histogram** (see gap 3) |
| Search latency p95 | < 2 s hot | `histogram_quantile(0.95, sum by (le) (rate(query_coordinator_latency_seconds_bucket{source="hot"}[5m])))` |
| Archive durability | 100 % of accepted events present in R2 | compare `archive_events_total` vs. `events_ingested{status="accepted"}` over a day |
| Alert time-to-page | < 60 s after threshold crossing | `TriggeredAt` vs. dispatch time — logs only today |
| Poison rate | < 0.1 % of events | `worker_events_dead_lettered_total / worker_events_processed_total` |

### 14.3 Dashboards & alerts

`configs/grafana/provisioning/dashboards/log-analytics-platform.json` is
**provisioned automatically** (datasource `prometheus`, 4 rows, 11 timeseries:
ingest rate by status, ingest p95, rejections; worker throughput, worker
failures + DLQ, R2 archive rate; detection throughput, alerts fired,
notification status; queries/s and p95 per tier). Note the README's "key
dashboards to create" list predates this file.

Alert rules worth adding (Prometheus format): `rate(worker_flush_failures_total[2m]) > 0`
(store unhealthy), per-group `kafka_consumergroup_lag > 1e6` (freshness debt),
`increase(gateway_events_rejected_total[1m]) > 0` sustained (quota),
`rate(alert_service_sent_total{status="failed"}[5m]) > 0` (notification path).

### 14.4 Logging

Structured `zap.NewProduction()` (JSON, level-filtered by env today) in every
service; no per-request ID, no sampling. Deliberately unnoisy: per-batch lines
are `Debug`, faults are `Error` with the offset/partition attached so a stuck
consumer can be located from one log line. **No log-shipping sidecar** is
configured — the platform does not dogfood its own ingest path, which is the
obvious first addition for a real deployment.

### 14.5 Gaps

1. **No `/health` on worker, archive-writer, alert-service** (metrics port only;
   detection has one on 9093), and no compose `healthcheck` on any application
   service → orchestration cannot distinguish "process up" from "consuming".
2. **Prometheus scrape targets are static DNS names** (`worker:9091` in
   `configs/prometheus.yml`), so `--scale worker=4` yields *one* non-deterministic
   replica per scrape, not four. Scaled capacity looks flat on dashboards. Needs
   Docker/Consul service discovery, per-pod targets, or Pushgateway.
3. **No freshness metric** (event-time → committed-time) and no per-request
   counters on ingest (only per-event), so the two most useful SLOs need one new
   histogram each.
4. **No tracing** (`trace_id` is a payload field), no continuous profiling, no
   Kafka lag exporter wired into Prometheus (JMX is scraped but no exporter is
   configured for it), and the DLQ has no depth metric at all.

---

## 15. Deployment topology & operations

### 15.1 Reference topology (docker-compose.yml)

| Service | Image | Ports | Depends on | Notes |
|---|---|---|---|---|
| `zookeeper` | `confluentinc/cp-zookeeper:7.5.0` | 2181 | — | healthcheck `nc -z` |
| `kafka` | `confluentinc/cp-kafka:7.5.0` | 9092, 9997 | zookeeper healthy | dual listener, RF=1, 12 parts, 168 h, 1 GiB segments |
| `gateway` | `Dockerfile.gateway` | 8080 | kafka healthy | scale unit 1…N |
| `worker` | `Dockerfile.worker` | — | kafka healthy | scale unit; Tinybird creds |
| `archive-writer` | `Dockerfile.archive-writer` | — | kafka healthy | scale unit; R2 creds |
| `detection` | `Dockerfile.detection` | — | kafka healthy | **single replica by design** |
| `query-coordinator` | `Dockerfile.query-coordinator` | 8081 | — | reads both tiers; only service that can start before Kafka |
| `alert-service` | `Dockerfile.alert-service` | — | kafka healthy | webhook |
| `ui` | `ui/Dockerfile` (node 18) | 3000 | query-coordinator | CRA dev server |
| `prometheus` | `prom/prometheus:2.48.0` | 9090 | — | volume `prometheus_data`, `--web.enable-lifecycle` |
| `grafana` | `grafana/grafana:10.2.0` | 3001 | prometheus | provisioning mounted read-only |
| `load-generator` | `Dockerfile.load-generator` | — | gateway | profile `benchmark` (not started by default) |

Not in compose by design: Tinybird, R2, OpenTelemetry Collector. All storage
failure domains therefore live outside `docker compose`.

### 15.2 Build & image strategy

Seven per-service Dockerfiles, each `golang:1.21` → static-ish multi-stage with
`CGO_ENABLED=1`, `-ldflags="-s -w"`, and librdkafka at both build
(`librdkafka-dev`) and run (`librdkafka1`). Two bases coexist for historical
reason: `bookworm-slim` (worker, gateway, detection, alert, archive) and `alpine`
(query-coordinator, load-generator). `Dockerfile` (root) builds all seven
binaries into one image and is **not referenced by compose** — an artifact of the
all-in-one layout.

Consequences to note: alpine + musl + cgo is the fiddliest combination in this
repo, so standardizing on the Debian base for every librdkafka service removes a
whole class of build failures; and `go mod download` is cached ahead of
`COPY . .`, which keeps rebuilds during an incident fast — worth preserving.

### 15.3 Configuration reference

Single flat env-var surface, read once at boot by `config.Load()`; no file
config, no hot reload, and **no validation of cross-field consistency** (a
malformed number silently becomes the default).

| Variable | Default | Consumed by | Notes |
|---|---|---|---|
| `KAFKA_BROKERS` | `localhost:9092` (`kafka:29092` in compose) | all Go services | internal listener |
| `KAFKA_TOPIC_LOGS` / `KAFKA_TOPIC_DLQ` | `logs` / `logs-dlq` | gateway, worker, detection, archive, alert | |
| `KAFKA_GROUP_WORKERS` / `_ARCHIVE` / `_DETECTION` | `processing-workers` / `archive-writers` / `detection-service` | respective consumers | **new group id = full replay** |
| `TINYBIRD_API_URL` | `https://api.tinybird.co` | worker, query | must match workspace region |
| `TINYBIRD_APPEND_TOKEN` | *required* | worker | fail-fast if empty |
| `TINYBIRD_READ_TOKEN` | *required* | query | fail-fast if empty |
| `TINYBIRD_DATASOURCE` | `logs` | worker, query | one knob for hot tier + replay target |
| `S3_ENDPOINT` | *required* | archive, query | `https://<acct>.r2.cloudflarestorage.com` |
| `S3_REGION` | `auto` | archive, query | R2 constant |
| `S3_BUCKET` | `log-archive` | archive, query | |
| `S3_ACCESS_KEY` / `S3_SECRET_KEY` | *required* | archive, query | static creds |
| `S3_ENSURE_BUCKET` | `false` | archive | local-S3 escape hatch only |
| `GATEWAY_PORT` / `GATEWAY_INGEST_QUOTA_PER_SEC` | 8080 / 100000 | gateway | quota is per process |
| `QUERY_COORDINATOR_PORT` / `QUERY_HOT_RETENTION_DAYS` | 8081 / 7 | query | routing window, not TTL |
| `QUERY_MAX_SCAN_BYTES` | 1 GiB | **unused** | cold path uses `maxScanItems = 10000` |
| `DETECTION_WINDOW_MINUTES` / `_ERROR_RATE_THRESHOLD` / `_MIN_EVENTS` | 5 / 0.05 / 100 | detection | per instance |
| `ALERT_SERVICE_PORT` | 8082 | **unused** | no HTTP server in alert service |
| `ALERT_WEBHOOK_URL` | `""` | alert | `""`/`PLACEHOLDER_…` → log-only mode |
| `REACT_APP_API_URL` | `http://localhost:8081` | ui | build-time inlined |

Fingerprint rule thresholds (`> 100`, 15 min) and the archive batch size (500)
are **code constants**, not config — the only two tuning knobs an operator
cannot set without a rebuild ([R9](#17-roadmap)).

### 15.4 Bring-up, rollout, teardown

```bash
cp .env.example .env                       # then provision per MANAGED_SERVICES_SETUP.md
docker compose up -d --build               # Kafka first (healthcheck-gated), then services
./scripts/init.sh                          # validate .env, create topics, probe Tinybird + R2
curl -sf localhost:8080/health             # ingest path up
docker compose up -d --scale worker=4 --scale gateway=4
./scripts/benchmark.sh                     # or: ./scripts/failure-test.sh
docker compose stop                        # state preserved
docker compose down                        # containers gone, Kafka volume intact
docker compose down -v                     # DESTRUCTIVE: Kafka + Prometheus/Grafana data
```

**Rolling upgrade of a stateless service** (gateway, query, UI): scale +1, drain,
scale −1, or `docker compose up -d --no-deps --force-recreate gateway`.

**Upgrade of a consumer** (worker, archive, detection, alert): plain
`--force-recreate` is safe *because of D3* — in-memory buffers are discarded and
the next process resumes from the last committed offset. The only ordering
requirement is **schema before code**: push the datasource first when a new field
is added, then roll the workers (a row whose column is missing quarantines →
`quarantined_rows > 0` → error → buffer restore → restart loop until the
datasource catches up, which is a self-healing but noisy failure mode).

**Kafka-side changes** — topic/retention/partitions — must go through
`scripts/init.sh`-equivalent admin steps; `CreateTopics` in the gateway never
alters existing topics (it tolerates `ErrTopicAlreadyExists`).

### 15.5 Runbook

| Symptom | First check | Likely cause | Action |
|---|---|---|---|
| UI shows nothing but gateway 200s | `worker_flush_failures_total`, group lag | Tinybird token/datasource/region | fix `.env`, `tb push` the datasource, restart workers |
| Hot results fine, cold error "exceeds scan limit" | requested range size | `maxScanItems` | narrow range/service; roadmap R4 |
| Lag grows while CPU is low | `worker_append_latency_seconds` p95 | append RTT, or workers > useful parallelism vs. 12 partitions | bigger `appendBatchSize`, more partitions |
| `worker_events_dead_lettered_total` climbing | a `logs-dlq` consumer's `error` field | producer format drift | fix producer; re-emit corrected events |
| Duplicate rows | hot `count()` vs. `count(DISTINCT event_id)` | restart between write and commit (expected) | dedup at read, or accept |
| Alerts missing during an incident | `detection_alerts_fired_total` vs. `alert_service_received_total` | fired-but-not-delivered → webhook failing | fix webhook; expect restart loop until it recovers |
| Restart-looping alert container | `alert_service_sent_total{status="failed"}` | unreachable/4xx webhook | set `ALERT_WEBHOOK_URL=""` for log-only mode |
| Only one replica visible in Grafana | scrape targets | static DNS + scaling | service discovery (gap 2) |
| Search ignores UI time range | `start_time` key in the request body | [L1](#161-confirmed-defects) | fix the UI field names, or add server-side camelCase aliases |

---

## 16. Tradeoffs, limitations & known defects

### 16.1 Confirmed defects

| # | Finding | Evidence | Impact | Fix |
|---|---|---|---|---|
| L1 | UI sends `startTime`/`endTime`; the API expects `start_time`/`end_time` | `ui/src/App.js:20-21` vs. `internal/models/models.go:48-49` | Range picker has no effect — every search is "last 24 h"; cold tier + `merged` tier unreachable from the UI; timeline/aggregates also hardcode `hours=24` | rename in `App.js` (+ optionally accept both server-side) |
| L2 | Gateway does not authenticate, though `README` says it "authenticates producers" | no auth path in `cmd/gateway/main.go`; `Authorization` appears only in CORS `AllowedHeaders` (`:196`) | unauthenticated write path, quota-only abuse control | per-tenant ingest keys validated at the gateway (§13.4 item 1) |
| L3 | Windows are tumbling, not sliding, while docs say "sliding-window rules" | `internal/detection/engine.go:71` resets at first event after expiry | alert sensitivity depends on window phase; a spike straddling a boundary splits and may cross `min_events` on neither side | bucketed ring (e.g. 12 × 30 s) or event-time watermarks |
| L4 | Error-rate alert has no engine-side cooldown | `engine.go:99-113` fires per event while rate ≥ threshold | `alerts` topic + goroutines scale with event rate during incidents (dedup absorbs *notification* only) | cooldown per fingerprint in the engine (rule table, [R5](#17-roadmap)) |
| L5 | Fingerprint alert threshold hardcoded (`entry.Count > 100`, 15 min) | `engine.go:138` | untunable false-positive control | move to config; enable the existing `models.AlertRule` (declared, unused) |
| L6 | Worker buffer is unbounded under sustained store failure | `cmd/worker/main.go:219` prepends failed batches | OOM risk / restart churn instead of graceful stall | cap + shed-to-DLQ, or block the poll loop |
| L7 | `QUERY_MAX_SCAN_BYTES` is loaded but never read | `internal/config/config.go:51,96`; cold limit is `maxScanItems = 10000` (`internal/storage/s3.go:201`) | documented byte budget does not exist; the real cap is event-count and errors rather than truncates | implement byte-budgeted scan or delete the knob |
| L8 | Archive partition path derives from `rawItems[0].Timestamp` only | `cmd/archive-writer/main.go:136` | a batch straddling **midnight** is filed entirely under the earlier day; a day-scoped query for the later day misses it | bucket buffers by (service, day/hour) and flush per bucket |
| L9 | Non-deterministic object key ⇒ duplicate objects on retry | `internal/storage/s3.go:154` (`batch-<unixnano>.jsonl`) | cold duplicates, extra list/scan cost | deterministic key `(partition, offset-range)` + conditional write, or read-time `event_id` dedup |
| L10 | Cold read errors are skipped with a `Warn` | `s3.go` `readObject` failure path (`continue`) | silent partial results returned as HTTP 200 | propagate per-object failures; report `partial:true` |
| L11 | Merged tier continues on a tier failure | `cmd/query-coordinator/main.go:259-276` (log-only) | callers cannot distinguish "no data" from "tier down" | surface `degraded`/`errors` in `QueryResponse` |
| L12 | Timeline & aggregates are hot-only | `handleTimeline`/`handleAggregates` → `hotTimeline`/`hotAggregateCount` | cold ranges render empty charts | cold-side bucketing or an explicit "hot window only" UI note |

### 16.2 Reliability & scalability limits

| # | Limit | Why it is a design choice | When it must be fixed |
|---|---|---|---|
| L13 | Cold tier holds **unredacted** payloads by design | [D5](#d5-the-archive-stores-original-bytes-not-normalized-ones) | before any PII/secret-bearing producer is onboarded |
| L14 | Single-broker Kafka, RF=1; `acks=all` therefore means "one copy" | dev topology, keeps the compose stack under 4 GB | any production use |
| L15 | 12 partitions ⇒ ≤12 hot-path consumers; ≤6 for alerts | partition count set at creation (`KAFKA_NUM_PARTITIONS`) | sustained > ~12 × single-worker throughput |
| L16 | No dedup layer; duplicates are inherent to at-least-once | [§8](#8-consistency-ordering--delivery-semantics) | whenever exact counts are contractual |
| L17 | Detection/alert state is per-process | [D9](#d9-detection-state-in-process-memory) | `detection` or `alert-service` beyond one replica |
| L18 | No graceful drain on gateway; `ctx` unused in shutdown | `cmd/gateway/main.go:248-255` | rolling upgrades at high batch size |
| L19 | No rate/row budget per query caller | stateless router | multi-tenant exposure of `:8081` |
| L20 | Cold query reads whole objects; no manifest, no pushdown | [§12.2](#122-cold-path-scan-cost) | any archive beyond a few GB |

### 16.3 Engineering & hygiene

| # | Note |
|---|---|
| L21 | **No tests**: 0 `*_test.go` in the repo. Highest-value first targets: `normalizeSeverity`, the three redaction regexes, `normalizeMessage`/fingerprint stability, `hotWhere`/`sqlLiteral` escaping (security), tier-selection branch boundaries, `commitOffsets` math, and `Engine` window transitions. `internal/detection` is pure and trivially testable today. |
| L22 | Docs/code drift: `README` states a worker "Batch size: 1000 events or 2-second flush", but `appendBatchSize` is 5 000 (`cmd/worker/main.go:77`); Dead code / drift: `models.RawEvent` and `models.AlertRule` unused; `S3Client.ArchiveEvents`, `ArchiveRaw`, `ListPartitions` unreferenced (and `ArchiveEvents` would panic on an `event_id` shorter than 8 chars, `s3.go:105`); `gopkg.in/yaml.v3` declared in `go.mod` but not imported; `Dockerfile` unused by compose; `configs/otel-collector-config.yaml` not deployed ([D14](#d14-reference-otel-config-is-shipped-but-not-deployed)); README's "dashboards to create" and "authenticates producers" overstate current state |
| L23 | Silent-default config parsing: a typo'd numeric env value falls back to the default with no log line — surprising in production. Prefer `Load() (*Config, error)` with strict parse |
| L24 | Error strings created without wrapping in a few paths (e.g. `fmt.Errorf("all brokers down")` at `internal/kafka/kafka.go:234`), weakening `errors.Is/As` |
| L25 | `load-generator` reports `-rejected` as ingest `failed`, conflating quota and validation in reports; harmless for benchmarking, confusing when comparing against `gateway_events_rejected_total` |

### 16.4 Alternatives considered at the architecture level

| Choice | Alternative | Why not |
|---|---|---|
| Kafka | NATS JetStream / Redpanda | both viable; Kafka's consumer-group + retention + tooling (and librdkafka client) were chosen for fidelity with production log pipelines. Swap cost is one `internal/kafka` adapter |
| Tinybird | self-hosted ClickHouse | cheaper at very high volume, but reintroduces the ops burden [D13](#d13-managed-hot-and-cold-storage-self-hosted-streaming) removes; the `Client` surface (append NDJSON + SQL) ports to ClickHouse almost directly |
| R2 | S3 + Glacier tiers | R2's zero egress pricing suits a read-mostly cold tier queried by scan |
| Go | Java/Kotlin (Kafka Streams), or Flink | Kafka Streams would give real state + rebalance-safe windows for detection at the cost of a heavier runtime for the other five services; Flink is the right answer for the *detection* component specifically |
| Go `embed`-free React | Next.js/SSR or Grafana-only UI | CRA keeps the client trivial and dependency-light; a Grafana-only UI would have removed the tiered-search story, which is the point of the exercise |

---

## 17. Roadmap

Prioritized by (risk removed) ÷ (effort), with the section that motivates each.

| # | Work | Fixes / enables | Notes |
|---|---|---|---|
| R1 | **Test suite** | L21 | unit tests for redaction, normalization, fingerprinting, `sqlLiteral`, tier boundaries, `commitOffsets`; property test on `ProduceBatch` counting; then an integration test over a testcontainer Kafka + fake Tinybird |
| R2 | **Bigger archive batches + compaction + manifest** | L8, L9, L20, §12.3 | flush on size-or-time (64–256 MB), deterministic keys, gzipped objects, per-object min/max event time + service set → cold queries prune instead of scanning |
| R3 | **Bounded consumer buffers + explicit shed policy** | L6 | cap ≈ N × batch; on overflow either stop polling (let Kafka absorb it — preferred, matches §9) or shed to DLQ with a metric |
| R4 | **Cold-tier budget & graceful truncation** | L7, L10, L11 | honor a byte budget, return `partial`/`degraded` instead of all-or-nothing errors |
| R5 | **Rules engine for detection** | L3, L4, L5, L17 | config-driven `AlertRule` (type, threshold, window, cooldown, per-service overrides), sliding buckets, engine-side cooldown, optional move to periodic Tinybird SQL for global correctness; state in Redis/RocksDB if it must survive rebalance |
| R6 | **Align hot routing window with hot TTL** | D8 | one knob, or route on data presence rather than a fixed day count |
| R7 | **Real ingest authN + query authZ; OTLP receiver** | L2, §13.1, [D14](#d14-reference-otel-config-is-shipped-but-not-deployed) | per-tenant keys, OIDC on the query API, an OTLP/HTTP-JSON adapter so the collector config becomes deployable, TLS/SASL for Kafka, CORS pinned to the UI origin |
| R8 | **Cold-tier privacy decision + R2 lifecycle** | L13 | documented redaction policy for the archive; expiry/CIA transitions for `raw/**` |
| R9 | **Config surface cleanup** | L5, L23, §15.3 | promote hardcoded thresholds to env, strict `Load() (*Config, error)`, delete dead knobs |
| R10 | **Operational completeness** | §14.5 | `/health` on all six services + compose healthchecks, service-discovery-based scraping, freshness histogram, DLQ depth metric, per-request ingest counter |
| R11 | **DLQ replay tool + `logs-dlq` retention** | §5.5 | `cmd/dlq-replay` that re-publishes fixed payloads; today the DLQ has no consumer at all |
| R12 | **Kubernetes deployment + HA Kafka (RF=3)** | L14, L18 | manifests/Helm, HPA on lag, `preStop` drain + `server.Shutdown`, and a graceful-drain fix for consumers |
| R13 | **Dogfood: ship platform logs to itself** | §14.4 | one OTel Collector per host with a `filelog` receiver → the platform's own gateway |
| R14 | **Read-time dedup option** | L16, L9 | `event_id`-based dedup (ReplacingMergeTree-style datasource, or `GROUP BY event_id` in the hot query) for count-exact tenants |

---

## 18. Appendices

### A. Code map (responsibility → file)

```
cmd/gateway/main.go          HTTP ingest, validation, quota, ProduceBatch ack, metrics     260 L
cmd/worker/main.go           consume→normalize→redact→5000-batch→Tinybird→commit; DLQ      427 L
cmd/archive-writer/main.go   consume→verbatim R2 JSONL per service, all-or-nothing commit   280 L
cmd/detection/main.go        consume→Engine→ produce to `alerts`; /health with window state  181 L
cmd/alert-service/main.go    consume `alerts`→dedup 15 min→webhook; fail-stop on error      226 L
cmd/query-coordinator/main.go tier routing, hot SQL, cold scan merge, timeline, aggregates  428 L
cmd/load-generator/main.go   closed-loop rate-limited producer + percentile report           395 L
internal/kafka/kafka.go      producer (acks=all, snappy, batch ack) / consumer (manual      333 L
                             commit, PollLoop) / CreateTopics
internal/storage/s3.go       R2 via S3 API: raw batch PUT, day-prefix scan, + 2 dead paths   334 L
internal/detection/engine.go windows, fingerprints, regex normalization, alert callbacks     218 L
internal/tinybird/tinybird.go gzip NDJSON append w/ row-count verification; SQL read        144 L
internal/models/models.go    LogEvent, RawEvent*, IngestRequest/Response, QueryRequest/      97 L
                             Response, AlertRule*, Alert, DeadLetterEvent  (* unused)
internal/config/config.go    env → typed sub-configs, defaults, no strict validation        151 L
ui/src/App.js                query state + 3 API calls; owns the (broken) time range         182 L
ui/src/components/*.js       SearchBar 153 · LogTable 313 · MetricsPanel 304 · Timeline 119
docker-compose.yml           12 services + 2 volumes; Kafka/zookeeper healthchecks           —
configs/prometheus.yml       7 static scrape jobs (5 s interval)                             —
configs/grafana/provisioning datasource + 4-row / 11-panel dashboard (auto-loaded)          —
tinybird/datasources/logs.datasource  hot schema, sort key, daily partition, 30 d TTL       22 L
scripts/init.sh              .env validation, topic creation, Tinybird+R2 probes             —
scripts/benchmark.sh         stepping + sustained + error-spike; JSON reports to            —
                             benchmark-results/
scripts/failure-test.sh      kill a worker under load, observe rebalance and drain          —
```

Go total: 2 197 lines in `cmd/` + 1 277 in `internal/` ≈ 3.5 K, plus ~1 070
lines of React. The whole platform is small enough that every invariant in this
document is checkable in one sitting — which is the point of the exercise.

### B. Invariants checklist (for reviewers)

Each is testable from outside in < 5 minutes:

1. Killing a worker during load loses nothing: `accepted` events ≤ rows in
   Tinybird after the group drains (duplicates allowed). *(D3)*
2. `docker compose stop worker` for 5 minutes then restarting drains lag with
   zero gateway errors. *(§9.1)*
3. `POST /v1/ingest` with 1 valid + 1 invalid event returns
   `accepted:1, failed:1` — not 400. *(§5.1)*
4. An unparseable payload appears on `logs-dlq` with `component:"processing-worker"`
   and its offset is committed (no hot loop). *(§6.2)*
5. Tinybird token wrong ⇒ `worker_flush_failures_total` rises, offsets stall, R2
   keeps advancing. *(D2)*
6. `ALERT_WEBHOOK_URL=""` ⇒ alerts are logged, offsets commit, no restart loop. *(§6.5)*
7. Query with `end_time` 10 days ago returns `source:"cold"` (via `curl`, not the
   UI — L1). *(§4.3)*
8. Quota: `-rate` above `GATEWAY_INGEST_QUOTA_PER_SEC` produces `429` +
   `Retry-After: 1`, and `gateway_events_rejected_total` matches. *(§9.2)*

### C. Glossary

| Term | Meaning here |
|---|---|
| **Hot tier** | Tinybird `logs` datasource — the interactive 7-day (routed) / 30-day (retained) window |
| **Cold tier** | R2 `raw/**` JSONL — immutable original bytes |
| **Tier routing** | Coordinator's choice of hot/cold/merged from the query's time range |
| **Durability handshake** | Ack to client only after Kafka ack; commit only after store ack |
| **Flush** | Worker/archive action of writing a buffered batch and (on success) committing |
| **Sentinel item** | A buffered entry with `msg` but no event — a DLQ'd record whose offset must still advance |
| **Fingerprint** | `service:md5(normalized template)` — identity of "the same recurring error" |
| **Freshness** | `event.timestamp` → searchable; bounded by the 2 s worker flush quantum |
| **Replay** | New consumer group at `earliest`, or re-derive from R2 with a new parser |
| **DLQ** | `logs-dlq`; poison payloads with the original bytes and the error |

### D. Change log

| Date | Change |
|---|---|
| 2026-09-09 | Initial architecture/design document covering `0d7fe8e` (single-commit history at time of writing) |
