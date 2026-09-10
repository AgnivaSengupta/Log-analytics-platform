# External services setup

This deployment runs Kafka, ClickHouse, the gateway, workers, detection,
alerting, and the user interface in Docker. ClickHouse is the hot analytics
store. Cloudflare R2 remains the raw, long-retention archive. Every `S3_*`
setting in `.env` points at R2 over its S3-compatible API.

## 1. ClickHouse (in Docker)

No Tinybird workspace is required. `docker compose up` starts ClickHouse and
applies `configs/clickhouse/init.sql`, which creates:

- table `logs` (MergeTree, daily partitions, 30-day TTL)
- insert-only user `logs_append` (workers)
- select-only user `logs_read` (query coordinator)

| Column | Type |
| --- | --- |
| event_id | String |
| timestamp | DateTime64(3) |
| service | LowCardinality(String) |
| severity | LowCardinality(String) |
| message | String |
| attributes | String |
| trace_id | String |
| source | LowCardinality(String) |
| region | LowCardinality(String) |
| version | LowCardinality(String) |

The `attributes` column stores JSON text so unstructured application fields
are retained without changing the schema.

HTTP interface: http://localhost:8123 (native protocol on 9000).

Default local passwords are in `.env.example`. Change
`CLICKHOUSE_APPEND_PASSWORD` / `CLICKHOUSE_READ_PASSWORD` together with
`configs/clickhouse/init.sql` if you do not want the demo credentials.

## 2. Create the R2 archive bucket

In the Cloudflare dashboard, create the bucket named by `S3_BUCKET` and an
R2 API token scoped to that bucket with **Object Read & Write** permission.
Copy the account ID, access-key ID, and secret into `.env`:

- `S3_ENDPOINT=https://<account-id>.r2.cloudflarestorage.com`
- `S3_REGION=auto` (always `auto` for R2)
- `S3_ACCESS_KEY` / `S3_SECRET_KEY` from the API token

Keep `S3_ENSURE_BUCKET=false`: R2 buckets are created in Cloudflare, not by
the application.

## 3. Start and verify

Copy `.env.example` to `.env`, replace every `REPLACE_WITH_...` R2 value, then run:

```bash
docker compose up -d --build
./scripts/init.sh
```

On Windows without Git Bash, use the PowerShell equivalent instead of
`init.sh`:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\init.ps1
```

Ingest a smoke-test event:

```bash
curl -X POST http://localhost:8080/v1/ingest \
  -H "Content-Type: application/json" \
  -d '{"events": [{"event_id": "smoke-001", "timestamp": "2026-09-09T00:00:00.000Z", "service": "smoke", "severity": "INFO", "message": "managed storage smoke test"}]}'
```

Then verify it in both stores:

- **ClickHouse:** `SELECT * FROM logs WHERE service = 'smoke' ORDER BY timestamp DESC LIMIT 10` via
  `curl -u logs_read:read --data-binary "SELECT * FROM logs WHERE service = 'smoke' ORDER BY timestamp DESC LIMIT 10 FORMAT Pretty" http://localhost:8123/`
  or search for it in the UI at http://localhost:3000.
- **R2:** browse the bucket in the Cloudflare dashboard for a new object
  under `raw/<YYYY>/<MM>/<DD>/<HH>/smoke/`.

The worker only commits a Kafka offset after ClickHouse acknowledges the
insert (`wait_end_of_query=1` plus written-row checks); the archive writer
independently commits only after R2 confirms the object write. If a store is
unreachable, its consumer group backlogs in Kafka and drains on recovery.
