# Managed services setup

This deployment keeps Kafka, the gateway, workers, detection, alerting, and
the user interface in Docker. Tinybird is the hot analytics store and
Cloudflare R2 is the raw, long-retention archive. No local database or
object-store containers are used: every `S3_*` setting in `.env` points at R2
over its S3-compatible API.

## 1. Create the Tinybird datasource

Create a datasource named `logs` (or set `TINYBIRD_DATASOURCE` to its name)
with this schema. The `attributes` column intentionally stores JSON text so
unstructured application fields are retained without changing the schema.

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

Two ways to create it:

- **Tinybird CLI (recommended, reproducible):** authenticate with `tb auth`,
  then from the repo root run
  `tb push tinybird/datasources/logs.datasource --force`. The checked-in
  `.datasource` file defines the schema above plus a sorting key, daily
  partitions, and a 30-day TTL.
- **Dashboard:** create an empty datasource and add the columns manually.

Then create two tokens in the Tinybird dashboard:

- an append token with `DATASOURCE:APPEND` on the `logs` datasource, stored
  as `TINYBIRD_APPEND_TOKEN` (used by the processing workers);
- a read token with read access to the datasource, stored as
  `TINYBIRD_READ_TOKEN` (used by the query coordinator).

Set `TINYBIRD_API_URL` to the API host for your Tinybird region (for example
`https://api.tinybird.co` for the default region).

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

Copy `.env.example` to `.env`, replace every `REPLACE_WITH_...` value, then run:

```bash
docker compose up -d --build
./scripts/init.sh
```

Ingest a smoke-test event:

```bash
curl -X POST http://localhost:8080/v1/ingest \
  -H "Content-Type: application/json" \
  -d '{"events": [{"event_id": "smoke-001", "timestamp": "2026-09-09T00:00:00.000Z", "service": "smoke", "severity": "INFO", "message": "managed storage smoke test"}]}'
```

Then verify it in both managed stores:

- **Tinybird:** query
  `SELECT * FROM logs WHERE service = 'smoke' ORDER BY timestamp DESC LIMIT 10`
  in the Tinybird dashboard, or search for it in the UI at
  http://localhost:3000.
- **R2:** browse the bucket in the Cloudflare dashboard for a new object
  under `raw/<YYYY>/<MM>/<DD>/<HH>/smoke/`.

The worker only commits a Kafka offset after Tinybird acknowledges the
append; the archive writer independently commits only after R2 confirms the
object write. If a managed service is unreachable, its consumer group
backlogs in Kafka and drains automatically on recovery.
