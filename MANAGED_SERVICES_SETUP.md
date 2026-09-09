# Managed services setup

This deployment keeps Kafka, the gateway, detection, alerting, and the user
interface in Docker. Tinybird is the hot analytics store and Cloudflare R2 is
the raw, long-retention archive. No local ClickHouse or MinIO instance is used
by the application.

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

Create two static Tinybird tokens: one with `DATASOURCE:APPEND` permission for
this datasource and one with read permission. Put them in
`TINYBIRD_APPEND_TOKEN` and `TINYBIRD_READ_TOKEN` in `.env`. Set
`TINYBIRD_API_URL` to the API host for your Tinybird region.

## 2. Create the R2 archive bucket

Create the bucket named by `S3_BUCKET` and an R2 API token with object read and
write permission scoped to that bucket. Copy its access-key ID, secret, and
account-specific S3 endpoint into `.env`. Keep `S3_REGION=auto` and
`S3_ENSURE_BUCKET=false`; R2 bucket creation belongs in Cloudflare, not in the
application.

## 3. Start and verify

After replacing every `REPLACE_WITH_...` value in `.env`, run:

```powershell
docker compose up --build
```

Submit a log to `http://localhost:8080/v1/logs`, then verify it in Tinybird and
in the R2 `raw/YYYY/MM/DD/...` object prefix. The worker only commits a Kafka
offset after Tinybird acknowledges the append; the archive writer independently
commits only after R2 confirms the object write.

The `legacy-local` Compose profile retains the old infrastructure containers
only for reference. It is not used by the managed application path.
