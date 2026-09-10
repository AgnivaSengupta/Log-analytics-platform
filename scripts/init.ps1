# Initialize the platform: validate config, create Kafka topics, probe ClickHouse + R2.
#
# Hot storage is self-hosted ClickHouse in docker-compose. The cold archive
# (Cloudflare R2) is provisioned outside Docker (see MANAGED_SERVICES_SETUP.md).
#
# PowerShell equivalent of init.sh (for Windows without Git Bash/WSL).
# Run:  powershell -ExecutionPolicy Bypass -File .\scripts\init.ps1

$ErrorActionPreference = 'Stop'
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location (Join-Path $ScriptDir '..')

Write-Host '==========================================='
Write-Host '  Platform Initialization'
Write-Host '==========================================='
Write-Host ''

# 1. Load .env into process environment
if (-not (Test-Path '.env')) {
    Write-Host 'ERROR: .env not found. Copy .env.example to .env and fill in R2 credentials (see MANAGED_SERVICES_SETUP.md).'
    exit 1
}
Get-Content '.env' | ForEach-Object {
    $line = $_.Trim()
    if ($line -eq '' -or $line.StartsWith('#')) { return }
    $parts = $line -split '=', 2
    if ($parts.Count -eq 2) {
        [Environment]::SetEnvironmentVariable($parts[0].Trim(), $parts[1].Trim(), 'Process')
    }
}

Write-Host 'Checking configuration...'
$missing = $false
$required = @('S3_ENDPOINT', 'S3_BUCKET', 'S3_ACCESS_KEY', 'S3_SECRET_KEY')
foreach ($var in $required) {
    $value = [Environment]::GetEnvironmentVariable($var, 'Process')
    if ([string]::IsNullOrEmpty($value)) {
        Write-Host "  MISSING: $var is empty"
        $missing = $true
    } elseif ($value.StartsWith('REPLACE_WITH_')) {
        Write-Host "  PLACEHOLDER: $var still has its template value"
        $missing = $true
    }
}
if ($missing) {
    Write-Host ''
    Write-Host 'Fill in .env first - see MANAGED_SERVICES_SETUP.md.'
    exit 1
}

if ($env:S3_ENDPOINT -notlike 'https://*.r2.cloudflarestorage.com') {
    Write-Host "  WARNING: S3_ENDPOINT does not look like an R2 endpoint: $env:S3_ENDPOINT"
}
if ($env:S3_REGION -ne 'auto') {
    Write-Host "  WARNING: S3_REGION should be 'auto' for R2 (got '$env:S3_REGION')"
}
Write-Host 'OK: configuration looks valid'
Write-Host ''

# 2. Wait for Kafka
Write-Host 'Waiting for Kafka...'
while ($true) {
    docker compose exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092 2>$null | Out-Null
    if ($LASTEXITCODE -eq 0) { break }
    Write-Host '  Kafka not ready, waiting...'
    Start-Sleep -Seconds 5
}
Write-Host 'OK: Kafka is ready'
Write-Host ''

# 3. Create Kafka topics
Write-Host 'Creating Kafka topics...'
docker compose exec kafka kafka-topics --create --bootstrap-server localhost:9092 --topic logs --partitions 12 --replication-factor 1 --if-not-exists
docker compose exec kafka kafka-topics --create --bootstrap-server localhost:9092 --topic logs-dlq --partitions 6 --replication-factor 1 --if-not-exists
docker compose exec kafka kafka-topics --create --bootstrap-server localhost:9092 --topic alerts --partitions 6 --replication-factor 1 --if-not-exists
Write-Host 'OK: Topics created'
Write-Host ''

# 4. Verify ClickHouse table
$chUrl = $env:CLICKHOUSE_URL
if ([string]::IsNullOrEmpty($chUrl) -or $chUrl -like '*clickhouse:8123*') {
    $chUrl = 'http://localhost:8123'
}
$chUser = $env:CLICKHOUSE_READ_USER
if ([string]::IsNullOrEmpty($chUser)) { $chUser = 'logs_read' }
$chPass = $env:CLICKHOUSE_READ_PASSWORD
if ([string]::IsNullOrEmpty($chPass)) { $chPass = 'read' }
$chTable = $env:CLICKHOUSE_TABLE
if ([string]::IsNullOrEmpty($chTable)) { $chTable = 'logs' }
Write-Host "Verifying ClickHouse ($chUrl)..."
try {
    $pair = '{0}:{1}' -f $chUser, $chPass
    $basic = [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes($pair))
    Invoke-RestMethod -Method Post -Uri "$chUrl/" `
        -Headers @{ Authorization = "Basic $basic" } `
        -ContentType 'text/plain' -Body "SELECT count() AS total FROM $chTable LIMIT 1 FORMAT JSON" -TimeoutSec 30 | Out-Null
    Write-Host "OK: ClickHouse table '$chTable' is queryable"
} catch {
    Write-Host ''
    Write-Host 'ERROR: ClickHouse query failed. Check that:'
    Write-Host '  - the clickhouse service is healthy (docker compose ps)'
    Write-Host "  - configs/clickhouse/init.sql created table '$chTable'"
    Write-Host '  - CLICKHOUSE_READ_USER / CLICKHOUSE_READ_PASSWORD can SELECT'
    exit 1
}
Write-Host ''

# 5. Verify R2 endpoint reachability (unsigned request proves DNS/TLS only)
Write-Host "Verifying R2 endpoint ($env:S3_ENDPOINT)..."
if (-not (Get-Command curl.exe -ErrorAction SilentlyContinue)) {
    Write-Host 'WARNING: curl.exe not found - skipping R2 reachability check.'
} else {
    $httpCode = (curl.exe -s -o NUL -w '%{http_code}' --max-time 15 "$($env:S3_ENDPOINT)/" 2>$null)
    if ([string]::IsNullOrEmpty($httpCode)) { $httpCode = '000' }
    if ($httpCode -eq '000') {
        Write-Host "ERROR: cannot reach $($env:S3_ENDPOINT) - check the endpoint and network."
        exit 1
    }
    Write-Host "OK: R2 endpoint reachable (HTTP $httpCode on unsigned request)"
}
Write-Host ''

# 6. Show services
Write-Host 'Verifying services...'
Write-Host ''
docker compose ps

Write-Host ''
Write-Host '==========================================='
Write-Host '  Platform ready!'
Write-Host '==========================================='
Write-Host ''
Write-Host '  UI:         http://localhost:3000'
Write-Host '  Gateway:    http://localhost:8080'
Write-Host '  Query API:  http://localhost:8081'
Write-Host '  ClickHouse: http://localhost:8123'
Write-Host '  Grafana:    http://localhost:3001'
Write-Host '  Prometheus: http://localhost:9090'
Write-Host ''
Write-Host 'Ingest the smoke-test event from MANAGED_SERVICES_SETUP.md,'
Write-Host 'then search for it at http://localhost:3000'
