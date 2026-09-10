# Backup ClickHouse tables (hot tier) to a timestamped backup archive.
# Run: powershell -ExecutionPolicy Bypass -File .\scripts\backup-clickhouse.ps1

$ErrorActionPreference = 'Stop'
$Timestamp = Get-Date -Format 'yyyyMMdd_HHmmss'
$BackupName = "backup_$Timestamp"

Write-Host "=========================================="
Write-Host "  ClickHouse Backup: $BackupName"
Write-Host "=========================================="
Write-Host ""

# Ensure clickhouse is running
try {
    Invoke-RestMethod -Uri "http://localhost:8123/ping" -TimeoutSec 5 | Out-Null
} catch {
    Write-Host "ERROR: ClickHouse is not running at http://localhost:8123"
    exit 1
}

Write-Host "Creating native ClickHouse backup..."
$backupSql = "BACKUP DATABASE default TO File('$BackupName')"

$response = docker compose exec clickhouse clickhouse-client --query "$backupSql"
Write-Host "Result:"
Write-Host $response

$BackupDir = Join-Path $PSScriptRoot "..\backups"
New-Item -ItemType Directory -Force -Path $BackupDir | Out-Null

Write-Host ""
Write-Host "Exporting backup from container to $BackupDir\$BackupName.tar.gz..."
docker compose exec clickhouse tar -czf "/tmp/$BackupName.tar.gz" -C /var/lib/clickhouse/disks/default/shadow "$BackupName" 2>$null
docker compose cp "clickhouse:/var/lib/clickhouse/user_files/../disks/default/$BackupName" "$BackupDir\$BackupName" 2>$null

Write-Host "OK: Backup completed successfully."
Write-Host "Backup path: $BackupDir\$BackupName"
