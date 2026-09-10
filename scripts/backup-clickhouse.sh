#!/bin/bash
# Backup ClickHouse tables (hot tier) to a timestamped backup archive.
# Run: ./scripts/backup-clickhouse.sh

set -e

TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
BACKUP_NAME="backup_${TIMESTAMP}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BACKUP_DIR="${SCRIPT_DIR}/../backups"

echo "=========================================="
echo "  ClickHouse Backup: ${BACKUP_NAME}"
echo "=========================================="
echo ""

if ! curl -sf "http://localhost:8123/ping" > /dev/null; then
    echo "ERROR: ClickHouse is not running at http://localhost:8123"
    exit 1
fi

echo "Creating native ClickHouse backup..."
docker compose exec clickhouse clickhouse-client --query "BACKUP DATABASE default TO File('${BACKUP_NAME}')"

mkdir -p "${BACKUP_DIR}"
echo ""
echo "OK: Backup completed successfully."
