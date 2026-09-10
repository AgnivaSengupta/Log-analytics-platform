param([string]$Label = "snapshot")

$RunDir = if ($env:RUNDIR) { $env:RUNDIR } else { "." }
$ts   = Get-Date -Format "yyyy-MM-dd_HH-mm-ss"
$file = Join-Path $RunDir "$Label-$ts.txt"

function Q([string]$expr) {
  try {
    $r = Invoke-RestMethod -Uri "http://localhost:9090/api/v1/query" -Body @{ query = $expr } -Method Get
    $out = foreach ($x in $r.data.result) {
      $labels = ($x.metric.PSObject.Properties | Where-Object {$_.Name -ne '__name__'} |
                 ForEach-Object { "$($_.Name)=$($_.Value)" }) -join ','
      "    [$labels] $($x.value[1])"
    }
    if (-not $out) { $out = "    (no data)" }
    "$expr`n$($out -join "`n")"
  } catch { "$expr`n    ERROR: $_" }
}

$queries = @(
  'sum(rate(gateway_events_ingested_total[1m]))',
  'sum(rate(gateway_events_rejected_total[1m]))',
  'histogram_quantile(0.95, sum(rate(gateway_ingest_latency_seconds_bucket[5m])) by (le))',
  'sum(rate(worker_events_processed_total[1m]))',
  'sum(rate(worker_events_appended_total[1m]))',
  'sum(rate(worker_events_dead_lettered_total[1m]))',
  'sum(rate(worker_flush_failures_total[1m]))',
  'histogram_quantile(0.95, sum(rate(worker_append_latency_seconds_bucket[5m])) by (le))',
  'sum(worker_append_inflight)',
  'sum(worker_batches_pending)',
  'sum(worker_uncommitted_batches)',
  'sum(rate(archive_events_total[1m]))',
  'histogram_quantile(0.95, sum(rate(archive_batch_latency_seconds_bucket[5m])) by (le))',
  'sum(archive_buffered_events)',
  'sum(rate(archive_flush_failures_total[1m]))',
  'sum(rate(detection_events_processed_total[1m]))',
  'sum(rate(detection_alerts_fired_total[1m]))',
  'sum(rate(alert_service_sent_total[1m])) by (status)',
  'sum(rate(query_coordinator_queries_total[1m])) by (source)',
  'histogram_quantile(0.95, sum(rate(query_coordinator_latency_seconds_bucket[5m])) by (le, source))'
)

$sb = New-Object System.Text.StringBuilder
[void]$sb.AppendLine("=== $Label @ $ts ===`n")
[void]$sb.AppendLine("--- PROMETHEUS ---")
foreach ($q in $queries) { [void]$sb.AppendLine((Q $q)); [void]$sb.AppendLine() }

[void]$sb.AppendLine("--- KAFKA CONSUMER LAG ---")
foreach ($g in @('processing-workers','archive-writers','detection-service')) {
  [void]$sb.AppendLine(">> $g")
  $lag = docker compose exec -T kafka kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group $g 2>$null
  [void]$sb.AppendLine(($lag -join "`n"))
  [void]$sb.AppendLine()
}

[void]$sb.AppendLine("--- DOCKER STATS ---")
[void]$sb.AppendLine(((docker stats --no-stream) -join "`n"))

$sb.ToString() | Tee-Object -FilePath $file
Write-Host "`nSaved: $file" -ForegroundColor Green
