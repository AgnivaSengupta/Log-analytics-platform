# Failure recovery test: kill a worker during sustained load and observe recovery.
#
# PowerShell equivalent of failure-test.sh (for Windows without Git Bash/WSL).
# Run:  powershell -ExecutionPolicy Bypass -File .\scripts\failure-test.ps1

$ErrorActionPreference = 'Stop'
$GatewayUrl = if ($env:GATEWAY_URL) { $env:GATEWAY_URL } else { 'http://localhost:8080' }

Write-Host '==========================================='
Write-Host '  Failure Recovery Test'
Write-Host '==========================================='
Write-Host ''

Write-Host 'Starting sustained load (25K events/sec, 180s)...'
docker compose --profile benchmark run -d --rm load-generator /bin/service -gateway $GatewayUrl -rate 25000 -duration 180s -workers 10 -batch 100 -error-rate 0.002 | Out-Null
Write-Host 'Waiting 30 seconds for steady state...'
Start-Sleep -Seconds 30

Write-Host ''
Write-Host '=== Before failure ==='
docker compose ps worker
Write-Host ''

Write-Host 'Killing a processing worker...'
$worker = (docker compose ps -q worker | Select-Object -First 1)
if ([string]::IsNullOrEmpty($worker)) {
    Write-Host 'No worker container found, starting one...'
    docker compose up -d worker
    Start-Sleep -Seconds 5
    $worker = (docker compose ps -q worker | Select-Object -First 1)
}
docker kill $worker | Out-Null
Write-Host "Worker killed: $worker"

Write-Host ''
Write-Host 'Waiting 15 seconds for rebalance...'
Start-Sleep -Seconds 15
Write-Host ''
Write-Host '=== After failure (rebalance in progress) ==='
docker compose ps worker

Write-Host ''
Write-Host 'Restarting worker...'
docker compose up -d worker
Start-Sleep -Seconds 10
Write-Host ''
Write-Host '=== After recovery ==='
docker compose ps worker

# Test runs 180s total; ~55s elapsed so far - wait out the remainder.
Write-Host ''
Write-Host 'Waiting for load test to complete (~2 min)...'
Start-Sleep -Seconds 140

Write-Host ''
Write-Host '==========================================='
Write-Host '  FAILURE RECOVERY TEST COMPLETE'
Write-Host '==========================================='
Write-Host ''
Write-Host 'Check Grafana for throughput during the kill window:'
Write-Host '  http://localhost:3001'
Write-Host ''
Write-Host 'Expected observations:'
Write-Host '  1. Worker throughput dipped when the worker was killed'
Write-Host '  2. Consumer group rebalanced partitions'
Write-Host '  3. Throughput recovered after worker restart'
Write-Host '  4. No acknowledged events were lost'
