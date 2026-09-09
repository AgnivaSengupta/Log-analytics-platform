# Benchmark suite for the distributed log analytics platform.
# Tests: throughput stepping, sustained load, and error-spike detection.
#
# PowerShell equivalent of benchmark.sh (for Windows without Git Bash/WSL).
# Run:  powershell -ExecutionPolicy Bypass -File .\scripts\benchmark.ps1

$ErrorActionPreference = 'Stop'
$GatewayUrl = if ($env:GATEWAY_URL) { $env:GATEWAY_URL } else { 'http://localhost:8080' }
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ResultsDir = Join-Path (Join-Path $ScriptDir '..') 'benchmark-results'
New-Item -ItemType Directory -Force -Path $ResultsDir | Out-Null
# Docker prefers forward slashes for -v host paths
$ResultsMount = $ResultsDir.Replace('\', '/')

Write-Host '==========================================='
Write-Host '  Benchmark Suite'
Write-Host '==========================================='
Write-Host ''
Write-Host "Gateway: $GatewayUrl"
Write-Host "Results: $ResultsDir"
Write-Host ''

try {
    Invoke-RestMethod -Uri "$GatewayUrl/health" -TimeoutSec 10 | Out-Null
} catch {
    Write-Host "Gateway not available at $GatewayUrl"
    Write-Host 'Start with: docker compose up -d'
    exit 1
}
Write-Host 'Gateway is healthy'
Write-Host ''

function Invoke-LoadTest {
    param([string]$LogName, [string]$ReportName, [string[]]$LoadArgs)
    Write-Host "--- $LogName ---"
    $logPath = Join-Path $ResultsDir $LogName
    # NOTE: stderr goes to a temp file, NOT 2>&1. With
    # $ErrorActionPreference='Stop', merged native stderr would abort the
    # script on harmless docker status lines.
    $errTmp = [IO.Path]::GetTempFileName()
    docker compose --profile benchmark --progress quiet run --rm -v "${ResultsMount}:/reports" load-generator /bin/service @LoadArgs -report "/reports/$ReportName" 2> $errTmp | Tee-Object -FilePath $logPath
    $code = $LASTEXITCODE
    if ((Get-Item $errTmp).Length -gt 0) {
        Add-Content -Path $logPath -Value ''
        Add-Content -Path $logPath -Value '--- stderr ---'
        Get-Content $errTmp | Add-Content -Path $logPath
    }
    Remove-Item $errTmp -Force
    if ($code -ne 0) { Write-Host "WARNING: load test exited with code $code" }
    Write-Host ''
}

Write-Host '==========================================='
Write-Host '  TEST 1: THROUGHPUT STEPPING (10K -> 100K)'
Write-Host '==========================================='
Invoke-LoadTest -LogName 'throughput-test.log' -ReportName 'throughput-report.json' -LoadArgs @('-gateway', $GatewayUrl, '-step', '-workers', '20', '-batch', '200', '-error-rate', '0.002')

Write-Host '==========================================='
Write-Host '  TEST 2: SUSTAINED LOAD (50K/sec, 5min)'
Write-Host '==========================================='
Invoke-LoadTest -LogName 'sustained-test.log' -ReportName 'sustained-report.json' -LoadArgs @('-gateway', $GatewayUrl, '-rate', '50000', '-duration', '300s', '-workers', '20', '-batch', '200', '-error-rate', '0.002')

Write-Host '==========================================='
Write-Host '  TEST 3: ERROR RATE SPIKE (DETECTION)'
Write-Host '==========================================='
Write-Host 'Phase 1: Normal traffic (0.2% errors, 60s)...'
Invoke-LoadTest -LogName 'detection-phase1.log' -ReportName 'detection-phase1-report.json' -LoadArgs @('-gateway', $GatewayUrl, '-rate', '25000', '-duration', '60s', '-workers', '10', '-batch', '100', '-error-rate', '0.002')
Write-Host 'Phase 2: Error spike (8% errors, 120s)...'
Invoke-LoadTest -LogName 'detection-phase2.log' -ReportName 'detection-phase2-report.json' -LoadArgs @('-gateway', $GatewayUrl, '-rate', '25000', '-duration', '120s', '-workers', '10', '-batch', '100', '-error-rate', '0.08')

Write-Host '==========================================='
Write-Host '  BENCHMARK COMPLETE'
Write-Host '==========================================='
Write-Host ''
Write-Host "Results saved to: $ResultsDir/"
Write-Host '(per-step JSON reports are the *-report.json files)'
Get-ChildItem $ResultsDir
Write-Host ''
Write-Host 'View Grafana dashboards at: http://localhost:3001'
Write-Host 'View UI at: http://localhost:3000'
