# Benchmark suite for the distributed log analytics platform.
# Tests: throughput stepping, sustained load, and error-spike detection.
#
# PowerShell equivalent of benchmark.sh (for Windows without Git Bash/WSL).
# Run:  powershell -ExecutionPolicy Bypass -File .\scripts\benchmark.ps1

$ErrorActionPreference = 'Stop'
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$GatewayUrl = if ($env:GATEWAY_URL) { $env:GATEWAY_URL } else { 'http://localhost:8080' }
$LoadGatewayUrl = if ($env:LOAD_GATEWAY_URL) { $env:LOAD_GATEWAY_URL } else { 'http://gateway:8080' }
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ResultsDir = Join-Path (Join-Path $ScriptDir '..') 'benchmark-results'
New-Item -ItemType Directory -Force -Path $ResultsDir | Out-Null
# Docker prefers forward slashes for -v host paths
$ResultsMount = $ResultsDir.Replace('\', '/')
# Quick mode is the default (~5 minutes total). Set $env:BENCHMARK_MODE='full'
# for the original ~15-minute suite.
$BenchmarkMode = if ($env:BENCHMARK_MODE) { $env:BENCHMARK_MODE } else { 'quick' }
if ($BenchmarkMode -eq 'full') {
    $QuickFlag = @()
    $SustainedSecs = '300'; $DetectP1Secs = '60'; $DetectP2Secs = '120'
} else {
    $QuickFlag = @('-quick')
    $SustainedSecs = '120'; $DetectP1Secs = '30'; $DetectP2Secs = '60'
}

Write-Host '==========================================='
Write-Host '  Benchmark Suite'
Write-Host '==========================================='
Write-Host ''
Write-Host "Gateway: $GatewayUrl"
Write-Host "Results: $ResultsDir"
Write-Host "Mode: $BenchmarkMode (BENCHMARK_MODE=full for the full suite)"
Write-Host "Load target: $LoadGatewayUrl"
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

# Rebuild so -report and the latest flags exist (profile-gated
# services are skipped by a plain 'up --build').
docker compose --profile benchmark build load-generator
Write-Host ''

function Invoke-LoadTest {
    param(
        [string]$LogName,
        [string]$ReportName,
        [string[]]$LoadArgs,
        [string]$SnapshotLabel = "",
        [int]$SnapshotDelaySecs = 15
    )
    Write-Host "--- $LogName ---"
    $logPath = Join-Path $ResultsDir $LogName

    # Schedule background snapshot during active load
    $job = $null
    if ($SnapshotLabel) {
        Write-Host "Scheduled auto-snapshot '$SnapshotLabel' in ${SnapshotDelaySecs}s during peak load..."
        $job = Start-Job -ScriptBlock {
            param($snapScript, $resDir, $label, $delay)
            Start-Sleep -Seconds $delay
            $env:RUNDIR = $resDir
            & $snapScript -Label $label
        } -ArgumentList (Join-Path $ScriptDir 'snapshot.ps1'), $ResultsDir, $SnapshotLabel, $SnapshotDelaySecs
    }

    # NOTE: docker stderr is intentionally left unredirected. Any 2>&1/2>
    # redirection of native stderr can abort this script under
    # $ErrorActionPreference='Stop'; unredirected output just displays.
    docker compose --profile benchmark --progress quiet run --rm -v "${ResultsMount}:/reports" load-generator /bin/service @LoadArgs -report "/reports/$ReportName" | Tee-Object -FilePath $logPath
    if ($LASTEXITCODE -ne 0) { Write-Host "WARNING: load test exited with code $LASTEXITCODE (see docker output above)" }

    if ($job) {
        Wait-Job $job -Timeout 10 | Out-Null
        Receive-Job $job | Out-Null
        Remove-Job $job -Force -ErrorAction SilentlyContinue
    }
    Write-Host ''
}

Write-Host '==========================================='
Write-Host '  TEST 1: THROUGHPUT STEPPING (10K -> 100K)'
Write-Host '==========================================='
Invoke-LoadTest -LogName 'throughput-test.log' -ReportName 'throughput-report.json' -LoadArgs (@('-gateway', $LoadGatewayUrl, '-step') + $QuickFlag + @('-workers', '20', '-batch', '200', '-error-rate', '0.002')) -SnapshotLabel '01-throughput-stepping-peak' -SnapshotDelaySecs 15

Write-Host '==========================================='
Write-Host "  TEST 2: SUSTAINED LOAD (50K/sec, ${SustainedSecs}s)"
Write-Host '==========================================='
Invoke-LoadTest -LogName 'sustained-test.log' -ReportName 'sustained-report.json' -LoadArgs @('-gateway', $LoadGatewayUrl, '-rate', '50000', '-duration', "${SustainedSecs}s", '-workers', '20', '-batch', '200', '-error-rate', '0.002') -SnapshotLabel '02-sustained-50k-peak' -SnapshotDelaySecs 30

Write-Host '==========================================='
Write-Host '  TEST 3: ERROR RATE SPIKE (DETECTION)'
Write-Host '==========================================='
Write-Host "Phase 1: Normal traffic (0.2% errors, ${DetectP1Secs}s)..."
Invoke-LoadTest -LogName 'detection-phase1.log' -ReportName 'detection-phase1-report.json' -LoadArgs @('-gateway', $LoadGatewayUrl, '-rate', '25000', '-duration', "${DetectP1Secs}s", '-workers', '10', '-batch', '100', '-error-rate', '0.002') -SnapshotLabel '03-detection-normal-peak' -SnapshotDelaySecs 12

Write-Host "Phase 2: Error spike (8% errors, ${DetectP2Secs}s)..."
Invoke-LoadTest -LogName 'detection-phase2.log' -ReportName 'detection-phase2-report.json' -LoadArgs @('-gateway', $LoadGatewayUrl, '-rate', '25000', '-duration', "${DetectP2Secs}s", '-workers', '10', '-batch', '100', '-error-rate', '0.08') -SnapshotLabel '04-detection-spike-peak' -SnapshotDelaySecs 20

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
