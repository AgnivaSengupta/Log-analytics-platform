$out = Join-Path $env:RUNDIR "05-query-latency.txt"

"=== QUERY LATENCY UNDER LOAD @ $(Get-Date -Format o) ===" | Out-File $out

# HOT query (last hour) x5
1..5 | ForEach-Object {
  $body = @{ service="payment"; limit=50;
             start_time=(Get-Date).ToUniversalTime().AddHours(-1).ToString("o");
             end_time=(Get-Date).ToUniversalTime().ToString("o") } | ConvertTo-Json
  $sw = [Diagnostics.Stopwatch]::StartNew()
  $r = Invoke-RestMethod -Method Post -Uri "http://localhost:8081/v1/search" `
        -ContentType "application/json" -Body $body
  $sw.Stop()
  "HOT  run=$_  ms=$($sw.ElapsedMilliseconds)  source=$($r.source)  total=$($r.total)" |
    Tee-Object -FilePath $out -Append
}

# TIMELINE + AGGREGATES (UI path)
$sw = [Diagnostics.Stopwatch]::StartNew()
Invoke-RestMethod "http://localhost:8081/v1/timeline?hours=24" | Out-Null
$sw.Stop(); "TIMELINE ms=$($sw.ElapsedMilliseconds)" | Tee-Object -FilePath $out -Append

$sw = [Diagnostics.Stopwatch]::StartNew()
Invoke-RestMethod "http://localhost:8081/v1/aggregates?hours=24" | Out-Null
$sw.Stop(); "AGGREGATES ms=$($sw.ElapsedMilliseconds)" | Tee-Object -FilePath $out -Append