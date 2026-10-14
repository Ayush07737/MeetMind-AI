param (
    [int]$Count = 10
)

$passCount = 0
$failCount = 0

for ($i = 1; $i -le $Count; $i++) {
    Write-Host "=========================================="
    Write-Host "  REWRITTEN B2 RUN $i of $Count"
    Write-Host "=========================================="
    $logPath = "logs/b2_rewritten_run_$($i).log"
    uv run pytest backend/asr/tests/acceptance/test_b2_worker_kill.py -v 2>&1 | Tee-Object -FilePath $logPath
    if ($LASTEXITCODE -eq 0) {
        $passCount++
        Write-Host "Run $($i): PASSED"
    } else {
        $failCount++
        Write-Host "Run $($i): FAILED"
    }
}

Write-Host "=========================================="
Write-Host "TOTAL: $passCount passed, $failCount failed out of $Count"
Write-Host "=========================================="
