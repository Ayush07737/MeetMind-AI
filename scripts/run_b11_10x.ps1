for ($i = 1; $i -le 10; $i++) {
    Write-Host "`n=================================================="
    Write-Host "=== TEST B11: RUN $i of 10 ==="
    Write-Host "=================================================="
    $sw = [System.Diagnostics.Stopwatch]::StartNew()
    uv run pytest backend/asr/tests/acceptance/test_b11_stream_trimming.py -v
    $code = $LASTEXITCODE
    $sw.Stop()
    $sec = [math]::Round($sw.Elapsed.TotalSeconds, 2)
    if ($code -eq 0) {
        Write-Host "[RESULT] RUN $i PASSED in ${sec}s"
    } else {
        Write-Host "[RESULT] RUN $i FAILED in ${sec}s (exit code $code)"
        exit $code
    }
}
Write-Host "`n[SUCCESS] ALL 10 RUNS OF TEST B11 PASSED (10/10)"
