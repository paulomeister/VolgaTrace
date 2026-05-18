# CP-01 - Producción de eventos ONLINE válidos
# Verifica que el productor ONLINE publique mensajes en el topic transactions-online.

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
Set-Location $repoRoot

$evidenceDir = Join-Path $repoRoot "docs\evidence"

$terminalLogPath = Join-Path $evidenceDir "cp01_terminal_log.txt"
$producerLogPath = Join-Path $evidenceDir "cp01_online_producer_log.txt"
$offsetsLogPath = Join-Path $evidenceDir "cp01_kafka_offsets_before_after.txt"
$validationSummaryPath = Join-Path $evidenceDir "cp01_validation_summary.txt"

$topic = "transactions-online"
$producerRate = 50
$durationSeconds = 60

function Add-TerminalLog {
    param (
        [AllowEmptyString()]
        [string]$Message
    )

    $Message | Add-Content -LiteralPath $terminalLogPath -Encoding UTF8
}

function Assert-CommandAvailable {
    param (
        [Parameter(Mandatory = $true)]
        [string]$CommandName
    )

    if (-not (Get-Command $CommandName -ErrorAction SilentlyContinue)) {
        throw "Required command '$CommandName' was not found in PATH."
    }
}

function Get-TopicEndOffsets {
    param (
        [Parameter(Mandatory = $true)]
        [string]$Topic
    )

    $result = docker exec kafka kafka-get-offsets.sh --bootstrap-server kafka:9094 --topic $Topic --time -1

    $offsets = @{}

    foreach ($line in $result) {
        if ($line -match "^(.+):(\d+):(\d+)$") {
            $partition = [int]$Matches[2]
            $offset = [int64]$Matches[3]
            $offsets[$partition] = $offset
        }
    }

    return $offsets
}

function Sum-Offsets {
    param (
        [Parameter(Mandatory = $true)]
        [hashtable]$Offsets
    )

    $sum = [int64]0

    foreach ($value in $Offsets.Values) {
        $sum += [int64]$value
    }

    return $sum
}

function Get-ProducerMetrics {
    param (
        [Parameter(Mandatory = $true)]
        [string]$LogPath
    )

    $content = Get-Content -LiteralPath $LogPath -Encoding UTF8
    $finalLine = $content | Where-Object { $_ -match "Fin\. Total enviados=" } | Select-Object -Last 1

    if (-not $finalLine) {
        return @{
            total = 0
            failures = "N/A"
            average_rate = 0.0
            final_line = "Final line not found"
        }
    }

    $total = 0
    $failures = "N/A"
    $averageRate = 0.0

    if ($finalLine -match "Total enviados=(\d+)") {
        $total = [int]$Matches[1]
    }

    if ($finalLine -match "fallos=(\d+)") {
        $failures = [int]$Matches[1]
    }

    if ($finalLine -match "tasa_promedio=([\d\.,]+)") {
        $averageRateText = $Matches[1].Replace(",", ".")
        $averageRate = [double]$averageRateText
    }

    return @{
        total = $total
        failures = $failures
        average_rate = $averageRate
        final_line = $finalLine
    }
}

"CP-01 - Producción de eventos ONLINE válidos" | Set-Content -LiteralPath $terminalLogPath -Encoding UTF8
"" | Set-Content -LiteralPath $producerLogPath -Encoding UTF8
"" | Set-Content -LiteralPath $offsetsLogPath -Encoding UTF8
"" | Set-Content -LiteralPath $validationSummaryPath -Encoding UTF8

Write-Host "CP-01 - Produccion de eventos ONLINE validos"
Write-Host "Repository root: $repoRoot"
Write-Host ""

if (-not (Test-Path $evidenceDir)) {
    throw "Required directory does not exist: $evidenceDir"
}

Assert-CommandAvailable -CommandName "docker"
Assert-CommandAvailable -CommandName "python"

Add-TerminalLog "Execution date: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')"
Add-TerminalLog "Repository root: $repoRoot"
Add-TerminalLog ""
Add-TerminalLog "Objective:"
Add-TerminalLog "Verify that the ONLINE producer publishes messages into topic $topic."
Add-TerminalLog ""
Add-TerminalLog "Configuration:"
Add-TerminalLog "Producer: producers/producer_online.py"
Add-TerminalLog "Topic: $topic"
Add-TerminalLog "Rate: $producerRate events/s"
Add-TerminalLog "Duration: $durationSeconds seconds"
Add-TerminalLog ""

Write-Host "Starting required services..."
docker compose up -d kafka kafka-ui

Add-TerminalLog "Command executed:"
Add-TerminalLog "docker compose up -d kafka kafka-ui"
Add-TerminalLog ""

Write-Host "Waiting for Kafka to be ready..."
Start-Sleep -Seconds 20

Write-Host "Collecting initial Kafka offsets..."

$offsetsBefore = Get-TopicEndOffsets -Topic $topic
$totalBefore = Sum-Offsets -Offsets $offsetsBefore

"CP-01 Kafka offsets before execution" | Set-Content -LiteralPath $offsetsLogPath -Encoding UTF8
"" | Add-Content -LiteralPath $offsetsLogPath -Encoding UTF8
"Topic: $topic" | Add-Content -LiteralPath $offsetsLogPath -Encoding UTF8
"Offsets before:" | Add-Content -LiteralPath $offsetsLogPath -Encoding UTF8
($offsetsBefore.GetEnumerator() | Sort-Object Name | ForEach-Object { "partition=$($_.Name) offset=$($_.Value)" }) | Add-Content -LiteralPath $offsetsLogPath -Encoding UTF8
"Total before: $totalBefore" | Add-Content -LiteralPath $offsetsLogPath -Encoding UTF8
"" | Add-Content -LiteralPath $offsetsLogPath -Encoding UTF8

Add-TerminalLog "Initial Kafka offsets collected."
Add-TerminalLog "Total offset before: $totalBefore"
Add-TerminalLog ""

Write-Host "Running ONLINE producer..."

$producerStdoutPath = Join-Path $evidenceDir "cp01_online_producer.stdout.tmp"
$producerStderrPath = Join-Path $evidenceDir "cp01_online_producer.stderr.tmp"

"" | Set-Content -LiteralPath $producerStdoutPath -Encoding UTF8
"" | Set-Content -LiteralPath $producerStderrPath -Encoding UTF8

$producerArgs = @(
    ".\producers\producer_online.py",
    "--rate",
    "$producerRate",
    "--duration",
    "$durationSeconds"
)

$producerProcess = Start-Process `
    -FilePath "python" `
    -ArgumentList $producerArgs `
    -WorkingDirectory $repoRoot `
    -RedirectStandardOutput $producerStdoutPath `
    -RedirectStandardError $producerStderrPath `
    -NoNewWindow `
    -PassThru

$producerProcess.WaitForExit()

$output = @()
$output += Get-Content -LiteralPath $producerStdoutPath -Encoding UTF8
$output += Get-Content -LiteralPath $producerStderrPath -Encoding UTF8
$output | Set-Content -LiteralPath $producerLogPath -Encoding UTF8

Remove-Item -LiteralPath $producerStdoutPath -Force -ErrorAction SilentlyContinue
Remove-Item -LiteralPath $producerStderrPath -Force -ErrorAction SilentlyContinue

if ($null -ne $producerProcess.ExitCode -and $producerProcess.ExitCode -ne 0) {
    throw "ONLINE producer failed with exit code $($producerProcess.ExitCode). Check log: $producerLogPath"
}

Add-TerminalLog "ONLINE producer executed successfully."
Add-TerminalLog "Producer log: $producerLogPath"
Add-TerminalLog ""

Write-Host "Waiting for Kafka to flush topic counters..."
Start-Sleep -Seconds 5

Write-Host "Collecting final Kafka offsets..."

$offsetsAfter = Get-TopicEndOffsets -Topic $topic
$totalAfter = Sum-Offsets -Offsets $offsetsAfter
$totalDelta = $totalAfter - $totalBefore

"CP-01 Kafka offsets after execution" | Add-Content -LiteralPath $offsetsLogPath -Encoding UTF8
"" | Add-Content -LiteralPath $offsetsLogPath -Encoding UTF8
"Topic: $topic" | Add-Content -LiteralPath $offsetsLogPath -Encoding UTF8
"Offsets after:" | Add-Content -LiteralPath $offsetsLogPath -Encoding UTF8
($offsetsAfter.GetEnumerator() | Sort-Object Name | ForEach-Object { "partition=$($_.Name) offset=$($_.Value)" }) | Add-Content -LiteralPath $offsetsLogPath -Encoding UTF8
"Total after: $totalAfter" | Add-Content -LiteralPath $offsetsLogPath -Encoding UTF8
"Delta: $totalDelta" | Add-Content -LiteralPath $offsetsLogPath -Encoding UTF8
"" | Add-Content -LiteralPath $offsetsLogPath -Encoding UTF8

Add-TerminalLog "Final Kafka offsets collected."
Add-TerminalLog "Total offset after: $totalAfter"
Add-TerminalLog "Total new messages according to offsets: $totalDelta"
Add-TerminalLog ""

$producerMetrics = Get-ProducerMetrics -LogPath $producerLogPath

$passed = $false
if ($totalDelta -gt 0 -and [int]$producerMetrics.total -gt 0) {
    $passed = $true
}

$summary = @"
CP-01 - ONLINE producer validation summary

Topic: $topic
Producer: producers/producer_online.py
Configured rate: $producerRate events/s
Duration: $durationSeconds seconds

Producer result:
Total sent: $($producerMetrics.total)
Failures: $($producerMetrics.failures)
Average rate: $($producerMetrics.average_rate) events/s
Final line: $($producerMetrics.final_line)

Kafka offsets:
Total before: $totalBefore
Total after: $totalAfter
Delta: $totalDelta

Acceptance criterion:
The test passes if the ONLINE producer finishes successfully and Kafka receives new messages in topic transactions-online.

Result:
$(if ($passed) { "PASSED" } else { "FAILED" })
"@

$summary | Set-Content -LiteralPath $validationSummaryPath -Encoding UTF8

Add-TerminalLog "Producer metrics:"
Add-TerminalLog "Total sent: $($producerMetrics.total)"
Add-TerminalLog "Failures: $($producerMetrics.failures)"
Add-TerminalLog "Average rate: $($producerMetrics.average_rate) events/s"
Add-TerminalLog ""
Add-TerminalLog "Validation summary:"
(Get-Content -LiteralPath $validationSummaryPath -Encoding UTF8) | ForEach-Object { Add-TerminalLog $_ }

Write-Host ""
Write-Host "CP-01 execution summary"
Write-Host "-----------------------"
Get-Content -LiteralPath $validationSummaryPath -Encoding UTF8 | ForEach-Object { Write-Host $_ }

Write-Host ""
Write-Host "Evidence files:"
Write-Host "- $terminalLogPath"
Write-Host "- $producerLogPath"
Write-Host "- $offsetsLogPath"
Write-Host "- $validationSummaryPath"

if ($passed) {
    Write-Host ""
    Write-Host "CP-01 validation: PASSED"
    Write-Host "ONLINE producer published messages to $topic."
}
else {
    Write-Host ""
    Write-Warning "CP-01 validation: FAILED"
    Write-Warning "No new messages were detected in $topic."
}

Write-Host ""
Write-Host "Recommended manual screenshot:"
Write-Host "- docs/evidence/cp01_kafka_ui_transactions_online.png"
Write-Host ""
Write-Host "CP-01 script finished."