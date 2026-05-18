# CP-09 - Minimum sustained throughput validation test
# Runs ONLINE and POS producers in parallel and validates combined throughput >= 100 events/s.

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
Set-Location $repoRoot

$evidenceDir = Join-Path $repoRoot "docs\evidence"

$terminalLogPath = Join-Path $evidenceDir "cp09_terminal_log.txt"
$onlineLogPath = Join-Path $evidenceDir "cp09_online_terminal_log.txt"
$posLogPath = Join-Path $evidenceDir "cp09_pos_terminal_log.txt"
$offsetsLogPath = Join-Path $evidenceDir "cp09_kafka_offsets_before_after.txt"
$summaryPath = Join-Path $evidenceDir "cp09_throughput_summary.txt"

$onlineTopic = "transactions-online"
$posTopic = "transactions-pos"

$onlineRate = 60
$posRate = 60
$durationSeconds = 120
$minimumThroughput = 100

function Assert-CommandAvailable {
    param (
        [Parameter(Mandatory = $true)]
        [string]$CommandName
    )

    if (-not (Get-Command $CommandName -ErrorAction SilentlyContinue)) {
        throw "Required command '$CommandName' was not found in PATH."
    }
}

function Clear-FileIfExists {
    param (
        [Parameter(Mandatory = $true)]
        [string]$Path
    )

    if (Test-Path $Path) {
        Remove-Item -LiteralPath $Path -Force
    }
}

function Add-TerminalLog {
    param (
        [AllowEmptyString()]
        [string]$Message
    )

    $Message | Add-Content -LiteralPath $terminalLogPath
}

function Merge-ProcessOutput {
    param (
        [Parameter(Mandatory = $true)]
        [string]$StdoutPath,

        [Parameter(Mandatory = $true)]
        [string]$StderrPath,

        [Parameter(Mandatory = $true)]
        [string]$OutputPath
    )

    $output = @()

    if (Test-Path $StdoutPath) {
        $output += Get-Content -LiteralPath $StdoutPath
    }

    if (Test-Path $StderrPath) {
        $output += Get-Content -LiteralPath $StderrPath
    }

    $output | Set-Content -LiteralPath $OutputPath
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

    $content = Get-Content -LiteralPath $LogPath
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

Write-Host "CP-09 - Minimum sustained throughput validation test"
Write-Host "Repository root: $repoRoot"
Write-Host ""

Write-Host "Validating local requirements..."

if (-not (Test-Path $evidenceDir)) {
    throw "Required directory does not exist: $evidenceDir"
}

Assert-CommandAvailable -CommandName "docker"
Assert-CommandAvailable -CommandName "python"

docker compose version | Out-Null

Clear-FileIfExists -Path $terminalLogPath
Clear-FileIfExists -Path $onlineLogPath
Clear-FileIfExists -Path $posLogPath
Clear-FileIfExists -Path $offsetsLogPath
Clear-FileIfExists -Path $summaryPath

Add-TerminalLog "CP-09 - Minimum sustained throughput validation test"
Add-TerminalLog "Execution date: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')"
Add-TerminalLog "Repository root: $repoRoot"
Add-TerminalLog ""
Add-TerminalLog "Objective:"
Add-TerminalLog "Validate that ONLINE + POS producers sustain at least $minimumThroughput events/s combined."
Add-TerminalLog ""
Add-TerminalLog "Configuration:"
Add-TerminalLog "ONLINE producer rate: $onlineRate events/s"
Add-TerminalLog "POS producer rate: $posRate events/s"
Add-TerminalLog "Duration: $durationSeconds seconds"
Add-TerminalLog "Minimum accepted combined throughput: $minimumThroughput events/s"
Add-TerminalLog ""

Write-Host "Starting services required for CP-09..."
docker compose up -d kafka kafka-ui flink-jobmanager flink-taskmanager flink-anomalies-job flink-aggregation-job

Add-TerminalLog "Command executed:"
Add-TerminalLog "docker compose up -d kafka kafka-ui flink-jobmanager flink-taskmanager flink-anomalies-job flink-aggregation-job"
Add-TerminalLog ""

Write-Host "Waiting for services to be ready..."
Start-Sleep -Seconds 45

Write-Host "Collecting initial Kafka offsets..."

$onlineOffsetsBefore = Get-TopicEndOffsets -Topic $onlineTopic
$posOffsetsBefore = Get-TopicEndOffsets -Topic $posTopic

$onlineBeforeTotal = Sum-Offsets -Offsets $onlineOffsetsBefore
$posBeforeTotal = Sum-Offsets -Offsets $posOffsetsBefore

"CP-09 Kafka offsets before execution" | Set-Content -LiteralPath $offsetsLogPath
"" | Add-Content -LiteralPath $offsetsLogPath
"Topic: $onlineTopic" | Add-Content -LiteralPath $offsetsLogPath
"Offsets before:" | Add-Content -LiteralPath $offsetsLogPath
($onlineOffsetsBefore.GetEnumerator() | Sort-Object Name | ForEach-Object { "partition=$($_.Name) offset=$($_.Value)" }) | Add-Content -LiteralPath $offsetsLogPath
"Total before: $onlineBeforeTotal" | Add-Content -LiteralPath $offsetsLogPath
"" | Add-Content -LiteralPath $offsetsLogPath
"Topic: $posTopic" | Add-Content -LiteralPath $offsetsLogPath
"Offsets before:" | Add-Content -LiteralPath $offsetsLogPath
($posOffsetsBefore.GetEnumerator() | Sort-Object Name | ForEach-Object { "partition=$($_.Name) offset=$($_.Value)" }) | Add-Content -LiteralPath $offsetsLogPath
"Total before: $posBeforeTotal" | Add-Content -LiteralPath $offsetsLogPath
"" | Add-Content -LiteralPath $offsetsLogPath

Add-TerminalLog "Initial Kafka offsets collected."
Add-TerminalLog "Online total offset before: $onlineBeforeTotal"
Add-TerminalLog "POS total offset before: $posBeforeTotal"
Add-TerminalLog ""

Write-Host "Running ONLINE and POS producers in parallel..."

$onlineStdoutPath = Join-Path $evidenceDir "cp09_online.stdout.tmp"
$onlineStderrPath = Join-Path $evidenceDir "cp09_online.stderr.tmp"
$posStdoutPath = Join-Path $evidenceDir "cp09_pos.stdout.tmp"
$posStderrPath = Join-Path $evidenceDir "cp09_pos.stderr.tmp"

Clear-FileIfExists -Path $onlineStdoutPath
Clear-FileIfExists -Path $onlineStderrPath
Clear-FileIfExists -Path $posStdoutPath
Clear-FileIfExists -Path $posStderrPath

$onlineArgs = @(
    ".\producers\producer_online.py",
    "--rate",
    "$onlineRate",
    "--duration",
    "$durationSeconds"
)

$posArgs = @(
    ".\producers\producer_pos.py",
    "--rate",
    "$posRate",
    "--duration",
    "$durationSeconds"
)

$onlineProcess = Start-Process `
    -FilePath "python" `
    -ArgumentList $onlineArgs `
    -WorkingDirectory $repoRoot `
    -RedirectStandardOutput $onlineStdoutPath `
    -RedirectStandardError $onlineStderrPath `
    -NoNewWindow `
    -PassThru

Start-Sleep -Seconds 1

$posProcess = Start-Process `
    -FilePath "python" `
    -ArgumentList $posArgs `
    -WorkingDirectory $repoRoot `
    -RedirectStandardOutput $posStdoutPath `
    -RedirectStandardError $posStderrPath `
    -NoNewWindow `
    -PassThru

$onlineProcess.WaitForExit()
$posProcess.WaitForExit()

Merge-ProcessOutput -StdoutPath $onlineStdoutPath -StderrPath $onlineStderrPath -OutputPath $onlineLogPath
Merge-ProcessOutput -StdoutPath $posStdoutPath -StderrPath $posStderrPath -OutputPath $posLogPath

Clear-FileIfExists -Path $onlineStdoutPath
Clear-FileIfExists -Path $onlineStderrPath
Clear-FileIfExists -Path $posStdoutPath
Clear-FileIfExists -Path $posStderrPath

if ($null -ne $onlineProcess.ExitCode -and $onlineProcess.ExitCode -ne 0) {
    throw "ONLINE producer failed with exit code $($onlineProcess.ExitCode). Check log: $onlineLogPath"
}

if ($null -ne $posProcess.ExitCode -and $posProcess.ExitCode -ne 0) {
    throw "POS producer failed with exit code $($posProcess.ExitCode). Check log: $posLogPath"
}

Add-TerminalLog "Producer commands executed successfully."
Add-TerminalLog "ONLINE log: $onlineLogPath"
Add-TerminalLog "POS log: $posLogPath"
Add-TerminalLog ""

Write-Host "Waiting for Kafka to flush topic counters..."
Start-Sleep -Seconds 10

Write-Host "Collecting final Kafka offsets..."

$onlineOffsetsAfter = Get-TopicEndOffsets -Topic $onlineTopic
$posOffsetsAfter = Get-TopicEndOffsets -Topic $posTopic

$onlineAfterTotal = Sum-Offsets -Offsets $onlineOffsetsAfter
$posAfterTotal = Sum-Offsets -Offsets $posOffsetsAfter

$onlineDelta = $onlineAfterTotal - $onlineBeforeTotal
$posDelta = $posAfterTotal - $posBeforeTotal
$totalDelta = $onlineDelta + $posDelta

$offsetThroughput = [math]::Round(($totalDelta / $durationSeconds), 2)

"" | Add-Content -LiteralPath $offsetsLogPath
"CP-09 Kafka offsets after execution" | Add-Content -LiteralPath $offsetsLogPath
"" | Add-Content -LiteralPath $offsetsLogPath
"Topic: $onlineTopic" | Add-Content -LiteralPath $offsetsLogPath
"Offsets after:" | Add-Content -LiteralPath $offsetsLogPath
($onlineOffsetsAfter.GetEnumerator() | Sort-Object Name | ForEach-Object { "partition=$($_.Name) offset=$($_.Value)" }) | Add-Content -LiteralPath $offsetsLogPath
"Total after: $onlineAfterTotal" | Add-Content -LiteralPath $offsetsLogPath
"Delta: $onlineDelta" | Add-Content -LiteralPath $offsetsLogPath
"" | Add-Content -LiteralPath $offsetsLogPath
"Topic: $posTopic" | Add-Content -LiteralPath $offsetsLogPath
"Offsets after:" | Add-Content -LiteralPath $offsetsLogPath
($posOffsetsAfter.GetEnumerator() | Sort-Object Name | ForEach-Object { "partition=$($_.Name) offset=$($_.Value)" }) | Add-Content -LiteralPath $offsetsLogPath
"Total after: $posAfterTotal" | Add-Content -LiteralPath $offsetsLogPath
"Delta: $posDelta" | Add-Content -LiteralPath $offsetsLogPath
"" | Add-Content -LiteralPath $offsetsLogPath
"Total delta: $totalDelta" | Add-Content -LiteralPath $offsetsLogPath
"Offset-based throughput: $offsetThroughput events/s" | Add-Content -LiteralPath $offsetsLogPath

$onlineMetrics = Get-ProducerMetrics -LogPath $onlineLogPath
$posMetrics = Get-ProducerMetrics -LogPath $posLogPath

$producerTotalEvents = [int]$onlineMetrics.total + [int]$posMetrics.total
$producerCombinedThroughput = [math]::Round(([double]$onlineMetrics.average_rate + [double]$posMetrics.average_rate), 2)
$producerEventsThroughput = [math]::Round(($producerTotalEvents / $durationSeconds), 2)

$passed = $producerCombinedThroughput -ge $minimumThroughput

$summary = @"
CP-09 - Throughput mínimo sostenido

Configuration:
ONLINE configured rate: $onlineRate events/s
POS configured rate: $posRate events/s
Configured total rate: $($onlineRate + $posRate) events/s
Duration: $durationSeconds seconds
Minimum accepted throughput: $minimumThroughput events/s

Producer ONLINE:
Total sent: $($onlineMetrics.total)
Failures: $($onlineMetrics.failures)
Average rate: $($onlineMetrics.average_rate) events/s
Final line: $($onlineMetrics.final_line)

Producer POS:
Total sent: $($posMetrics.total)
Failures: $($posMetrics.failures)
Average rate: $($posMetrics.average_rate) events/s
Final line: $($posMetrics.final_line)

Combined result from producer logs:
Total sent: $producerTotalEvents
Combined average throughput: $producerCombinedThroughput events/s
Combined throughput from total/duration: $producerEventsThroughput events/s

Kafka offsets:
$onlineTopic delta: $onlineDelta
$posTopic delta: $posDelta
Total Kafka delta: $totalDelta
Kafka offset-based throughput: $offsetThroughput events/s

Criterion:
PASS if combined average throughput >= $minimumThroughput events/s

Result:
$(if ($passed) { "PASSED" } else { "FAILED" })

Technical observation:
The producers simulate realistic traffic and may generate a small percentage of invalid messages and anomaly-triggering events. Therefore, if Flink jobs are running, messages may also appear in transactions-dlq, alerts and transactions-aggregated. This does not invalidate CP-09 because the objective is to validate input throughput in transactions-online and transactions-pos.
"@

$summary | Set-Content -LiteralPath $summaryPath

Add-TerminalLog "Final Kafka offsets collected."
Add-TerminalLog "Online delta: $onlineDelta"
Add-TerminalLog "POS delta: $posDelta"
Add-TerminalLog "Total delta: $totalDelta"
Add-TerminalLog "Kafka offset-based throughput: $offsetThroughput events/s"
Add-TerminalLog ""
Add-TerminalLog "CP-09 validation result: $(if ($passed) { "PASSED" } else { "FAILED" })"
Add-TerminalLog "Combined producer throughput: $producerCombinedThroughput events/s"
Add-TerminalLog "Summary file: $summaryPath"

Write-Host ""
Write-Host "CP-09 execution summary"
Write-Host "-----------------------"
Write-Host "ONLINE total sent: $($onlineMetrics.total)"
Write-Host "ONLINE average rate: $($onlineMetrics.average_rate) events/s"
Write-Host "POS total sent: $($posMetrics.total)"
Write-Host "POS average rate: $($posMetrics.average_rate) events/s"
Write-Host "Combined producer throughput: $producerCombinedThroughput events/s"
Write-Host "Kafka offset-based throughput: $offsetThroughput events/s"
Write-Host ""
Write-Host "Evidence files:"
Write-Host "- $terminalLogPath"
Write-Host "- $onlineLogPath"
Write-Host "- $posLogPath"
Write-Host "- $offsetsLogPath"
Write-Host "- $summaryPath"

if ($passed) {
    Write-Host ""
    Write-Host "CP-09 validation: PASSED"
    Write-Host "Combined throughput is >= $minimumThroughput events/s."
}
else {
    Write-Host ""
    Write-Warning "CP-09 validation: FAILED"
    Write-Warning "Combined throughput is below $minimumThroughput events/s."
}

Write-Host ""
Write-Host "Recommended manual screenshot:"
Write-Host "- docs/evidence/cp09_kafka_ui_topics.png"
Write-Host ""
Write-Host "CP-09 script finished."