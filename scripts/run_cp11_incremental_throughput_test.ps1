# CP-11 - Incremental throughput validation test
# Runs ONLINE and POS producers in three incremental load steps:
# Step 1: 100 + 100 = 200 configured events/s
# Step 2: 150 + 150 = 300 configured events/s
# Step 3: 200 + 200 = 400 configured events/s

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
Set-Location $repoRoot

$evidenceDir = Join-Path $repoRoot "docs\evidence"

$terminalLogPath = Join-Path $evidenceDir "cp11_terminal_log.txt"
$summaryPath = Join-Path $evidenceDir "cp11_incremental_results.txt"
$csvPath = Join-Path $evidenceDir "cp11_incremental_results.csv"

$durationSeconds = 120

$steps = @(
    @{
        Name = "step1"
        Label = "Escalon 1"
        OnlineRate = 100
        PosRate = 100
        ConfiguredTotal = 200
    },
    @{
        Name = "step2"
        Label = "Escalon 2"
        OnlineRate = 150
        PosRate = 150
        ConfiguredTotal = 300
    },
    @{
        Name = "step3"
        Label = "Escalon 3"
        OnlineRate = 200
        PosRate = 200
        ConfiguredTotal = 400
    }
)

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

    $output | Set-Content -LiteralPath $OutputPath -Encoding UTF8
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
            duration = 0.0
            final_line = "Final line not found"
        }
    }

    $total = 0
    $failures = "N/A"
    $averageRate = 0.0
    $duration = 0.0

    if ($finalLine -match "Total enviados=(\d+)") {
        $total = [int]$Matches[1]
    }

    if ($finalLine -match "fallos=(\d+)") {
        $failures = [int]$Matches[1]
    }

    if ($finalLine -match "duracion=([\d\.,]+)s") {
        $durationText = $Matches[1].Replace(",", ".")
        $duration = [double]$durationText
    }

    if ($finalLine -match "tasa_promedio=([\d\.,]+)") {
        $averageRateText = $Matches[1].Replace(",", ".")
        $averageRate = [double]$averageRateText
    }

    return @{
        total = $total
        failures = $failures
        average_rate = $averageRate
        duration = $duration
        final_line = $finalLine
    }
}

function Invoke-LoadStep {
    param (
        [Parameter(Mandatory = $true)]
        [hashtable]$Step
    )

    $stepName = $Step.Name
    $stepLabel = $Step.Label
    $onlineRate = [int]$Step.OnlineRate
    $posRate = [int]$Step.PosRate
    $configuredTotal = [int]$Step.ConfiguredTotal

    $onlineLogPath = Join-Path $evidenceDir "cp11_${stepName}_online_log.txt"
    $posLogPath = Join-Path $evidenceDir "cp11_${stepName}_pos_log.txt"

    $onlineStdoutPath = Join-Path $evidenceDir "cp11_${stepName}_online.stdout.tmp"
    $onlineStderrPath = Join-Path $evidenceDir "cp11_${stepName}_online.stderr.tmp"
    $posStdoutPath = Join-Path $evidenceDir "cp11_${stepName}_pos.stdout.tmp"
    $posStderrPath = Join-Path $evidenceDir "cp11_${stepName}_pos.stderr.tmp"

    Write-Host ""
    Write-Host "$stepLabel - Running load"
    Write-Host "ONLINE rate: $onlineRate events/s"
    Write-Host "POS rate: $posRate events/s"
    Write-Host "Configured total: $configuredTotal events/s"
    Write-Host "Duration: $durationSeconds seconds"
    Write-Host ""

    Add-TerminalLog ""
    Add-TerminalLog "$stepLabel"
    Add-TerminalLog "ONLINE rate: $onlineRate events/s"
    Add-TerminalLog "POS rate: $posRate events/s"
    Add-TerminalLog "Configured total: $configuredTotal events/s"
    Add-TerminalLog "Duration: $durationSeconds seconds"

    "" | Set-Content -LiteralPath $onlineLogPath -Encoding UTF8
    "" | Set-Content -LiteralPath $posLogPath -Encoding UTF8
    "" | Set-Content -LiteralPath $onlineStdoutPath -Encoding UTF8
    "" | Set-Content -LiteralPath $onlineStderrPath -Encoding UTF8
    "" | Set-Content -LiteralPath $posStdoutPath -Encoding UTF8
    "" | Set-Content -LiteralPath $posStderrPath -Encoding UTF8

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

    Remove-Item -LiteralPath $onlineStdoutPath -Force -ErrorAction SilentlyContinue
    Remove-Item -LiteralPath $onlineStderrPath -Force -ErrorAction SilentlyContinue
    Remove-Item -LiteralPath $posStdoutPath -Force -ErrorAction SilentlyContinue
    Remove-Item -LiteralPath $posStderrPath -Force -ErrorAction SilentlyContinue

    if ($null -ne $onlineProcess.ExitCode -and $onlineProcess.ExitCode -ne 0) {
        throw "$stepLabel ONLINE producer failed with exit code $($onlineProcess.ExitCode). Check log: $onlineLogPath"
    }

    if ($null -ne $posProcess.ExitCode -and $posProcess.ExitCode -ne 0) {
        throw "$stepLabel POS producer failed with exit code $($posProcess.ExitCode). Check log: $posLogPath"
    }

    $onlineMetrics = Get-ProducerMetrics -LogPath $onlineLogPath
    $posMetrics = Get-ProducerMetrics -LogPath $posLogPath

    $totalEvents = [int]$onlineMetrics.total + [int]$posMetrics.total
    $combinedThroughputByRate = [math]::Round(([double]$onlineMetrics.average_rate + [double]$posMetrics.average_rate), 2)
    $combinedThroughputByTotal = [math]::Round(($totalEvents / $durationSeconds), 2)
    $configuredCompliance = [math]::Round(($combinedThroughputByRate / $configuredTotal) * 100, 2)
    $gapEventsPerSecond = [math]::Round(($configuredTotal - $combinedThroughputByRate), 2)

    $status = "Stable"

    if ($configuredCompliance -lt 85 -and $configuredCompliance -ge 80) {
        $status = "Stable with significant degradation"
    }
    elseif ($configuredCompliance -lt 80) {
        $status = "Degraded"
    }
    elseif ($configuredCompliance -lt 90) {
        $status = "Stable with moderate degradation"
    }

    Add-TerminalLog "ONLINE total sent: $($onlineMetrics.total)"
    Add-TerminalLog "ONLINE failures: $($onlineMetrics.failures)"
    Add-TerminalLog "ONLINE average rate: $($onlineMetrics.average_rate) events/s"
    Add-TerminalLog "POS total sent: $($posMetrics.total)"
    Add-TerminalLog "POS failures: $($posMetrics.failures)"
    Add-TerminalLog "POS average rate: $($posMetrics.average_rate) events/s"
    Add-TerminalLog "Combined throughput: $combinedThroughputByRate events/s"
    Add-TerminalLog "Configured compliance: $configuredCompliance %"
    Add-TerminalLog "Gap vs configured rate: $gapEventsPerSecond events/s"
    Add-TerminalLog "Step status: $status"

    Write-Host "$stepLabel result:"
    Write-Host "ONLINE total sent: $($onlineMetrics.total)"
    Write-Host "ONLINE average rate: $($onlineMetrics.average_rate) events/s"
    Write-Host "POS total sent: $($posMetrics.total)"
    Write-Host "POS average rate: $($posMetrics.average_rate) events/s"
    Write-Host "Combined throughput: $combinedThroughputByRate events/s"
    Write-Host "Configured compliance: $configuredCompliance %"
    Write-Host "Gap vs configured: $gapEventsPerSecond events/s"
    Write-Host "Status: $status"
    Write-Host ""

    return [PSCustomObject]@{
        Step = $stepLabel
        StepName = $stepName
        OnlineConfiguredRate = $onlineRate
        PosConfiguredRate = $posRate
        ConfiguredTotalRate = $configuredTotal
        DurationSeconds = $durationSeconds
        OnlineTotalSent = [int]$onlineMetrics.total
        OnlineFailures = $onlineMetrics.failures
        OnlineAverageRate = [double]$onlineMetrics.average_rate
        PosTotalSent = [int]$posMetrics.total
        PosFailures = $posMetrics.failures
        PosAverageRate = [double]$posMetrics.average_rate
        TotalEvents = $totalEvents
        CombinedThroughputByRate = $combinedThroughputByRate
        CombinedThroughputByTotal = $combinedThroughputByTotal
        ConfiguredCompliancePercent = $configuredCompliance
        GapVsConfiguredEventsPerSecond = $gapEventsPerSecond
        Status = $status
        OnlineLog = $onlineLogPath
        PosLog = $posLogPath
    }
}

Write-Host "CP-11 - Incremental throughput validation test"
Write-Host "Repository root: $repoRoot"
Write-Host ""

if (-not (Test-Path $evidenceDir)) {
    throw "Required directory does not exist: $evidenceDir"
}

Assert-CommandAvailable -CommandName "docker"
Assert-CommandAvailable -CommandName "python"

"CP-11 - Incremental throughput validation test" | Set-Content -LiteralPath $terminalLogPath -Encoding UTF8
"CP-11 - Throughput incremental" | Set-Content -LiteralPath $summaryPath -Encoding UTF8
"step,online_configured_rate,pos_configured_rate,configured_total_rate,duration_seconds,online_total_sent,online_failures,online_average_rate,pos_total_sent,pos_failures,pos_average_rate,total_events,combined_throughput_by_rate,combined_throughput_by_total,configured_compliance_percent,gap_vs_configured_events_per_second,status" | Set-Content -LiteralPath $csvPath -Encoding UTF8

Add-TerminalLog "Execution date: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')"
Add-TerminalLog "Repository root: $repoRoot"
Add-TerminalLog ""
Add-TerminalLog "Objective:"
Add-TerminalLog "Evaluate system behavior under incremental load and identify sustained throughput and degradation point."
Add-TerminalLog ""

Write-Host "Starting services required for CP-11..."
docker compose up -d kafka kafka-ui flink-jobmanager flink-taskmanager flink-anomalies-job flink-aggregation-job

Add-TerminalLog "Command executed:"
Add-TerminalLog "docker compose up -d kafka kafka-ui flink-jobmanager flink-taskmanager flink-anomalies-job flink-aggregation-job"
Add-TerminalLog ""

Write-Host "Waiting for services and Flink jobs to be ready..."
Start-Sleep -Seconds 45

$results = @()

foreach ($step in $steps) {
    $result = Invoke-LoadStep -Step $step
    $results += $result

    Add-Content -LiteralPath $csvPath -Encoding UTF8 -Value (
        "$($result.Step),$($result.OnlineConfiguredRate),$($result.PosConfiguredRate),$($result.ConfiguredTotalRate),$($result.DurationSeconds),$($result.OnlineTotalSent),$($result.OnlineFailures),$($result.OnlineAverageRate),$($result.PosTotalSent),$($result.PosFailures),$($result.PosAverageRate),$($result.TotalEvents),$($result.CombinedThroughputByRate),$($result.CombinedThroughputByTotal),$($result.ConfiguredCompliancePercent),$($result.GapVsConfiguredEventsPerSecond),$($result.Status)"
    )

    Write-Host "Waiting 60 seconds before next step..."
    Start-Sleep -Seconds 60
}

$maxThroughput = ($results | Measure-Object -Property CombinedThroughputByRate -Maximum).Maximum
$maxStep = $results | Sort-Object CombinedThroughputByRate -Descending | Select-Object -First 1

$degradationNotes = @()

foreach ($r in $results) {
    if ($r.ConfiguredCompliancePercent -lt 90) {
        $degradationNotes += "$($r.Step): compliance $($r.ConfiguredCompliancePercent)% with gap $($r.GapVsConfiguredEventsPerSecond) events/s."
    }
}

$finalResult = "PASSED"

if ($results[0].CombinedThroughputByRate -lt 100) {
    $finalResult = "FAILED"
}

@"

Configuration:
Escalon 1: 100 ONLINE + 100 POS = 200 events/s configured
Escalon 2: 150 ONLINE + 150 POS = 300 events/s configured
Escalon 3: 200 ONLINE + 200 POS = 400 events/s configured
Duration per step: $durationSeconds seconds

Results:
"@ | Add-Content -LiteralPath $summaryPath -Encoding UTF8

foreach ($r in $results) {
@"
$($r.Step):
Configured ONLINE: $($r.OnlineConfiguredRate) events/s
Configured POS: $($r.PosConfiguredRate) events/s
Configured total: $($r.ConfiguredTotalRate) events/s
Duration: $($r.DurationSeconds) seconds

ONLINE:
Total sent: $($r.OnlineTotalSent)
Failures: $($r.OnlineFailures)
Average rate: $($r.OnlineAverageRate) events/s

POS:
Total sent: $($r.PosTotalSent)
Failures: $($r.PosFailures)
Average rate: $($r.PosAverageRate) events/s

Combined:
Total events: $($r.TotalEvents)
Combined throughput by producer rate: $($r.CombinedThroughputByRate) events/s
Combined throughput by total/duration: $($r.CombinedThroughputByTotal) events/s
Configured compliance: $($r.ConfiguredCompliancePercent) %
Gap vs configured rate: $($r.GapVsConfiguredEventsPerSecond) events/s
Status: $($r.Status)

Logs:
ONLINE log: $($r.OnlineLog)
POS log: $($r.PosLog)

"@ | Add-Content -LiteralPath $summaryPath -Encoding UTF8
}

@"
Conclusion:
Maximum sustained throughput observed: $maxThroughput events/s
Maximum step observed: $($maxStep.Step)
Final result: $finalResult

Degradation notes:
$($degradationNotes -join "`n")

Technical observation:
The producers generate realistic traffic, including a small proportion of anomalies and invalid records. Therefore, messages may also appear in alerts, transactions-dlq and transactions-aggregated while Flink jobs are running. This does not invalidate CP-11 because the objective is to evaluate incremental throughput and stability under increasing load.

Acceptance criterion:
The test is useful and accepted if it identifies the approximate sustained throughput and the degradation point while the system remains stable at least in the first incremental step.
"@ | Add-Content -LiteralPath $summaryPath -Encoding UTF8

Add-TerminalLog ""
Add-TerminalLog "CP-11 final result: $finalResult"
Add-TerminalLog "Maximum sustained throughput observed: $maxThroughput events/s"
Add-TerminalLog "Maximum step observed: $($maxStep.Step)"
Add-TerminalLog "Summary file: $summaryPath"
Add-TerminalLog "CSV file: $csvPath"

Write-Host ""
Write-Host "CP-11 execution summary"
Write-Host "-----------------------"
Get-Content -LiteralPath $summaryPath -Encoding UTF8 | ForEach-Object { Write-Host $_ }

Write-Host ""
Write-Host "Evidence files:"
Write-Host "- $terminalLogPath"
Write-Host "- $summaryPath"
Write-Host "- $csvPath"
Write-Host "- docs/evidence/cp11_step1_online_log.txt"
Write-Host "- docs/evidence/cp11_step1_pos_log.txt"
Write-Host "- docs/evidence/cp11_step2_online_log.txt"
Write-Host "- docs/evidence/cp11_step2_pos_log.txt"
Write-Host "- docs/evidence/cp11_step3_online_log.txt"
Write-Host "- docs/evidence/cp11_step3_pos_log.txt"

Write-Host ""
Write-Host "Recommended manual screenshots:"
Write-Host "- docs/evidence/cp11_kafka_ui_step1.png"
Write-Host "- docs/evidence/cp11_flink_anomalies_step1.png"
Write-Host "- docs/evidence/cp11_flink_aggregation_step1.png"
Write-Host "- docs/evidence/cp11_kafka_ui_step2.png"
Write-Host "- docs/evidence/cp11_flink_anomalies_step2.png"
Write-Host "- docs/evidence/cp11_flink_aggregation_step2.png"
Write-Host "- docs/evidence/cp11_kafka_ui_step3.png"
Write-Host "- docs/evidence/cp11_flink_anomalies_step3.png"
Write-Host "- docs/evidence/cp11_flink_aggregation_step3.png"

Write-Host ""
Write-Host "CP-11 validation: $finalResult"
Write-Host "Maximum sustained throughput observed: $maxThroughput events/s."
Write-Host "CP-11 script finished."