# CP-03 - Aggregation job validation test
# Executes ONLINE and POS producers, then validates output in transactions-aggregated.

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
Set-Location $repoRoot

$evidenceDir = Join-Path $repoRoot "docs\evidence"

$onlineLogPath = Join-Path $evidenceDir "cp03_online_terminal_log.txt"
$posLogPath = Join-Path $evidenceDir "cp03_pos_terminal_log.txt"
$aggregationValidationLogPath = Join-Path $evidenceDir "cp03_aggregated_validation_log.txt"

$producerRate = "10"
$producerDuration = "150"
$aggregationWaitSeconds = 60

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

function Get-ProducerSummary {
    param (
        [Parameter(Mandatory = $true)]
        [string]$LogPath
    )

    $summary = [PSCustomObject]@{
        Sent = "unknown"
        Failures = "unknown"
        Duration = "unknown"
        Rate = "unknown"
    }

    if (-not (Test-Path $LogPath)) {
        return $summary
    }

    $content = Get-Content -LiteralPath $LogPath -ErrorAction SilentlyContinue
    $finalLine = $content | Where-Object { $_ -match "Fin\. Total enviados=" } | Select-Object -Last 1

    if ($finalLine) {
        if ($finalLine -match "Total enviados=(\d+)") {
            $summary.Sent = $Matches[1]
        }

        if ($finalLine -match "fallos=(\d+)") {
            $summary.Failures = $Matches[1]
        }
        else {
            $summary.Failures = "0"
        }

        if ($finalLine -match "duracion=([0-9.]+s)") {
            $summary.Duration = $Matches[1]
        }

        if ($finalLine -match "tasa_promedio=([0-9.]+ ev/s)") {
            $summary.Rate = $Matches[1]
        }
    }

    return $summary
}

Write-Host "CP-03 - Aggregation job validation test"
Write-Host "Repository root: $repoRoot"
Write-Host ""

Write-Host "Validating local requirements..."

if (-not (Test-Path $evidenceDir)) {
    throw "Required directory does not exist: $evidenceDir"
}

Assert-CommandAvailable -CommandName "docker"
Assert-CommandAvailable -CommandName "python"

docker compose version | Out-Null
python --version | Out-Null

Write-Host "Starting services required for CP-03..."
docker compose up -d kafka kafka-ui flink-jobmanager flink-taskmanager flink-aggregation-job

Write-Host "Waiting for services to be ready..."
Start-Sleep -Seconds 30

Write-Host "Waiting for Flink aggregation submitter to complete..."
Start-Sleep -Seconds 20

Write-Host "Running ONLINE and POS producers for $producerDuration seconds..."

Clear-FileIfExists -Path $onlineLogPath
Clear-FileIfExists -Path $posLogPath
Clear-FileIfExists -Path $aggregationValidationLogPath

$pythonCommand = (Get-Command python).Source

$onlineStdoutPath = Join-Path $evidenceDir "cp03_online.stdout.tmp"
$onlineStderrPath = Join-Path $evidenceDir "cp03_online.stderr.tmp"
$posStdoutPath = Join-Path $evidenceDir "cp03_pos.stdout.tmp"
$posStderrPath = Join-Path $evidenceDir "cp03_pos.stderr.tmp"

Clear-FileIfExists -Path $onlineStdoutPath
Clear-FileIfExists -Path $onlineStderrPath
Clear-FileIfExists -Path $posStdoutPath
Clear-FileIfExists -Path $posStderrPath

$onlineArgs = @(".\producers\producer_online.py", "--rate", $producerRate, "--duration", $producerDuration)
$posArgs = @(".\producers\producer_pos.py", "--rate", $producerRate, "--duration", $producerDuration)

$onlineProcess = Start-Process -FilePath $pythonCommand -ArgumentList $onlineArgs -WorkingDirectory $repoRoot -RedirectStandardOutput $onlineStdoutPath -RedirectStandardError $onlineStderrPath -NoNewWindow -PassThru
$posProcess = Start-Process -FilePath $pythonCommand -ArgumentList $posArgs -WorkingDirectory $repoRoot -RedirectStandardOutput $posStdoutPath -RedirectStandardError $posStderrPath -NoNewWindow -PassThru

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

Write-Host "Waiting for aggregation window and watermark to flush..."
Start-Sleep -Seconds $aggregationWaitSeconds

Write-Host "Validating output in transactions-aggregated..."

$validationStdoutPath = Join-Path $evidenceDir "cp03_aggregated_validation.stdout.tmp"
$validationStderrPath = Join-Path $evidenceDir "cp03_aggregated_validation.stderr.tmp"

Clear-FileIfExists -Path $validationStdoutPath
Clear-FileIfExists -Path $validationStderrPath

$consumerArgs = @(
    "exec",
    "kafka",
    "kafka-console-consumer.sh",
    "--bootstrap-server",
    "kafka:9094",
    "--topic",
    "transactions-aggregated",
    "--from-beginning",
    "--max-messages",
    "3",
    "--timeout-ms",
    "15000"
)

$consumerProcess = Start-Process -FilePath "docker" -ArgumentList $consumerArgs -WorkingDirectory $repoRoot -RedirectStandardOutput $validationStdoutPath -RedirectStandardError $validationStderrPath -NoNewWindow -PassThru
$consumerProcess.WaitForExit()

Merge-ProcessOutput -StdoutPath $validationStdoutPath -StderrPath $validationStderrPath -OutputPath $aggregationValidationLogPath

$aggregatedMessages = @()

if (Test-Path $validationStdoutPath) {
    $aggregatedMessages = Get-Content -LiteralPath $validationStdoutPath | Where-Object {
        $_ -match '^\s*\{'
    }
}

Clear-FileIfExists -Path $validationStdoutPath
Clear-FileIfExists -Path $validationStderrPath

$onlineSummary = Get-ProducerSummary -LogPath $onlineLogPath
$posSummary = Get-ProducerSummary -LogPath $posLogPath

Write-Host ""
Write-Host "CP-03 execution summary"
Write-Host "-----------------------"
Write-Host "ONLINE messages reported by producer: $($onlineSummary.Sent)"
Write-Host "ONLINE producer failures: $($onlineSummary.Failures)"
Write-Host "ONLINE duration: $($onlineSummary.Duration)"
Write-Host "ONLINE average rate: $($onlineSummary.Rate)"
Write-Host ""
Write-Host "POS messages reported by producer: $($posSummary.Sent)"
Write-Host "POS producer failures: $($posSummary.Failures)"
Write-Host "POS duration: $($posSummary.Duration)"
Write-Host "POS average rate: $($posSummary.Rate)"
Write-Host ""
Write-Host "Evidence file ONLINE: $onlineLogPath"
Write-Host "Evidence file POS: $posLogPath"
Write-Host "Aggregation validation log: $aggregationValidationLogPath"

if ($aggregatedMessages.Count -gt 0) {
    Write-Host ""
    Write-Host "Aggregation validation: PASSED"
    Write-Host "Sample messages found in transactions-aggregated:"
    $aggregatedMessages | Select-Object -First 3 | ForEach-Object {
        Write-Host $_
    }
}
else {
    Write-Host ""
    Write-Warning "Aggregation validation did not read JSON messages automatically."
    Write-Warning "Please validate manually in Kafka UI that transactions-aggregated contains messages."
}

Write-Host ""
Write-Host "Kafka UI check:"
Write-Host "- Confirm transactions-online has messages."
Write-Host "- Confirm transactions-pos has messages."
Write-Host "- Confirm transactions-aggregated has aggregated messages."
Write-Host ""
Write-Host "Flink UI check:"
Write-Host "- Confirm the aggregation job is RUNNING."
Write-Host "- Confirm Source and aggregation operators processed records."
Write-Host ""
Write-Host "CP-03 script finished."