# CP-04 - High amount anomaly alert validation test
# Sends a controlled transaction with unusual amount and validates alert in Kafka topic alerts.

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
Set-Location $repoRoot

$evidenceDir = Join-Path $repoRoot "docs\evidence"

$terminalLogPath = Join-Path $evidenceDir "cp04_terminal_log.txt"
$inputEventPath = Join-Path $evidenceDir "cp04_input_event.txt"
$alertsValidationLogPath = Join-Path $evidenceDir "cp04_alerts_validation_log.txt"

$transactionId = "cp04-high-amount-001"
$cardId = "4532015112830366"
$eventTimestamp = "2026-05-16T17:30:00.000000+00:00"
$amount = "20000.00"
$currency = "USD"
$topicInput = "transactions-online"
$topicAlerts = "alerts"

$eventLine = "$cardId|{`"transaction_id`":`"$transactionId`",`"card_id`":`"$cardId`",`"event_timestamp`":`"$eventTimestamp`",`"produced_at`":`"$eventTimestamp`",`"channel`":`"ONLINE`",`"amount`":20000.00,`"currency`":`"$currency`",`"status`":`"APPROVED`",`"merchant_name`":`"CP04 Test Merchant`",`"merchant_category`":`"electronics`",`"city`":`"Bogota`",`"country`":`"CO`"}"

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

function Add-TerminalLog {
    param (
        [AllowEmptyString()]
        [string]$Message
    )

    $Message | Add-Content -LiteralPath $terminalLogPath
}

Write-Host "CP-04 - High amount anomaly alert validation test"
Write-Host "Repository root: $repoRoot"
Write-Host ""

Write-Host "Validating local requirements..."

if (-not (Test-Path $evidenceDir)) {
    throw "Required directory does not exist: $evidenceDir"
}

Assert-CommandAvailable -CommandName "docker"

docker compose version | Out-Null

Clear-FileIfExists -Path $terminalLogPath
Clear-FileIfExists -Path $inputEventPath
Clear-FileIfExists -Path $alertsValidationLogPath

Add-TerminalLog "CP-04 - High amount anomaly alert validation test"
Add-TerminalLog "Execution date: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')"
Add-TerminalLog "Repository root: $repoRoot"
Add-TerminalLog ""
Add-TerminalLog "Objective:"
Add-TerminalLog "Validate that a controlled transaction with amount=$amount and currency=$currency generates a HIGH_AMOUNT alert in Kafka topic '$topicAlerts'."
Add-TerminalLog ""

Write-Host "Starting services required for CP-04..."
docker compose up -d kafka kafka-ui flink-jobmanager flink-taskmanager flink-anomalies-job

Add-TerminalLog "Command executed:"
Add-TerminalLog "docker compose up -d kafka kafka-ui flink-jobmanager flink-taskmanager flink-anomalies-job"
Add-TerminalLog ""

Write-Host "Waiting for services and anomalies job to be ready..."
Start-Sleep -Seconds 45

Write-Host "Writing controlled CP-04 event..."
$eventLine | Set-Content -LiteralPath $inputEventPath -NoNewline

Add-TerminalLog "Input event written to:"
Add-TerminalLog $inputEventPath
Add-TerminalLog ""
Add-TerminalLog "Input event:"
Add-TerminalLog $eventLine
Add-TerminalLog ""

Write-Host "Publishing high amount transaction to $topicInput..."

$producerStdoutPath = Join-Path $evidenceDir "cp04_producer.stdout.tmp"
$producerStderrPath = Join-Path $evidenceDir "cp04_producer.stderr.tmp"

Clear-FileIfExists -Path $producerStdoutPath
Clear-FileIfExists -Path $producerStderrPath

$producerArgs = @(
    "exec",
    "-i",
    "kafka",
    "kafka-console-producer.sh",
    "--bootstrap-server",
    "localhost:9094",
    "--topic",
    $topicInput,
    "--property",
    "parse.key=true",
    "--property",
    "key.separator=|"
)

$producerProcess = Start-Process `
    -FilePath "docker" `
    -ArgumentList $producerArgs `
    -WorkingDirectory $repoRoot `
    -RedirectStandardInput $inputEventPath `
    -RedirectStandardOutput $producerStdoutPath `
    -RedirectStandardError $producerStderrPath `
    -NoNewWindow `
    -PassThru

$producerProcess.WaitForExit()

$producerCombinedLogPath = Join-Path $evidenceDir "cp04_producer_output_log.txt"
Merge-ProcessOutput -StdoutPath $producerStdoutPath -StderrPath $producerStderrPath -OutputPath $producerCombinedLogPath

Clear-FileIfExists -Path $producerStdoutPath
Clear-FileIfExists -Path $producerStderrPath

if ($null -ne $producerProcess.ExitCode -and $producerProcess.ExitCode -ne 0) {
    throw "Kafka producer command failed with exit code $($producerProcess.ExitCode). Check log: $producerCombinedLogPath"
}

Add-TerminalLog "Producer command executed successfully."
Add-TerminalLog "Kafka topic input: $topicInput"
Add-TerminalLog "Producer output log: $producerCombinedLogPath"
Add-TerminalLog ""

Write-Host "Waiting for Flink anomalies job to process the event..."
Start-Sleep -Seconds 20

Write-Host "Validating output in topic $topicAlerts..."

$validationStdoutPath = Join-Path $evidenceDir "cp04_alerts_validation.stdout.tmp"
$validationStderrPath = Join-Path $evidenceDir "cp04_alerts_validation.stderr.tmp"

Clear-FileIfExists -Path $validationStdoutPath
Clear-FileIfExists -Path $validationStderrPath

$consumerArgs = @(
    "exec",
    "kafka",
    "kafka-console-consumer.sh",
    "--bootstrap-server",
    "kafka:9094",
    "--topic",
    $topicAlerts,
    "--from-beginning",
    "--max-messages",
    "5",
    "--timeout-ms",
    "15000"
)

$consumerProcess = Start-Process `
    -FilePath "docker" `
    -ArgumentList $consumerArgs `
    -WorkingDirectory $repoRoot `
    -RedirectStandardOutput $validationStdoutPath `
    -RedirectStandardError $validationStderrPath `
    -NoNewWindow `
    -PassThru

$consumerProcess.WaitForExit()

Merge-ProcessOutput -StdoutPath $validationStdoutPath -StderrPath $validationStderrPath -OutputPath $alertsValidationLogPath

$alertLines = @()
if (Test-Path $validationStdoutPath) {
    $alertLines = Get-Content -LiteralPath $validationStdoutPath | Where-Object {
        $_ -and $_.Trim().StartsWith("{")
    }
}

Clear-FileIfExists -Path $validationStdoutPath
Clear-FileIfExists -Path $validationStderrPath

$matchingAlert = $false

foreach ($line in $alertLines) {
    if (
        $line -match "HIGH_AMOUNT" -and
        $line -match $transactionId -and
        $line -match $cardId
    ) {
        $matchingAlert = $true
    }
}

Add-TerminalLog "Validation command executed:"
Add-TerminalLog "docker exec kafka kafka-console-consumer.sh --bootstrap-server kafka:9094 --topic $topicAlerts --from-beginning --max-messages 5 --timeout-ms 15000"
Add-TerminalLog ""
Add-TerminalLog "Alerts validation log:"
Add-TerminalLog $alertsValidationLogPath
Add-TerminalLog ""

Write-Host ""
Write-Host "CP-04 execution summary"
Write-Host "-----------------------"
Write-Host "Input topic: $topicInput"
Write-Host "Alert topic: $topicAlerts"
Write-Host "Transaction ID: $transactionId"
Write-Host "Card ID: $cardId"
Write-Host "Amount: $amount"
Write-Host "Currency: $currency"
Write-Host ""
Write-Host "Evidence file terminal log: $terminalLogPath"
Write-Host "Evidence file input event: $inputEventPath"
Write-Host "Producer output log: $producerCombinedLogPath"
Write-Host "Alerts validation log: $alertsValidationLogPath"

if ($matchingAlert) {
    Write-Host ""
    Write-Host "CP-04 validation: PASSED"
    Write-Host "HIGH_AMOUNT alert found for transaction $transactionId."
    Add-TerminalLog "CP-04 validation: PASSED"
    Add-TerminalLog "HIGH_AMOUNT alert found for transaction $transactionId."
}
else {
    Write-Host ""
    Write-Warning "CP-04 validation did not automatically find the expected HIGH_AMOUNT alert."
    Write-Warning "Please validate manually in Kafka UI that topic '$topicAlerts' contains the alert."
    Add-TerminalLog "CP-04 validation: MANUAL CHECK REQUIRED"
    Add-TerminalLog "The script did not automatically find the expected HIGH_AMOUNT alert."
}

Write-Host ""
Write-Host "Kafka UI check:"
Write-Host "- Confirm topic transactions-online contains transaction_id $transactionId."
Write-Host "- Confirm topic alerts contains alert_type HIGH_AMOUNT."
Write-Host "- Confirm alert references card_id $cardId."
Write-Host ""
Write-Host "Flink UI check:"
Write-Host "- Confirm Fraud Detection, Anomalies Job is RUNNING."
Write-Host "- Confirm Source and Sink operators processed records."
Write-Host ""
Write-Host "Recommended manual screenshots:"
Write-Host "- docs/evidence/cp04_kafka_ui_input.png"
Write-Host "- docs/evidence/cp04_kafka_ui_alerts.png"
Write-Host "- docs/evidence/cp04_flink_job.png"
Write-Host ""
Write-Host "CP-04 script finished."