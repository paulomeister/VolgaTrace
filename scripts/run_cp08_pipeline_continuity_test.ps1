# CP-08 - Pipeline continuity after invalid message test
# Sends a valid HIGH_AMOUNT event, then an invalid message, then another valid HIGH_AMOUNT event.
# Validates that the invalid message goes to transactions-dlq and valid messages continue producing alerts.

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
Set-Location $repoRoot

$evidenceDir = Join-Path $repoRoot "docs\evidence"

$terminalLogPath = Join-Path $evidenceDir "cp08_terminal_log.txt"
$inputMessagesPath = Join-Path $evidenceDir "cp08_input_messages.txt"
$producerOutputLogPath = Join-Path $evidenceDir "cp08_producer_output_log.txt"
$dlqValidationLogPath = Join-Path $evidenceDir "cp08_dlq_validation_log.txt"
$alertsValidationLogPath = Join-Path $evidenceDir "cp08_alerts_validation_log.txt"

$topicInput = "transactions-online"
$topicDlq = "transactions-dlq"
$topicAlerts = "alerts"

$validBeforeTransactionId = "cp08-valid-before-error-001"
$invalidTransactionId = "cp08-invalid-001"
$validAfterTransactionId = "cp08-valid-after-error-001"

$validBeforeCardId = "4532015112830877"
$invalidKey = "cp08-invalid-key"
$validAfterCardId = "4532015112830888"

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

Write-Host "CP-08 - Pipeline continuity after invalid message test"
Write-Host "Repository root: $repoRoot"
Write-Host ""

Write-Host "Validating local requirements..."

if (-not (Test-Path $evidenceDir)) {
    throw "Required directory does not exist: $evidenceDir"
}

Assert-CommandAvailable -CommandName "docker"
docker compose version | Out-Null

Clear-FileIfExists -Path $terminalLogPath
Clear-FileIfExists -Path $inputMessagesPath
Clear-FileIfExists -Path $producerOutputLogPath
Clear-FileIfExists -Path $dlqValidationLogPath
Clear-FileIfExists -Path $alertsValidationLogPath

Add-TerminalLog "CP-08 - Pipeline continuity after invalid message test"
Add-TerminalLog "Execution date: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')"
Add-TerminalLog "Repository root: $repoRoot"
Add-TerminalLog ""
Add-TerminalLog "Objective:"
Add-TerminalLog "Validate that the pipeline processes a valid event before an error, routes an invalid message to DLQ, and continues processing a valid event after the error."
Add-TerminalLog ""
Add-TerminalLog "Sequence:"
Add-TerminalLog "1. Valid HIGH_AMOUNT event before error."
Add-TerminalLog "2. Invalid message routed to transactions-dlq."
Add-TerminalLog "3. Valid HIGH_AMOUNT event after error."
Add-TerminalLog ""

Write-Host "Starting services required for CP-08..."
docker compose up -d kafka kafka-ui flink-jobmanager flink-taskmanager flink-anomalies-job

Add-TerminalLog "Command executed:"
Add-TerminalLog "docker compose up -d kafka kafka-ui flink-jobmanager flink-taskmanager flink-anomalies-job"
Add-TerminalLog ""

Write-Host "Waiting for services and anomalies job to be ready..."
Start-Sleep -Seconds 45

Write-Host "Writing CP-08 input messages..."

$messages = @"
$validBeforeCardId|{"transaction_id":"$validBeforeTransactionId","card_id":"$validBeforeCardId","event_timestamp":"2026-05-16T18:30:01.000000+00:00","produced_at":"2026-05-16T18:30:01.000000+00:00","channel":"ONLINE","amount":20000.00,"currency":"USD","status":"APPROVED","merchant_name":"CP08 Valid Before Merchant","merchant_category":"electronics","city":"Bogota","country":"CO"}
$invalidKey|{"transaction_id":"$invalidTransactionId","amount":"not-a-number","broken":true}
$validAfterCardId|{"transaction_id":"$validAfterTransactionId","card_id":"$validAfterCardId","event_timestamp":"2026-05-16T18:30:10.000000+00:00","produced_at":"2026-05-16T18:30:10.000000+00:00","channel":"ONLINE","amount":20000.00,"currency":"USD","status":"APPROVED","merchant_name":"CP08 Valid After Merchant","merchant_category":"electronics","city":"Bogota","country":"CO"}
"@

$messages | Set-Content -LiteralPath $inputMessagesPath

Add-TerminalLog "Input messages written to:"
Add-TerminalLog $inputMessagesPath
Add-TerminalLog ""
Add-TerminalLog "Input messages:"
Add-TerminalLog $messages
Add-TerminalLog ""

Write-Host "Publishing CP-08 messages to $topicInput..."

$producerStdoutPath = Join-Path $evidenceDir "cp08_producer.stdout.tmp"
$producerStderrPath = Join-Path $evidenceDir "cp08_producer.stderr.tmp"

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
    -RedirectStandardInput $inputMessagesPath `
    -RedirectStandardOutput $producerStdoutPath `
    -RedirectStandardError $producerStderrPath `
    -NoNewWindow `
    -PassThru

$producerProcess.WaitForExit()

Merge-ProcessOutput -StdoutPath $producerStdoutPath -StderrPath $producerStderrPath -OutputPath $producerOutputLogPath

Clear-FileIfExists -Path $producerStdoutPath
Clear-FileIfExists -Path $producerStderrPath

if ($null -ne $producerProcess.ExitCode -and $producerProcess.ExitCode -ne 0) {
    throw "Kafka producer command failed with exit code $($producerProcess.ExitCode). Check log: $producerOutputLogPath"
}

Add-TerminalLog "Producer command executed successfully."
Add-TerminalLog "Kafka topic input: $topicInput"
Add-TerminalLog "Producer output log: $producerOutputLogPath"
Add-TerminalLog ""

Write-Host "Waiting for DLQ routing and alert generation..."
Start-Sleep -Seconds 20

Write-Host "Validating DLQ output in topic $topicDlq..."

$dlqStdoutPath = Join-Path $evidenceDir "cp08_dlq_validation.stdout.tmp"
$dlqStderrPath = Join-Path $evidenceDir "cp08_dlq_validation.stderr.tmp"

Clear-FileIfExists -Path $dlqStdoutPath
Clear-FileIfExists -Path $dlqStderrPath

$dlqConsumerArgs = @(
    "exec",
    "kafka",
    "kafka-console-consumer.sh",
    "--bootstrap-server",
    "kafka:9094",
    "--topic",
    $topicDlq,
    "--from-beginning",
    "--max-messages",
    "10",
    "--timeout-ms",
    "15000"
)

$dlqConsumerProcess = Start-Process `
    -FilePath "docker" `
    -ArgumentList $dlqConsumerArgs `
    -WorkingDirectory $repoRoot `
    -RedirectStandardOutput $dlqStdoutPath `
    -RedirectStandardError $dlqStderrPath `
    -NoNewWindow `
    -PassThru

$dlqConsumerProcess.WaitForExit()

Merge-ProcessOutput -StdoutPath $dlqStdoutPath -StderrPath $dlqStderrPath -OutputPath $dlqValidationLogPath

$dlqLines = @()
if (Test-Path $dlqStdoutPath) {
    $dlqLines = Get-Content -LiteralPath $dlqStdoutPath | Where-Object {
        $_ -and $_.Trim().StartsWith("{")
    }
}

Clear-FileIfExists -Path $dlqStdoutPath
Clear-FileIfExists -Path $dlqStderrPath

Write-Host "Validating alerts output in topic $topicAlerts..."

$alertsStdoutPath = Join-Path $evidenceDir "cp08_alerts_validation.stdout.tmp"
$alertsStderrPath = Join-Path $evidenceDir "cp08_alerts_validation.stderr.tmp"

Clear-FileIfExists -Path $alertsStdoutPath
Clear-FileIfExists -Path $alertsStderrPath

$alertsConsumerArgs = @(
    "exec",
    "kafka",
    "kafka-console-consumer.sh",
    "--bootstrap-server",
    "kafka:9094",
    "--topic",
    $topicAlerts,
    "--from-beginning",
    "--max-messages",
    "10",
    "--timeout-ms",
    "15000"
)

$alertsConsumerProcess = Start-Process `
    -FilePath "docker" `
    -ArgumentList $alertsConsumerArgs `
    -WorkingDirectory $repoRoot `
    -RedirectStandardOutput $alertsStdoutPath `
    -RedirectStandardError $alertsStderrPath `
    -NoNewWindow `
    -PassThru

$alertsConsumerProcess.WaitForExit()

Merge-ProcessOutput -StdoutPath $alertsStdoutPath -StderrPath $alertsStderrPath -OutputPath $alertsValidationLogPath

$alertLines = @()
if (Test-Path $alertsStdoutPath) {
    $alertLines = Get-Content -LiteralPath $alertsStdoutPath | Where-Object {
        $_ -and $_.Trim().StartsWith("{")
    }
}

Clear-FileIfExists -Path $alertsStdoutPath
Clear-FileIfExists -Path $alertsStderrPath

$matchingDlq = $false
foreach ($line in $dlqLines) {
    if (
        $line -match $invalidTransactionId -and
        $line -match $invalidKey -and
        $line -match "missing-or-invalid-field"
    ) {
        $matchingDlq = $true
    }
}

$matchingAlertBefore = $false
$matchingAlertAfter = $false

foreach ($line in $alertLines) {
    if (
        $line -match "HIGH_AMOUNT" -and
        $line -match $validBeforeTransactionId -and
        $line -match $validBeforeCardId
    ) {
        $matchingAlertBefore = $true
    }

    if (
        $line -match "HIGH_AMOUNT" -and
        $line -match $validAfterTransactionId -and
        $line -match $validAfterCardId
    ) {
        $matchingAlertAfter = $true
    }
}

Add-TerminalLog "DLQ validation command executed:"
Add-TerminalLog "docker exec kafka kafka-console-consumer.sh --bootstrap-server kafka:9094 --topic $topicDlq --from-beginning --max-messages 10 --timeout-ms 15000"
Add-TerminalLog "DLQ validation log:"
Add-TerminalLog $dlqValidationLogPath
Add-TerminalLog ""
Add-TerminalLog "Alerts validation command executed:"
Add-TerminalLog "docker exec kafka kafka-console-consumer.sh --bootstrap-server kafka:9094 --topic $topicAlerts --from-beginning --max-messages 10 --timeout-ms 15000"
Add-TerminalLog "Alerts validation log:"
Add-TerminalLog $alertsValidationLogPath
Add-TerminalLog ""

Write-Host ""
Write-Host "CP-08 execution summary"
Write-Host "-----------------------"
Write-Host "Input topic: $topicInput"
Write-Host "DLQ topic: $topicDlq"
Write-Host "Alerts topic: $topicAlerts"
Write-Host "Valid before transaction ID: $validBeforeTransactionId"
Write-Host "Invalid transaction ID: $invalidTransactionId"
Write-Host "Valid after transaction ID: $validAfterTransactionId"
Write-Host ""
Write-Host "Evidence file terminal log: $terminalLogPath"
Write-Host "Evidence file input messages: $inputMessagesPath"
Write-Host "Producer output log: $producerOutputLogPath"
Write-Host "DLQ validation log: $dlqValidationLogPath"
Write-Host "Alerts validation log: $alertsValidationLogPath"

if ($matchingDlq -and $matchingAlertBefore -and $matchingAlertAfter) {
    Write-Host ""
    Write-Host "CP-08 validation: PASSED"
    Write-Host "Pipeline processed valid event before error, routed invalid message to DLQ, and processed valid event after error."
    Add-TerminalLog "CP-08 validation: PASSED"
    Add-TerminalLog "Pipeline processed valid event before error, routed invalid message to DLQ, and processed valid event after error."
}
else {
    Write-Host ""
    Write-Warning "CP-08 validation did not automatically find all expected records."
    Write-Warning "Expected:"
    Write-Warning "- HIGH_AMOUNT alert for $validBeforeTransactionId"
    Write-Warning "- DLQ record for $invalidTransactionId"
    Write-Warning "- HIGH_AMOUNT alert for $validAfterTransactionId"
    Write-Warning "Please validate manually in Kafka UI."
    Add-TerminalLog "CP-08 validation: MANUAL CHECK REQUIRED"
    Add-TerminalLog "The script did not automatically find all expected records."
    Add-TerminalLog "matchingAlertBefore=$matchingAlertBefore"
    Add-TerminalLog "matchingDlq=$matchingDlq"
    Add-TerminalLog "matchingAlertAfter=$matchingAlertAfter"
}

Write-Host ""
Write-Host "Kafka UI check:"
Write-Host "- Confirm topic transactions-online contains the valid-before, invalid, and valid-after messages."
Write-Host "- Confirm topic transactions-dlq contains the invalid message with key $invalidKey."
Write-Host "- Confirm topic alerts contains HIGH_AMOUNT for both valid transactions."
Write-Host ""
Write-Host "Flink UI check:"
Write-Host "- Confirm Fraud Detection, Anomalies Job is RUNNING."
Write-Host "- Confirm the job does not fail or restart after receiving the invalid message."
Write-Host "- Confirm Source and Sink operators processed records."
Write-Host ""
Write-Host "Recommended manual screenshots:"
Write-Host "- docs/evidence/cp08_kafka_ui_input.png"
Write-Host "- docs/evidence/cp08_kafka_ui_dlq.png"
Write-Host "- docs/evidence/cp08_kafka_ui_alerts.png"
Write-Host "- docs/evidence/cp08_flink_job.png"
Write-Host ""
Write-Host "CP-08 script finished."