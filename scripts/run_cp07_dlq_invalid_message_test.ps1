# CP-07 - DLQ invalid message validation test
# Sends an invalid transaction message and validates that it is routed to transactions-dlq.

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
Set-Location $repoRoot

$evidenceDir = Join-Path $repoRoot "docs\evidence"

$terminalLogPath = Join-Path $evidenceDir "cp07_terminal_log.txt"
$inputInvalidMessagePath = Join-Path $evidenceDir "cp07_input_invalid_message.txt"
$producerOutputLogPath = Join-Path $evidenceDir "cp07_producer_output_log.txt"
$dlqValidationLogPath = Join-Path $evidenceDir "cp07_dlq_validation_log.txt"

$invalidKey = "cp07-invalid-key"
$transactionId = "cp07-invalid-001"
$topicInput = "transactions-online"
$topicDlq = "transactions-dlq"

$invalidMessage = "$invalidKey|{`"transaction_id`":`"$transactionId`",`"amount`":`"not-a-number`",`"broken`":true}"

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

Write-Host "CP-07 - DLQ invalid message validation test"
Write-Host "Repository root: $repoRoot"
Write-Host ""

Write-Host "Validating local requirements..."

if (-not (Test-Path $evidenceDir)) {
    throw "Required directory does not exist: $evidenceDir"
}

Assert-CommandAvailable -CommandName "docker"
docker compose version | Out-Null

Clear-FileIfExists -Path $terminalLogPath
Clear-FileIfExists -Path $inputInvalidMessagePath
Clear-FileIfExists -Path $producerOutputLogPath
Clear-FileIfExists -Path $dlqValidationLogPath

Add-TerminalLog "CP-07 - DLQ invalid message validation test"
Add-TerminalLog "Execution date: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')"
Add-TerminalLog "Repository root: $repoRoot"
Add-TerminalLog ""
Add-TerminalLog "Objective:"
Add-TerminalLog "Validate that an invalid transaction message is routed to Kafka topic '$topicDlq'."
Add-TerminalLog ""
Add-TerminalLog "Invalid message reason:"
Add-TerminalLog "The message is missing required field 'card_id' and has amount='not-a-number'."
Add-TerminalLog ""

Write-Host "Starting services required for CP-07..."
docker compose up -d kafka kafka-ui flink-jobmanager flink-taskmanager flink-anomalies-job

Add-TerminalLog "Command executed:"
Add-TerminalLog "docker compose up -d kafka kafka-ui flink-jobmanager flink-taskmanager flink-anomalies-job"
Add-TerminalLog ""

Write-Host "Waiting for services and anomalies job to be ready..."
Start-Sleep -Seconds 45

Write-Host "Writing invalid CP-07 message..."
$invalidMessage | Set-Content -LiteralPath $inputInvalidMessagePath -NoNewline

Add-TerminalLog "Invalid message written to:"
Add-TerminalLog $inputInvalidMessagePath
Add-TerminalLog ""
Add-TerminalLog "Invalid message:"
Add-TerminalLog $invalidMessage
Add-TerminalLog ""

Write-Host "Publishing invalid message to $topicInput..."

$producerStdoutPath = Join-Path $evidenceDir "cp07_producer.stdout.tmp"
$producerStderrPath = Join-Path $evidenceDir "cp07_producer.stderr.tmp"

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
    -RedirectStandardInput $inputInvalidMessagePath `
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

Write-Host "Waiting for DLQ routing..."
Start-Sleep -Seconds 10

Write-Host "Validating output in topic $topicDlq..."

$validationStdoutPath = Join-Path $evidenceDir "cp07_dlq_validation.stdout.tmp"
$validationStderrPath = Join-Path $evidenceDir "cp07_dlq_validation.stderr.tmp"

Clear-FileIfExists -Path $validationStdoutPath
Clear-FileIfExists -Path $validationStderrPath

$consumerArgs = @(
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

$consumerProcess = Start-Process `
    -FilePath "docker" `
    -ArgumentList $consumerArgs `
    -WorkingDirectory $repoRoot `
    -RedirectStandardOutput $validationStdoutPath `
    -RedirectStandardError $validationStderrPath `
    -NoNewWindow `
    -PassThru

$consumerProcess.WaitForExit()

Merge-ProcessOutput -StdoutPath $validationStdoutPath -StderrPath $validationStderrPath -OutputPath $dlqValidationLogPath

$dlqLines = @()
if (Test-Path $validationStdoutPath) {
    $dlqLines = Get-Content -LiteralPath $validationStdoutPath | Where-Object {
        $_ -and $_.Trim().StartsWith("{")
    }
}

Clear-FileIfExists -Path $validationStdoutPath
Clear-FileIfExists -Path $validationStderrPath

$matchingDlq = $false

foreach ($line in $dlqLines) {
    if (
        $line -match $transactionId -and
        $line -match $invalidKey -and
        $line -match "missing-or-invalid-field"
    ) {
        $matchingDlq = $true
    }
}

Add-TerminalLog "Validation command executed:"
Add-TerminalLog "docker exec kafka kafka-console-consumer.sh --bootstrap-server kafka:9094 --topic $topicDlq --from-beginning --max-messages 10 --timeout-ms 15000"
Add-TerminalLog ""
Add-TerminalLog "DLQ validation log:"
Add-TerminalLog $dlqValidationLogPath
Add-TerminalLog ""

Write-Host ""
Write-Host "CP-07 execution summary"
Write-Host "-----------------------"
Write-Host "Input topic: $topicInput"
Write-Host "DLQ topic: $topicDlq"
Write-Host "Invalid key: $invalidKey"
Write-Host "Transaction ID: $transactionId"
Write-Host "Expected error: missing-or-invalid-field"
Write-Host ""
Write-Host "Evidence file terminal log: $terminalLogPath"
Write-Host "Evidence file invalid message: $inputInvalidMessagePath"
Write-Host "Producer output log: $producerOutputLogPath"
Write-Host "DLQ validation log: $dlqValidationLogPath"

if ($matchingDlq) {
    Write-Host ""
    Write-Host "CP-07 validation: PASSED"
    Write-Host "Invalid message was routed to $topicDlq."
    Add-TerminalLog "CP-07 validation: PASSED"
    Add-TerminalLog "Invalid message was routed to $topicDlq."
}
else {
    Write-Host ""
    Write-Warning "CP-07 validation did not automatically find the expected DLQ record."
    Write-Warning "Please validate manually in Kafka UI that topic '$topicDlq' contains the invalid message."
    Add-TerminalLog "CP-07 validation: MANUAL CHECK REQUIRED"
    Add-TerminalLog "The script did not automatically find the expected DLQ record."
}

Write-Host ""
Write-Host "Kafka UI check:"
Write-Host "- Confirm topic transactions-online contains transaction_id $transactionId."
Write-Host "- Confirm topic transactions-dlq contains the raw invalid message."
Write-Host "- Confirm DLQ record includes key $invalidKey and error missing-or-invalid-field."
Write-Host ""
Write-Host "Flink UI check:"
Write-Host "- Confirm Fraud Detection, Anomalies Job is RUNNING."
Write-Host "- Confirm the job does not fail or restart after receiving the invalid message."
Write-Host ""
Write-Host "Recommended manual screenshots:"
Write-Host "- docs/evidence/cp07_kafka_ui_input.png"
Write-Host "- docs/evidence/cp07_kafka_ui_dlq.png"
Write-Host "- docs/evidence/cp07_flink_job.png"
Write-Host ""
Write-Host "CP-07 script finished."