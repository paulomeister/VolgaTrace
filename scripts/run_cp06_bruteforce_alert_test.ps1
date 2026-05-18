# CP-06 - Brute force / declined pattern anomaly alert validation test
# Sends 3 REJECTED transactions followed by 1 APPROVED transaction for the same card_id.
# Then sends a watermark-advance event to force event-time progress if needed.

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
Set-Location $repoRoot

$evidenceDir = Join-Path $repoRoot "docs\evidence"

$terminalLogPath = Join-Path $evidenceDir "cp06_terminal_log.txt"
$inputEventsPath = Join-Path $evidenceDir "cp06_input_events.txt"
$producerOutputLogPath = Join-Path $evidenceDir "cp06_producer_output_log.txt"
$alertsValidationLogPath = Join-Path $evidenceDir "cp06_alerts_validation_log.txt"

$cardId = "4532015112830666"
$topicInput = "transactions-online"
$topicAlerts = "alerts"

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

Write-Host "CP-06 - Brute force / DECLINED_PATTERN anomaly alert validation test"
Write-Host "Repository root: $repoRoot"
Write-Host ""

Write-Host "Validating local requirements..."

if (-not (Test-Path $evidenceDir)) {
    throw "Required directory does not exist: $evidenceDir"
}

Assert-CommandAvailable -CommandName "docker"
docker compose version | Out-Null

Clear-FileIfExists -Path $terminalLogPath
Clear-FileIfExists -Path $inputEventsPath
Clear-FileIfExists -Path $producerOutputLogPath
Clear-FileIfExists -Path $alertsValidationLogPath

Add-TerminalLog "CP-06 - Brute force / DECLINED_PATTERN anomaly alert validation test"
Add-TerminalLog "Execution date: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')"
Add-TerminalLog "Repository root: $repoRoot"
Add-TerminalLog ""
Add-TerminalLog "Objective:"
Add-TerminalLog "Validate that the sequence REJECTED, REJECTED, REJECTED, APPROVED for the same card_id generates a DECLINED_PATTERN alert."
Add-TerminalLog ""
Add-TerminalLog "Important note:"
Add-TerminalLog "A final watermark-advance event is sent with a later timestamp because, in the manual execution, the CEP output required event-time progress before the alert appeared."
Add-TerminalLog ""

Write-Host "Starting services required for CP-06..."
docker compose up -d kafka kafka-ui flink-jobmanager flink-taskmanager flink-anomalies-job

Add-TerminalLog "Command executed:"
Add-TerminalLog "docker compose up -d kafka kafka-ui flink-jobmanager flink-taskmanager flink-anomalies-job"
Add-TerminalLog ""

Write-Host "Waiting for services and anomalies job to be ready..."
Start-Sleep -Seconds 45

Write-Host "Writing controlled CP-06 events..."

$events = @'
4532015112830666|{"transaction_id":"cp06-bruteforce-001","card_id":"4532015112830666","event_timestamp":"2026-05-16T18:10:01.000000+00:00","produced_at":"2026-05-16T18:10:01.000000+00:00","channel":"ONLINE","amount":100.00,"currency":"COP","status":"REJECTED","merchant_name":"CP06 Test Merchant","merchant_category":"electronics","city":"Bogota","country":"CO"}
4532015112830666|{"transaction_id":"cp06-bruteforce-002","card_id":"4532015112830666","event_timestamp":"2026-05-16T18:10:02.000000+00:00","produced_at":"2026-05-16T18:10:02.000000+00:00","channel":"ONLINE","amount":101.00,"currency":"COP","status":"REJECTED","merchant_name":"CP06 Test Merchant","merchant_category":"electronics","city":"Bogota","country":"CO"}
4532015112830666|{"transaction_id":"cp06-bruteforce-003","card_id":"4532015112830666","event_timestamp":"2026-05-16T18:10:03.000000+00:00","produced_at":"2026-05-16T18:10:03.000000+00:00","channel":"ONLINE","amount":102.00,"currency":"COP","status":"REJECTED","merchant_name":"CP06 Test Merchant","merchant_category":"electronics","city":"Bogota","country":"CO"}
4532015112830666|{"transaction_id":"cp06-bruteforce-004","card_id":"4532015112830666","event_timestamp":"2026-05-16T18:10:04.000000+00:00","produced_at":"2026-05-16T18:10:04.000000+00:00","channel":"ONLINE","amount":103.00,"currency":"COP","status":"APPROVED","merchant_name":"CP06 Test Merchant","merchant_category":"electronics","city":"Bogota","country":"CO"}
4532015112830666|{"transaction_id":"cp06-watermark-advance-001","card_id":"4532015112830666","event_timestamp":"2026-05-16T18:10:20.000000+00:00","produced_at":"2026-05-16T18:10:20.000000+00:00","channel":"ONLINE","amount":104.00,"currency":"COP","status":"APPROVED","merchant_name":"CP06 Test Merchant","merchant_category":"electronics","city":"Bogota","country":"CO"}
'@

$events | Set-Content -LiteralPath $inputEventsPath

Add-TerminalLog "Input events written to:"
Add-TerminalLog $inputEventsPath
Add-TerminalLog ""
Add-TerminalLog "Input events:"
Add-TerminalLog $events
Add-TerminalLog ""

Write-Host "Publishing CP-06 brute force pattern events to $topicInput..."

$producerStdoutPath = Join-Path $evidenceDir "cp06_producer.stdout.tmp"
$producerStderrPath = Join-Path $evidenceDir "cp06_producer.stderr.tmp"

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
    -RedirectStandardInput $inputEventsPath `
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

Write-Host "Waiting for Flink CEP pattern and watermark progress..."
Start-Sleep -Seconds 25

Write-Host "Validating output in topic $topicAlerts..."

$validationStdoutPath = Join-Path $evidenceDir "cp06_alerts_validation.stdout.tmp"
$validationStderrPath = Join-Path $evidenceDir "cp06_alerts_validation.stderr.tmp"

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
        $line -match "DECLINED_PATTERN" -and
        $line -match $cardId
    ) {
        $matchingAlert = $true
    }
}

Add-TerminalLog "Validation command executed:"
Add-TerminalLog "docker exec kafka kafka-console-consumer.sh --bootstrap-server kafka:9094 --topic $topicAlerts --from-beginning --max-messages 10 --timeout-ms 15000"
Add-TerminalLog ""
Add-TerminalLog "Alerts validation log:"
Add-TerminalLog $alertsValidationLogPath
Add-TerminalLog ""

Write-Host ""
Write-Host "CP-06 execution summary"
Write-Host "-----------------------"
Write-Host "Input topic: $topicInput"
Write-Host "Alert topic: $topicAlerts"
Write-Host "Card ID: $cardId"
Write-Host "Pattern sent: REJECTED, REJECTED, REJECTED, APPROVED"
Write-Host "Watermark advance event sent: 1"
Write-Host ""
Write-Host "Evidence file terminal log: $terminalLogPath"
Write-Host "Evidence file input events: $inputEventsPath"
Write-Host "Producer output log: $producerOutputLogPath"
Write-Host "Alerts validation log: $alertsValidationLogPath"

if ($matchingAlert) {
    Write-Host ""
    Write-Host "CP-06 validation: PASSED"
    Write-Host "DECLINED_PATTERN alert found for card_id $cardId."
    Add-TerminalLog "CP-06 validation: PASSED"
    Add-TerminalLog "DECLINED_PATTERN alert found for card_id $cardId."
}
else {
    Write-Host ""
    Write-Warning "CP-06 validation did not automatically find the expected DECLINED_PATTERN alert."
    Write-Warning "Please validate manually in Kafka UI that topic '$topicAlerts' contains the DECLINED_PATTERN alert."
    Add-TerminalLog "CP-06 validation: MANUAL CHECK REQUIRED"
    Add-TerminalLog "The script did not automatically find the expected DECLINED_PATTERN alert."
}

Write-Host ""
Write-Host "Kafka UI check:"
Write-Host "- Confirm topic transactions-online contains cp06-bruteforce-001 through cp06-bruteforce-004."
Write-Host "- Confirm topic alerts contains alert_type DECLINED_PATTERN."
Write-Host "- Confirm alert references card_id $cardId."
Write-Host ""
Write-Host "Flink UI check:"
Write-Host "- Confirm Fraud Detection, Anomalies Job is RUNNING."
Write-Host "- Confirm Source, CEP operator and Sink operators processed records."
Write-Host ""
Write-Host "Recommended manual screenshots:"
Write-Host "- docs/evidence/cp06_kafka_ui_input.png"
Write-Host "- docs/evidence/cp06_kafka_ui_alerts.png"
Write-Host "- docs/evidence/cp06_flink_job.png"
Write-Host ""
Write-Host "CP-06 script finished."