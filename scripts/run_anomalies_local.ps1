param(
    [string]$BootstrapServers = "localhost:9092",
    [switch]$Build,
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$jobDir = Join-Path $repoRoot "flink\anomalies-job"
$jarPath = Join-Path $jobDir "target\anomalies-job-1.0-SNAPSHOT.jar"

if ($Build) {
    Write-Host "Building anomalies-job with Maven profile 'local'..."
    Push-Location $jobDir
    try {
        & mvn -Plocal -DskipTests package
    } finally {
        Pop-Location
    }
}

if (-not (Test-Path $jarPath)) {
    throw "JAR not found at: $jarPath. Run with -Build or build manually."
}

$javaArgs = @(
    "-jar",
    $jarPath,
    "--bootstrap.servers",
    $BootstrapServers
)

if ($DryRun) {
    Write-Host "DryRun: java $($javaArgs -join ' ')"
    exit 0
}

Write-Host "Starting anomalies job..."
& java @javaArgs

