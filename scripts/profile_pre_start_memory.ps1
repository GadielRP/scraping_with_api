param(
    [ValidateSet("baseline", "optimized")]
    [string]$Mode = "optimized",

    [string]$MemoryLimit = "1g",

    [string]$EnvFile = ".env.prod",

    [string]$ContainerName = "",

    [switch]$Build
)

$ErrorActionPreference = "Stop"

if (-not $ContainerName) {
    $timestamp = Get-Date -Format "yyyyMMdd-HHmmss"
    $ContainerName = "sofascore-prestart-memory-$Mode-$timestamp"
}

$composeArgs = @(
    "compose",
    "-f", "compose.yaml",
    "-f", "compose.memory-test.yaml"
)

$env:APP_MEMORY_TEST_LIMIT = $MemoryLimit
$env:APP_ENV_FILE = $EnvFile

if (-not (Test-Path -LiteralPath $EnvFile)) {
    throw "Environment file '$EnvFile' does not exist."
}

if ($Build) {
    & docker @composeArgs build app
    if ($LASTEXITCODE -ne 0) {
        throw "Docker image build failed with exit code $LASTEXITCODE"
    }
}

$existingContainer = & docker ps -a --filter "name=^/$ContainerName$" --format "{{.Names}}"
if ($LASTEXITCODE -ne 0) {
    throw "Could not query Docker containers"
}
if ($existingContainer) {
    throw "Container '$ContainerName' already exists. Choose a unique -ContainerName."
}

if ($Mode -eq "baseline") {
    $workerArgs = @(
        "-e", "ALERT_PIPELINE_WORKERS=4",
        "-e", "PILLAR_PIPELINE_WORKERS=4",
        "-e", "MATCHUP_TEAM_HISTORY_WORKERS=2",
        "-e", "MATCHUP_H2H_MAX_EVENTS=100000"
    )
} else {
    $workerArgs = @(
        "-e", "ALERT_PIPELINE_WORKERS=1",
        "-e", "PILLAR_PIPELINE_WORKERS=1",
        "-e", "MATCHUP_TEAM_HISTORY_WORKERS=1",
        "-e", "MATCHUP_H2H_MAX_EVENTS=200"
    )
}

$isolationArgs = @(
    # Keep simulated +30 minute timestamps stable and isolate the H2H/history
    # alert path from odds ingestion and timestamp-correction side effects.
    "-e", "ENABLE_TIMESTAMP_CORRECTION=false",
    "-e", "ENABLE_ODDS_EXTRACTION=false",
    "-e", "ENABLE_ODDSPAPI_PRE_START_ODDS=false",
    "-e", "ENABLE_LEGACY_ALERT_PIPELINE=true",
    "-e", "ENABLE_PILLAR_PIPELINE=false"
)

Write-Host "Running $Mode pre-start profile in container '$ContainerName' with limit $MemoryLimit and no swap."
Write-Warning "Use a disposable/restored database snapshot. The real pre-start command persists data."

$startedAt = Get-Date
& docker @composeArgs run --name $ContainerName @workerArgs @isolationArgs app
$runExitCode = $LASTEXITCODE
$duration = (Get-Date) - $startedAt

$stateJson = & docker inspect $ContainerName --format "{{json .State}}"
if ($LASTEXITCODE -ne 0) {
    throw "Could not inspect profiling container '$ContainerName'"
}
$state = $stateJson | ConvertFrom-Json

$memoryLog = & docker logs $ContainerName 2>&1 |
    Select-String -Pattern "Operation (started|finished).*pre_start_check|Previous process ended without a clean shutdown"

Write-Host ""
Write-Host "Result"
Write-Host "  mode: $Mode"
Write-Host "  container: $ContainerName"
Write-Host "  command_exit_code: $runExitCode"
Write-Host "  container_exit_code: $($state.ExitCode)"
Write-Host "  oom_killed: $($state.OOMKilled)"
Write-Host "  duration_seconds: $([math]::Round($duration.TotalSeconds, 1))"
Write-Host "  memory evidence:"
if ($memoryLog) {
    $memoryLog | ForEach-Object { Write-Host "    $($_.Line)" }
} else {
    Write-Host "    No completed operation line was found. Inspect logs/runtime_state.json and docker logs."
}

Write-Host ""
Write-Host "Keep this container until its result is recorded:"
Write-Host "  docker logs $ContainerName"
Write-Host "  docker inspect $ContainerName --format '{{.State.OOMKilled}} {{.State.ExitCode}}'"

if ($runExitCode -ne 0 -or $state.OOMKilled) {
    exit 1
}
