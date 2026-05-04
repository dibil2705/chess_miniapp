param(
    [string]$NgrokPath = "D:\ngrok.exe",
    [int]$BackendPort = 12315,
    [string]$EnvPath = ".env"
)

$ErrorActionPreference = "Stop"

if (-not (Test-Path -LiteralPath $NgrokPath)) {
    throw "ngrok not found: $NgrokPath"
}

$repoRoot = $PSScriptRoot
$envFile = Join-Path $repoRoot $EnvPath
if (-not (Test-Path -LiteralPath $envFile)) {
    throw ".env not found: $envFile"
}

$pythonExe = (Get-Command python -ErrorAction Stop).Source

$ngrokOutLog = Join-Path $repoRoot "ngrok_backend.out.log"
$ngrokErrLog = Join-Path $repoRoot "ngrok_backend.err.log"
$mainOutLog = Join-Path $repoRoot "backend_main.out.log"
$mainErrLog = Join-Path $repoRoot "backend_main.err.log"

foreach ($log in @($ngrokOutLog, $ngrokErrLog, $mainOutLog, $mainErrLog)) {
    if (Test-Path -LiteralPath $log) {
        Remove-Item -LiteralPath $log -Force
    }
}

# Start ngrok for the full backend first, so we can inject fresh MINI_APP_URL into .env.
$ngrokProc = Start-Process -FilePath $NgrokPath `
    -ArgumentList @("http", "127.0.0.1:$BackendPort") `
    -WorkingDirectory $repoRoot `
    -WindowStyle Hidden `
    -PassThru `
    -RedirectStandardOutput $ngrokOutLog `
    -RedirectStandardError $ngrokErrLog

$publicUrl = ""
for ($i = 0; $i -lt 40; $i++) {
    Start-Sleep -Milliseconds 500
    if ($ngrokProc.HasExited) {
        throw "ngrok exited early. See $ngrokErrLog"
    }
    try {
        $tunnels = Invoke-RestMethod -Uri "http://127.0.0.1:4040/api/tunnels" -TimeoutSec 2
        $httpsTunnel = $tunnels.tunnels | Where-Object { $_.public_url -like "https://*" } | Select-Object -First 1
        if ($httpsTunnel) {
            $publicUrl = [string]$httpsTunnel.public_url
            break
        }
    }
    catch {
        # keep waiting
    }
}

if (-not $publicUrl) {
    throw "Could not get ngrok URL from http://127.0.0.1:4040/api/tunnels"
}

$miniAppUrl = "$publicUrl/index.html"

# Ensure main.py and monitor use the same local backend port.
$env:PORT = [string]$BackendPort
$env:TELEGRAM_BACKEND_PORT = [string]$BackendPort
$env:MONITOR_HEALTH_URL = "http://127.0.0.1:$BackendPort/health"

$mainProc = Start-Process -FilePath $pythonExe `
    -ArgumentList @("main.py") `
    -WorkingDirectory $repoRoot `
    -WindowStyle Hidden `
    -PassThru `
    -RedirectStandardOutput $mainOutLog `
    -RedirectStandardError $mainErrLog

$healthOk = $false
for ($i = 0; $i -lt 40; $i++) {
    Start-Sleep -Milliseconds 500
    if ($mainProc.HasExited) {
        throw "main.py exited early. See $mainErrLog"
    }
    try {
        $resp = Invoke-RestMethod -Uri "http://127.0.0.1:$BackendPort/health" -TimeoutSec 2
        if ($resp.ok -eq $true) {
            $healthOk = $true
            break
        }
    }
    catch {
        # keep waiting
    }
}

if (-not $healthOk) {
    throw "Backend health-check failed on http://127.0.0.1:$BackendPort/health"
}

Write-Host ""
Write-Host "Full backend mode is running."
Write-Host "Public Mini App URL (direct): $miniAppUrl"
Write-Host "Telegram /start Mini App URL: https://t.me/chess_every_day_bot/app?startapp=test&mode=fullscreen"
Write-Host "Public analyze page: $publicUrl/analysis.html"
Write-Host "Local health: http://127.0.0.1:$BackendPort/health"
Write-Host ""
Write-Host "PIDs: main=$($mainProc.Id), ngrok=$($ngrokProc.Id)"
Write-Host "Logs: $mainOutLog, $mainErrLog, $ngrokOutLog, $ngrokErrLog"
Write-Host "Press Enter to stop."
[void][Console]::ReadLine()

foreach ($proc in @($mainProc, $ngrokProc)) {
    if ($proc -and -not $proc.HasExited) {
        try { Stop-Process -Id $proc.Id -Force } catch {}
    }
}
