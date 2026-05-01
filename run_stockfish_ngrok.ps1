param(
    [int]$LocalPort = 18080,
    [string]$LocalHost = "127.0.0.1",
    [string]$NgrokPath = "D:\ngrok.exe",
    [string]$NgrokDomain = "",
    [string]$SiteBaseUrl = ""
)

$ErrorActionPreference = "Stop"

if (-not (Test-Path -LiteralPath $NgrokPath)) {
    throw "ngrok not found: $NgrokPath"
}

$pythonCmd = Get-Command python -ErrorAction Stop
$pythonExe = $pythonCmd.Source
$repoRoot = $PSScriptRoot

$apiOutLog = Join-Path $repoRoot "stockfish_api.out.log"
$apiErrLog = Join-Path $repoRoot "stockfish_api.err.log"
$ngrokOutLog = Join-Path $repoRoot "ngrok.out.log"
$ngrokErrLog = Join-Path $repoRoot "ngrok.err.log"

foreach ($log in @($apiOutLog, $apiErrLog, $ngrokOutLog, $ngrokErrLog)) {
    if (Test-Path -LiteralPath $log) {
        Remove-Item -LiteralPath $log -Force
    }
}

$apiArgs = @(
    "-m", "uvicorn",
    "server_chess_wind.server:app",
    "--host", $LocalHost,
    "--port", "$LocalPort"
)

$apiProc = Start-Process -FilePath $pythonExe `
    -ArgumentList $apiArgs `
    -WorkingDirectory $repoRoot `
    -PassThru `
    -RedirectStandardOutput $apiOutLog `
    -RedirectStandardError $apiErrLog

$ngrokArgs = @("http", "$LocalHost`:$LocalPort")
if ($NgrokDomain) {
    $ngrokArgs += @("--domain", $NgrokDomain)
}

$ngrokProc = Start-Process -FilePath $NgrokPath `
    -ArgumentList $ngrokArgs `
    -WorkingDirectory $repoRoot `
    -PassThru `
    -RedirectStandardOutput $ngrokOutLog `
    -RedirectStandardError $ngrokErrLog

try {
    $healthOk = $false
    $healthError = ""
    for ($i = 0; $i -lt 30; $i++) {
        Start-Sleep -Milliseconds 500
        if ($apiProc.HasExited) {
            throw "Stockfish API exited early. See $apiErrLog"
        }
        try {
            $resp = Invoke-RestMethod -Uri "http://$LocalHost`:$LocalPort/health" -TimeoutSec 2
            if ($resp.ok -eq $true) {
                $healthOk = $true
                break
            }
            $healthError = [string]$resp.startup_error
        } catch {
            # keep waiting
        }
    }
    if (-not $healthOk) {
        if ($healthError) {
            throw "Stockfish API reports engine error: $healthError"
        }
        throw "Stockfish API health-check failed on http://$LocalHost`:$LocalPort/health"
    }

    $publicUrl = ""
    for ($i = 0; $i -lt 30; $i++) {
        Start-Sleep -Milliseconds 500
        if ($ngrokProc.HasExited) {
            throw "ngrok exited early. See $ngrokErrLog"
        }
        try {
            $tunnels = Invoke-RestMethod -Uri "http://127.0.0.1:4040/api/tunnels" -TimeoutSec 2
            $httpsTunnel = $tunnels.tunnels | Where-Object { $_.public_url -like "https://*" } | Select-Object -First 1
            if ($httpsTunnel) {
                $publicUrl = $httpsTunnel.public_url
                break
            }
        } catch {
            # keep waiting
        }
    }

    if (-not $publicUrl) {
        throw "Could not get ngrok public URL from http://127.0.0.1:4040/api/tunnels"
    }

    Write-Host ""
    Write-Host "Stockfish API is running."
    Write-Host "Local health: http://$LocalHost`:$LocalPort/health"
    Write-Host "ngrok public URL: $publicUrl"
    Write-Host "Set this on Wispbyte:"
    Write-Host "STOCKFISH_INTERNAL_BASE=$publicUrl"
    $encodedApiBase = [uri]::EscapeDataString($publicUrl)
    if ($SiteBaseUrl) {
        $trimmedSite = $SiteBaseUrl.TrimEnd("/")
        Write-Host "GitHub Pages links:"
        Write-Host "$trimmedSite/index.html?api_base=$encodedApiBase"
        Write-Host "$trimmedSite/analysis.html?api_base=$encodedApiBase"
    } else {
        Write-Host "For GitHub Pages add query param api_base=$encodedApiBase"
    }
    Write-Host ""
    Write-Host "API PID: $($apiProc.Id), ngrok PID: $($ngrokProc.Id)"
    Write-Host "Logs: $apiOutLog, $apiErrLog, $ngrokOutLog, $ngrokErrLog"
    Write-Host "Press Enter to stop both processes."
    [void][Console]::ReadLine()
}
finally {
    foreach ($proc in @($ngrokProc, $apiProc)) {
        if ($proc -and -not $proc.HasExited) {
            try {
                Stop-Process -Id $proc.Id -Force
            } catch {
                # best effort
            }
        }
    }
}
