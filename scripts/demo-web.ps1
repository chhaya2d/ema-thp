<#
.SYNOPSIS
  Stop, start, or restart the EmaTHP browser demo (Gradle run --args web). Default HTTP port is 8080.

.EXAMPLE
  .\scripts\demo-web.ps1 stop
.EXAMPLE
  .\scripts\demo-web.ps1 start
.EXAMPLE
  .\scripts\demo-web.ps1 restart
#>
param(
    [Parameter(Position = 0)]
    [ValidateSet("stop", "start", "restart")]
    [string] $Command = "restart"
)

$ErrorActionPreference = "Stop"
$RepoRoot = Split-Path $PSScriptRoot -Parent
if (-not (Test-Path (Join-Path $RepoRoot "gradlew.bat"))) {
    throw "gradlew.bat not found under $RepoRoot"
}

function Stop-Port8080 {
    $seen = @{}
    Get-NetTCPConnection -LocalPort 8080 -State Listen -ErrorAction SilentlyContinue |
        ForEach-Object {
            $id = $_.OwningProcess
            if (-not $seen.ContainsKey($id)) {
                $seen[$id] = $true
                try {
                    Stop-Process -Id $id -Force -ErrorAction Stop
                    Write-Host "Stopped process $id (was listening on 8080)."
                } catch {
                    Write-Host "Could not stop PID ${id}: $_"
                }
            }
        }
    if ($seen.Count -eq 0) {
        Write-Host "No LISTENING process found on port 8080."
    }
}

function Start-DemoWeb {
    Set-Location $RepoRoot
    Write-Host "Starting demo web UI from: $RepoRoot"
    & (Join-Path $RepoRoot "gradlew.bat") @("run", "--args", "web", "--no-daemon")
}

switch ($Command) {
    "stop" { Stop-Port8080 }
    "start" { Start-DemoWeb }
    "restart" {
        Stop-Port8080
        Start-Sleep -Milliseconds 500
        Start-DemoWeb
    }
}
