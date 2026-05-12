# Run from repo root: .\scripts\setup-git-hooks.ps1
$ErrorActionPreference = "Stop"
Set-Location (Split-Path -Parent $PSScriptRoot)
git config core.hooksPath .githooks
Write-Host "Set core.hooksPath=.githooks (pre-push runs scripts/generate-pr-description.sh)"
