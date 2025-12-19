$ErrorActionPreference = "Stop"

function Invoke-Compose {
  param(
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]] $Args
  )

  if (Get-Command docker -ErrorAction SilentlyContinue) {
    & docker compose @Args
    return
  }

  if (Get-Command docker-compose -ErrorAction SilentlyContinue) {
    & docker-compose @Args
    return
  }

  throw "Docker is not installed or not in PATH (expected `docker` or `docker-compose`)."
}

if (-not (Test-Path ".env")) {
  Copy-Item ".env.example" ".env"
  (Get-Content ".env") -replace '^POSTGRES_HOST=.*$', 'POSTGRES_HOST=postgres' | Set-Content ".env"
  Write-Host "Created .env from .env.example (POSTGRES_HOST=postgres). Set TELOXIDE_TOKEN in .env, then rerun."
}

$envText = Get-Content -Raw ".env"
$tokenLine = ($envText -split "`r?`n" | Where-Object { $_ -match '^TELOXIDE_TOKEN=' } | Select-Object -First 1)
$tokenValue = $null
if ($tokenLine) {
  $tokenValue = ($tokenLine -split '=', 2)[1].Trim()
}

if (-not $tokenValue -or $tokenValue -eq "0123456789:XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX") {
  Write-Warning "TELOXIDE_TOKEN is not set. Starting only Postgres (service: postgres)."
  Invoke-Compose -p dickgrowerbot-py up -d --build postgres
  Invoke-Compose -p dickgrowerbot-py ps
  exit 0
}

Invoke-Compose -p dickgrowerbot-py up -d --build
Invoke-Compose -p dickgrowerbot-py ps
