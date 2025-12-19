$ErrorActionPreference = "Stop"

if (Get-Command docker -ErrorAction SilentlyContinue) {
  docker compose -p dickgrowerbot-py down
  exit 0
}

if (Get-Command docker-compose -ErrorAction SilentlyContinue) {
  docker-compose -p dickgrowerbot-py down
  exit 0
}

throw "Docker is not installed or not in PATH (expected `docker` or `docker-compose`)."
