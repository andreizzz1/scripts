# Python bot (async)

This is a Python async rewrite of the Rust bot, reusing the same Postgres schema (the SQL files in `../migrations`).

## Run locally

1) Create a venv and install deps:

```bash
cd pybot
python -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -e ".[dev]"
```

2) Export env vars (or reuse the repo `.env`):

Required:
- `TELOXIDE_TOKEN` (Telegram Bot API token)
- `DATABASE_URL` (Postgres URL, e.g. `postgres://user:pass@localhost:5432/dbname`)

Optional:
- `RUST_LOG` is ignored (use `LOG_LEVEL` instead)
- `LOG_LEVEL` (default: `INFO`)

3) Run:

```bash
python -m dickgrowerbot.main
```

## Run with Docker

From the repository root:

```bash
cp .env.example .env
# Set TELOXIDE_TOKEN in .env (and keep POSTGRES_HOST=postgres for Docker Compose).
docker compose -p dickgrowerbot-py up -d --build
docker compose -p dickgrowerbot-py logs -f dickgrowerbot-py
```

If you see `TelegramConflictError: ... only one bot instance is running`, stop any other container/process using the same bot token (only one polling `getUpdates` consumer is allowed).

## VS Code: Docker extension doesn't show containers?

The Docker extension shows containers from the current Docker context/daemon.
If you use VS Code Remote (WSL/SSH/Dev Container), make sure the Docker extension is running against the same Docker engine that you run `docker compose` on (Command Palette: `Docker: Switch Context`).

## Tests

Unit tests:

```bash
pytest
```

DB integration tests require `DATABASE_URL` to be set.
