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
docker-compose -p dickgrowerbot-py -f docker-compose.python.yml up -d --build
docker logs -f dickgrowerbot-py
```

If you see `TelegramConflictError: ... only one bot instance is running`, stop any other container/process using the same bot token (only one polling `getUpdates` consumer is allowed).

## Tests

Unit tests:

```bash
pytest
```

DB integration tests require `DATABASE_URL` to be set.
