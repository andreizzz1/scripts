from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import asyncpg


def _migration_sort_key(path: Path) -> tuple[int, str]:
    name = path.name
    num = ""
    for ch in name:
        if ch.isdigit():
            num += ch
        else:
            break
    try:
        n = int(num) if num else 10**9
    except ValueError:
        n = 10**9
    return n, name


@dataclass(frozen=True)
class Database:
    pool: asyncpg.Pool

    @classmethod
    async def connect(cls) -> "Database":
        url = os.getenv("DATABASE_URL")
        if not url:
            raise RuntimeError("DATABASE_URL is required")
        pool = await asyncpg.create_pool(url)
        return cls(pool=pool)


async def apply_sql_migrations(pool: asyncpg.Pool, migrations_dir: Path) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS schema_migrations (
              filename text PRIMARY KEY,
              applied_at timestamptz NOT NULL DEFAULT current_timestamp
            )
            """
        )
        applied = {
            r["filename"]
            for r in await conn.fetch("SELECT filename FROM schema_migrations")
        }

        migration_files: Iterable[Path] = sorted(
            (p for p in migrations_dir.glob("*.sql") if p.is_file()),
            key=_migration_sort_key,
        )
        for path in migration_files:
            if path.name in applied:
                continue
            sql = path.read_text(encoding="utf-8")
            async with conn.transaction():
                await conn.execute(sql)
                await conn.execute(
                    "INSERT INTO schema_migrations(filename) VALUES ($1)",
                    path.name,
                )
