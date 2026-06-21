"""Verify DATABASE_URL without printing credentials or the full DSN."""

from __future__ import annotations

import asyncio
import os
import sys

import asyncpg
from dotenv import load_dotenv


async def main() -> int:
    load_dotenv()
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        print("ERROR: DATABASE_URL is not set.", file=sys.stderr)
        return 1

    try:
        conn = await asyncpg.connect(
            database_url,
            statement_cache_size=0,
            timeout=20,
            command_timeout=20,
        )
        try:
            await conn.execute("SELECT 1")
            row = await conn.fetchrow(
                "SELECT current_database() AS database_name, current_user AS database_user"
            )
        finally:
            await conn.close()
    except Exception as exc:
        print(f"ERROR: Database connection failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1

    print("Database connection succeeded.")
    print(f"Database: {row['database_name']}")
    print(f"User: {row['database_user']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
