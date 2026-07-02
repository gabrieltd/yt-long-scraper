"""Fail-fast PostgreSQL connectivity check for local/ngrok-backed CI databases."""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from urllib.parse import ParseResult, urlparse, urlunparse

import asyncpg


LOCAL_HOSTS = {"localhost", "127.0.0.1", "::1"}


def _sanitize_database_url(database_url: str) -> str:
    parsed = urlparse(database_url)
    if not parsed.scheme or not parsed.netloc:
        return "<invalid database url>"

    host = parsed.hostname or ""
    port = f":{parsed.port}" if parsed.port else ""
    username = parsed.username or ""
    userinfo = f"{username}:***@" if username else ""
    netloc = f"{userinfo}{host}{port}"
    sanitized = ParseResult(
        scheme=parsed.scheme,
        netloc=netloc,
        path=parsed.path,
        params=parsed.params,
        query=parsed.query,
        fragment="",
    )
    return urlunparse(sanitized)


def _is_local_database_url(database_url: str) -> bool:
    parsed = urlparse(database_url)
    host = parsed.hostname
    return host is not None and host.lower() in LOCAL_HOSTS


async def _check_database(database_url: str, timeout_seconds: float) -> None:
    conn = await asyncpg.connect(
        database_url,
        statement_cache_size=0,
        timeout=timeout_seconds,
        command_timeout=timeout_seconds,
    )
    try:
        value = await conn.fetchval("SELECT 1")
        if value != 1:
            raise RuntimeError(f"Unexpected SELECT 1 result: {value!r}")
    finally:
        await conn.close()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Check PostgreSQL connectivity.")
    parser.add_argument(
        "--database-url",
        default=os.getenv("DATABASE_URL"),
        help="PostgreSQL connection URL. Defaults to DATABASE_URL.",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=20,
        help="Connection and query timeout in seconds.",
    )
    parser.add_argument(
        "--expect-remote",
        action="store_true",
        help="Fail if the DB host is localhost/127.0.0.1/::1.",
    )
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    database_url = args.database_url
    if not database_url:
        print("ERROR: DATABASE_URL is not set.", file=sys.stderr)
        return 2

    safe_url = _sanitize_database_url(database_url)
    if args.expect_remote and _is_local_database_url(database_url):
        print(
            "ERROR: DATABASE_URL points to localhost. On GitHub-hosted runners, "
            "localhost is the runner itself, not your PC. Use the ngrok TCP host "
            f"and port instead. Current URL: {safe_url}",
            file=sys.stderr,
        )
        return 2

    print(f"Checking PostgreSQL connectivity: {safe_url}")
    try:
        asyncio.run(_check_database(database_url, args.timeout_seconds))
    except Exception as exc:
        print(
            "ERROR: Could not connect to PostgreSQL. Confirm Docker/Postgres is "
            "running, ngrok tcp is active, and the GitHub DATABASE_URL secret has "
            f"the current ngrok host/port. URL: {safe_url}. Details: {exc}",
            file=sys.stderr,
        )
        return 1

    print("Database connectivity OK.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
