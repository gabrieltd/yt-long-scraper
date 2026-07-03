"""Manually compact the DB to marked channels and minimum UI data.

This script is intentionally not wired into the pipeline. It keeps marked
channel metadata/relevance and removes derived stats plus raw video data.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from dataclasses import dataclass

import asyncpg
from dotenv import load_dotenv


VALID_LANGUAGES = {"es", "en"}


@dataclass(frozen=True)
class PurgeCounts:
    channels_raw: int
    marked_channels: int
    unmarked_channels_raw: int
    relevance_rows: int
    relevance_null_rows: int
    channels_processed: int
    unmarked_channels_processed: int
    channel_stats_rows: int | None
    channel_videos_raw: int
    videos_normalized: int
    videos_raw: int
    search_runs: int
    channels_discovery_claims: int


def _table(base_name: str, language: str) -> str:
    if language not in VALID_LANGUAGES:
        raise ValueError(f"Unsupported language: {language}")
    return f"{base_name}_{language}"


def _staging_tables(language: str) -> list[str]:
    return [
        _table("videos_normalized", language),
        _table("videos_raw", language),
        _table("search_runs", language),
        _table("channels_discovery_claims", language),
    ]


def _tables_to_truncate(language: str) -> list[str]:
    return [_table("channel_videos_raw", language), *_staging_tables(language)]


def _vacuum_tables(language: str) -> list[str]:
    return [
        _table("channel_videos_raw", language),
        *_staging_tables(language),
        _table("channels_raw", language),
        _table("channel_relevance", language),
        _table("channels_processed", language),
    ]


async def _count(conn: asyncpg.Connection, sql: str) -> int:
    value = await conn.fetchval(sql)
    return int(value or 0)


async def _relation_exists(conn: asyncpg.Connection, name: str) -> bool:
    return bool(await conn.fetchval("SELECT to_regclass($1)", name))


async def _count_relation(conn: asyncpg.Connection, name: str) -> int | None:
    if not await _relation_exists(conn, name):
        return None
    return await _count(conn, f"SELECT COUNT(*) FROM {name}")


async def collect_counts(conn: asyncpg.Connection, language: str) -> PurgeCounts:
    channels_raw = _table("channels_raw", language)
    channel_relevance = _table("channel_relevance", language)
    channels_processed = _table("channels_processed", language)
    channel_stats = _table("channel_stats", language)
    channel_videos_raw = _table("channel_videos_raw", language)
    videos_normalized = _table("videos_normalized", language)
    videos_raw = _table("videos_raw", language)
    search_runs = _table("search_runs", language)
    channels_discovery_claims = _table("channels_discovery_claims", language)

    marked_exists = (
        f"SELECT 1 FROM {channel_relevance} rel "
        "WHERE rel.channel_url = {alias}.channel_url "
        "AND rel.is_relevant IS NOT NULL"
    )

    return PurgeCounts(
        channels_raw=await _count(conn, f"SELECT COUNT(*) FROM {channels_raw}"),
        marked_channels=await _count(
            conn,
            f"""
            SELECT COUNT(DISTINCT rel.channel_url)
            FROM {channel_relevance} rel
            WHERE rel.is_relevant IS NOT NULL
            """,
        ),
        unmarked_channels_raw=await _count(
            conn,
            f"""
            SELECT COUNT(*)
            FROM {channels_raw} cr
            WHERE NOT EXISTS ({marked_exists.format(alias="cr")})
            """,
        ),
        relevance_rows=await _count(conn, f"SELECT COUNT(*) FROM {channel_relevance}"),
        relevance_null_rows=await _count(
            conn,
            f"SELECT COUNT(*) FROM {channel_relevance} WHERE is_relevant IS NULL",
        ),
        channels_processed=await _count(conn, f"SELECT COUNT(*) FROM {channels_processed}"),
        unmarked_channels_processed=await _count(
            conn,
            f"""
            SELECT COUNT(*)
            FROM {channels_processed} cp
            WHERE NOT EXISTS ({marked_exists.format(alias="cp")})
            """,
        ),
        channel_stats_rows=await _count_relation(conn, channel_stats),
        channel_videos_raw=await _count(conn, f"SELECT COUNT(*) FROM {channel_videos_raw}"),
        videos_normalized=await _count(conn, f"SELECT COUNT(*) FROM {videos_normalized}"),
        videos_raw=await _count(conn, f"SELECT COUNT(*) FROM {videos_raw}"),
        search_runs=await _count(conn, f"SELECT COUNT(*) FROM {search_runs}"),
        channels_discovery_claims=await _count(
            conn,
            f"SELECT COUNT(*) FROM {channels_discovery_claims}",
        ),
    )


async def purge_unmarked_data(conn: asyncpg.Connection, language: str) -> None:
    channels_raw = _table("channels_raw", language)
    channel_relevance = _table("channel_relevance", language)
    channels_processed = _table("channels_processed", language)
    channel_stats = _table("channel_stats", language)
    truncate_tables = ", ".join(_tables_to_truncate(language))

    marked_exists = (
        f"SELECT 1 FROM {channel_relevance} rel "
        "WHERE rel.channel_url = {alias}.channel_url "
        "AND rel.is_relevant IS NOT NULL"
    )

    async with conn.transaction():
        await conn.execute(f"DELETE FROM {channel_relevance} WHERE is_relevant IS NULL")
        await conn.execute(
            f"""
            DELETE FROM {channels_raw} cr
            WHERE NOT EXISTS ({marked_exists.format(alias="cr")})
            """
        )
        await conn.execute(
            f"""
            DELETE FROM {channels_processed} cp
            WHERE NOT EXISTS ({marked_exists.format(alias="cp")})
            """
        )
        await conn.execute(f"TRUNCATE TABLE {truncate_tables};")
        await conn.execute(f"DROP MATERIALIZED VIEW IF EXISTS {channel_stats};")


async def vacuum_full(conn: asyncpg.Connection, language: str) -> None:
    for table_name in _vacuum_tables(language):
        print(f"Running VACUUM (FULL, ANALYZE) {table_name}...")
        await conn.execute(f"VACUUM (FULL, ANALYZE) {table_name};")


def print_counts(title: str, counts: PurgeCounts) -> None:
    print(f"\n{title}")
    print("-" * len(title))
    print(f"channels_raw: {counts.channels_raw}")
    print(f"marked channels: {counts.marked_channels}")
    print(f"channels_raw to delete: {counts.unmarked_channels_raw}")
    print(f"channel_relevance rows: {counts.relevance_rows}")
    print(f"channel_relevance null rows to delete: {counts.relevance_null_rows}")
    print(f"channels_processed: {counts.channels_processed}")
    print(f"channels_processed to delete: {counts.unmarked_channels_processed}")
    stats_rows = "missing" if counts.channel_stats_rows is None else counts.channel_stats_rows
    print(f"channel_stats rows to drop: {stats_rows}")
    print(f"channel_videos_raw to truncate: {counts.channel_videos_raw}")
    print(f"videos_normalized to truncate: {counts.videos_normalized}")
    print(f"videos_raw to truncate: {counts.videos_raw}")
    print(f"search_runs to truncate: {counts.search_runs}")
    print(f"channels_discovery_claims to truncate: {counts.channels_discovery_claims}")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Compact one language's DB data to marked channels only.",
    )
    lang_group = parser.add_mutually_exclusive_group(required=True)
    lang_group.add_argument("--ES", action="store_const", const="es", dest="language")
    lang_group.add_argument("--EN", action="store_const", const="en", dest="language")
    parser.add_argument(
        "--database-url",
        default=os.getenv("DATABASE_URL"),
        help="PostgreSQL connection URL. Defaults to DATABASE_URL.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print counts without deleting anything.",
    )
    parser.add_argument(
        "--vacuum-full",
        action="store_true",
        help="Run VACUUM FULL after the purge to reclaim disk space.",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=120,
        help="Connection and command timeout in seconds.",
    )
    return parser


async def async_main(args: argparse.Namespace) -> int:
    database_url = args.database_url
    if not database_url:
        print("ERROR: DATABASE_URL is not set.", file=sys.stderr)
        return 2

    conn = await asyncpg.connect(
        database_url,
        statement_cache_size=0,
        timeout=args.timeout_seconds,
        command_timeout=args.timeout_seconds,
    )
    try:
        language = args.language
        before = await collect_counts(conn, language)
        print_counts(f"Before purge ({language})", before)
        print("\nNote: channel_stats is removed because derived metrics are no longer valid.")

        if args.dry_run:
            print("\nDry run only. No data was changed.")
            return 0

        await purge_unmarked_data(conn, language)
        after = await collect_counts(conn, language)
        print_counts(f"After purge ({language})", after)

        if args.vacuum_full:
            await vacuum_full(conn, language)
            print("VACUUM FULL completed.")
        else:
            print("\nSkipped VACUUM FULL. Add --vacuum-full to reclaim disk space.")

        return 0
    finally:
        await conn.close()


def main() -> int:
    load_dotenv()
    args = _build_parser().parse_args()
    try:
        return asyncio.run(async_main(args))
    except Exception as exc:
        print(f"ERROR: purge failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
