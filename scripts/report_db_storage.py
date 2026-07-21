"""Report PostgreSQL storage use for the ES/EN pipeline tables.

The command is read-only and is safe to run against production. Row counts are
PostgreSQL statistics estimates by default; use ``--exact-rows`` when an exact
count is worth the additional table scans.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from dataclasses import dataclass, replace

import asyncpg
from dotenv import load_dotenv


@dataclass(frozen=True)
class RelationSize:
    language: str
    table_name: str
    rows: int
    heap_bytes: int
    index_bytes: int
    toast_bytes: int
    total_bytes: int


REPORT_SQL = r"""
    SELECT
        CASE
            WHEN c.relname LIKE '%\_es' ESCAPE '\' THEN 'es'
            ELSE 'en'
        END AS language,
        c.relname AS table_name,
        GREATEST(COALESCE(st.n_live_tup, c.reltuples)::BIGINT, 0) AS rows,
        pg_relation_size(c.oid)::BIGINT AS heap_bytes,
        pg_indexes_size(c.oid)::BIGINT AS index_bytes,
        CASE WHEN c.reltoastrelid = 0 THEN 0
             ELSE pg_total_relation_size(c.reltoastrelid)::BIGINT
        END AS toast_bytes,
        pg_total_relation_size(c.oid)::BIGINT AS total_bytes
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    LEFT JOIN pg_stat_user_tables st ON st.relid = c.oid
    WHERE n.nspname = current_schema()
      AND c.relkind IN ('r', 'p')
      AND ($1::TEXT[] IS NULL OR
           ($1 @> ARRAY['es']::TEXT[] AND c.relname LIKE '%\_es' ESCAPE '\') OR
           ($1 @> ARRAY['en']::TEXT[] AND c.relname LIKE '%\_en' ESCAPE '\'))
    ORDER BY language, total_bytes DESC, table_name
"""


def _human_size(value: int) -> str:
    amount = float(value)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if amount < 1024 or unit == "TiB":
            return f"{amount:.0f} {unit}" if unit == "B" else f"{amount:.1f} {unit}"
        amount /= 1024
    raise AssertionError("unreachable")


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


async def collect_report(
    conn: asyncpg.Connection,
    languages: list[str],
    *,
    exact_rows: bool = False,
) -> list[RelationSize]:
    records = await conn.fetch(REPORT_SQL, languages)
    result = [RelationSize(**dict(record)) for record in records]
    if exact_rows:
        exact: list[RelationSize] = []
        for item in result:
            count = await conn.fetchval(
                f"SELECT COUNT(*) FROM {_quote_identifier(item.table_name)}"
            )
            exact.append(replace(item, rows=int(count or 0)))
        result = exact
    return result


def print_report(rows: list[RelationSize], *, exact_rows: bool) -> None:
    row_label = "rows" if exact_rows else "rows(est.)"
    widths = {
        "language": 4,
        "table": max([5, *(len(row.table_name) for row in rows)]),
        "rows": max([len(row_label), *(len(f"{row.rows:,}") for row in rows)]),
    }
    header = (
        f"{'lang':<{widths['language']}}  {'table':<{widths['table']}}  "
        f"{row_label:>{widths['rows']}}  {'heap':>10}  {'indexes':>10}  "
        f"{'TOAST':>10}  {'total':>10}"
    )
    print(header)
    print("-" * len(header))
    for row in rows:
        print(
            f"{row.language:<{widths['language']}}  "
            f"{row.table_name:<{widths['table']}}  "
            f"{row.rows:>{widths['rows']},}  "
            f"{_human_size(row.heap_bytes):>10}  "
            f"{_human_size(row.index_bytes):>10}  "
            f"{_human_size(row.toast_bytes):>10}  "
            f"{_human_size(row.total_bytes):>10}"
        )

    for language in sorted({row.language for row in rows}):
        language_rows = [row for row in rows if row.language == language]
        print(
            f"\n{language.upper()} total: "
            f"{_human_size(sum(row.total_bytes for row in language_rows))} "
            f"across {len(language_rows)} tables"
        )
    if not rows:
        print("No ES/EN pipeline tables found in the current schema.")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Report DB storage by table and language.")
    language = parser.add_mutually_exclusive_group()
    language.add_argument("--ES", action="store_const", const=["es"], dest="languages")
    language.add_argument("--EN", action="store_const", const=["en"], dest="languages")
    parser.set_defaults(languages=["es", "en"])
    parser.add_argument("--database-url", default=os.getenv("DATABASE_URL"))
    parser.add_argument(
        "--exact-rows",
        action="store_true",
        help="Run COUNT(*) for every table instead of using PostgreSQL estimates.",
    )
    parser.add_argument("--timeout-seconds", type=float, default=120)
    return parser


async def async_main(args: argparse.Namespace) -> int:
    if not args.database_url:
        print("ERROR: DATABASE_URL is not set.", file=sys.stderr)
        return 2
    conn = await asyncpg.connect(
        args.database_url,
        statement_cache_size=0,
        timeout=args.timeout_seconds,
        command_timeout=args.timeout_seconds,
    )
    try:
        rows = await collect_report(conn, args.languages, exact_rows=args.exact_rows)
        print_report(rows, exact_rows=args.exact_rows)
        return 0
    finally:
        await conn.close()


def main() -> int:
    load_dotenv()
    args = _build_parser().parse_args()
    try:
        return asyncio.run(async_main(args))
    except Exception as exc:
        print(f"ERROR: storage report failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
