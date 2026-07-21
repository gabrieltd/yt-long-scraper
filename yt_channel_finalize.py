"""Clean heavy discovery staging after channel workers finish."""

from __future__ import annotations

import argparse
import asyncio

from dotenv import load_dotenv

from db import (
    close_db,
    count_pending_channels_for_discovery,
    init_db,
    purge_pipeline_staging_tables,
)


async def finalize(
    *,
    language: str = "es",
    dsn: str | None = None,
    ensure_schema: bool = True,
) -> None:
    await init_db(
        dsn,
        min_size=1,
        max_size=2,
        language=language,
        ensure_schema=ensure_schema,
    )
    try:
        purged = await purge_pipeline_staging_tables(language)
        print(f"[channel-finalize][purge] cleaned: {', '.join(purged)}")

        pending = await count_pending_channels_for_discovery()
        if pending:
            print(
                "[channel-finalize][candidates] "
                f"{pending} channels remain pending for a later run"
            )
        else:
            print("[channel-finalize][candidates] queue drained")
    finally:
        await close_db()


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Finalize channel discovery for one language"
    )
    parser.add_argument("--dsn", default=None)
    parser.add_argument("--skip-schema", action="store_false", dest="ensure_schema")
    language = parser.add_mutually_exclusive_group()
    language.add_argument("--EN", action="store_const", const="en", dest="lang")
    language.add_argument("--ES", action="store_const", const="es", dest="lang")
    parser.set_defaults(lang="es", ensure_schema=True)
    return parser


if __name__ == "__main__":
    load_dotenv()
    args = _parser().parse_args()
    try:
        asyncio.run(finalize(
            language=args.lang,
            dsn=args.dsn,
            ensure_schema=args.ensure_schema,
        ))
    except KeyboardInterrupt:
        print("\n[channel-finalize] stopped by user")
