"""Retry pending first-video enrichment without repeating channel discovery."""

from __future__ import annotations

import argparse
import time
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

from dotenv import load_dotenv

from db import (
    claim_channels_for_first_video_enrichment,
    close_db,
    init_db,
    update_first_video_results,
)
from youtube_oldest_video import OldestVideoError, YouTubeOldestVideoClient
from yt_channel_discovery import _DBRunner, _request_stop, resolve_first_video


DEFAULT_WORKERS = 5


def run(
    *,
    language: str = "es",
    workers: int = DEFAULT_WORKERS,
    limit: int = 1_000_000,
    batch_size: int = 50,
    timeout_seconds: int = 20,
    stale_after_minutes: int = 60,
    dsn: str | None = None,
    ensure_schema: bool = True,
) -> None:
    if workers < 1:
        raise ValueError("workers must be positive")
    if batch_size < 1:
        raise ValueError("batch_size must be positive")
    started = time.perf_counter()
    cycle_started_at = datetime.now(timezone.utc)
    db = _DBRunner()
    db.start()
    try:
        db.run(init_db(
            dsn,
            min_size=1,
            max_size=4,
            language=language,
            ensure_schema=ensure_schema,
        ))
        try:
            client = YouTubeOldestVideoClient.initialize(
                timeout_seconds=timeout_seconds
            )
        except OldestVideoError as exc:
            print(f"[first-video] global initialization failed; retry skipped: {exc}")
            return
        totals = {
            "success": 0,
            "no_public_videos": 0,
            "pending": 0,
            "innertube": 0,
        }
        claimed_total = 0
        remaining = limit
        in_flight_limit = workers * 2
        executor = ThreadPoolExecutor(max_workers=workers)
        active_profiles: list[dict[str, str]] = []
        futures = {}
        try:
            while remaining > 0:
                profiles = db.run(claim_channels_for_first_video_enrichment(
                    min(batch_size, remaining),
                    stale_after_minutes=stale_after_minutes,
                    eligible_before=cycle_started_at,
                ))
                if not profiles:
                    break
                active_profiles = profiles
                claimed_total += len(profiles)
                remaining -= len(profiles)
                print(
                    f"[first-video] claimed={len(profiles)} "
                    f"total={claimed_total} workers={workers}"
                )

                for start_index in range(0, len(profiles), in_flight_limit):
                    window = profiles[start_index:start_index + in_flight_limit]
                    futures = {
                        executor.submit(
                            resolve_first_video,
                            profile["channel_url"],
                            profile["channel_id"],
                            client,
                            allow_ytdlp_fallback=False,
                        ): profile
                        for profile in window
                    }
                    results = []
                    for future in as_completed(futures):
                        profile = futures[future]
                        channel_url = profile["channel_url"]
                        try:
                            result = future.result()
                        except Exception as exc:
                            result = {
                                "first_video_status": "pending",
                                "first_video_last_attempt_at": datetime.now(timezone.utc),
                                "first_video_last_error": f"Unexpected worker error: {exc}",
                            }
                        result["channel_url"] = channel_url
                        status = result["first_video_status"]
                        if status == "success":
                            totals[result["first_video_source"]] += 1
                        elif status == "no_public_videos":
                            print(f"[first-video][empty] {channel_url}")
                        else:
                            error = result.get("first_video_last_error") or "Unknown enrichment error"
                            print(f"[first-video][error] {channel_url}: {error}")
                        totals[status] += 1
                        results.append(result)
                    db.run(update_first_video_results(results))
                    active_profiles = profiles[start_index + len(window):]
                    if totals["success"] and totals["success"] % 50 == 0:
                        print(f"[first-video][progress] success={totals['success']}")
                active_profiles = []
                futures = {}
            if claimed_total == 0:
                print("[first-video] no pending channels")
        except KeyboardInterrupt:
            _request_stop()
            for future in futures:
                future.cancel()
            if active_profiles:
                attempted_at = datetime.now(timezone.utc)
                db.run(update_first_video_results([
                    {
                        "channel_url": profile["channel_url"],
                        "first_video_status": "pending",
                        "first_video_last_attempt_at": attempted_at,
                        "first_video_last_error": "Enrichment interrupted by user",
                    }
                    for profile in active_profiles
                ]))
            executor.shutdown(wait=False, cancel_futures=True)
            raise
        else:
            executor.shutdown(wait=True)

        duration = time.perf_counter() - started
        print(
            "[first-video][done] "
            f"total={claimed_total} success={totals['success']} "
            f"empty={totals['no_public_videos']} failed={totals['pending']} "
            f"innertube={totals['innertube']} "
            f"seconds={duration:.2f}"
        )
    finally:
        try:
            db.run(close_db())
        finally:
            db.stop()


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Retry pending first-video enrichment")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS)
    parser.add_argument("--limit", type=int, default=1_000_000)
    parser.add_argument("--batch-size", type=int, default=50)
    parser.add_argument("--timeout-seconds", type=int, default=20)
    parser.add_argument("--stale-after-minutes", type=int, default=60)
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
        run(
            language=args.lang,
            workers=args.workers,
            limit=args.limit,
            batch_size=args.batch_size,
            timeout_seconds=args.timeout_seconds,
            stale_after_minutes=args.stale_after_minutes,
            dsn=args.dsn,
            ensure_schema=args.ensure_schema,
        )
    except KeyboardInterrupt:
        print("\n[first-video] stopped by user")
