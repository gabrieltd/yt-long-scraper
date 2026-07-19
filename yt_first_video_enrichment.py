"""Retry pending first-video enrichment without repeating channel discovery."""

from __future__ import annotations

import argparse
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from dotenv import load_dotenv

from db import (
    claim_channels_for_first_video_enrichment,
    close_db,
    init_db,
    update_first_video_failure,
    update_first_video_no_public,
    update_first_video_success,
)
from youtube_oldest_video import OldestVideoError, YouTubeOldestVideoClient
from yt_channel_discovery import _DBRunner, resolve_first_video


DEFAULT_WORKERS = 5


def run(
    *,
    language: str = "es",
    workers: int = DEFAULT_WORKERS,
    limit: int = 1_000_000,
    timeout_seconds: int = 20,
    fallback_timeout_seconds: int = 180,
    stale_after_minutes: int = 60,
    dsn: str | None = None,
) -> None:
    if workers < 1:
        raise ValueError("workers must be positive")
    started = time.perf_counter()
    db = _DBRunner()
    db.start()
    try:
        db.run(init_db(dsn, min_size=1, max_size=4, language=language))
        try:
            client = YouTubeOldestVideoClient.initialize(
                timeout_seconds=timeout_seconds
            )
        except OldestVideoError as exc:
            print(f"[first-video] global initialization failed; retry skipped: {exc}")
            return
        profiles = db.run(claim_channels_for_first_video_enrichment(
            limit,
            stale_after_minutes=stale_after_minutes,
        ))
        if not profiles:
            print("[first-video] no pending channels")
            return

        print(f"[first-video] claimed={len(profiles)} workers={workers}")
        totals = {
            "success": 0,
            "no_public_videos": 0,
            "pending": 0,
            "innertube": 0,
            "yt_dlp": 0,
        }
        executor = ThreadPoolExecutor(max_workers=workers)
        futures = {
            executor.submit(
                resolve_first_video,
                profile["channel_url"],
                profile["channel_id"],
                client,
                fallback_timeout_seconds=fallback_timeout_seconds,
            ): profile
            for profile in profiles
        }
        try:
            for future in as_completed(futures):
                profile = futures[future]
                channel_url = profile["channel_url"]
                try:
                    result = future.result()
                except Exception as exc:
                    result = {
                        "first_video_status": "pending",
                        "first_video_last_error": f"Unexpected worker error: {exc}",
                    }

                status = result["first_video_status"]
                if status == "success":
                    db.run(update_first_video_success(
                        channel_url,
                        video_id=result["first_video_id"],
                        published_at=result["first_video_published_at"],
                        source=result["first_video_source"],
                    ))
                    totals[result["first_video_source"]] += 1
                    print(
                        f"[first-video][ok] {channel_url}: "
                        f"{result['first_video_id']} via {result['first_video_source']}"
                    )
                elif status == "no_public_videos":
                    db.run(update_first_video_no_public(
                        channel_url,
                        reason=result.get("first_video_last_error") or "No public videos",
                    ))
                    print(f"[first-video][empty] {channel_url}")
                else:
                    error = result.get("first_video_last_error") or "Unknown enrichment error"
                    db.run(update_first_video_failure(channel_url, error=error))
                    print(f"[first-video][error] {channel_url}: {error}")
                totals[status] += 1
        except KeyboardInterrupt:
            for future in futures:
                future.cancel()
            executor.shutdown(wait=False, cancel_futures=True)
            raise
        else:
            executor.shutdown(wait=True)

        duration = time.perf_counter() - started
        print(
            "[first-video][done] "
            f"total={len(profiles)} success={totals['success']} "
            f"empty={totals['no_public_videos']} failed={totals['pending']} "
            f"innertube={totals['innertube']} yt-dlp={totals['yt_dlp']} "
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
    parser.add_argument("--timeout-seconds", type=int, default=20)
    parser.add_argument("--fallback-timeout-seconds", type=int, default=180)
    parser.add_argument("--stale-after-minutes", type=int, default=60)
    parser.add_argument("--dsn", default=None)
    language = parser.add_mutually_exclusive_group()
    language.add_argument("--EN", action="store_const", const="en", dest="lang")
    language.add_argument("--ES", action="store_const", const="es", dest="lang")
    parser.set_defaults(lang="es")
    return parser


if __name__ == "__main__":
    load_dotenv()
    args = _parser().parse_args()
    try:
        run(
            language=args.lang,
            workers=args.workers,
            limit=args.limit,
            timeout_seconds=args.timeout_seconds,
            fallback_timeout_seconds=args.fallback_timeout_seconds,
            stale_after_minutes=args.stale_after_minutes,
            dsn=args.dsn,
        )
    except KeyboardInterrupt:
        print("\n[first-video] stopped by user")
