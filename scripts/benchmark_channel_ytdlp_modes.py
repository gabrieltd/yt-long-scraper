"""Compare channel extraction parity and wall time for both yt-dlp modes."""

from __future__ import annotations

import argparse
import multiprocessing
import sys
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Callable

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from yt_channel_discovery import (
    parse_channel_raw,
    parse_channel_videos_raw,
    run_ytdlp_channel_dump,
    run_ytdlp_channel_dump_api,
)


def _signature(channel_url: str, dump: dict[str, Any], max_videos: int) -> dict[str, Any]:
    channel = parse_channel_raw(channel_url, dump)
    videos = parse_channel_videos_raw(channel_url, dump, max_videos=max_videos)
    return {
        "channel_id": channel.get("channel_id"),
        "channel_name": channel.get("channel_name"),
        "videos": [
            (
                video.get("video_id"),
                video.get("title"),
                video.get("upload_date"),
                video.get("duration_seconds"),
            )
            for video in videos
        ],
    }


def _structural_signature(signature: dict[str, Any] | None) -> Any:
    """Exclude titles that YouTube may translate between identical requests."""
    if signature is None:
        return None
    return (
        signature["channel_id"],
        tuple(
            (video_id, upload_date, duration)
            for video_id, _title, upload_date, duration in signature["videos"]
        ),
    )


def _measure(
    urls: list[str],
    *,
    workers: int,
    extractor: Callable[..., dict[str, Any]],
    executor_type: type[ThreadPoolExecutor] | type[ProcessPoolExecutor],
    max_videos: int,
    timeout_seconds: int,
) -> tuple[float, dict[str, dict[str, Any]], dict[str, str]]:
    results: dict[str, dict[str, Any]] = {}
    errors: dict[str, str] = {}
    started = time.perf_counter()
    executor_options = {"max_workers": workers}
    if executor_type is ProcessPoolExecutor:
        executor_options["mp_context"] = multiprocessing.get_context("spawn")
    with executor_type(**executor_options) as executor:
        futures = {
            executor.submit(
                extractor,
                url,
                max_videos=max_videos,
                timeout_seconds=timeout_seconds,
            ): url
            for url in urls
        }
        for future in as_completed(futures):
            url = futures[future]
            try:
                results[url] = _signature(url, future.result(), max_videos)
            except Exception as exc:
                errors[url] = str(exc)
    return time.perf_counter() - started, results, errors


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("url_file", type=Path, help="Text file with one channel URL per line")
    parser.add_argument("--channels", type=int, default=20)
    parser.add_argument("--workers", type=int, default=6)
    parser.add_argument("--max-videos", type=int, default=60)
    parser.add_argument("--timeout-seconds", type=int, default=180)
    parser.add_argument("--minimum-improvement", type=float, default=20.0)
    parser.add_argument("--parity-retries", type=int, default=1)
    args = parser.parse_args()

    urls = [
        line.strip().rstrip("/")
        for line in args.url_file.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ][:args.channels]
    if len(urls) < 20:
        parser.error("at least 20 channel URLs are required for promotion")

    subprocess_seconds, subprocess_results, subprocess_errors = _measure(
        urls,
        workers=args.workers,
        extractor=run_ytdlp_channel_dump,
        executor_type=ThreadPoolExecutor,
        max_videos=args.max_videos,
        timeout_seconds=args.timeout_seconds,
    )
    pool_seconds, pool_results, pool_errors = _measure(
        urls,
        workers=args.workers,
        extractor=run_ytdlp_channel_dump_api,
        executor_type=ProcessPoolExecutor,
        max_videos=args.max_videos,
        timeout_seconds=args.timeout_seconds,
    )
    initial_structural_mismatches = [
        url for url in urls
        if _structural_signature(subprocess_results.get(url))
        != _structural_signature(pool_results.get(url))
    ]
    structural_mismatches = list(initial_structural_mismatches)
    transient_mismatches: list[str] = []
    for _attempt in range(max(0, args.parity_retries)):
        if not structural_mismatches:
            break
        remaining: list[str] = []
        for url in structural_mismatches:
            try:
                subprocess_retry = _signature(
                    url,
                    run_ytdlp_channel_dump(
                        url,
                        max_videos=args.max_videos,
                        timeout_seconds=args.timeout_seconds,
                    ),
                    args.max_videos,
                )
                pool_retry = _signature(
                    url,
                    run_ytdlp_channel_dump_api(
                        url,
                        max_videos=args.max_videos,
                        timeout_seconds=args.timeout_seconds,
                    ),
                    args.max_videos,
                )
            except Exception:
                remaining.append(url)
                continue
            if _structural_signature(subprocess_retry) == _structural_signature(pool_retry):
                transient_mismatches.append(url)
            else:
                remaining.append(url)
        structural_mismatches = remaining
    localized_mismatches = [
        url for url in urls
        if subprocess_results.get(url) != pool_results.get(url)
    ]
    improvement = (
        (subprocess_seconds - pool_seconds) / subprocess_seconds * 100
        if subprocess_seconds else 0.0
    )
    print(
        f"subprocess={subprocess_seconds:.2f}s errors={len(subprocess_errors)} "
        f"process_pool={pool_seconds:.2f}s errors={len(pool_errors)} "
        f"improvement={improvement:.1f}% "
        f"initial_structural_mismatches={len(initial_structural_mismatches)} "
        f"structural_mismatches={len(structural_mismatches)} "
        f"localized_mismatches={len(localized_mismatches)}"
    )
    if transient_mismatches:
        print("transient structural differences:", ", ".join(transient_mismatches))
    if structural_mismatches:
        print("structurally mismatched channels:", ", ".join(structural_mismatches))
    if localized_mismatches:
        print(
            "localized/title differences (informational):",
            ", ".join(localized_mismatches),
        )
    promoted = (
        not structural_mismatches
        and len(pool_errors) <= len(subprocess_errors)
        and improvement >= args.minimum_improvement
    )
    print("promotion criterion:", "PASS" if promoted else "FAIL")
    return 0 if promoted else 2


if __name__ == "__main__":
    raise SystemExit(main())
