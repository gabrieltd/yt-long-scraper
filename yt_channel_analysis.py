"""Channel analysis module (FINAL stage, post yt-dlp enrichment).

Strict contract (per requirements):
- Runs AFTER: discovery -> normalization/validation -> yt_channel_discovery.
- MUST NOT run yt-dlp.
- MUST NOT use Playwright.
- MUST NOT modify raw data.
- MUST NOT create DB connections / pools / tables here.
- MUST use db.py functions for all persistence.

Question answered:
- "Does this channel show real recent performance on long videos?"

This module is intentionally simple, deterministic, and auditable.
"""

from __future__ import annotations

import argparse
import asyncio
import statistics
from dataclasses import dataclass
from datetime import date
from typing import Any

from dotenv import load_dotenv

from db import (
	close_db,
	claim_channels_for_analysis,
	fetch_channel_long_videos,
	init_db,
	insert_channel_analysis,
	insert_channel_analysis_bulk,
)


LONG_VIDEO_SECONDS = 1080
GAP_DAYS = 30 * 5  # 5 months

MIN_SUBSCRIBERS = 100
MIN_LONG_VIDEOS_TOTAL = 2

MIN_CYCLE_LONG_VIDEOS = 2
MIN_HIGH_RATIO_VIDEOS = 2
HIGH_RATIO_THRESHOLD = 0.3
MEDIAN_RATIO_THRESHOLD = 0.25


@dataclass(frozen=True)
class _CycleResult:
	cycle_start_date: date | None
	cycle_videos: list[dict[str, Any]]


def _median_int(values: list[int]) -> int | None:
	if not values:
		return None
	return int(statistics.median(values))


def _median_float(values: list[float]) -> float | None:
	if not values:
		return None
	return float(statistics.median(values))


def _detect_current_cycle(videos_desc: list[dict[str, Any]]) -> _CycleResult:
	"""Detect current cycle based on the first gap >= GAP_DAYS.

	Input must be ONLY long videos with valid upload_date, sorted by upload_date DESC.
	"""
	if not videos_desc:
		return _CycleResult(None, [])

	cycle: list[dict[str, Any]] = []
	for i, v in enumerate(videos_desc):
		cycle.append(v)
		if i + 1 >= len(videos_desc):
			break
		curr_date = videos_desc[i].get("upload_date")
		next_date = videos_desc[i + 1].get("upload_date")
		if not isinstance(curr_date, date) or not isinstance(next_date, date):
			continue
		gap_days = (curr_date - next_date).days
		if gap_days >= GAP_DAYS:
			break

	cycle_start = min((v["upload_date"] for v in cycle if isinstance(v.get("upload_date"), date)), default=None)
	return _CycleResult(cycle_start, cycle)


def _decision_reason(
	*,
	cycle_long_videos_count: int,
	high_ratio_videos_count: int,
	median_views_ratio: float | None,
) -> tuple[bool, str | None]:
	"""Apply initial decision rules.

	Returns:
		(qualified, reason_if_not_qualified)
	"""
	if cycle_long_videos_count < MIN_CYCLE_LONG_VIDEOS:
		return (False, "cycle_long_videos_lt_3")
	if high_ratio_videos_count < MIN_HIGH_RATIO_VIDEOS:
		return (False, "lt_2_videos_with_views_ratio_ge_0_3")
	if median_views_ratio is None:
		return (False, "median_views_ratio_missing")
	if median_views_ratio < MEDIAN_RATIO_THRESHOLD:
		return (False, "median_views_ratio_below_0_25")
	return (True, None)


async def _analyze_one_channel(channel_url: str, subscriber_count: int | None) -> dict[str, Any]:
	"""Analyze a single channel and return the result row (does NOT persist)."""
	# Step 2: Pre-filter
	if subscriber_count is None:
		return {
			"channel_url": channel_url,
			"subscriber_count": None,
			"qualified": False,
			"analysis_reason": "subscriber_count_missing",
		}

	if subscriber_count < MIN_SUBSCRIBERS:
		return {
			"channel_url": channel_url,
			"subscriber_count": int(subscriber_count),
			"qualified": False,
			"analysis_reason": "subscriber_count_below_100",
		}

	videos_long = await fetch_channel_long_videos(channel_url)
	if len(videos_long) < MIN_LONG_VIDEOS_TOTAL:
		return {
			"channel_url": channel_url,
			"subscriber_count": int(subscriber_count),
			"qualified": False,
			"analysis_reason": "lt_3_long_videos",
		}

	# Step 3: Detect current cycle (requires valid upload_date)
	videos_dated = [v for v in videos_long if isinstance(v.get("upload_date"), date)]
	if not videos_dated:
		return {
			"channel_url": channel_url,
			"subscriber_count": int(subscriber_count),
			"qualified": False,
			"analysis_reason": "upload_date_missing",
		}

	# Deterministic ordering: date DESC, then video_id DESC.
	videos_dated.sort(
		key=lambda v: (
			v.get("upload_date"),
			v.get("video_id") if isinstance(v.get("video_id"), str) else "",
		),
		reverse=True,
	)

	cycle = _detect_current_cycle(videos_dated)
	cycle_videos = cycle.cycle_videos
	cycle_count = len(cycle_videos)

	# Step 4: Cycle metrics
	views: list[int] = []
	ratios: list[float] = []
	for v in cycle_videos:
		vc = v.get("view_count")
		view_count = int(vc) if isinstance(vc, int) else 0
		views.append(max(0, view_count))
		ratios.append((max(0, view_count) / float(subscriber_count)) if subscriber_count > 0 else 0.0)

	median_views = _median_int(views)
	max_views = max(views) if views else None
	median_views_ratio = _median_float(ratios)
	max_views_ratio = max(ratios) if ratios else None

	high_ratio_videos_count = sum(1 for r in ratios if r >= HIGH_RATIO_THRESHOLD)

	# Step 5: Decision rules
	qualified, decision_reason = _decision_reason(
		cycle_long_videos_count=cycle_count,
		high_ratio_videos_count=high_ratio_videos_count,
		median_views_ratio=median_views_ratio,
	)

	return {
		"channel_url": channel_url,
		"subscriber_count": int(subscriber_count),
		"cycle_start_date": cycle.cycle_start_date,
		"cycle_long_videos_count": int(cycle_count),
		"median_views": median_views,
		"max_views": max_views,
		"median_views_ratio": median_views_ratio,
		"max_views_ratio": max_views_ratio,
		"qualified": qualified,
		"analysis_reason": decision_reason,
	}


def _coerce_int(value: Any) -> int | None:
	if isinstance(value, bool):
		return None
	if isinstance(value, int):
		return value
	if isinstance(value, float):
		return int(value)
	if isinstance(value, str):
		try:
			return int(value)
		except ValueError:
			return None
	return None


def parse_args() -> argparse.Namespace:
	parser = argparse.ArgumentParser(description="Analyze channels (post yt-dlp), long-videos only")
	parser.add_argument(
		"--limit",
		"-n",
		type=int,
		default=None,
		help="Max channels to analyze in this run (default: no limit)",
	)
	parser.add_argument(
		"--batch-size",
		type=int,
		default=100,
		help="Batch size for bulk insertion (default: 100).",
	)
	parser.add_argument(
		"--force-single-insert",
		action="store_true",
		help="Force individual row insertion (disables bulk insert).",
	)
	return parser.parse_args()


def main() -> None:
	args = parse_args()

	async def _main_async() -> None:
		load_dotenv()
		await init_db()
		try:
			candidates = await claim_channels_for_analysis(
				limit=args.limit or 100
			)
			print(f"🔎 Pending channels for analysis: {len(candidates)}")

			buffer: list[dict[str, Any]] = []

			async def _flush_buffer() -> None:
				if not buffer:
					return
				print(f"💾 Flushing {len(buffer)} records...")
				await insert_channel_analysis_bulk(buffer)
				buffer.clear()

			for row in candidates:
				channel_url = row.get("channel_url")
				if not isinstance(channel_url, str) or not channel_url:
					continue

				subscriber_count = _coerce_int(row.get("subscriber_count"))

				try:
					result_row = await _analyze_one_channel(channel_url, subscriber_count)
					print(f"✅ analyzed: {channel_url}")
					
					if args.force_single_insert:
						await insert_channel_analysis(result_row)
					else:
						buffer.append(result_row)
						if len(buffer) >= args.batch_size:
							await _flush_buffer()

				except Exception as e:
					# Per requirements: do not abort the process.
					reason = f"error: {type(e).__name__}: {str(e)[:500]}"
					print(f"❌ failed: {channel_url} :: {reason}")
					
					fail_row = {
						"channel_url": channel_url,
						"subscriber_count": subscriber_count,
						"qualified": False,
						"analysis_reason": reason,
					}
					
					if args.force_single_insert:
						try:
							await insert_channel_analysis(fail_row)
						except Exception:
							pass
					else:
						buffer.append(fail_row)
						if len(buffer) >= args.batch_size:
							await _flush_buffer()

			# Final flush
			if not args.force_single_insert:
				await _flush_buffer()

		finally:
			await close_db()

	asyncio.run(_main_async())


if __name__ == "__main__":
	main()
