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
	fetch_channel_long_videos,
	fetch_channels_pending_analysis,
	init_db,
	insert_channel_analysis,
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


async def _analyze_one_channel(channel_url: str, subscriber_count: int | None) -> None:
	"""Analyze a single channel and persist exactly one row in channels_analysis."""
	# Step 2: Pre-filter
	if subscriber_count is None:
		await insert_channel_analysis(
			{
				"channel_url": channel_url,
				"subscriber_count": None,
				"qualified": False,
				"analysis_reason": "subscriber_count_missing",
			}
		)
		return

	if subscriber_count < MIN_SUBSCRIBERS:
		await insert_channel_analysis(
			{
				"channel_url": channel_url,
				"subscriber_count": int(subscriber_count),
				"qualified": False,
				"analysis_reason": "subscriber_count_below_100",
			}
		)
		return

	videos_long = await fetch_channel_long_videos(channel_url)
	if len(videos_long) < MIN_LONG_VIDEOS_TOTAL:
		await insert_channel_analysis(
			{
				"channel_url": channel_url,
				"subscriber_count": int(subscriber_count),
				"qualified": False,
				"analysis_reason": "lt_3_long_videos",
			}
		)
		return

	# Step 3: Detect current cycle (requires valid upload_date)
	videos_dated = [v for v in videos_long if isinstance(v.get("upload_date"), date)]
	if not videos_dated:
		await insert_channel_analysis(
			{
				"channel_url": channel_url,
				"subscriber_count": int(subscriber_count),
				"qualified": False,
				"analysis_reason": "upload_date_missing",
			}
		)
		return

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

	await insert_channel_analysis(
		{
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
	)


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
	return parser.parse_args()


def main() -> None:
	args = parse_args()

	async def _main_async() -> None:
		load_dotenv()
		await init_db()
		try:
			candidates = await fetch_channels_pending_analysis(limit=args.limit)
			print(f"🔎 Pending channels for analysis: {len(candidates)}")

			for row in candidates:
				channel_url = row.get("channel_url")
				if not isinstance(channel_url, str) or not channel_url:
					continue

				subscriber_count = _coerce_int(row.get("subscriber_count"))

				try:
					await _analyze_one_channel(channel_url, subscriber_count)
					print(f"✅ analyzed: {channel_url}")
				except Exception as e:
					# Per requirements: do not abort the process.
					reason = f"error: {type(e).__name__}: {str(e)[:500]}"
					try:
						await insert_channel_analysis(
							{
								"channel_url": channel_url,
								"subscriber_count": subscriber_count,
								"qualified": False,
								"analysis_reason": reason,
							}
						)
					except Exception:
						# If even persisting the failure fails, keep going.
						pass
					print(f"❌ failed: {channel_url} :: {reason}")
		finally:
			await close_db()

	asyncio.run(_main_async())


if __name__ == "__main__":
	main()
