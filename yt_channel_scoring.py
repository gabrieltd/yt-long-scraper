"""Channel scoring module (runs AFTER yt_channel_analysis.py).

Strict contract (per requirements):
- MUST NOT run yt-dlp.
- MUST NOT use Playwright.
- MUST NOT recalculate analysis metrics.
- MUST read exclusively from channels_analysis.
- MUST NOT create DB pools / direct connections / schema here.
- MUST persist via db.py.

Purpose:
- Assign a numeric score to already analyzed channels to enable ranking.

Scoring is deterministic, explainable, and easy to adjust (weights are explicit).
"""

from __future__ import annotations

import argparse
import asyncio
import math
from typing import Any

from dotenv import load_dotenv

from db import (
	close_db,
	fetch_channels_for_scoring,
	init_db,
	upsert_channel_score,
	upsert_channel_scores_bulk,
)


# ---- Weights (fixed) ----
W_PERF = 0.40
W_PEAK = 0.25
W_CONSISTENCY = 0.20
W_SIZE = 0.15


def _clamp(value: float, lo: float = 0.0, hi: float = 1.0) -> float:
	if value < lo:
		return lo
	if value > hi:
		return hi
	return value


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


def _coerce_float(value: Any) -> float | None:
	if isinstance(value, bool):
		return None
	if isinstance(value, (int, float)):
		return float(value)
	if isinstance(value, str):
		try:
			return float(value)
		except ValueError:
			return None
	return None


def _score_components(
	*,
	subscriber_count: int,
	cycle_long_videos_count: int,
	median_views_ratio: float,
	max_views_ratio: float,
) -> tuple[float, float, float, float, float]:
	"""Compute normalized components and final score.

	All components are normalized to [0.0, 1.0].
	"""
	s_perf = _clamp(median_views_ratio / 1.0, 0.0, 1.0)
	s_peak = _clamp(max_views_ratio / 2.0, 0.0, 1.0)

	# log2(cycle_count) / log2(10)
	if cycle_long_videos_count <= 0:
		s_consistency = 0.0
	else:
		s_consistency = _clamp(math.log2(float(cycle_long_videos_count)) / math.log2(10.0), 0.0, 1.0)

	# 1 - clamp(log10(subscribers)/6, 0, 1)
	if subscriber_count <= 0:
		s_size = 1.0
	else:
		s_size = 1.0 - _clamp(math.log10(float(subscriber_count)) / 6.0, 0.0, 1.0)

	final_score = (
		W_PERF * s_perf
		+ W_PEAK * s_peak
		+ W_CONSISTENCY * s_consistency
		+ W_SIZE * s_size
	)
	final_score = _clamp(final_score, 0.0, 1.0)
	return (final_score, s_perf, s_peak, s_consistency, s_size)


def _missing_reason(
	*,
	subscriber_count: int | None,
	cycle_long_videos_count: int | None,
	median_views_ratio: float | None,
	max_views_ratio: float | None,
	qualified: bool | None,
) -> str | None:
	if qualified is False:
		return "qualified_false"
	if qualified is None:
		return "qualified_missing"
	if subscriber_count is None:
		return "subscriber_count_missing"
	if cycle_long_videos_count is None:
		return "cycle_long_videos_count_missing"
	if median_views_ratio is None:
		return "median_views_ratio_missing"
	if max_views_ratio is None:
		return "max_views_ratio_missing"
	return None


def _score_one(row: dict[str, Any]) -> dict[str, Any] | None:
	"""Calculate score for one channel row.

	Returns:
		Dict with score fields ready for DB upsert, or None if skipped.
		If validation fails/excluded, returns a dict with final_score=0.
	"""
	channel_url = row.get("channel_url")
	if not isinstance(channel_url, str) or not channel_url:
		return None

	subscriber_count = _coerce_int(row.get("subscriber_count"))
	cycle_long_videos_count = _coerce_int(row.get("cycle_long_videos_count"))
	median_views_ratio = _coerce_float(row.get("median_views_ratio"))
	max_views_ratio = _coerce_float(row.get("max_views_ratio"))
	qualified = row.get("qualified") if isinstance(row.get("qualified"), bool) else None

	reason = _missing_reason(
		subscriber_count=subscriber_count,
		cycle_long_videos_count=cycle_long_videos_count,
		median_views_ratio=median_views_ratio,
		max_views_ratio=max_views_ratio,
		qualified=qualified,
	)

	if reason is not None:
		# Per requirements: final_score = 0 when excluded.
		print(f"⛔ score=0 :: {channel_url} :: {reason}")
		return {
			"channel_url": channel_url,
			"final_score": 0.0,
			"s_perf": 0.0,
			"s_peak": 0.0,
			"s_consistency": 0.0,
			"s_size": 0.0,
		}

	final_score, s_perf, s_peak, s_consistency, s_size = _score_components(
		subscriber_count=int(subscriber_count),
		cycle_long_videos_count=int(cycle_long_videos_count),
		median_views_ratio=float(median_views_ratio),
		max_views_ratio=float(max_views_ratio),
	)

	print(f"✅ calculated: {channel_url} :: {final_score:.4f}")
	return {
		"channel_url": channel_url,
		"final_score": final_score,
		"s_perf": s_perf,
		"s_peak": s_peak,
		"s_consistency": s_consistency,
		"s_size": s_size,
	}


def parse_args() -> argparse.Namespace:
	parser = argparse.ArgumentParser(description="Score analyzed channels (channels_analysis -> channels_score)")
	parser.add_argument(
		"--limit",
		"-n",
		type=int,
		default=None,
		help="Max channels to score in this run (default: no limit)",
	)
	parser.add_argument(
		"--individual",
		action="store_true",
		help="Upsert scores one by one immediately (slower, easier debugging)",
	)
	parser.add_argument(
		"--batch-size",
		type=int,
		default=100,
		help="Batch size for bulk upsert (default: 100)",
	)
	return parser.parse_args()


def main() -> None:
	args = parse_args()

	async def _main_async() -> None:
		load_dotenv()
		await init_db()
		try:
			rows = await fetch_channels_for_scoring(limit=args.limit)
			print(f"🔢 Channels fetched for scoring: {len(rows)}")

			buffer: list[dict[str, Any]] = []

			for r in rows:
				channel_url = r.get("channel_url")
				if not isinstance(channel_url, str) or not channel_url:
					continue
				
				score_data = None
				try:
					score_data = _score_one(r)
				except Exception as e:
					# Per requirements: do not abort; persist score=0.
					reason = f"error: {type(e).__name__}: {str(e)[:500]}"
					score_data = {
						"channel_url": channel_url,
						"final_score": 0.0,
						"s_perf": 0.0,
						"s_peak": 0.0,
						"s_consistency": 0.0,
						"s_size": 0.0,
					}
					print(f"❌ score calculation failed: {channel_url} :: {reason}")

				if not score_data:
					continue

				if args.individual:
					try:
						await upsert_channel_score(score_data)
						# print(f"  -> saved: {channel_url}")
					except Exception as e:
						print(f"  -> save failed: {channel_url} : {e}")
				else:
					buffer.append(score_data)
					if len(buffer) >= args.batch_size:
						count = await upsert_channel_scores_bulk(buffer)
						print(f"📦 Bulk upserted {count} scores...")
						buffer = []

			# Final flush
			if buffer and not args.individual:
				count = await upsert_channel_scores_bulk(buffer)
				print(f"📦 Bulk upserted final {count} scores.")
		finally:
			await close_db()

	asyncio.run(_main_async())


if __name__ == "__main__":
	main()
