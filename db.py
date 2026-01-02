"""PostgreSQL persistence layer for YouTube discovery.

Design goals (per requirements):
- All DB logic lives here (no SQL / no connections in the scraper script).
- Async PostgreSQL via asyncpg with a shared connection pool.
- Safe for parallel workers: idempotent schema creation + INSERT ... ON CONFLICT DO NOTHING.
- Persist raw fields only; no business analysis or filtering.

Configuration:
- Set DATABASE_URL (recommended) or POSTGRES_DSN.
  Example:
    set DATABASE_URL=postgresql://user:pass@localhost:5432/mydb

Notes:
- We generate deterministic fields derived from video_id (video_url, thumbnail_url).
- We do not download thumbnails.
"""

from __future__ import annotations

import os
import uuid
from datetime import date, datetime, timezone
from typing import Any, Iterable

import asyncpg

_POOL: asyncpg.Pool | None = None


def _utcnow() -> datetime:
	return datetime.now(timezone.utc)


def _get_dsn(explicit_dsn: str | None) -> str:
	if explicit_dsn:
		return explicit_dsn
	dsn = os.getenv("DATABASE_URL") or os.getenv("POSTGRES_DSN")
	if not dsn:
		raise RuntimeError(
			"PostgreSQL DSN not configured. Set DATABASE_URL (or POSTGRES_DSN)."
		)
	return dsn


async def init_db(
	dsn: str | None = None,
	*,
	min_size: int = 1,
	max_size: int = 20,
) -> None:
	"""Initialize the asyncpg pool and create schema if needed.

	Safe to call multiple times and from multiple workers.
	"""
	global _POOL
	if _POOL is not None:
		return

	_POOL = await asyncpg.create_pool(
		dsn=_get_dsn(dsn),
		min_size=min_size,
		max_size=max_size,
		command_timeout=60,
		statement_cache_size=0,
	)

	async with _POOL.acquire() as conn:
		# Idempotent schema creation.
		await conn.execute(
			"""
			CREATE TABLE IF NOT EXISTS search_runs (
				id UUID PRIMARY KEY,
				query TEXT,
				mode TEXT,
				started_at TIMESTAMPTZ,
				finished_at TIMESTAMPTZ
			);
			"""
		)
		await conn.execute(
			"""
			CREATE TABLE IF NOT EXISTS videos_raw (
				video_id TEXT PRIMARY KEY,
				search_run_id UUID REFERENCES search_runs(id),
				query TEXT,
				video_url TEXT,
				channel_url TEXT,
				duration_text TEXT,
				views_text TEXT,
				published_text TEXT,
				thumbnail_url TEXT,
				video_type TEXT,
				is_multi_creator BOOLEAN,
				discovered_at TIMESTAMPTZ DEFAULT now()
			);
			"""
		)

		await conn.execute(
			"""
			CREATE TABLE IF NOT EXISTS videos_normalized (
				video_id TEXT PRIMARY KEY REFERENCES videos_raw(video_id),
				channel_url TEXT,
				query TEXT,
				views_estimated BIGINT,
				published_at_estimated TIMESTAMPTZ,
				duration_seconds_estimated INTEGER,
				validation_passed BOOLEAN,
				validation_reason TEXT,
				normalized_at TIMESTAMPTZ DEFAULT now()
			);
			"""
		)

		# Channel enrichment tables (raw channel metadata + last-N videos, and a processed marker).
		await conn.execute(
			"""
			CREATE TABLE IF NOT EXISTS channels_raw (
				channel_url TEXT PRIMARY KEY,
				channel_id TEXT,
				channel_name TEXT,
				subscriber_count BIGINT,
				is_verified BOOLEAN,
				extracted_at TIMESTAMPTZ DEFAULT now()
			);
			"""
		)

		try:
			await conn.execute("""
				ALTER TABLE channels_raw
				DROP COLUMN IF EXISTS channel_created_at,
				DROP COLUMN IF EXISTS country;
			""")
		except Exception:
			pass

		await conn.execute(
			"""
			CREATE TABLE IF NOT EXISTS channel_videos_raw (
				channel_url TEXT NOT NULL,
				video_id TEXT NOT NULL,
				upload_date TEXT,
				duration_seconds INTEGER,
				view_count BIGINT,
				PRIMARY KEY (channel_url, video_id)
			);
			"""
		)

		await conn.execute(
			"""
			CREATE TABLE IF NOT EXISTS channels_processed (
				channel_url TEXT PRIMARY KEY,
				processed_at TIMESTAMPTZ DEFAULT now(),
				status TEXT DEFAULT 'success'
			);
			"""
		)

		await conn.execute(
			"""
			CREATE TABLE IF NOT EXISTS channels_discovery_claims (
				channel_url TEXT PRIMARY KEY,
				claimed_at TIMESTAMPTZ DEFAULT now()
			);
			"""
		)

		# Final pipeline stage: channel-level analysis (post yt-dlp enrichment).
		# IMPORTANT: this table is created here only (no schema creation elsewhere).
		await conn.execute(
			"""
			CREATE TABLE IF NOT EXISTS channels_analysis (
				channel_url TEXT PRIMARY KEY,

				subscriber_count INTEGER,

				cycle_start_date DATE,
				cycle_long_videos_count INTEGER,

				median_views INTEGER,
				max_views INTEGER,

				median_views_ratio REAL,
				max_views_ratio REAL,

				qualified BOOLEAN,
				analysis_reason TEXT,

				analyzed_at TIMESTAMPTZ DEFAULT now()
			);
			"""
		)

		await conn.execute(
			"""
			CREATE TABLE IF NOT EXISTS channels_analysis_claims (
				channel_url TEXT PRIMARY KEY,
				claimed_at TIMESTAMPTZ DEFAULT now()
			);
			"""
		)

		# Post-analysis stage: channel scoring / ranking.
		await conn.execute(
			"""
			CREATE TABLE IF NOT EXISTS channels_score (
				channel_url TEXT PRIMARY KEY,

				final_score REAL,

				s_perf REAL,
				s_peak REAL,
				s_consistency REAL,
				s_size REAL,

				scored_at TIMESTAMPTZ DEFAULT now()
			);
			"""
		)

		# Migration for existing tables
		try:
			await conn.execute("""
				ALTER TABLE channels_processed ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'success';
			""")
		except Exception:
			# If it fails, we assume it exists or some other non-critical race.
			pass

		await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_videos_raw_channel_url
            ON videos_raw (channel_url);
        """)

		await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_videos_raw_discovered_at
            ON videos_raw (discovered_at);
        """)

		await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_videos_raw_search_run_id
            ON videos_raw (search_run_id);
        """)

		await conn.execute("""
			CREATE INDEX IF NOT EXISTS idx_videos_normalized_validation_passed
			ON videos_normalized (validation_passed);
		""")

		await conn.execute("""
			CREATE INDEX IF NOT EXISTS idx_videos_normalized_normalized_at
			ON videos_normalized (normalized_at);
		""")

		await conn.execute(
			"""
			CREATE INDEX IF NOT EXISTS idx_channels_processed_processed_at
			ON channels_processed (processed_at);
			"""
		)

		await conn.execute(
			"""
			CREATE INDEX IF NOT EXISTS idx_channel_videos_raw_channel_url
			ON channel_videos_raw (channel_url);
			"""
		)

		await conn.execute(
			"""
			CREATE INDEX IF NOT EXISTS idx_channels_raw_extracted_at
			ON channels_raw (extracted_at);
			"""
		)

		await conn.execute(
			"""
			CREATE INDEX IF NOT EXISTS idx_channels_analysis_analyzed_at
			ON channels_analysis (analyzed_at);
			"""
		)

		await conn.execute(
			"""
			CREATE INDEX IF NOT EXISTS idx_channels_score_scored_at
			ON channels_score (scored_at);
			"""
		)

		await conn.execute(
			"""
			CREATE INDEX IF NOT EXISTS idx_channels_score_final_score
			ON channels_score (final_score);
			"""
		)

		await conn.execute(
			"""
			CREATE INDEX IF NOT EXISTS idx_channel_videos_raw_upload_date
			ON channel_videos_raw (upload_date);
			"""
		)


def _parse_upload_date(value: str | None) -> date | None:
	"""Parse channel video upload_date into a date.

	Expected formats (best-effort, minimal):
	- 'YYYYMMDD'
	- 'YYYY-MM-DD'
	- 'YYYY/MM/DD'

	Returns None when parsing fails.
	"""
	if not value or not isinstance(value, str):
		return None
	s = value.strip()
	if not s:
		return None

	# Common yt-dlp style: YYYYMMDD
	if len(s) >= 8 and s[:8].isdigit():
		y = int(s[0:4])
		m = int(s[4:6])
		d = int(s[6:8])
		try:
			return date(y, m, d)
		except ValueError:
			return None

	# ISO-ish: YYYY-MM-DD or YYYY/MM/DD
	if len(s) >= 10 and s[4] in ("-", "/") and s[7] in ("-", "/"):
		try:
			y = int(s[0:4])
			m = int(s[5:7])
			d = int(s[8:10])
			return date(y, m, d)
		except ValueError:
			return None

	return None


async def fetch_channels_pending_analysis(*, limit: int | None = None) -> list[dict[str, Any]]:
	"""Select channels that exist in channels_raw but not yet in channels_analysis."""
	if limit is not None and limit <= 0:
		return []

	pool = _require_pool()
	base_sql = """
			SELECT r.channel_url, r.subscriber_count
			FROM channels_raw r
			LEFT JOIN channels_analysis a
				ON a.channel_url = r.channel_url
			WHERE r.channel_url IS NOT NULL
				AND r.channel_url <> ''
				AND a.channel_url IS NULL
			ORDER BY r.extracted_at ASC
	"""
	async with pool.acquire() as conn:
		if limit is None:
			rows = await conn.fetch(base_sql + ";")
		else:
			rows = await conn.fetch(base_sql + "\n\t\t\tLIMIT $1;", limit)
	return [dict(row) for row in rows]


async def claim_channels_for_analysis(*, limit: int) -> list[dict[str, Any]]:
	"""
	Atomically claim channels for analysis so multiple workers
	never analyze the same channel concurrently.
	"""
	pool = _require_pool()

	async with pool.acquire() as conn:
		claimed = await conn.fetch(
			"""
			WITH candidates AS (
				SELECT r.channel_url
				FROM channels_raw r
				LEFT JOIN channels_analysis a
					ON a.channel_url = r.channel_url
				LEFT JOIN channels_analysis_claims c
					ON c.channel_url = r.channel_url
				WHERE a.channel_url IS NULL
				  AND c.channel_url IS NULL
				ORDER BY r.extracted_at ASC
				LIMIT $1
			)
			INSERT INTO channels_analysis_claims (channel_url)
			SELECT channel_url FROM candidates
			ON CONFLICT DO NOTHING
			RETURNING channel_url;
			""",
			limit,
		)

	if not claimed:
		return []

	claimed_urls = [r["channel_url"] for r in claimed]

	async with pool.acquire() as conn:
		rows = await conn.fetch(
			"""
			SELECT r.*
			FROM channels_raw r
			WHERE r.channel_url = ANY($1::text[]);
			""",
			claimed_urls,
		)

	return [dict(r) for r in rows]


async def fetch_channel_long_videos(channel_url: str) -> list[dict[str, Any]]:
	"""Fetch long videos (duration_seconds >= 1200) for a channel.

	Returns dicts with:
	- video_id: str
	- upload_date: datetime.date (parsed) or None
	- duration_seconds: int|None
	- view_count: int|None
	"""
	if not channel_url:
		return []

	pool = _require_pool()
	async with pool.acquire() as conn:
		rows = await conn.fetch(
			"""
			SELECT video_id, upload_date, duration_seconds, view_count
			FROM channel_videos_raw
			WHERE channel_url = $1
				AND duration_seconds IS NOT NULL
				AND duration_seconds >= 1200;
			""",
			channel_url,
		)

	result: list[dict[str, Any]] = []
	for row in rows:
		upload_raw = row.get("upload_date")
		upload_dt = _parse_upload_date(upload_raw if isinstance(upload_raw, str) else None)
		result.append(
			{
				"video_id": str(row["video_id"]),
				"upload_date": upload_dt,
				"duration_seconds": int(row["duration_seconds"]) if row.get("duration_seconds") is not None else None,
				"view_count": int(row["view_count"]) if row.get("view_count") is not None else None,
			}
		)

	return result


async def insert_channel_analysis(row: dict[str, Any]) -> None:
	"""Insert one channels_analysis row.

	Idempotency:
	- Uses ON CONFLICT DO NOTHING (analysis is saved once per channel_url).
	"""
	channel_url = row.get("channel_url")
	if not isinstance(channel_url, str) or not channel_url:
		raise ValueError("channel_url is required")

	pool = _require_pool()
	async with pool.acquire() as conn:
		await conn.execute(
			"""
			INSERT INTO channels_analysis (
				channel_url,
				subscriber_count,
				cycle_start_date,
				cycle_long_videos_count,
				median_views,
				max_views,
				median_views_ratio,
				max_views_ratio,
				qualified,
				analysis_reason
			)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
			ON CONFLICT (channel_url) DO NOTHING;
			""",
			channel_url,
			row.get("subscriber_count") if isinstance(row.get("subscriber_count"), int) else None,
			row.get("cycle_start_date") if isinstance(row.get("cycle_start_date"), date) else None,
			row.get("cycle_long_videos_count") if isinstance(row.get("cycle_long_videos_count"), int) else None,
			row.get("median_views") if isinstance(row.get("median_views"), int) else None,
			row.get("max_views") if isinstance(row.get("max_views"), int) else None,
			float(row.get("median_views_ratio")) if isinstance(row.get("median_views_ratio"), (int, float)) else None,
			float(row.get("max_views_ratio")) if isinstance(row.get("max_views_ratio"), (int, float)) else None,
			row.get("qualified") if isinstance(row.get("qualified"), bool) else None,
			row.get("analysis_reason") if isinstance(row.get("analysis_reason"), str) else None,
		)


async def insert_channel_analysis_bulk(rows: list[dict[str, Any]]) -> int:
	"""Bulk insert channels_analysis rows.

	Idempotency:
	- Uses ON CONFLICT (channel_url) DO NOTHING.
	"""
	if not rows:
		return 0

	pool = _require_pool()

	channel_urls: list[str] = []
	subscriber_counts: list[int | None] = []
	cycle_start_dates: list[date | None] = []
	cycle_long_videos_counts: list[int | None] = []
	median_views_list: list[int | None] = []
	max_views_list: list[int | None] = []
	median_views_ratios: list[float | None] = []
	max_views_ratios: list[float | None] = []
	qualified_list: list[bool | None] = []
	analysis_reasons: list[str | None] = []

	for r in rows:
		channel_url = r.get("channel_url")
		if not isinstance(channel_url, str) or not channel_url:
			continue

		channel_urls.append(channel_url)
		subscriber_counts.append(r.get("subscriber_count") if isinstance(r.get("subscriber_count"), int) else None)
		cycle_start_dates.append(r.get("cycle_start_date") if isinstance(r.get("cycle_start_date"), date) else None)
		cycle_long_videos_counts.append(r.get("cycle_long_videos_count") if isinstance(r.get("cycle_long_videos_count"), int) else None)
		median_views_list.append(r.get("median_views") if isinstance(r.get("median_views"), int) else None)
		max_views_list.append(r.get("max_views") if isinstance(r.get("max_views"), int) else None)
		
		# Coerce floats safely
		med_r = r.get("median_views_ratio")
		median_views_ratios.append(float(med_r) if isinstance(med_r, (int, float)) else None)
		
		max_r = r.get("max_views_ratio")
		max_views_ratios.append(float(max_r) if isinstance(max_r, (int, float)) else None)
		
		qualified_list.append(r.get("qualified") if isinstance(r.get("qualified"), bool) else None)
		analysis_reasons.append(r.get("analysis_reason") if isinstance(r.get("analysis_reason"), str) else None)

	if not channel_urls:
		return 0

	async with pool.acquire() as conn:
		res = await conn.execute(
			"""
			INSERT INTO channels_analysis (
				channel_url,
				subscriber_count,
				cycle_start_date,
				cycle_long_videos_count,
				median_views,
				max_views,
				median_views_ratio,
				max_views_ratio,
				qualified,
				analysis_reason
			)
			SELECT
				v.channel_url,
				v.subscriber_count,
				v.cycle_start_date,
				v.cycle_long_videos_count,
				v.median_views,
				v.max_views,
				v.median_views_ratio,
				v.max_views_ratio,
				v.qualified,
				v.analysis_reason
			FROM UNNEST(
				$1::text[],
				$2::int[],
				$3::date[],
				$4::int[],
				$5::int[],
				$6::int[],
				$7::real[],
				$8::real[],
				$9::boolean[],
				$10::text[]
			) AS v(
				channel_url,
				subscriber_count,
				cycle_start_date,
				cycle_long_videos_count,
				median_views,
				max_views,
				median_views_ratio,
				max_views_ratio,
				qualified,
				analysis_reason
			)
			ON CONFLICT (channel_url) DO NOTHING;
			""",
			channel_urls,
			subscriber_counts,
			cycle_start_dates,
			cycle_long_videos_counts,
			median_views_list,
			max_views_list,
			median_views_ratios,
			max_views_ratios,
			qualified_list,
			analysis_reasons,
		)
		# "INSERT 0 123" -> 123
		try:
			return int(res.split(" ")[-1])
		except (IndexError, ValueError):
			return 0


async def fetch_channels_for_scoring(*, limit: int | None = None) -> list[dict[str, Any]]:
	"""Fetch channels_analysis rows for scoring.

	Scoring can be re-run; this function does NOT exclude channels already scored.
	"""
	if limit is not None and limit <= 0:
		return []

	pool = _require_pool()
	base_sql = """
			SELECT
				channel_url,
				subscriber_count,
				cycle_long_videos_count,
				median_views_ratio,
				max_views_ratio,
				qualified,
				analysis_reason
			FROM channels_analysis
			ORDER BY analyzed_at ASC
	"""
	async with pool.acquire() as conn:
		if limit is None:
			rows = await conn.fetch(base_sql + ";")
		else:
			rows = await conn.fetch(base_sql + "\n\t\t\tLIMIT $1;", limit)
	return [dict(r) for r in rows]


async def upsert_channel_score(row: dict[str, Any]) -> None:
	"""Upsert one channels_score row.

	Re-scoring contract:
	- channel_url is stable PK
	- re-running updates the score components and scored_at
	"""
	channel_url = row.get("channel_url")
	if not isinstance(channel_url, str) or not channel_url:
		raise ValueError("channel_url is required")

	pool = _require_pool()
	async with pool.acquire() as conn:
		await conn.execute(
			"""
			INSERT INTO channels_score (
				channel_url,
				final_score,
				s_perf,
				s_peak,
				s_consistency,
				s_size,
				scored_at
			)
			VALUES ($1, $2, $3, $4, $5, $6, COALESCE($7, now()))
			ON CONFLICT (channel_url) DO UPDATE SET
				final_score = EXCLUDED.final_score,
				s_perf = EXCLUDED.s_perf,
				s_peak = EXCLUDED.s_peak,
				s_consistency = EXCLUDED.s_consistency,
				s_size = EXCLUDED.s_size,
				scored_at = EXCLUDED.scored_at;
			""",
			channel_url,
			float(row.get("final_score")) if isinstance(row.get("final_score"), (int, float)) else None,
			float(row.get("s_perf")) if isinstance(row.get("s_perf"), (int, float)) else None,
			float(row.get("s_peak")) if isinstance(row.get("s_peak"), (int, float)) else None,
			float(row.get("s_consistency")) if isinstance(row.get("s_consistency"), (int, float)) else None,
			float(row.get("s_size")) if isinstance(row.get("s_size"), (int, float)) else None,
			row.get("scored_at") if isinstance(row.get("scored_at"), datetime) else None,
		)


async def upsert_channel_scores_bulk(rows: list[dict[str, Any]]) -> int:
	"""Bulk upsert channel scores.

	Returns:
		Number of rows inserted/updated.
	"""
	if not rows:
		return 0

	pool = _require_pool()

	channel_urls: list[str] = []
	final_scores: list[float | None] = []
	s_perfs: list[float | None] = []
	s_peaks: list[float | None] = []
	s_consistencies: list[float | None] = []
	s_sizes: list[float | None] = []
	scored_ats: list[datetime | None] = []

	for r in rows:
		channel_url = r.get("channel_url")
		if not isinstance(channel_url, str) or not channel_url:
			continue

		channel_urls.append(channel_url)

		def _f(k: str) -> float | None:
			v = r.get(k)
			return float(v) if isinstance(v, (int, float)) else None

		final_scores.append(_f("final_score"))
		s_perfs.append(_f("s_perf"))
		s_peaks.append(_f("s_peak"))
		s_consistencies.append(_f("s_consistency"))
		s_sizes.append(_f("s_size"))

		ts = r.get("scored_at")
		scored_ats.append(ts if isinstance(ts, datetime) else None)

	if not channel_urls:
		return 0

	async with pool.acquire() as conn:
		res = await conn.execute(
			"""
			INSERT INTO channels_score (
				channel_url,
				final_score,
				s_perf,
				s_peak,
				s_consistency,
				s_size,
				scored_at
			)
			SELECT
				v.channel_url,
				v.final_score,
				v.s_perf,
				v.s_peak,
				v.s_consistency,
				v.s_size,
				COALESCE(v.scored_at, now())
			FROM UNNEST(
				$1::text[],
				$2::real[],
				$3::real[],
				$4::real[],
				$5::real[],
				$6::real[],
				$7::timestamptz[]
			) AS v(
				channel_url,
				final_score,
				s_perf,
				s_peak,
				s_consistency,
				s_size,
				scored_at
			)
			ON CONFLICT (channel_url) DO UPDATE SET
				final_score = EXCLUDED.final_score,
				s_perf = EXCLUDED.s_perf,
				s_peak = EXCLUDED.s_peak,
				s_consistency = EXCLUDED.s_consistency,
				s_size = EXCLUDED.s_size,
				scored_at = EXCLUDED.scored_at;
			""",
			channel_urls,
			final_scores,
			s_perfs,
			s_peaks,
			s_consistencies,
			s_sizes,
			scored_ats,
		)
		# "INSERT 0 123" -> 123
		try:
			return int(res.split(" ")[-1])
		except (IndexError, ValueError):
			return 0


async def fetch_candidate_channel_urls(*, limit: int | None = None) -> list[str]:
	"""Return candidate channel URLs for enrichment.

	Selection rules (per pipeline contract):
	- Take channels that have at least one validated video (videos_normalized.validation_passed = true).
	- Exclude channels already in channels_processed.
	- Do NOT group by channel (duplicates are allowed).
	"""
	if limit is not None and limit <= 0:
		return []

	pool = _require_pool()
	base_sql = """
			SELECT n.channel_url
			FROM videos_normalized n
			LEFT JOIN channels_processed p
				ON p.channel_url = n.channel_url
			WHERE n.validation_passed = true
				AND n.channel_url IS NOT NULL
				AND n.channel_url <> ''
				AND p.channel_url IS NULL
			ORDER BY n.normalized_at ASC
	"""
	async with pool.acquire() as conn:
		if limit is None:
			rows = await conn.fetch(base_sql + ";")
		else:
			rows = await conn.fetch(base_sql + "\n\t\t\tLIMIT $1;", limit)
	return [str(r["channel_url"]) for r in rows if r.get("channel_url")]


async def claim_channels_for_discovery(limit: int) -> list[str]:
	"""Atomically claim candidate channels for discovery.

	This turns PostgreSQL into a distributed work queue for parallel workers.
	Only newly-claimed channel URLs are returned.
	"""
	if limit <= 0:
		return []

	pool = _require_pool()
	async with pool.acquire() as conn:
		rows = await conn.fetch(
			"""
			WITH candidates AS (
				SELECT
					n.channel_url,
					MIN(n.normalized_at) AS first_seen
				FROM videos_normalized n
				LEFT JOIN channels_processed p
					ON p.channel_url = n.channel_url
				LEFT JOIN channels_discovery_claims c
					ON c.channel_url = n.channel_url
				WHERE n.validation_passed = true
					AND n.channel_url IS NOT NULL
					AND n.channel_url <> ''
					AND p.channel_url IS NULL
					AND c.channel_url IS NULL
				GROUP BY n.channel_url
				ORDER BY first_seen ASC
				LIMIT $1
			)
			INSERT INTO channels_discovery_claims (channel_url)
			SELECT channel_url FROM candidates
			ON CONFLICT DO NOTHING
			RETURNING channel_url;
			""",
			limit,
		)

	return [str(r["channel_url"]) for r in rows if r.get("channel_url")]


async def is_channel_processed(channel_url: str) -> bool:
	"""Check if a channel has already been processed (yt-dlp executed)."""
	if not channel_url:
		return False
	pool = _require_pool()
	async with pool.acquire() as conn:
		row = await conn.fetchrow(
			"""
			SELECT 1
			FROM channels_processed
			WHERE channel_url = $1
			LIMIT 1;
			""",
			channel_url,
		)
	return row is not None


async def upsert_channel_raw(channel: dict[str, Any]) -> None:
	"""Upsert one raw channel row.

	Expected keys (nullable):
	- channel_url (required)
	- channel_id, channel_name, subscriber_count, is_verified, extracted_at
	"""
	channel_url = channel.get("channel_url")
	if not isinstance(channel_url, str) or not channel_url:
		raise ValueError("channel_url is required")

	pool = _require_pool()
	async with pool.acquire() as conn:
		await conn.execute(
			"""
			INSERT INTO channels_raw (
				channel_url,
				channel_id,
				channel_name,
				subscriber_count,
				is_verified,
				extracted_at
			)
			VALUES ($1, $2, $3, $4, $5, COALESCE($6, now()))
			ON CONFLICT (channel_url) DO UPDATE SET
				channel_id = COALESCE(EXCLUDED.channel_id, channels_raw.channel_id),
				channel_name = COALESCE(EXCLUDED.channel_name, channels_raw.channel_name),
				subscriber_count = COALESCE(EXCLUDED.subscriber_count, channels_raw.subscriber_count),
				is_verified = COALESCE(EXCLUDED.is_verified, channels_raw.is_verified),
				extracted_at = EXCLUDED.extracted_at;
			""",
			channel_url,
			channel.get("channel_id") if isinstance(channel.get("channel_id"), str) else None,
			channel.get("channel_name") if isinstance(channel.get("channel_name"), str) else None,
			channel.get("subscriber_count") if isinstance(channel.get("subscriber_count"), int) else None,
			channel.get("is_verified") if isinstance(channel.get("is_verified"), bool) else None,
			channel.get("extracted_at") if isinstance(channel.get("extracted_at"), datetime) else None,
		)


async def upsert_channel_videos_raw(channel_url: str, videos: list[dict[str, Any]]) -> tuple[int, int]:
	"""Batch upsert raw channel videos.

	Returns:
		(upserted_count, ignored_count)
	"""
	if not channel_url or not videos:
		return (0, 0)

	pool = _require_pool()

	channel_urls: list[str] = []
	video_ids: list[str] = []
	upload_dates: list[str | None] = []
	durations: list[int | None] = []
	view_counts: list[int | None] = []

	seen_video_ids = set()
	for v in videos:
		video_id = v.get("video_id")
		if not isinstance(video_id, str) or not video_id:
			continue
		if video_id in seen_video_ids:
			continue
		seen_video_ids.add(video_id)

		channel_urls.append(channel_url)
		video_ids.append(video_id)
		upload_dates.append(v.get("upload_date") if isinstance(v.get("upload_date"), str) else None)
		durations.append(v.get("duration_seconds") if isinstance(v.get("duration_seconds"), int) else None)
		view_counts.append(v.get("view_count") if isinstance(v.get("view_count"), int) else None)

	if not video_ids:
		return (0, 0)

	attempted = len(video_ids)
	async with pool.acquire() as conn:
		rows = await conn.fetch(
			"""
			INSERT INTO channel_videos_raw (
				channel_url,
				video_id,
				upload_date,
				duration_seconds,
				view_count
			)
			SELECT
				v.channel_url,
				v.video_id,
				v.upload_date,
				v.duration_seconds,
				v.view_count
			FROM UNNEST(
				$1::text[],
				$2::text[],
				$3::text[],
				$4::int[],
				$5::bigint[]
			) AS v(
				channel_url,
				video_id,
				upload_date,
				duration_seconds,
				view_count
			)
			ON CONFLICT (channel_url, video_id) DO UPDATE SET
				upload_date = COALESCE(EXCLUDED.upload_date, channel_videos_raw.upload_date),
				duration_seconds = COALESCE(EXCLUDED.duration_seconds, channel_videos_raw.duration_seconds),
				view_count = COALESCE(EXCLUDED.view_count, channel_videos_raw.view_count)
			RETURNING video_id;
			""",
			channel_urls,
			video_ids,
			upload_dates,
			durations,
			view_counts,
		)

	upserted = len(rows)
	ignored = max(0, attempted - upserted)
	return (upserted, ignored)


async def mark_channel_processed(
	channel_url: str,
	*,
	processed_at: datetime | None = None,
	status: str = "success",
) -> None:
	"""Mark a channel as processed (yt-dlp executed successfully or failed permanently)."""
	if not channel_url:
		raise ValueError("channel_url is required")

	pool = _require_pool()
	async with pool.acquire() as conn:
		await conn.execute(
			"""
			INSERT INTO channels_processed (channel_url, processed_at, status)
			VALUES ($1, COALESCE($2, now()), $3)
			ON CONFLICT (channel_url) DO UPDATE SET
				processed_at = EXCLUDED.processed_at,
				status = EXCLUDED.status;
			""",
			channel_url,
			processed_at,
			status,
		)


async def fetch_unprocessed_videos_raw(*, limit: int | None = None) -> list[dict[str, Any]]:
	"""Fetch raw videos that have not yet been normalized.

	Idempotency:
	- A raw row is considered processed if a matching `videos_normalized.video_id` exists.

	Args:
		limit: Maximum number of raw rows to return.
"""
	if limit is not None and limit <= 0:
		return []

	pool = _require_pool()
	base_sql = """
			SELECT
				r.video_id,
				r.channel_url,
				r.query,
				r.duration_text,
				r.views_text,
				r.published_text
			FROM videos_raw r
			LEFT JOIN videos_normalized n
				ON n.video_id = r.video_id
			WHERE n.video_id IS NULL
			ORDER BY r.discovered_at ASC
	"""
	async with pool.acquire() as conn:
		if limit is None:
			rows = await conn.fetch(base_sql + ";")
		else:
			rows = await conn.fetch(base_sql + "\n\t\t\tLIMIT $1;", limit)
	return [dict(row) for row in rows]


async def insert_videos_normalized(
	rows: list[dict[str, Any]],
) -> tuple[int, int]:
	"""Batch insert normalized videos.

	Rows are inserted with `ON CONFLICT DO NOTHING` to be safe for re-runs.

	Returns:
		(inserted_count, ignored_duplicates_count)
"""
	if not rows:
		return (0, 0)

	pool = _require_pool()

	video_ids: list[str] = []
	channel_urls: list[str | None] = []
	queries: list[str | None] = []
	views_estimated: list[int | None] = []
	published_at_estimated: list[datetime | None] = []
	duration_seconds_estimated: list[int | None] = []
	validation_passed: list[bool | None] = []
	validation_reason: list[str | None] = []
	normalized_at: list[datetime | None] = []

	seen_ids = set()
	for r in rows:
		video_id = r.get("video_id")
		if not isinstance(video_id, str) or not video_id:
			continue

		if video_id in seen_ids:
			continue
		seen_ids.add(video_id)

		video_ids.append(video_id)
		channel_urls.append(r.get("channel_url") if isinstance(r.get("channel_url"), str) else None)
		queries.append(r.get("query") if isinstance(r.get("query"), str) else None)
		views_estimated.append(r.get("views_estimated") if isinstance(r.get("views_estimated"), int) else None)
		published_at_estimated.append(
			r.get("published_at_estimated") if isinstance(r.get("published_at_estimated"), datetime) else None
		)
		duration_seconds_estimated.append(
			r.get("duration_seconds_estimated")
			if isinstance(r.get("duration_seconds_estimated"), int)
			else None
		)
		validation_passed.append(r.get("validation_passed") if isinstance(r.get("validation_passed"), bool) else None)
		validation_reason.append(r.get("validation_reason") if isinstance(r.get("validation_reason"), str) else None)
		normalized_at.append(r.get("normalized_at") if isinstance(r.get("normalized_at"), datetime) else None)

	if not video_ids:
		return (0, 0)

	attempted_count = len(video_ids)

	async with pool.acquire() as conn:
		inserted = await conn.fetch(
			"""
			INSERT INTO videos_normalized (
				video_id,
				channel_url,
				query,
				views_estimated,
				published_at_estimated,
				duration_seconds_estimated,
				validation_passed,
				validation_reason,
				normalized_at
			)
			SELECT
				v.video_id,
				v.channel_url,
				v.query,
				v.views_estimated,
				v.published_at_estimated,
				v.duration_seconds_estimated,
				v.validation_passed,
				v.validation_reason,
				COALESCE(v.normalized_at, now())
			FROM UNNEST(
				$1::text[],
				$2::text[],
				$3::text[],
				$4::bigint[],
				$5::timestamptz[],
				$6::int[],
				$7::boolean[],
				$8::text[],
				$9::timestamptz[]
			) AS v(
				video_id,
				channel_url,
				query,
				views_estimated,
				published_at_estimated,
				duration_seconds_estimated,
				validation_passed,
				validation_reason,
				normalized_at
			)
			ON CONFLICT (video_id) DO NOTHING
			RETURNING video_id;
			""",
			video_ids,
			channel_urls,
			queries,
			views_estimated,
			published_at_estimated,
			duration_seconds_estimated,
			validation_passed,
			validation_reason,
			normalized_at,
		)

	inserted_count = len(inserted)
	ignored = max(0, attempted_count - inserted_count)
	return (inserted_count, ignored)


async def close_db() -> None:
	"""Close the connection pool."""
	global _POOL
	if _POOL is None:
		return
	await _POOL.close()
	_POOL = None


def _require_pool() -> asyncpg.Pool:
	if _POOL is None:
		raise RuntimeError("DB not initialized. Call init_db() first.")
	return _POOL


async def create_search_run(query: str, mode: str = "exploration") -> uuid.UUID:
	"""Create a search run row and return its UUID."""
	pool = _require_pool()
	search_run_id = uuid.uuid4()
	started_at = _utcnow()

	async with pool.acquire() as conn:
		await conn.execute(
			"""
			INSERT INTO search_runs (id, query, mode, started_at, finished_at)
			VALUES ($1, $2, $3, $4, NULL);
			""",
			search_run_id,
			query,
			mode,
			started_at,
		)

	return search_run_id


def _thumbnail_url(video_id: str) -> str:
	return f"https://i.ytimg.com/vi/{video_id}/hqdefault.jpg"


def _video_url(video_id: str) -> str:
	return f"https://www.youtube.com/watch?v={video_id}"


def _extract_channel_url(raw: dict[str, Any]) -> str | None:
	# Scraper provides `channels: list[{name,url}]`.
	channels = raw.get("channels")
	if isinstance(channels, list) and channels:
		first = channels[0]
		if isinstance(first, dict):
			url = first.get("url")
			if isinstance(url, str) and url:
				return url
	# Allow future compatibility if scraper ever emits channel_url directly.
	url = raw.get("channel_url")
	if isinstance(url, str) and url:
		return url
	return None


async def insert_videos_raw(search_run_id: uuid.UUID, videos: list[dict[str, Any]]) -> tuple[int, int]:
	"""Batch insert raw video rows.

	Returns:
		(inserted_count, ignored_duplicates_count)

	Concurrency:
	- Uses a single INSERT with UNNEST + ON CONFLICT DO NOTHING.
	- Safe when multiple workers insert overlapping video_ids.
	"""
	if not videos:
		return (0, 0)

	pool = _require_pool()

	video_ids: list[str] = []
	search_run_ids: list[uuid.UUID] = []
	queries: list[str | None] = []
	video_urls: list[str | None] = []
	channel_urls: list[str | None] = []
	duration_texts: list[str | None] = []
	views_texts: list[str | None] = []
	published_texts: list[str | None] = []
	thumbnail_urls: list[str | None] = []
	video_types: list[str | None] = []
	is_multi_creators: list[bool | None] = []

	seen_ids = set()
	for raw in videos:
		video_id = raw.get("video_id")
		if not isinstance(video_id, str) or not video_id:
			# Cannot persist without primary key.
			continue

		if video_id in seen_ids:
			continue
		seen_ids.add(video_id)

		video_ids.append(video_id)
		search_run_ids.append(search_run_id)
		queries.append(raw.get("query") if isinstance(raw.get("query"), str) else None)
		video_urls.append(_video_url(video_id))
		channel_urls.append(_extract_channel_url(raw))
		duration_texts.append(raw.get("duration") if isinstance(raw.get("duration"), str) else None)
		views_texts.append(raw.get("views_text") if isinstance(raw.get("views_text"), str) else None)
		published_texts.append(
			raw.get("published_text") if isinstance(raw.get("published_text"), str) else None
		)
		thumbnail_urls.append(_thumbnail_url(video_id))
		video_types.append(raw.get("video_type") if isinstance(raw.get("video_type"), str) else None)
		is_multi_creators.append(bool(raw.get("is_multi_creator")) if raw.get("is_multi_creator") is not None else None)

	if not video_ids:
		return (0, 0)

	attempted_count = len(video_ids)

	async with pool.acquire() as conn:
		rows = await conn.fetch(
			"""
			INSERT INTO videos_raw (
				video_id,
				search_run_id,
				query,
				video_url,
				channel_url,
				duration_text,
				views_text,
				published_text,
				thumbnail_url,
				video_type,
				is_multi_creator
			)
			SELECT
				v.video_id,
				v.search_run_id,
				v.query,
				v.video_url,
				v.channel_url,
				v.duration_text,
				v.views_text,
				v.published_text,
				v.thumbnail_url,
				v.video_type,
				v.is_multi_creator
			FROM UNNEST(
				$1::text[],
				$2::uuid[],
				$3::text[],
				$4::text[],
				$5::text[],
				$6::text[],
				$7::text[],
				$8::text[],
				$9::text[],
				$10::text[],
				$11::boolean[]
			) AS v(
				video_id,
				search_run_id,
				query,
				video_url,
				channel_url,
				duration_text,
				views_text,
				published_text,
				thumbnail_url,
				video_type,
				is_multi_creator
			)
			ON CONFLICT (video_id) DO NOTHING
			RETURNING video_id;
			""",
			video_ids,
			search_run_ids,
			queries,
			video_urls,
			channel_urls,
			duration_texts,
			views_texts,
			published_texts,
			thumbnail_urls,
			video_types,
			is_multi_creators,
		)

	inserted_count = len(rows)
	ignored_duplicates_count = max(0, attempted_count - inserted_count)
	return (inserted_count, ignored_duplicates_count)


async def finish_search_run(search_run_id: uuid.UUID) -> None:
	"""Mark a search run as finished."""
	pool = _require_pool()
	finished_at = _utcnow()
	async with pool.acquire() as conn:
		await conn.execute(
			"""
			UPDATE search_runs
			SET finished_at = $2
			WHERE id = $1;
			""",
			search_run_id,
			finished_at,
		)
