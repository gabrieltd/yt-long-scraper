"""SQLite persistence layer for YouTube discovery.

Refactored from Postgres to use SQLite (via aiosqlite).
"""

from __future__ import annotations

import os
import uuid
import asyncio
from datetime import datetime, timezone
from typing import Any

import aiosqlite

_DB_CONN: aiosqlite.Connection | None = None
DB_FILE = "youtube.db"


def _utcnow() -> datetime:
	return datetime.now(timezone.utc)


async def init_db(dsn: str | None = None) -> None:
	"""Initialize the SQLite connection and schema."""
	global _DB_CONN
	if _DB_CONN is not None:
		return

	# dsn arg is ignored, using local file
	_DB_CONN = await aiosqlite.connect(DB_FILE)
	_DB_CONN.row_factory = aiosqlite.Row

	await _DB_CONN.execute("PRAGMA journal_mode=WAL;")
	await _DB_CONN.execute("PRAGMA foreign_keys=ON;")

	# Schema creation
	await _DB_CONN.execute("""
		CREATE TABLE IF NOT EXISTS search_runs (
			id TEXT PRIMARY KEY,
			query TEXT,
			mode TEXT,
			started_at TEXT,
			finished_at TEXT
		);
	""")

	# videos_raw: stores raw scrape data
	await _DB_CONN.execute("""
		CREATE TABLE IF NOT EXISTS videos_raw (
			video_id TEXT PRIMARY KEY,
			search_run_id TEXT REFERENCES search_runs(id),
			query TEXT,
			video_url TEXT,
			channel_url TEXT,
			duration_text TEXT,
			views_text TEXT,
			published_text TEXT,
			thumbnail_url TEXT,
			video_type TEXT,
			is_multi_creator INTEGER,
			discovered_at TEXT DEFAULT CURRENT_TIMESTAMP
		);
	""")

	# videos_normalized: filtered and normalized data
	await _DB_CONN.execute("""
		CREATE TABLE IF NOT EXISTS videos_normalized (
			video_id TEXT PRIMARY KEY REFERENCES videos_raw(video_id),
			channel_url TEXT,
			query TEXT,
			views_estimated INTEGER,
			published_at_estimated TEXT,
			duration_seconds_estimated INTEGER,
			validation_passed INTEGER,
			validation_reason TEXT,
			normalized_at TEXT DEFAULT CURRENT_TIMESTAMP
		);
	""")

	# channels_raw: channel metadata from yt-dlp
	await _DB_CONN.execute("""
		CREATE TABLE IF NOT EXISTS channels_raw (
			channel_url TEXT PRIMARY KEY,
			channel_id TEXT,
			channel_name TEXT,
			subscriber_count INTEGER,
			is_verified INTEGER,
			extracted_at TEXT DEFAULT CURRENT_TIMESTAMP
		);
	""")

	# channel_videos_raw: last-N videos from channel fetch
	await _DB_CONN.execute("""
		CREATE TABLE IF NOT EXISTS channel_videos_raw (
			channel_url TEXT NOT NULL,
			video_id TEXT NOT NULL,
			upload_date TEXT,
			duration_seconds INTEGER,
			view_count INTEGER,
			PRIMARY KEY (channel_url, video_id)
		);
	""")

	# channels_processed: status tracking
	await _DB_CONN.execute("""
		CREATE TABLE IF NOT EXISTS channels_processed (
			channel_url TEXT PRIMARY KEY,
			processed_at TEXT DEFAULT CURRENT_TIMESTAMP,
			status TEXT DEFAULT 'success'
		);
	""")

	# channels_discovery_claims: worker coordination
	await _DB_CONN.execute("""
		CREATE TABLE IF NOT EXISTS channels_discovery_claims (
			channel_url TEXT PRIMARY KEY,
			claimed_at TEXT DEFAULT CURRENT_TIMESTAMP
		);
	""")

	# Indices
	indices = [
		"CREATE INDEX IF NOT EXISTS idx_videos_raw_channel_url ON videos_raw (channel_url);",
		"CREATE INDEX IF NOT EXISTS idx_videos_raw_discovered_at ON videos_raw (discovered_at);",
		"CREATE INDEX IF NOT EXISTS idx_videos_raw_search_run_id ON videos_raw (search_run_id);",
		"CREATE INDEX IF NOT EXISTS idx_videos_normalized_validation_passed ON videos_normalized (validation_passed);",
		"CREATE INDEX IF NOT EXISTS idx_videos_normalized_normalized_at ON videos_normalized (normalized_at);",
		"CREATE INDEX IF NOT EXISTS idx_channels_processed_processed_at ON channels_processed (processed_at);",
		"CREATE INDEX IF NOT EXISTS idx_channel_videos_raw_channel_url ON channel_videos_raw (channel_url);",
		"CREATE INDEX IF NOT EXISTS idx_channels_raw_extracted_at ON channels_raw (extracted_at);",
	]
	for idx in indices:
		await _DB_CONN.execute(idx)

	await _DB_CONN.commit()


async def close_db() -> None:
	global _DB_CONN
	if _DB_CONN is None:
		return
	await _DB_CONN.close()
	_DB_CONN = None


def _require_conn() -> aiosqlite.Connection:
	if _DB_CONN is None:
		raise RuntimeError("DB not initialized. Call init_db() first.")
	return _DB_CONN


def _adapt_datetime(dt: datetime | None) -> str | None:
	if dt is None:
		return None
	if dt.tzinfo is None:
		dt = dt.replace(tzinfo=timezone.utc)
	return dt.isoformat()


async def create_search_run(query: str, mode: str = "exploration") -> uuid.UUID:
	"""Create a search run row and return its UUID."""
	conn = _require_conn()
	run_id = uuid.uuid4()
	started_at = _adapt_datetime(_utcnow())
	await conn.execute(
		"INSERT INTO search_runs (id, query, mode, started_at) VALUES (?, ?, ?, ?)",
		(str(run_id), query, mode, started_at)
	)
	await conn.commit()
	return run_id


async def finish_search_run(search_run_id: uuid.UUID) -> None:
	"""Mark a search run as finished."""
	conn = _require_conn()
	finished_at = _adapt_datetime(_utcnow())
	await conn.execute(
		"UPDATE search_runs SET finished_at = ? WHERE id = ?",
		(finished_at, str(search_run_id))
	)
	await conn.commit()


async def get_executed_queries() -> set[str]:
	"""Return a set of distinct queries that have been logged in search_runs."""
	conn = _require_conn()
	async with conn.execute("SELECT DISTINCT query FROM search_runs") as cursor:
		rows = await cursor.fetchall()
	
	# rows are Row objects (behaving like dicts/tuples)
	return {row["query"] for row in rows if row["query"]}


async def insert_videos_raw(search_run_id: uuid.UUID, videos: list[dict[str, Any]]) -> tuple[int, int]:
	"""Batch insert raw video rows."""
	if not videos:
		return (0, 0)
	conn = _require_conn()

	tuples = []
	seen = set()
	for v in videos:
		vid = v.get("video_id")
		if not vid or not isinstance(vid, str):
			continue
		if vid in seen:
			continue
		seen.add(vid)

		# Helpers logic inlined
		video_url = v.get("video_url") or f"https://www.youtube.com/watch?v={vid}"
		channel_url = v.get("channel_url")
		if not channel_url and v.get("channels") and isinstance(v.get("channels"), list):
			# Extract from channels list if needed
			try:
				channel_url = v.get("channels")[0].get("url")
			except (IndexError, AttributeError):
				pass
		
		thumbnail_url = v.get("thumbnail_url") or f"https://i.ytimg.com/vi/{vid}/hqdefault.jpg"

		tuples.append((
			vid,
			str(search_run_id),
			v.get("query"),
			video_url,
			channel_url,
			v.get("duration"),
			v.get("views_text"),
			v.get("published_text"),
			thumbnail_url,
			v.get("video_type"),
			1 if v.get("is_multi_creator") else 0
		))

	if not tuples:
		return (0, 0)

	cursor = await conn.executemany(
		"""INSERT OR IGNORE INTO videos_raw (
			video_id, search_run_id, query, video_url, channel_url, 
			duration_text, views_text, published_text, thumbnail_url, 
			video_type, is_multi_creator
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
		tuples
	)
	await conn.commit()

	inserted_count = cursor.rowcount
	# Ignored includes:
	# 1. Duplicates within the input list (skipped before tuples)
	# 2. Invalid inputs (skipped before tuples)
	# 3. Duplicates in DB (INSERT OR IGNORE failed)
	ignored_count = len(videos) - inserted_count
	return inserted_count, ignored_count


async def fetch_unprocessed_videos_raw(limit: int | None = None) -> list[dict[str, Any]]:
	"""Fetch raw videos that have not yet been normalized."""
	conn = _require_conn()
	sql = """
		SELECT r.video_id, r.channel_url, r.query, r.duration_text, r.views_text, r.published_text
		FROM videos_raw r
		LEFT JOIN videos_normalized n ON n.video_id = r.video_id
		WHERE n.video_id IS NULL
		ORDER BY r.discovered_at ASC
	"""
	if limit:
		sql += f" LIMIT {limit}"

	async with conn.execute(sql) as cursor:
		rows = await cursor.fetchall()

	return [dict(row) for row in rows]


async def insert_videos_normalized(rows: list[dict[str, Any]]) -> tuple[int, int]:
	"""Batch insert normalized videos."""
	if not rows:
		return (0, 0)
	conn = _require_conn()

	tuples = []
	seen = set()
	for r in rows:
		vid = r.get("video_id")
		if not vid or not isinstance(vid, str):
			continue
		if vid in seen:
			continue
		seen.add(vid)

		tuples.append((
			vid,
			r.get("channel_url"),
			r.get("query"),
			r.get("views_estimated"),
			_adapt_datetime(r.get("published_at_estimated")),
			r.get("duration_seconds_estimated"),
			1 if r.get("validation_passed") else 0,
			r.get("validation_reason"),
			_adapt_datetime(r.get("normalized_at") or _utcnow())
		))

	cursor = await conn.executemany(
		"""INSERT OR IGNORE INTO videos_normalized (
			video_id, channel_url, query, views_estimated, published_at_estimated,
			duration_seconds_estimated, validation_passed, validation_reason, normalized_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
		tuples
	)
	await conn.commit()
	return cursor.rowcount, len(tuples) - cursor.rowcount


async def claim_channels_for_discovery(limit: int) -> list[str]:
	"""Atomically claim candidate channels for discovery."""
	if limit <= 0:
		return []
	conn = _require_conn()

	# 1. Select candidates that are validated but not processed and not claimed
	select_sql = """
		SELECT n.channel_url
		FROM videos_normalized n
		LEFT JOIN channels_processed p ON p.channel_url = n.channel_url
		LEFT JOIN channels_discovery_claims c ON c.channel_url = n.channel_url
		WHERE n.validation_passed = 1
		  AND n.channel_url IS NOT NULL 
		  AND n.channel_url <> ''
		  AND p.channel_url IS NULL
		  AND c.channel_url IS NULL
		GROUP BY n.channel_url
		ORDER BY MIN(n.normalized_at) ASC
		LIMIT ?
	"""

	async with conn.execute(select_sql, (limit,)) as cursor:
		rows = await cursor.fetchall()

	candidates = [r["channel_url"] for r in rows]
	if not candidates:
		return []

	# 2. Insert into claims
	claim_tuples = [(url, _adapt_datetime(_utcnow())) for url in candidates]
	
	try:
		await conn.executemany(
			"INSERT OR IGNORE INTO channels_discovery_claims (channel_url, claimed_at) VALUES (?, ?)",
			claim_tuples
		)
		await conn.commit()
	except Exception:
		# In case of race condition or error
		pass

	return candidates


async def upsert_channel_raw(channel: dict[str, Any]) -> None:
	"""Upsert one raw channel row."""
	conn = _require_conn()
	url = channel.get("channel_url")
	if not url:
		raise ValueError("channel_url is required")

	fields = [
		url,
		channel.get("channel_id"),
		channel.get("channel_name"),
		channel.get("subscriber_count"),
		1 if channel.get("is_verified") else 0,
		_adapt_datetime(channel.get("extracted_at") or _utcnow())
	]

	await conn.execute("""
		INSERT INTO channels_raw (channel_url, channel_id, channel_name, subscriber_count, is_verified, extracted_at)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT(channel_url) DO UPDATE SET
			channel_id=coalesce(excluded.channel_id, channels_raw.channel_id),
			channel_name=coalesce(excluded.channel_name, channels_raw.channel_name),
			subscriber_count=coalesce(excluded.subscriber_count, channels_raw.subscriber_count),
			is_verified=coalesce(excluded.is_verified, channels_raw.is_verified),
			extracted_at=excluded.extracted_at
	""", fields)
	await conn.commit()


async def upsert_channel_videos_raw(channel_url: str, videos: list[dict[str, Any]]) -> tuple[int, int]:
	"""Batch upsert raw channel videos."""
	if not videos:
		return (0, 0)
	conn = _require_conn()

	tuples = []
	seen = set()
	for v in videos:
		vid = v.get("video_id")
		if not vid or not isinstance(vid, str):
			continue
		if vid in seen:
			continue
		seen.add(vid)

		tuples.append((
			channel_url,
			vid,
			v.get("upload_date"),
			v.get("duration_seconds"),
			v.get("view_count")
		))

	if not tuples:
		return (0, 0)

	cursor = await conn.executemany("""
		INSERT INTO channel_videos_raw (channel_url, video_id, upload_date, duration_seconds, view_count)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(channel_url, video_id) DO UPDATE SET
			upload_date=coalesce(excluded.upload_date, channel_videos_raw.upload_date),
			duration_seconds=coalesce(excluded.duration_seconds, channel_videos_raw.duration_seconds),
			view_count=coalesce(excluded.view_count, channel_videos_raw.view_count)
	""", tuples)
	await conn.commit()
	
	# rowcount for upsert is implementation specific, returning approx
	return cursor.rowcount, 0


async def mark_channel_processed(channel_url: str, *, processed_at: datetime | None = None, status: str = "success") -> None:
	"""Mark a channel as processed."""
	conn = _require_conn()
	p_at = _adapt_datetime(processed_at or _utcnow())

	await conn.execute("""
		INSERT INTO channels_processed (channel_url, processed_at, status)
		VALUES (?, ?, ?)
		ON CONFLICT(channel_url) DO UPDATE SET
			processed_at=excluded.processed_at,
			status=excluded.status
	""", (channel_url, p_at, status))
	await conn.commit()


async def is_channel_processed(channel_url: str) -> bool:
	"""Check if a channel has already been processed."""
	if not channel_url:
		return False
	conn = _require_conn()
	async with conn.execute("SELECT 1 FROM channels_processed WHERE channel_url = ?", (channel_url,)) as cursor:
		ret = await cursor.fetchone()
	return ret is not None
