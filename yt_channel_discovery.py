"""Channel enrichment module (yt-dlp, no analysis).

Responsibilities (strict):
- Read candidate channels from DB (based on videos_normalized.validation_passed).
- Run yt-dlp to fetch REAL channel metadata and last-N videos (no downloads).
- Persist raw metadata into channels_raw + channel_videos_raw.
- Mark channel as processed in channels_processed.

This module MUST NOT:
- create DB schema / pools / DSNs / SQL schema
- perform any channel analysis, filtering, or performance heuristics
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import subprocess
import sys
import threading
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Iterator
from urllib.request import urlopen, Request
from urllib.error import URLError
from dotenv import load_dotenv

from db import (
	claim_channels_for_discovery,
	close_db,
	init_db,
	is_channel_processed,
	mark_channel_processed,
	upsert_channel_raw,
	upsert_channel_videos_raw,
)


# Conservative default for parallel yt-dlp workers.
#
# IMPORTANT:
# - This module spawns multiple yt-dlp subprocesses (I/O bound).
# - Start conservative to avoid rate limits / network saturation.
# - Increase only after you validate stability.
MAX_WORKERS = 6


# Number of channels to claim per DB round-trip.
DISCOVERY_BATCH_SIZE = 200


PROJECT_ROOT = Path(__file__).resolve().parent
LOCAL_YTDLP_ROOT = PROJECT_ROOT / "yt-dlp" / "yt-dlp"
LOCAL_YTDLP_MAIN = LOCAL_YTDLP_ROOT / "yt_dlp" / "__main__.py"


def _local_ytdlp_env() -> dict[str, str]:
	"""Return an environment that resolves yt_dlp from the vendored source tree."""
	if not LOCAL_YTDLP_MAIN.is_file():
		raise RuntimeError(
			"Local yt-dlp source is missing. Expected "
			f"{LOCAL_YTDLP_MAIN}. Clone or vendor yt-dlp into yt-dlp/yt-dlp."
		)

	env = os.environ.copy()
	pythonpath = env.get("PYTHONPATH")
	env["PYTHONPATH"] = (
		str(LOCAL_YTDLP_ROOT)
		if not pythonpath
		else os.pathsep.join([str(LOCAL_YTDLP_ROOT), pythonpath])
	)
	return env


class _DBRunner:
	"""Runs db.py coroutines on a dedicated asyncio loop thread.

	Why:
	- db.py is async (asyncpg) and expects to live on a single event loop.
	- Workers run in threads; they must not create/await their own DB loops.
	- We use run_coroutine_threadsafe to execute DB work on the DB loop.
	"""

	def __init__(self) -> None:
		self._loop: asyncio.AbstractEventLoop | None = None
		self._thread: threading.Thread | None = None
		self._started = threading.Event()
		self._stopped = threading.Event()

	def start(self) -> None:
		# Start exactly one background thread that owns the asyncio event loop.
		# All async DB calls (asyncpg) must run on that loop.
		if self._thread is not None:
			return

		def _thread_main() -> None:
			# Dedicated event loop for all DB operations.
			# (asyncpg connections/pool are bound to a loop)
			if sys.platform == "win32":
				# ProactorEventLoop (default on Py3.8+ Win) can cause "WinError 121"
				# (Semaphore timeout) with asyncpg under load or specific network conditions.
				# SelectorEventLoop is more stable for pure socket I/O (no subprocesses needed here).
				loop = asyncio.WindowsSelectorEventLoopPolicy().new_event_loop()
			else:
				loop = asyncio.new_event_loop()
			
			asyncio.set_event_loop(loop)
			self._loop = loop
			self._started.set()
			try:
				loop.run_forever()
			finally:
				try:
					loop.close()
				finally:
					self._stopped.set()

		self._thread = threading.Thread(target=_thread_main, name="db-runner-loop", daemon=True)
		self._thread.start()
		self._started.wait(timeout=10)
		if self._loop is None:
			raise RuntimeError("Failed to start DB event loop thread")

	def run(self, coro: "asyncio.Future[Any] | asyncio.coroutines.Coroutine[Any, Any, Any]") -> Any:
		"""Run a coroutine on the DB loop and block for its result."""
		# NOTE: This is the bridge between worker threads and the DB loop.
		# Workers remain synchronous; DB work remains async, but centralized.
		loop = self._loop
		if loop is None:
			raise RuntimeError("DBRunner not started")
		future = asyncio.run_coroutine_threadsafe(coro, loop)
		return future.result()

	def stop(self) -> None:
		# Stop the DB loop thread.
		loop = self._loop
		thread = self._thread
		if loop is None or thread is None:
			return
		loop.call_soon_threadsafe(loop.stop)
		thread.join(timeout=10)
		self._loop = None
		self._thread = None


def _utcnow() -> datetime:
	return datetime.now(timezone.utc)


def _coerce_int(value: Any) -> int | None:
	if isinstance(value, bool):
		return None
	if isinstance(value, int):
		return value
	if isinstance(value, float):
		# yt-dlp can emit floats for some numeric fields
		return int(value)
	if isinstance(value, str):
		try:
			return int(value)
		except ValueError:
			return None
	return None


def _coerce_bool(value: Any) -> bool | None:
	if isinstance(value, bool):
		return value
	return None


def _string_or_none(value: Any) -> str | None:
	return value.strip() if isinstance(value, str) and value.strip() else None


def _best_thumbnail_url(thumbnails: Any, *, kind: str) -> str | None:
	"""Pick the most useful yt-dlp thumbnail for a channel or video."""
	if not isinstance(thumbnails, list):
		return None

	valid = [item for item in thumbnails if isinstance(item, dict) and _string_or_none(item.get("url"))]
	if not valid:
		return None

	preferred_id = f"{kind}_uncropped"
	for item in valid:
		if item.get("id") == preferred_id:
			return _string_or_none(item.get("url"))

	def area(item: dict[str, Any]) -> int:
		return (_coerce_int(item.get("width")) or 0) * (_coerce_int(item.get("height")) or 0)

	if kind == "avatar":
		square = [
			item for item in valid
			if (_coerce_int(item.get("width")) or 0) > 0
			and (_coerce_int(item.get("height")) or 0) > 0
			and 0.8 <= (_coerce_int(item.get("width")) or 0) / (_coerce_int(item.get("height")) or 1) <= 1.25
		]
		if square:
			return _string_or_none(max(square, key=area).get("url"))
	elif kind == "banner":
		wide = [
			item for item in valid
			if (_coerce_int(item.get("width")) or 0) > (_coerce_int(item.get("height")) or 0)
		]
		if wide:
			return _string_or_none(max(wide, key=area).get("url"))

	return _string_or_none(max(valid, key=area).get("url"))


# ── YouTube RSS feed helper ──────────────────────────────────────────
_YOUTUBE_RSS_URL = "https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}"
_RSS_ATOM_NS = "{http://www.w3.org/2005/Atom}"
_RSS_MEDIA_NS = "{http://search.yahoo.com/mrss/}"
_RSS_YT_NS = "{http://www.youtube.com/xml/schemas/2015}"


def fetch_rss_dates(
	channel_id: str,
	*,
	timeout_seconds: int = 15,
) -> tuple[dict[str, str], str | None]:
	"""Fetch video dates from the YouTube channel RSS/Atom feed.

	Returns:
		(video_dates, last_upload_date)

		video_dates:
			dict mapping video_id -> upload_date string (YYYYMMDD).
			Contains up to the 15 most recent videos.

		last_upload_date:
			The most recent upload date as YYYYMMDD, or None if the feed
			could not be fetched / parsed.
	"""
	if not channel_id:
		return {}, None

	url = _YOUTUBE_RSS_URL.format(channel_id=channel_id)
	try:
		req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
		with urlopen(req, timeout=timeout_seconds) as resp:
			xml_bytes = resp.read()
	except (URLError, OSError, TimeoutError) as e:
		print(f"\033[93m[{_utcnow().strftime('%H:%M:%S')}][rss] failed to fetch feed for {channel_id}: {e}\033[0m")
		return {}, None

	try:
		root = ET.fromstring(xml_bytes)
	except ET.ParseError as e:
		print(f"\033[93m[{_utcnow().strftime('%H:%M:%S')}][rss] failed to parse XML for {channel_id}: {e}\033[0m")
		return {}, None

	video_dates: dict[str, str] = {}
	latest_date: str | None = None

	for entry in root.findall(f"{_RSS_ATOM_NS}entry"):
		# Skip YouTube Shorts: their <link> href contains /shorts/
		link_el = entry.find(f"{_RSS_ATOM_NS}link")
		if link_el is not None:
			href = link_el.get("href", "")
			if "/shorts/" in href:
				continue

		# Extract video ID from <yt:videoId>VIDEO_ID</yt:videoId>
		vid_el = entry.find(f"{_RSS_YT_NS}videoId")
		pub_el = entry.find(f"{_RSS_ATOM_NS}published")

		if vid_el is None or pub_el is None:
			continue
		if vid_el.text is None or pub_el.text is None:
			continue

		video_id = vid_el.text.strip()
		# The <published> tag looks like: 2025-01-15T14:00:00+00:00
		try:
			dt = datetime.fromisoformat(pub_el.text.strip())
			date_str = dt.strftime("%Y%m%d")
		except ValueError:
			continue

		video_dates[video_id] = date_str
		if latest_date is None or date_str > latest_date:
			latest_date = date_str

	if video_dates:
		print(f"\033[92m[{_utcnow().strftime('%H:%M:%S')}][rss] got {len(video_dates)} dates from feed (latest={latest_date})\033[0m")

	return video_dates, latest_date



def run_ytdlp_channel_dump(
	channel_url: str,
	*,
	max_videos: int = 40,
	timeout_seconds: int = 180,
) -> dict[str, Any]:
	"""Run yt-dlp for a channel URL and return the parsed JSON.

	Implementation notes:
	- Uses subprocess (NO downloads).
	- This is intentionally synchronous and I/O bound.
	- Each worker thread executes one yt-dlp subprocess at a time.
	- Raises RuntimeError on failure.
	"""
	if not channel_url:
		raise ValueError("channel_url is required")
	if max_videos <= 0:
		max_videos = 1

	channel_url_videos = channel_url + "/videos"
	env = _local_ytdlp_env()
	cmd = [
		sys.executable,
		"-m",
		"yt_dlp",
		"--dump-single-json",
		"--flat-playlist",
		"--extractor-args",
		"youtubetab:approximate_date",
		"--playlist-end",
		str(max_videos),
		"--skip-download",
		"--no-warnings",
		channel_url_videos,
	]

	print(f"\033[94m[{_utcnow().strftime('%H:%M:%S')}][yt-dlp] fetching: {channel_url}...\033[0m")

	try:
		proc = subprocess.run(
			cmd,
			capture_output=True,
			text=True,
			timeout=timeout_seconds,
			env=env,
		)
	except subprocess.TimeoutExpired as e:
		raise RuntimeError(f"yt-dlp timeout for {channel_url}") from e

	if proc.returncode != 0:
		err = (proc.stderr or "").strip()
		out = (proc.stdout or "").strip()
		suffix = err or out
		msg = f"yt-dlp failed for {channel_url}"
		if suffix:
			msg += f": {suffix[:5000]}"
		raise RuntimeError(msg)

	stdout = (proc.stdout or "").strip()
	if not stdout:
		raise RuntimeError(f"yt-dlp produced empty output for {channel_url}")

	try:
		data = json.loads(stdout)
	except json.JSONDecodeError as e:
		raise RuntimeError(f"yt-dlp output was not valid JSON for {channel_url}") from e

	if not isinstance(data, dict):
		raise RuntimeError(f"yt-dlp JSON root was not an object for {channel_url}")
	return data


def parse_channel_raw(
	channel_url: str,
	dump: dict[str, Any],
	*,
	last_upload_date: str | None = None,
	first_upload_date: str | None = None,
) -> dict[str, Any]:
	"""Extract raw channel metadata from a yt-dlp dump.

Missing fields are left as None.
"""
	channel_id = dump.get("channel_id")
	if not isinstance(channel_id, str) or not channel_id:
		fallback = dump.get("uploader_id")
		channel_id = fallback if isinstance(fallback, str) and fallback else None

	channel_name = dump.get("channel")
	if not isinstance(channel_name, str) or not channel_name:
		fallback = dump.get("uploader")
		channel_name = fallback if isinstance(fallback, str) and fallback else None

	subscriber_count = _coerce_int(dump.get("subscriber_count"))
	if subscriber_count is None:
		subscriber_count = _coerce_int(dump.get("channel_follower_count"))

	is_verified = _coerce_bool(dump.get("verified"))
	channel_tags = dump.get("tags")
	if not isinstance(channel_tags, list):
		channel_tags = []

	return {
		"channel_url": channel_url,
		"channel_id": channel_id,
		"channel_name": channel_name,
		"subscriber_count": subscriber_count,
		"is_verified": is_verified,
		"channel_description": _string_or_none(dump.get("description")),
		"channel_tags": [tag for tag in (_string_or_none(value) for value in channel_tags) if tag],
		"avatar_url": _best_thumbnail_url(dump.get("thumbnails"), kind="avatar"),
		"banner_url": _best_thumbnail_url(dump.get("thumbnails"), kind="banner"),
		"uploader_id": _string_or_none(dump.get("uploader_id")),
		"uploader_url": _string_or_none(dump.get("uploader_url")),
		"last_upload_date": last_upload_date,
		"first_upload_date": first_upload_date,
		"extracted_at": _utcnow(),
	}


def _flatten_entries(entries: list[Any]) -> Iterator[dict[str, Any]]:
	"""Recursively yields video entries, skipping Shorts and Live playlists."""
	if not entries:
		return

	for entry in entries:
		if not isinstance(entry, dict):
			continue

		# If it's a nested playlist (e.g. "Videos", "Shorts", "Live")
		if "entries" in entry:
			title = entry.get("title", "").lower()
			# filter out shorts and live
			if "shorts" in title or "live" in title:
				continue
			
			# Recurse into "Videos" or other playlists
			yield from _flatten_entries(entry["entries"])
		else:
			# It's a video entry
			yield entry



def parse_channel_videos_raw(
	channel_url: str,
	dump: dict[str, Any],
	*,
	max_videos: int = 40,
) -> list[dict[str, Any]]:
	"""Extract last-N videos from a yt-dlp dump (flat playlist entries)."""
	raw_entries = dump.get("entries")
	if not raw_entries or not isinstance(raw_entries, list):
		return []

	results: list[dict[str, Any]] = []
	# Use our flattener to get actual videos, then slice
	flattened = _flatten_entries(raw_entries)

	count = 0
	for entry in flattened:
		if count >= max_videos:
			break
		
		if not isinstance(entry, dict):
			continue

		video_id = entry.get("id")
		if not isinstance(video_id, str) or not video_id:
			continue

		upload_date = entry.get("upload_date")
		upload_date_str = upload_date if isinstance(upload_date, str) and upload_date else None

		# Fallback: if upload_date is missing (e.g. flat-playlist), try timestamp/release_timestamp.
		if not upload_date_str:
			ts = entry.get("timestamp") or entry.get("release_timestamp")
			if isinstance(ts, (int, float)):
				try:
					# Format to YYYYMMDD to matches yt-dlp's standard upload_date format.
					dt = datetime.fromtimestamp(ts, tz=timezone.utc)
					upload_date_str = dt.strftime("%Y%m%d")
				except (ValueError, OSError):
					pass

		duration_seconds = _coerce_int(entry.get("duration"))
		view_count = _coerce_int(entry.get("view_count"))
		video_url = _string_or_none(entry.get("url")) or f"https://www.youtube.com/watch?v={video_id}"

		results.append(
			{
				"channel_url": channel_url,
				"video_id": video_id,
				"upload_date": upload_date_str,
				"duration_seconds": duration_seconds,
				"view_count": view_count,
				"title": _string_or_none(entry.get("title")),
				"video_url": video_url,
				"thumbnail_url": _best_thumbnail_url(entry.get("thumbnails"), kind="video"),
			}
		)
		count += 1

	return results


def process_one_channel(
	channel_url: str,
	db: _DBRunner,
	*,
	max_videos: int = 40,
	timeout_seconds: int = 180,
	# Note: DB operations are executed on the db runner loop.
) -> tuple[str, str]:
	"""Process a single channel (ONE job = ONE channel).

Returns:
	(channel_url, status)

	status is one of:
	- "processed": yt-dlp ok, persisted, and marked in channels_processed
	- "skipped": already present in channels_processed
	- "failed": yt-dlp or persistence failed (NOT marked processed)
"""
	if not channel_url:
		# Defensive: empty URL is a failed unit of work.
		return (channel_url, "failed")

	# Idempotency check: if already processed, skip.
	if bool(db.run(is_channel_processed(channel_url))):
		print(f"\033[93m[{_utcnow().strftime('%H:%M:%S')}][skip] already processed: {channel_url}\033[0m")
		return (channel_url, "skipped")

	try:
		# 1) Fetch real channel data with yt-dlp (subprocess).
		dump = run_ytdlp_channel_dump(
			channel_url,
			max_videos=max_videos,
			timeout_seconds=timeout_seconds,
		)
		# 2) Parse the JSON into raw rows.
		channel_row = parse_channel_raw(channel_url, dump)
		video_rows = parse_channel_videos_raw(channel_url, dump, max_videos=max_videos)

		# 3) Supplement dates from YouTube RSS feed.
		#    yt-dlp --flat-playlist often returns no upload_date.
		#    The RSS feed gives exact dates for the 15 most recent videos.
		channel_id = channel_row.get("channel_id")
		if channel_id:
			rss_dates, last_upload_date = fetch_rss_dates(channel_id)

			# Backfill missing upload_date on video rows.
			if rss_dates:
				backfilled = 0
				for vrow in video_rows:
					if not vrow.get("upload_date"):
						rss_date = rss_dates.get(vrow["video_id"])
						if rss_date:
							vrow["upload_date"] = rss_date
							backfilled += 1
				if backfilled:
					print(f"\033[96m[{_utcnow().strftime('%H:%M:%S')}][rss] backfilled {backfilled} missing dates\033[0m")

			# Store last_upload_date on the channel row.
			channel_row["last_upload_date"] = last_upload_date

		# 3b) Derive first_upload_date from the oldest non-Short video.
		#     Shorts typically have duration <= 60s.
		oldest_date: str | None = None
		for vrow in video_rows:
			vdate = vrow.get("upload_date")
			if not vdate:
				continue
			dur = vrow.get("duration_seconds")
			# Skip Shorts (duration <= 60s) when available
			if dur is not None and dur <= 60:
				continue
			if oldest_date is None or vdate < oldest_date:
				oldest_date = vdate
		channel_row["first_upload_date"] = oldest_date

		# 4) Persist raw data via db.py (async), executed on the DB loop thread.
		db.run(upsert_channel_raw(channel_row))
		db.run(upsert_channel_videos_raw(channel_url, video_rows))
		# 5) Mark processed ONLY after successful fetch + persistence.
		db.run(mark_channel_processed(channel_url, status="success"))
		print(f"\033[92m[{_utcnow().strftime('%H:%M:%S')}][ok] processed: {channel_url} (videos={len(video_rows)})\033[0m")
		return (channel_url, "processed")
	except Exception as e:
		msg = str(e)
		# Detect permanent failures (404 / channel gone / blocking).
		# "Failed to resolve url" is typical for 404 or deleted channels in yt-dlp.
		# "HTTP Error 404" is explicit.
		if "Failed to resolve url" in msg or "HTTP Error 404" in msg or "does the playlist exist" in msg:
			print(f"\033[91m[{_utcnow().strftime('%H:%M:%S')}][failed-permanent] {channel_url}: Marking as failed. Reason: {msg[:100]}\033[0m")
			# Mark as processed so we don't retry. Status = "failed".
			db.run(mark_channel_processed(channel_url, status="failed"))
			return (channel_url, "failed")

		# Transient failure: do NOT mark as processed. Retry next time.
		print(f"\033[91m[{_utcnow().strftime('%H:%M:%S')}][error] {channel_url}: {e}\033[0m")
		return (channel_url, "failed")


def run(
	*,
	limit_channels: int | None = None,
	max_videos: int = 50,
	dsn: str | None = None,
	timeout_seconds: int = 180,
	language: str = "es",
) -> None:
	"""Main orchestration: fetch candidates -> process in parallel workers.

	Concurrency model:
	- DB: single asyncio loop thread (asyncpg-safe)
	- Workers: ThreadPoolExecutor (each worker runs yt-dlp subprocess + then DB calls via _DBRunner)
	"""
	db = _DBRunner()
	print(f"\033[94m[info] starting DB loop thread\033[0m")
	db.start()
	try:
		# Keep asyncpg (db.py) on a single dedicated event loop/thread.
		# init_db() creates the pool and (as designed in db.py) will create tables idempotently.
		# Use a small pool size to allow high parallelism of jobs (e.g. 20 jobs * 4 conn = 80 total).
		db.run(init_db(dsn, min_size=1, max_size=4, language=language))
		print(f"\033[92m[info] running workers: max_workers={MAX_WORKERS}\033[0m")

		processed = 0
		skipped = 0
		failed = 0

		remaining = limit_channels
		while True:
			if remaining is not None and remaining <= 0:
				break

			batch_limit = DISCOVERY_BATCH_SIZE
			if remaining is not None:
				batch_limit = min(batch_limit, remaining)

			claimed = db.run(claim_channels_for_discovery(limit=batch_limit))
			if not claimed:
				break

			if remaining is not None:
				remaining -= len(claimed)

			print(f"\033[92m[info] claimed batch: {len(claimed)}\033[0m")

			with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
				try:
					# Submit 1 job per channel.
					futures = {
						executor.submit(
							process_one_channel,
							channel_url,
							db,
							max_videos=max_videos,
							timeout_seconds=timeout_seconds,
						): channel_url
						for channel_url in claimed
					}

					# Consume results as they complete (out-of-order completion is expected).
					for future in as_completed(futures):
						channel_url, status = future.result()
						if status == "processed":
							processed += 1
						elif status == "skipped":
							skipped += 1
						else:
							failed += 1
				except KeyboardInterrupt:
					print(f"\n\033[91m[{_utcnow().strftime('%H:%M:%S')}][system] Interrupted by user. Exiting immediately...\033[0m")
					# Force exit to kill threads immediately
					os._exit(1)

		print(f"\033[92m[{_utcnow().strftime('%H:%M:%S')}][done] processed={processed} skipped={skipped} failed={failed}\033[0m")
	finally:
		# Ensure pool is closed on the DB loop thread.
		try:
			print(f"\033[94m[{_utcnow().strftime('%H:%M:%S')}][info] closing DB pool\033[0m")
			db.run(close_db())
		except Exception:
			# Best-effort shutdown; do not mask prior errors.
			pass
		print(f"\033[94m[{_utcnow().strftime('%H:%M:%S')}][info] stopping DB loop thread\033[0m")
		db.stop()


def _build_arg_parser() -> argparse.ArgumentParser:
	p = argparse.ArgumentParser(description="YouTube channel enrichment (yt-dlp, no analysis)")
	p.add_argument("--limit-channels", type=int, default=None, help="Max channels to process (default: no limit)")
	p.add_argument("--max-videos", type=int, default=40)
	p.add_argument("--timeout-seconds", type=int, default=180)
	p.add_argument(
		"--dsn",
		type=str,
		default=None,
		help="Optional PostgreSQL DSN override (otherwise uses DATABASE_URL/POSTGRES_DSN)",
	)
	# Language selection
	lang_group = p.add_mutually_exclusive_group()
	lang_group.add_argument("--EN", action="store_const", const="en", dest="lang", help="Use English tables")
	lang_group.add_argument("--ES", action="store_const", const="es", dest="lang", help="Use Spanish tables (default)")
	p.set_defaults(lang="es")
	return p


if __name__ == "__main__":
	load_dotenv()
	args = _build_arg_parser().parse_args()

	# Keep orchestration synchronous; workers execute yt-dlp concurrently.
	run(
		limit_channels=args.limit_channels,
		max_videos=args.max_videos,
		dsn=args.dsn,
		timeout_seconds=args.timeout_seconds,
		language=args.lang,
	)
