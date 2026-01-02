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
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Iterator
from dotenv import load_dotenv

from db import (
	close_db,
	fetch_candidate_channel_urls,
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



def run_ytdlp_channel_dump(
	channel_url: str,
	*,
	max_videos: int = 25,
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
		channel_url,
	]

	print(f"\033[94m[{_utcnow().strftime('%H:%M:%S')}][yt-dlp] fetching: {channel_url}...\033[0m")

	try:
		proc = subprocess.run(
			cmd,
			capture_output=True,
			text=True,
			timeout=timeout_seconds,
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


def parse_channel_raw(channel_url: str, dump: dict[str, Any]) -> dict[str, Any]:
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

	return {
		"channel_url": channel_url,
		"channel_id": channel_id,
		"channel_name": channel_name,
		"subscriber_count": subscriber_count,
		"is_verified": is_verified,
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
	max_videos: int = 25,
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

		results.append(
			{
				"channel_url": channel_url,
				"video_id": video_id,
				"upload_date": upload_date_str,
				"duration_seconds": duration_seconds,
				"view_count": view_count,
			}
		)
		count += 1

	return results


def process_one_channel(
	channel_url: str,
	db: _DBRunner,
	*,
	max_videos: int = 25,
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

		# 3) Persist raw data via db.py (async), executed on the DB loop thread.
		db.run(upsert_channel_raw(channel_row))
		db.run(upsert_channel_videos_raw(channel_url, video_rows))
		# 4) Mark processed ONLY after successful fetch + persistence.
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
	max_videos: int = 25,
	dsn: str | None = None,
	timeout_seconds: int = 180,
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
		db.run(init_db(dsn))
		# Fetch candidate channel URLs exactly as provided by db.py (NO grouping).
		candidates = db.run(fetch_candidate_channel_urls(limit=limit_channels))
		print(f"\033[92m[info] candidates fetched: {len(candidates)}\033[0m")
		print(f"\033[92m[info] running workers: max_workers={MAX_WORKERS}\033[0m")

		processed = 0
		skipped = 0
		failed = 0

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
					for channel_url in candidates
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
	p.add_argument("--max-videos", type=int, default=25)
	p.add_argument("--timeout-seconds", type=int, default=180)
	p.add_argument(
		"--dsn",
		type=str,
		default=None,
		help="Optional PostgreSQL DSN override (otherwise uses DATABASE_URL/POSTGRES_DSN)",
	)
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
	)
