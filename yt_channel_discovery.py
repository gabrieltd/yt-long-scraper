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
import copy
import functools
import json
import multiprocessing
import os
import subprocess
import sys
import threading
import uuid
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from concurrent.futures import (
	ProcessPoolExecutor,
	ThreadPoolExecutor,
	TimeoutError as FutureTimeoutError,
	as_completed,
)
from pathlib import Path
from typing import Any, Iterator

import requests
from dotenv import load_dotenv

from youtube_oldest_video import (
	FirstVideoMetadata,
	NoPublicVideosError,
	OldestVideoError,
	YouTubeOldestVideoClient,
	fetch_first_video_with_ytdlp,
	terminate_active_fallback_processes,
)

from db import (
	claim_channels_for_discovery,
	close_db,
	count_pending_channels_for_discovery,
	init_db,
	is_channel_processed,
	mark_channel_processed,
	persist_channel_discovery_result,
	purge_pipeline_staging_tables,
	release_channel_discovery_claim,
	release_channel_discovery_claims,
	refresh_channel_stats,
)


# Conservative default for parallel yt-dlp workers.
#
# IMPORTANT:
# - This module spawns multiple yt-dlp subprocesses (I/O bound).
# - Start conservative to avoid rate limits / network saturation.
# - Increase only after you validate stability.
MAX_WORKERS = 6


# Number of channels to claim per DB round-trip.
DEFAULT_CLAIM_STALE_MINUTES = 60
EMPTY_CLAIM_RETRIES = 3
EMPTY_CLAIM_RETRY_SECONDS = 0.25


PROJECT_ROOT = Path(__file__).resolve().parent
LOCAL_YTDLP_ROOT = PROJECT_ROOT / "yt-dlp" / "yt-dlp"
LOCAL_YTDLP_MAIN = LOCAL_YTDLP_ROOT / "yt_dlp" / "__main__.py"
_STOP_EVENT = threading.Event()
_ACTIVE_YTDLP_LOCK = threading.Lock()
_ACTIVE_YTDLP_PROCESSES: set[subprocess.Popen[str]] = set()


def _claim_channel_batch(
	db: "_DBRunner",
	*,
	limit: int,
	claim_owner: str,
	stale_after_minutes: int,
	retries: int = EMPTY_CLAIM_RETRIES,
) -> list[str]:
	"""Acquire a batch, tolerating a transient empty result under contention."""
	for attempt in range(retries + 1):
		claimed = db.run(claim_channels_for_discovery(
			limit=limit,
			claim_owner=claim_owner,
			stale_after_minutes=stale_after_minutes,
		))
		if claimed or _STOP_EVENT.is_set() or attempt >= retries:
			return claimed
		delay = EMPTY_CLAIM_RETRY_SECONDS * (attempt + 1)
		print(
			f"\033[93m[info] empty claim; retrying shared queue "
			f"({attempt + 1}/{retries}) in {delay:.2f}s\033[0m"
		)
		_STOP_EVENT.wait(delay)
	return []


@functools.lru_cache(maxsize=16)
def _ytdlp_api_options(max_videos: int, timeout_seconds: int) -> dict[str, Any]:
	"""Parse the exact CLI options once per persistent yt-dlp worker process."""
	if str(LOCAL_YTDLP_ROOT) not in sys.path:
		sys.path.insert(0, str(LOCAL_YTDLP_ROOT))
	from yt_dlp import parse_options

	parsed = parse_options([
		"--dump-single-json",
		"--flat-playlist",
		"--extractor-args",
		"youtubetab:approximate_date",
		"--playlist-end",
		str(max(1, max_videos)),
		"--socket-timeout",
		str(timeout_seconds),
		"--skip-download",
		"--no-warnings",
	])
	return parsed.ydl_opts


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


def _terminate_process_tree(process: subprocess.Popen[Any]) -> None:
	"""Terminate one child process created by this module."""
	if process.poll() is not None:
		return
	try:
		if os.name == "nt":
			subprocess.run(
				["taskkill", "/PID", str(process.pid), "/T", "/F"],
				stdout=subprocess.DEVNULL,
				stderr=subprocess.DEVNULL,
				check=False,
				timeout=5,
			)
		else:
			process.terminate()
			try:
				process.wait(timeout=3)
			except subprocess.TimeoutExpired:
				process.kill()
	except (OSError, subprocess.SubprocessError):
		try:
			process.kill()
		except OSError:
			pass


def _request_stop() -> None:
	_STOP_EVENT.set()
	terminate_active_fallback_processes()
	with _ACTIVE_YTDLP_LOCK:
		processes = list(_ACTIVE_YTDLP_PROCESSES)
	for process in processes:
		_terminate_process_tree(process)


def _shutdown_process_pool(executor: ProcessPoolExecutor | None) -> None:
	if executor is None:
		return
	process_map = getattr(executor, "_processes", None) or {}
	processes = list(process_map.values())
	executor.shutdown(wait=False, cancel_futures=True)
	for process in processes:
		if process.is_alive():
			process.terminate()
	for process in processes:
		process.join(timeout=2)
		if process.is_alive():
			process.kill()


def run_ytdlp_channel_dump_api(
	channel_url: str,
	*,
	max_videos: int = 60,
	timeout_seconds: int = 180,
) -> dict[str, Any]:
	"""Extract a channel through yt-dlp's API inside a persistent worker."""
	if str(LOCAL_YTDLP_ROOT) not in sys.path:
		sys.path.insert(0, str(LOCAL_YTDLP_ROOT))
	from yt_dlp import YoutubeDL

	options = copy.deepcopy(_ytdlp_api_options(max_videos, timeout_seconds))
	with YoutubeDL(options) as ydl:
		info = ydl.extract_info(channel_url + "/videos", download=False)
		data = ydl.sanitize_info(info)
	if not isinstance(data, dict):
		raise RuntimeError(f"yt-dlp API returned invalid data for {channel_url}")
	return data


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


def resolve_first_video(
	channel_url: str,
	channel_id: str,
	client: YouTubeOldestVideoClient,
	*,
	fallback_timeout_seconds: int = 180,
	allow_ytdlp_fallback: bool = True,
) -> dict[str, Any]:
	"""Resolve first-video metadata, optionally falling back after an individual failure."""
	attempted_at = _utcnow()
	try:
		metadata = client.fetch_first_video(channel_id)
	except NoPublicVideosError as exc:
		return {
			"first_video_status": "no_public_videos",
			"first_video_checked_at": attempted_at,
			"first_video_last_attempt_at": attempted_at,
			"first_video_last_error": str(exc),
		}
	except OldestVideoError as primary_error:
		if not allow_ytdlp_fallback:
			return {
				"first_video_status": "pending",
				"first_video_last_attempt_at": attempted_at,
				"first_video_last_error": f"Innertube: {primary_error}",
			}
		try:
			metadata = fetch_first_video_with_ytdlp(
				channel_url,
				expected_channel_id=channel_id,
				known_video_id=primary_error.video_id,
				timeout_seconds=fallback_timeout_seconds,
				project_root=PROJECT_ROOT,
			)
		except NoPublicVideosError as exc:
			return {
				"first_video_status": "no_public_videos",
				"first_video_checked_at": attempted_at,
				"first_video_last_attempt_at": attempted_at,
				"first_video_last_error": str(exc),
			}
		except OldestVideoError as fallback_error:
			return {
				"first_video_status": "pending",
				"first_video_last_attempt_at": attempted_at,
				"first_video_last_error": (
					f"Innertube: {primary_error}; yt-dlp: {fallback_error}"
				),
			}

	if not isinstance(metadata, FirstVideoMetadata):
		raise RuntimeError("First-video resolver returned an invalid result")
	return {
		"first_video_id": metadata.video_id,
		"first_video_published_at": metadata.published_at,
		"first_video_checked_at": attempted_at,
		"first_video_last_attempt_at": attempted_at,
		"first_video_status": "success",
		"first_video_source": metadata.source,
		"first_video_last_error": None,
	}


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
_RSS_YT_NS = "{http://www.youtube.com/xml/schemas/2015}"
_RSS_THREAD_LOCAL = threading.local()


def _rss_session() -> requests.Session:
	session = getattr(_RSS_THREAD_LOCAL, "session", None)
	if session is None:
		session = requests.Session()
		session.headers.update({"User-Agent": "Mozilla/5.0"})
		_RSS_THREAD_LOCAL.session = session
	return session


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
		response = _rss_session().get(url, timeout=timeout_seconds)
		response.raise_for_status()
		xml_bytes = response.content
	except requests.RequestException as e:
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
	max_videos: int = 60,
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
	if _STOP_EVENT.is_set():
		raise InterruptedError("Channel discovery stopped by user")
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
		"--socket-timeout",
		str(timeout_seconds),
		"--skip-download",
		"--no-warnings",
		channel_url_videos,
	]

	print(f"\033[94m[{_utcnow().strftime('%H:%M:%S')}][yt-dlp] fetching: {channel_url}...\033[0m")

	try:
		process = subprocess.Popen(
			cmd,
			stdout=subprocess.PIPE,
			stderr=subprocess.PIPE,
			text=True,
			env=env,
		)
	except OSError as exc:
		raise RuntimeError(f"Unable to start yt-dlp for {channel_url}: {exc}") from exc
	with _ACTIVE_YTDLP_LOCK:
		_ACTIVE_YTDLP_PROCESSES.add(process)
	if _STOP_EVENT.is_set():
		_terminate_process_tree(process)
	try:
		stdout, stderr = process.communicate(timeout=timeout_seconds)
	except subprocess.TimeoutExpired as exc:
		_terminate_process_tree(process)
		process.communicate()
		raise RuntimeError(f"yt-dlp timeout for {channel_url}") from exc
	finally:
		with _ACTIVE_YTDLP_LOCK:
			_ACTIVE_YTDLP_PROCESSES.discard(process)

	if process.returncode != 0:
		err = (stderr or "").strip()
		out = (stdout or "").strip()
		suffix = err or out
		msg = f"yt-dlp failed for {channel_url}"
		if suffix:
			msg += f": {suffix[:5000]}"
		raise RuntimeError(msg)

	stdout = (stdout or "").strip()
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
		"first_video_status": "pending",
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
	max_videos: int = 60,
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
	claim_owner: str,
	oldest_video_client: YouTubeOldestVideoClient | None = None,
	oldest_video_client_error: str | None = None,
	http_executor: ThreadPoolExecutor | None = None,
	ytdlp_mode: str = "subprocess",
	ytdlp_executor: ProcessPoolExecutor | None = None,
	max_videos: int = 60,
	timeout_seconds: int = 180,
	preclaimed: bool = False,
	# Note: DB operations are executed on the db runner loop.
) -> tuple[str, str, str, str | None]:
	"""Process a single channel (ONE job = ONE channel).

Returns:
	(channel_url, discovery_status, first_video_status, first_video_source)

	status is one of:
	- "processed": yt-dlp ok, persisted, and marked in channels_processed
	- "skipped": already present in channels_processed
	- "failed": yt-dlp or persistence failed (NOT marked processed)
"""
	if not channel_url:
		# Defensive: empty URL is a failed unit of work.
		return (channel_url, "failed", "pending", None)
	if _STOP_EVENT.is_set():
		return (channel_url, "failed", "pending", None)

	# Atomic claims already exclude processed channels. Keep this check for
	# independent callers that did not obtain the URL through the claim query.
	if not preclaimed and bool(db.run(is_channel_processed(channel_url))):
		db.run(release_channel_discovery_claim(channel_url, claim_owner))
		print(f"\033[93m[{_utcnow().strftime('%H:%M:%S')}][skip] already processed: {channel_url}\033[0m")
		return (channel_url, "skipped", "pending", None)

	try:
		# 1) Fetch real channel data using the selected yt-dlp execution mode.
		if ytdlp_mode == "process-pool":
			if ytdlp_executor is None:
				raise RuntimeError("yt-dlp process pool is not initialized")
			future = ytdlp_executor.submit(
				run_ytdlp_channel_dump_api,
				channel_url,
				max_videos=max_videos,
				timeout_seconds=timeout_seconds,
			)
			try:
				dump = future.result(timeout=timeout_seconds)
			except FutureTimeoutError as exc:
				future.cancel()
				raise RuntimeError(f"yt-dlp process-pool timeout for {channel_url}") from exc
		else:
			dump = run_ytdlp_channel_dump(
				channel_url,
				max_videos=max_videos,
				timeout_seconds=timeout_seconds,
			)
		# 2) Parse the JSON into raw rows.
		channel_row = parse_channel_raw(channel_url, dump)
		video_rows = parse_channel_videos_raw(channel_url, dump, max_videos=max_videos)

		# 3) Resolve RSS and first-video metadata concurrently.
		#    yt-dlp --flat-playlist often returns no upload_date.
		#    The RSS feed gives exact dates for the 15 most recent videos.
		channel_id = channel_row.get("channel_id")
		rss_future = None
		first_video_future = None
		if channel_id and http_executor is not None:
			rss_future = http_executor.submit(fetch_rss_dates, channel_id)
			if oldest_video_client is not None:
				first_video_future = http_executor.submit(
					resolve_first_video,
					channel_url,
					channel_id,
					oldest_video_client,
					fallback_timeout_seconds=timeout_seconds,
				)

		if rss_future is not None:
			rss_dates, _rss_latest = rss_future.result()
		elif channel_id:
			rss_dates, _rss_latest = fetch_rss_dates(channel_id)
		else:
			rss_dates = {}

		# Exact RSS dates replace both missing and approximate flat-playlist dates.
		corrected = 0
		for vrow in video_rows:
			rss_date = rss_dates.get(vrow["video_id"])
			if rss_date and vrow.get("upload_date") != rss_date:
				vrow["upload_date"] = rss_date
				corrected += 1
		if corrected:
			print(f"\033[96m[{_utcnow().strftime('%H:%M:%S')}][rss] corrected {corrected} dates\033[0m")

		# Prefer the exact RSS date for the newest long video. When the RSS
		# window no longer contains it, preserve yt-dlp's approximate date.
		if video_rows:
			newest = video_rows[0]
			channel_row["last_upload_date"] = (
				rss_dates.get(newest["video_id"]) or newest.get("upload_date")
			)

		# 3b) Resolve the actual oldest public entry from the channel's Videos tab.
		first_video = None
		if first_video_future is not None:
			first_video = first_video_future.result()
		elif oldest_video_client is not None and channel_id:
			first_video = resolve_first_video(
				channel_url,
				channel_id,
				oldest_video_client,
				fallback_timeout_seconds=timeout_seconds,
			)
		if first_video is not None:
			channel_row.update(first_video)
			first_status = first_video["first_video_status"]
			if first_status == "success":
				print(
					f"\033[96m[{_utcnow().strftime('%H:%M:%S')}][first-video] "
					f"{channel_url}: {first_video['first_video_id']} "
					f"via {first_video['first_video_source']}\033[0m"
				)
			elif first_status == "no_public_videos":
				print(f"\033[93m[{_utcnow().strftime('%H:%M:%S')}][first-video-empty] {channel_url}\033[0m")
			else:
				print(
					f"\033[91m[{_utcnow().strftime('%H:%M:%S')}][first-video-error] "
					f"{channel_url}: {first_video.get('first_video_last_error')}\033[0m"
				)
		elif oldest_video_client_error:
			channel_row["first_video_last_error"] = oldest_video_client_error

		# 4) Persist raw data via db.py (async), executed on the DB loop thread.
		db.run(persist_channel_discovery_result(
			channel_row,
			video_rows,
			claim_owner=claim_owner,
			status="success",
		))
		print(f"\033[92m[{_utcnow().strftime('%H:%M:%S')}][ok] processed: {channel_url} (videos={len(video_rows)})\033[0m")
		return (
			channel_url,
			"processed",
			channel_row.get("first_video_status", "pending"),
			channel_row.get("first_video_source"),
		)
	except Exception as e:
		msg = str(e)
		# Detect permanent failures (604 / channel gone / blocking).
		# "Failed to resolve url" is typical for 404 or deleted channels in yt-dlp.
		# "HTTP Error 404" is explicit.
		if "Failed to resolve url" in msg or "HTTP Error 404" in msg or "does the playlist exist" in msg:
			print(f"\033[91m[{_utcnow().strftime('%H:%M:%S')}][failed-permanent] {channel_url}: Marking as failed. Reason: {msg[:100]}\033[0m")
			# Mark as processed so we don't retry. Status = "failed".
			db.run(mark_channel_processed(
				channel_url,
				status="failed",
				claim_owner=claim_owner,
			))
			return (channel_url, "failed", "pending", None)

		# Transient failure: do NOT mark as processed. Retry next time.
		print(f"\033[91m[{_utcnow().strftime('%H:%M:%S')}][error] {channel_url}: {e}\033[0m")
		return (channel_url, "failed", "pending", None)


def run(
	*,
	limit_channels: int | None = None,
	max_videos: int = 50,
	dsn: str | None = None,
	timeout_seconds: int = 180,
	first_video_timeout_seconds: int = 20,
	language: str = "es",
	max_workers: int = MAX_WORKERS,
	claim_batch_size: int | None = None,
	claim_stale_minutes: int = DEFAULT_CLAIM_STALE_MINUTES,
	ensure_schema: bool = True,
	finalize: bool = True,
	ytdlp_mode: str = "process-pool",
) -> None:
	"""Main orchestration: fetch candidates -> process in parallel workers.

	Concurrency model:
	- DB: single asyncio loop thread (asyncpg-safe)
	- Workers: ThreadPoolExecutor (each worker runs yt-dlp subprocess + then DB calls via _DBRunner)
	"""
	if max_workers < 1:
		raise ValueError("max_workers must be positive")
	if claim_stale_minutes < 1:
		raise ValueError("claim_stale_minutes must be positive")
	if ytdlp_mode not in {"subprocess", "process-pool"}:
		raise ValueError("ytdlp_mode must be subprocess or process-pool")
	batch_size = claim_batch_size or max_workers * 2
	if batch_size < 1:
		raise ValueError("claim_batch_size must be positive")

	_STOP_EVENT.clear()
	claim_owner = str(uuid.uuid4())
	db = _DBRunner()
	worker_executor: ThreadPoolExecutor | None = None
	http_executor: ThreadPoolExecutor | None = None
	ytdlp_executor: ProcessPoolExecutor | None = None
	print(f"\033[94m[info] starting DB loop thread\033[0m")
	db.start()
	try:
		# Keep asyncpg (db.py) on a single dedicated event loop/thread.
		# init_db() creates the pool and (as designed in db.py) will create tables idempotently.
		# Use a small pool size to allow high parallelism of jobs (e.g. 20 jobs * 4 conn = 80 total).
		db.run(init_db(
			dsn,
			min_size=1,
			max_size=4,
			language=language,
			ensure_schema=ensure_schema,
		))
		print(
			f"\033[92m[info] running workers: max_workers={max_workers} "
			f"claim_batch_size={batch_size} ytdlp_mode={ytdlp_mode}\033[0m"
		)
		oldest_video_client: YouTubeOldestVideoClient | None = None
		oldest_video_client_error: str | None = None
		try:
			oldest_video_client = YouTubeOldestVideoClient.initialize(
				timeout_seconds=first_video_timeout_seconds
			)
			print("\033[92m[info] YouTube first-video client initialized\033[0m")
		except OldestVideoError as exc:
			oldest_video_client_error = str(exc)
			print(
				"\033[91m[warning] first-video enrichment disabled for this run: "
				f"{exc}\033[0m"
			)

		processed = 0
		skipped = 0
		failed = 0
		first_video_counts = {
			"success": 0,
			"no_public_videos": 0,
			"pending": 0,
			"innertube": 0,
			"yt_dlp": 0,
		}
		if ytdlp_mode == "process-pool":
			ytdlp_executor = ProcessPoolExecutor(
				max_workers=max_workers,
				mp_context=multiprocessing.get_context("spawn"),
			)
		worker_slots = max_workers * 2 if ytdlp_executor is not None else max_workers
		worker_executor = ThreadPoolExecutor(max_workers=worker_slots)
		http_executor = ThreadPoolExecutor(max_workers=max_workers * 2)

		remaining = limit_channels
		while True:
			if _STOP_EVENT.is_set():
				break
			if remaining is not None and remaining <= 0:
				break

			batch_limit = batch_size
			if remaining is not None:
				batch_limit = min(batch_limit, remaining)

			claimed = _claim_channel_batch(
				db,
				limit=batch_limit,
				claim_owner=claim_owner,
				stale_after_minutes=claim_stale_minutes,
			)
			if not claimed:
				break

			if remaining is not None:
				remaining -= len(claimed)

			print(f"\033[92m[info] claimed batch: {len(claimed)}\033[0m")

			futures = {}
			try:
				# Submit 1 job per channel. In process-pool mode, the larger
				# thread window overlaps HTTP persistence with later extraction.
				futures = {
					worker_executor.submit(
							process_one_channel,
							channel_url,
							db,
							claim_owner=claim_owner,
							oldest_video_client=oldest_video_client,
							oldest_video_client_error=oldest_video_client_error,
							http_executor=http_executor,
							ytdlp_mode=ytdlp_mode,
							ytdlp_executor=ytdlp_executor,
							max_videos=max_videos,
							timeout_seconds=timeout_seconds,
							preclaimed=True,
						): channel_url
						for channel_url in claimed
				}

				# Consume results as they complete (out-of-order completion is expected).
				for future in as_completed(futures):
					channel_url, status, first_status, first_source = future.result()
					if status == "processed":
						processed += 1
						first_video_counts[first_status] = first_video_counts.get(first_status, 0) + 1
						if first_source:
							first_video_counts[first_source] = first_video_counts.get(first_source, 0) + 1
					elif status == "skipped":
						skipped += 1
					else:
						failed += 1
			except KeyboardInterrupt:
				print(f"\n\033[91m[{_utcnow().strftime('%H:%M:%S')}][system] Interrupted by user. Stopping...\033[0m")
				_request_stop()
				for future in futures:
					future.cancel()
				db.run(release_channel_discovery_claims(claim_owner))
				raise

		worker_executor.shutdown(wait=True)
		worker_executor = None
		http_executor.shutdown(wait=True)
		http_executor = None
		if ytdlp_executor is not None:
			ytdlp_executor.shutdown(wait=True)
			ytdlp_executor = None

		if finalize and processed:
			print(f"\033[94m[{_utcnow().strftime('%H:%M:%S')}][stats] refreshing channel stats...\033[0m")
			refreshed = bool(db.run(refresh_channel_stats(language)))
			status = "refreshed" if refreshed else "already refreshing elsewhere"
			print(f"\033[92m[{_utcnow().strftime('%H:%M:%S')}][stats] {status}\033[0m")

		if finalize:
			pending = int(db.run(count_pending_channels_for_discovery()))
			if pending:
				print(
					f"\033[93m[{_utcnow().strftime('%H:%M:%S')}][purge-skip] "
					f"preserving staging: {pending} channels remain pending\033[0m"
				)
			else:
				print(f"\033[94m[{_utcnow().strftime('%H:%M:%S')}][purge] truncating pipeline staging tables for language={language}...\033[0m")
				purged_tables = db.run(purge_pipeline_staging_tables(language))
				print(f"\033[92m[{_utcnow().strftime('%H:%M:%S')}][purge] truncated: {', '.join(purged_tables)}\033[0m")

		print(f"\033[92m[{_utcnow().strftime('%H:%M:%S')}][done] processed={processed} skipped={skipped} failed={failed}\033[0m")
		print(
			f"\033[92m[{_utcnow().strftime('%H:%M:%S')}][first-video-totals] "
			f"success={first_video_counts['success']} "
			f"empty={first_video_counts['no_public_videos']} "
			f"pending={first_video_counts['pending']} "
			f"innertube={first_video_counts['innertube']} "
			f"yt-dlp={first_video_counts['yt_dlp']}\033[0m"
		)
	except KeyboardInterrupt:
		_request_stop()
		try:
			db.run(release_channel_discovery_claims(claim_owner))
		except Exception:
			pass
		raise
	finally:
		if _STOP_EVENT.is_set():
			try:
				db.run(release_channel_discovery_claims(claim_owner))
			except Exception:
				pass
		if worker_executor is not None:
			worker_executor.shutdown(wait=False, cancel_futures=True)
		if http_executor is not None:
			http_executor.shutdown(wait=False, cancel_futures=True)
		if ytdlp_executor is not None:
			_shutdown_process_pool(ytdlp_executor)
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
	p.add_argument("--max-videos", type=int, default=60)
	p.add_argument("--max-workers", type=int, default=MAX_WORKERS)
	p.add_argument("--claim-batch-size", type=int, default=None)
	p.add_argument(
		"--claim-stale-minutes",
		type=int,
		default=DEFAULT_CLAIM_STALE_MINUTES,
	)
	p.add_argument("--timeout-seconds", type=int, default=180)
	p.add_argument("--first-video-timeout-seconds", type=int, default=20)
	p.add_argument("--skip-schema", action="store_false", dest="ensure_schema")
	p.add_argument("--skip-finalize", action="store_false", dest="finalize")
	p.add_argument(
		"--ytdlp-mode",
		choices=("subprocess", "process-pool"),
		default="process-pool",
	)
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
	p.set_defaults(lang="es", ensure_schema=True, finalize=True)
	return p


if __name__ == "__main__":
	load_dotenv()
	args = _build_arg_parser().parse_args()

	# Keep orchestration synchronous; workers execute yt-dlp concurrently.
	try:
		run(
			limit_channels=args.limit_channels,
			max_videos=args.max_videos,
			dsn=args.dsn,
			timeout_seconds=args.timeout_seconds,
			first_video_timeout_seconds=args.first_video_timeout_seconds,
			language=args.lang,
			max_workers=args.max_workers,
			claim_batch_size=args.claim_batch_size,
			claim_stale_minutes=args.claim_stale_minutes,
			ensure_schema=args.ensure_schema,
			finalize=args.finalize,
			ytdlp_mode=args.ytdlp_mode,
		)
	except KeyboardInterrupt:
		_request_stop()
		print("\n\033[91m[system] Stopped by user\033[0m")
