from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable

import db
from dotenv import load_dotenv

def _utcnow() -> datetime:
	return datetime.now(timezone.utc)


def normalize_text(value: str | None) -> str | None:
	"""Basic text normalization: strip + collapse whitespace."""
	if value is None:
		return None
	if not isinstance(value, str):
		return None
	text = value.strip()
	if not text:
		return None
	text = re.sub(r"\s+", " ", text)
	return text


_VIEWS_NUMBER_RE = re.compile(
	r"(?P<num>(?:\d{1,3}(?:[\.,]\d{3})+|\d+)(?:[\.,]\d+)?)\s*(?P<suf>[kmb]|mil|millon|millones|millón|billones|billon|bn)?",
	flags=re.IGNORECASE,
)


def parse_views_text(views_text: str | None) -> int | None:
	"""Parse a YouTube views string into an integer.

	Handles common formats:
	- "1,234 views", "1.234 visualizaciones"
	- "1.2K views", "3,4 mil"
	- "2.1M", "1B"
"""
	text = normalize_text(views_text)
	if not text:
		return None

	lower = text.lower()
	# Common “no views” patterns.
	# Common “no views” patterns.
	if any(p in lower for p in ("no views", "sin vistas")):
		return 0

	# Remove words around the number.
	cleaned = lower
	for token in (
		"views",
		"view",
		"vistas",
		"visualizaciones",
		"reproducciones",
		"de",
		"•",
	):
		cleaned = cleaned.replace(token, " ")
	cleaned = normalize_text(cleaned) or ""

	m = _VIEWS_NUMBER_RE.search(cleaned)
	if not m:
		return None

	num_raw = m.group("num")
	suf_raw = (m.group("suf") or "").lower()

	# Normalize thousand separators / decimal separators.
	# Strategy:
	# - If both '.' and ',' appear, assume last separator is decimal and others are thousands.
	# - If only one appears, treat it as decimal when suffix exists, else as thousands.
	try:
		num = _parse_human_number(num_raw, has_suffix=bool(suf_raw))
	except ValueError:
		return None

	multiplier = 1
	if suf_raw in ("k", "mil"):
		multiplier = 1_000
	elif suf_raw in ("m", "millon", "millón", "millones"):
		multiplier = 1_000_000
	elif suf_raw in ("b", "bn", "billon", "billones"):
		multiplier = 1_000_000_000
	elif suf_raw == "mb":
		# Edge-case: regex might capture 'mb' from some locale; interpret as millions.
		multiplier = 1_000_000

	value = int(num * multiplier)
	return max(0, value)


def _parse_human_number(num_raw: str, *, has_suffix: bool) -> float:
	"""Parse numbers like '1,234', '1.234', '1,2' into float."""
	s = num_raw.strip()
	if not s:
		raise ValueError("empty")

	if "," in s and "." in s:
		# Decide decimal by the last seen separator.
		last_comma = s.rfind(",")
		last_dot = s.rfind(".")
		if last_comma > last_dot:
			# comma is decimal
			thousands_sep = "."
			decimal_sep = ","
		else:
			thousands_sep = ","
			decimal_sep = "."
		s = s.replace(thousands_sep, "")
		s = s.replace(decimal_sep, ".")
		return float(s)

	if "," in s:
		if has_suffix:
			# likely decimal in many locales: "3,4 mil"
			return float(s.replace(",", "."))
		# likely thousands: "1,234"
		return float(s.replace(",", ""))

	if "." in s:
		if has_suffix:
			# "1.2K" is decimal
			return float(s)
		# could be thousands: "1.234" (ES) OR decimal; assume thousands when 3 digits after.
		parts = s.split(".")
		if len(parts) == 2 and len(parts[1]) == 3:
			return float(parts[0] + parts[1])
		return float(s)

	return float(s)


def parse_duration_text(duration_text: str | None) -> int | None:
	"""Parse duration into seconds.

	Supports:
	- "HH:MM:SS" / "MM:SS"
	- "1 hour 2 minutes" / "1 h 2 min" / "1 hora 2 minutos"
"""
	text = normalize_text(duration_text)
	if not text:
		return None

	# Common colon format.
	if ":" in text:
		parts = text.split(":")
		if all(p.isdigit() for p in parts) and 2 <= len(parts) <= 3:
			nums = [int(p) for p in parts]
			if len(nums) == 2:
				mm, ss = nums
				return mm * 60 + ss
			hh, mm, ss = nums
			return hh * 3600 + mm * 60 + ss

	# Word format.
	lower = text.lower()
	# Normalize separators.
	lower = lower.replace(",", " ").replace("·", " ")
	lower = re.sub(r"\s+", " ", lower).strip()

	def _find(unit_patterns: Iterable[str]) -> int:
		for pat in unit_patterns:
			m = re.search(rf"(\d+)\s*{pat}\b", lower)
			if m:
				return int(m.group(1))
		return 0

	hours = _find(("h", "hr", "hrs", "hour", "hours", "hora", "horas"))
	minutes = _find(("m", "min", "mins", "minute", "minutes", "minuto", "minutos"))
	seconds = _find(("s", "sec", "secs", "second", "seconds", "segundo", "segundos"))

	if hours == 0 and minutes == 0 and seconds == 0:
		return None

	return hours * 3600 + minutes * 60 + seconds


def parse_published_text(published_text: str | None, *, now: datetime | None = None) -> datetime | None:
	"""Parse published text into an estimated UTC datetime.

	Supports relative formats (EN/ES):
	- "2 days ago", "hace 2 días", "Streamed 3 weeks ago"
	- "yesterday" / "ayer"

	Also tries a few absolute formats: "Jan 3, 2024", "2024-01-03".
"""
	text = normalize_text(published_text)
	if not text:
		return None
	if now is None:
		now = _utcnow()
	# ensure timezone-aware
	if now.tzinfo is None:
		now = now.replace(tzinfo=timezone.utc)

	lower = text.lower()
	# Remove leading phrases.
	for prefix in (
		"premiered",
		"streamed",
		"hace",
		"emitido",
		"estrenado",
		"transmitido",
		"se emitió",
	):
		lower = lower.replace(prefix, " ")
	lower = normalize_text(lower) or ""

	if "yesterday" in lower or "ayer" in lower:
		return now - timedelta(days=1)

	# Relative pattern: "X unit ago" or "hace X unidad"
	m = re.search(r"(\d+)\s*(minute|min|hour|day|week|month|year)s?\b", lower)
	if not m:
		m = re.search(r"(\d+)\s*(minuto|hora|d[ií]a|dia|semana|mes|a[nñ]o)s?\b", lower)

	if m:
		qty = int(m.group(1))
		unit = m.group(2)
		unit = (
			unit.replace("í", "i")
			.replace("ñ", "n")
			.replace("á", "a")
			.replace("é", "e")
			.replace("ó", "o")
			.replace("ú", "u")
		)
		unit = unit.lower()
		if unit in ("minute", "min", "minuto"):
			delta = timedelta(minutes=qty)
		elif unit in ("hour", "hora"):
			delta = timedelta(hours=qty)
		elif unit in ("day", "dia"):
			delta = timedelta(days=qty)
		elif unit in ("week", "semana"):
			delta = timedelta(weeks=qty)
		elif unit in ("month", "mes"):
			delta = timedelta(days=30 * qty)
		elif unit in ("year", "ano"):
			delta = timedelta(days=365 * qty)
		else:
			delta = None
		return (now - delta) if delta else None

	# Absolute formats (best-effort)
	for fmt in ("%Y-%m-%d", "%b %d, %Y", "%B %d, %Y", "%d %b %Y", "%d %B %Y"):
		try:
			dt = datetime.strptime(text, fmt)
			return dt.replace(tzinfo=timezone.utc)
		except ValueError:
			continue

	return None


@dataclass(frozen=True)
class ValidationResult:
	passed: bool
	reason: str | None


def validate_video(
	*,
	views_estimated: int | None,
	published_at_estimated: datetime | None,
	duration_seconds_estimated: int | None,
	now: datetime | None = None,
) -> ValidationResult:
	"""Apply minimal video-level validation rules."""
	if now is None:
		now = _utcnow()
	if now.tzinfo is None:
		now = now.replace(tzinfo=timezone.utc)

	if duration_seconds_estimated is not None and duration_seconds_estimated < 180:
		return ValidationResult(False, "duration_too_low")

	if views_estimated is not None and views_estimated < 1000:
		return ValidationResult(False, "views_too_low")

	return ValidationResult(True, None)


def normalize_raw_video(raw: dict[str, Any], *, now: datetime | None = None) -> dict[str, Any] | None:
	"""Normalize a raw video row into a `videos_normalized` insert payload."""
	if now is None:
		now = _utcnow()
	if now.tzinfo is None:
		now = now.replace(tzinfo=timezone.utc)

	video_id = raw.get("video_id")
	if not isinstance(video_id, str) or not video_id:
		return None

	channel_url = normalize_text(raw.get("channel_url") if isinstance(raw.get("channel_url"), str) else None)
	query = normalize_text(raw.get("query") if isinstance(raw.get("query"), str) else None)
	views_text = raw.get("views_text") if isinstance(raw.get("views_text"), str) else None
	published_text = raw.get("published_text") if isinstance(raw.get("published_text"), str) else None
	duration_text = raw.get("duration_text") if isinstance(raw.get("duration_text"), str) else None

	views_estimated = parse_views_text(views_text)
	published_at_estimated = parse_published_text(published_text, now=now)
	duration_seconds_estimated = parse_duration_text(duration_text)

	vr = validate_video(
		views_estimated=views_estimated,
		published_at_estimated=published_at_estimated,
		duration_seconds_estimated=duration_seconds_estimated,
		now=now,
	)

	return {
		"video_id": video_id,
		"channel_url": channel_url,
		"query": query,
		"views_estimated": views_estimated,
		"published_at_estimated": published_at_estimated,
		"duration_seconds_estimated": duration_seconds_estimated,
		"validation_passed": vr.passed,
		"validation_reason": vr.reason,
		"normalized_at": now,
	}


async def run_normalization(*, limit: int | None = None, bulk: bool = True) -> dict[str, int]:
	"""Fetch unprocessed raw videos, normalize+validate, and persist results.
	
	Args:
		limit: fast exit after fetching this many raw rows (approx).
		bulk: if True, batch insert all results at once (faster).
			  if False, insert one by one (slower, but maybe safer for partial failures).
	"""
	raw_rows = await db.fetch_unprocessed_videos_raw(limit=limit)
	if not raw_rows:
		return {"fetched": 0, "prepared": 0, "inserted": 0, "ignored": 0}

	now = _utcnow()
	
	# Common stats
	stats = {"fetched": len(raw_rows), "prepared": 0, "inserted": 0, "ignored": 0}
	
	prepared: list[dict[str, Any]] = []
	for r in raw_rows:
		row = normalize_raw_video(r, now=now)
		if row is not None:
			prepared.append(row)
	
	stats["prepared"] = len(prepared)
	
	if not prepared:
		return stats

	if bulk:
		inserted, ignored = await db.insert_videos_normalized(prepared)
		stats["inserted"] = inserted
		stats["ignored"] = ignored
	else:
		# Individual insert mode
		for p in prepared:
			inserted, ignored = await db.insert_videos_normalized([p])
			stats["inserted"] += inserted
			stats["ignored"] += ignored

	return stats


async def main() -> None:
	load_dotenv()
	await db.init_db()
	stats = await run_normalization()
	await db.close_db()
	print(stats)


if __name__ == "__main__":
	asyncio.run(main())
