"""Archive completed channel data from Supabase into local PostgreSQL.

The default mode is read-only. ``--copy`` performs an idempotent merge into the
local database, while ``--copy --truncate-after-verify`` additionally truncates
the heavy remote channel tables only after every copied row has been verified.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import sys
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Iterable, Sequence
from urllib.parse import urlsplit

import asyncpg
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import db
from scripts.report_db_storage import collect_report, print_report


VALID_LANGUAGES = ("es", "en")
TERMINAL_FIRST_VIDEO_STATUSES = {"success", "no_public_videos"}

CHANNEL_FIELDS = (
    "channel_url",
    "channel_id",
    "channel_name",
    "subscriber_count",
    "is_verified",
    "channel_description",
    "channel_tags",
    "avatar_url",
    "banner_url",
    "uploader_id",
    "uploader_url",
    "last_upload_date",
    "first_video_id",
    "first_video_published_at",
    "first_video_checked_at",
    "first_video_last_attempt_at",
    "first_video_status",
    "first_video_source",
    "first_video_last_error",
    "first_video_claimed_at",
    "extracted_at",
)
CHANNEL_COMPARE_FIELDS = (
    "channel_id",
    "channel_name",
    "subscriber_count",
    "is_verified",
    "channel_description",
    "channel_tags",
    "avatar_url",
    "banner_url",
    "uploader_id",
    "uploader_url",
    "last_upload_date",
    "extracted_at",
)
VIDEO_FIELDS = (
    "channel_url",
    "video_id",
    "upload_date",
    "duration_seconds",
    "view_count",
    "title",
)
RELEVANCE_FIELDS = ("channel_url", "is_relevant", "notes", "tags", "marked_at")
PROCESSED_FIELDS = ("channel_url", "processed_at", "status")
SEARCH_RUN_FIELDS = (
    "id",
    "query",
    "mode",
    "started_at",
    "finished_at",
    "status",
    "result_count",
    "last_error",
)


@dataclass(frozen=True)
class ConnectionIdentity:
    database: str
    server_address: str
    server_port: int
    postmaster_started_at: datetime
    read_only: bool

    @property
    def label(self) -> str:
        return f"{self.server_address}:{self.server_port}/{self.database}"


@dataclass(frozen=True)
class LanguageCounts:
    channels: int
    videos: int
    relevance: int
    processed: int
    search_runs: int
    candidates: int
    claims: int
    staging: int


@dataclass
class ArchiveTotals:
    channels: int = 0
    videos: int = 0
    relevance: int = 0
    processed: int = 0
    search_runs: int = 0


class ArchiveVerificationError(RuntimeError):
    """Raised when the destination is not an exact safe archive of the source."""


class CanonicalDigest:
    def __init__(self) -> None:
        self._digest = hashlib.sha256()

    def add(self, kind: str, rows: Iterable[dict[str, Any]], fields: Sequence[str]) -> None:
        for row in rows:
            payload = [kind, *(_canonical(row.get(field)) for field in fields)]
            encoded = json.dumps(
                payload,
                ensure_ascii=False,
                separators=(",", ":"),
            ).encode("utf-8")
            self._digest.update(len(encoded).to_bytes(8, "big"))
            self._digest.update(encoded)

    def hexdigest(self) -> str:
        return self._digest.hexdigest()


def _canonical(value: Any) -> Any:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value.normalize())
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, (list, tuple)):
        return [_canonical(item) for item in value]
    return value


def _json_payload(rows: Iterable[dict[str, Any]]) -> str:
    return json.dumps(
        list(rows),
        default=_canonical,
        ensure_ascii=False,
        separators=(",", ":"),
    )


def _as_dicts(records: Iterable[asyncpg.Record]) -> list[dict[str, Any]]:
    return [dict(record) for record in records]


def _chunks(rows: Sequence[dict[str, Any]], size: int) -> Iterable[list[dict[str, Any]]]:
    for index in range(0, len(rows), size):
        yield list(rows[index:index + size])


def _safe_url_label(value: str) -> str:
    parsed = urlsplit(value)
    database = parsed.path.lstrip("/") or "postgres"
    return f"{parsed.hostname or '?'}:{parsed.port or 5432}/{database}"


async def _connect(dsn: str, timeout: float) -> asyncpg.Connection:
    return await asyncpg.connect(
        dsn,
        statement_cache_size=0,
        timeout=timeout,
        command_timeout=timeout,
    )


async def _identity(conn: asyncpg.Connection) -> ConnectionIdentity:
    row = await conn.fetchrow("""
        SELECT current_database() AS database,
               COALESCE(inet_server_addr()::TEXT, 'local-socket') AS server_address,
               inet_server_port() AS server_port,
               pg_postmaster_start_time() AS postmaster_started_at,
               current_setting('transaction_read_only') = 'on' AS read_only
    """)
    return ConnectionIdentity(**dict(row))


def _same_database(source: ConnectionIdentity, destination: ConnectionIdentity) -> bool:
    return (
        source.database == destination.database
        and source.server_address == destination.server_address
        and source.server_port == destination.server_port
        and source.postmaster_started_at == destination.postmaster_started_at
    )


async def _has_schema(conn: asyncpg.Connection, language: str) -> bool:
    return bool(await conn.fetchval(
        "SELECT to_regclass($1) IS NOT NULL",
        f"public.channels_raw_{language}",
    ))


async def _is_compact_schema(conn: asyncpg.Connection, language: str) -> bool:
    """Recognize the fresh compact schema and reject legacy local layouts."""
    return bool(await conn.fetchval(f"""
        SELECT
            to_regclass('public.channels_raw_{language}') IS NOT NULL
            AND to_regclass('public.channel_videos_raw_{language}') IS NOT NULL
            AND to_regclass('public.discovery_videos_staging_{language}') IS NOT NULL
            AND EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name = 'channels_raw_{language}'
                  AND column_name = 'id'
                  AND data_type = 'bigint'
            )
            AND EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name = 'channel_videos_raw_{language}'
                  AND column_name = 'channel_key'
                  AND data_type = 'bigint'
            )
            AND EXISTS (
                SELECT 1
                FROM pg_class class
                JOIN pg_namespace namespace ON namespace.oid = class.relnamespace
                WHERE namespace.nspname = 'public'
                  AND class.relname = 'channel_stats_{language}'
                  AND class.relkind = 'r'
            )
    """))


async def _language_counts(conn: asyncpg.Connection, language: str) -> LanguageCounts:
    values = await conn.fetchrow(f"""
        SELECT
            (SELECT COUNT(*) FROM channels_raw_{language}) AS channels,
            (SELECT COUNT(*) FROM channel_videos_raw_{language}) AS videos,
            (SELECT COUNT(*) FROM channel_relevance_{language}) AS relevance,
            (SELECT COUNT(*) FROM channels_processed_{language}) AS processed,
            (SELECT COUNT(*) FROM search_runs_{language}) AS search_runs,
            (SELECT COUNT(*) FROM channel_candidates_{language}) AS candidates,
            (SELECT COUNT(*) FROM channels_discovery_claims_{language}) AS claims,
            (SELECT COUNT(*) FROM discovery_videos_staging_{language}) AS staging
    """)
    return LanguageCounts(**{key: int(value) for key, value in dict(values).items()})


def _print_counts(title: str, language: str, counts: LanguageCounts) -> None:
    print(
        f"{title} {language.upper()}: channels={counts.channels:,} "
        f"videos={counts.videos:,} relevance={counts.relevance:,} "
        f"processed={counts.processed:,} search_runs={counts.search_runs:,} "
        f"candidates={counts.candidates:,} claims={counts.claims:,} "
        f"staging={counts.staging:,}"
    )


async def _ensure_local_schema(dsn: str, languages: Sequence[str]) -> None:
    for language in languages:
        await db.init_db(
            dsn=dsn,
            min_size=1,
            max_size=2,
            language=language,
            ensure_schema=True,
        )
        await db.close_db()


async def _ensure_archive_batches(conn: asyncpg.Connection) -> None:
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS archive_batches (
            id UUID PRIMARY KEY,
            language TEXT NOT NULL CHECK (language IN ('es', 'en')),
            source_identity TEXT NOT NULL,
            status TEXT NOT NULL CHECK (
                status IN ('running', 'verified', 'purged', 'failed')
            ),
            started_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            completed_at TIMESTAMPTZ,
            remote_purged_at TIMESTAMPTZ,
            channel_count BIGINT NOT NULL DEFAULT 0,
            video_count BIGINT NOT NULL DEFAULT 0,
            relevance_count BIGINT NOT NULL DEFAULT 0,
            processed_count BIGINT NOT NULL DEFAULT 0,
            search_run_count BIGINT NOT NULL DEFAULT 0,
            channel_checksum TEXT,
            video_checksum TEXT,
            relevance_checksum TEXT,
            error TEXT
        )
    """)


async def _start_archive_batch(
    conn: asyncpg.Connection,
    language: str,
    source_identity: str,
) -> uuid.UUID:
    batch_id = uuid.uuid4()
    await conn.execute("""
        INSERT INTO archive_batches (id, language, source_identity, status)
        VALUES ($1, $2, $3, 'running')
    """, batch_id, language, source_identity)
    return batch_id


async def _finish_archive_batch(
    conn: asyncpg.Connection,
    batch_id: uuid.UUID,
    *,
    status: str,
    totals: ArchiveTotals,
    channel_checksum: str | None = None,
    video_checksum: str | None = None,
    relevance_checksum: str | None = None,
    error: str | None = None,
) -> None:
    await conn.execute("""
        UPDATE archive_batches
        SET status = $2,
            completed_at = CASE WHEN $2 IN ('verified', 'purged', 'failed')
                                THEN CURRENT_TIMESTAMP ELSE completed_at END,
            remote_purged_at = CASE WHEN $2 = 'purged'
                                    THEN CURRENT_TIMESTAMP ELSE remote_purged_at END,
            channel_count = $3,
            video_count = $4,
            relevance_count = $5,
            processed_count = $6,
            search_run_count = $7,
            channel_checksum = $8,
            video_checksum = $9,
            relevance_checksum = $10,
            error = $11
        WHERE id = $1
    """,
        batch_id,
        status,
        totals.channels,
        totals.videos,
        totals.relevance,
        totals.processed,
        totals.search_runs,
        channel_checksum,
        video_checksum,
        relevance_checksum,
        error[:4000] if error else None,
    )


async def _upsert_channels(
    conn: asyncpg.Connection,
    language: str,
    rows: list[dict[str, Any]],
) -> None:
    if not rows:
        return
    await conn.execute(f"""
        WITH incoming AS (
            SELECT *
            FROM jsonb_to_recordset($1::JSONB) AS item(
                channel_url TEXT,
                channel_id TEXT,
                channel_name TEXT,
                subscriber_count BIGINT,
                is_verified BOOLEAN,
                channel_description TEXT,
                channel_tags TEXT[],
                avatar_url TEXT,
                banner_url TEXT,
                uploader_id TEXT,
                uploader_url TEXT,
                last_upload_date DATE,
                first_video_id TEXT,
                first_video_published_at TIMESTAMPTZ,
                first_video_checked_at TIMESTAMPTZ,
                first_video_last_attempt_at TIMESTAMPTZ,
                first_video_status TEXT,
                first_video_source TEXT,
                first_video_last_error TEXT,
                first_video_claimed_at TIMESTAMPTZ,
                extracted_at TIMESTAMPTZ
            )
        )
        INSERT INTO channels_raw_{language} (
            channel_url, channel_id, channel_name, subscriber_count, is_verified,
            channel_description, channel_tags, avatar_url, banner_url, uploader_id,
            uploader_url, last_upload_date, first_video_id,
            first_video_published_at, first_video_checked_at,
            first_video_last_attempt_at, first_video_status, first_video_source,
            first_video_last_error, first_video_claimed_at, extracted_at
        )
        SELECT channel_url, channel_id, channel_name, subscriber_count, is_verified,
               channel_description, channel_tags, avatar_url, banner_url, uploader_id,
               uploader_url, last_upload_date, first_video_id,
               first_video_published_at, first_video_checked_at,
               first_video_last_attempt_at, COALESCE(first_video_status, 'pending'),
               first_video_source, first_video_last_error,
               first_video_claimed_at, extracted_at
        FROM incoming
        ON CONFLICT (channel_url) DO UPDATE SET
            channel_id = COALESCE(EXCLUDED.channel_id, channels_raw_{language}.channel_id),
            channel_name = COALESCE(EXCLUDED.channel_name, channels_raw_{language}.channel_name),
            subscriber_count = COALESCE(EXCLUDED.subscriber_count, channels_raw_{language}.subscriber_count),
            is_verified = COALESCE(EXCLUDED.is_verified, channels_raw_{language}.is_verified),
            channel_description = COALESCE(EXCLUDED.channel_description, channels_raw_{language}.channel_description),
            channel_tags = COALESCE(EXCLUDED.channel_tags, channels_raw_{language}.channel_tags),
            avatar_url = COALESCE(EXCLUDED.avatar_url, channels_raw_{language}.avatar_url),
            banner_url = COALESCE(EXCLUDED.banner_url, channels_raw_{language}.banner_url),
            uploader_id = COALESCE(EXCLUDED.uploader_id, channels_raw_{language}.uploader_id),
            uploader_url = COALESCE(EXCLUDED.uploader_url, channels_raw_{language}.uploader_url),
            last_upload_date = COALESCE(EXCLUDED.last_upload_date, channels_raw_{language}.last_upload_date),
            first_video_id = CASE
                WHEN channels_raw_{language}.first_video_status IN ('success', 'no_public_videos')
                    THEN channels_raw_{language}.first_video_id
                ELSE COALESCE(EXCLUDED.first_video_id, channels_raw_{language}.first_video_id)
            END,
            first_video_published_at = CASE
                WHEN channels_raw_{language}.first_video_status IN ('success', 'no_public_videos')
                    THEN channels_raw_{language}.first_video_published_at
                ELSE COALESCE(EXCLUDED.first_video_published_at, channels_raw_{language}.first_video_published_at)
            END,
            first_video_checked_at = CASE
                WHEN channels_raw_{language}.first_video_status IN ('success', 'no_public_videos')
                    THEN channels_raw_{language}.first_video_checked_at
                ELSE COALESCE(EXCLUDED.first_video_checked_at, channels_raw_{language}.first_video_checked_at)
            END,
            first_video_last_attempt_at = GREATEST(
                channels_raw_{language}.first_video_last_attempt_at,
                EXCLUDED.first_video_last_attempt_at
            ),
            first_video_status = CASE
                WHEN channels_raw_{language}.first_video_status IN ('success', 'no_public_videos')
                    THEN channels_raw_{language}.first_video_status
                ELSE EXCLUDED.first_video_status
            END,
            first_video_source = CASE
                WHEN channels_raw_{language}.first_video_status IN ('success', 'no_public_videos')
                    THEN channels_raw_{language}.first_video_source
                ELSE COALESCE(EXCLUDED.first_video_source, channels_raw_{language}.first_video_source)
            END,
            first_video_last_error = CASE
                WHEN channels_raw_{language}.first_video_status IN ('success', 'no_public_videos')
                    THEN channels_raw_{language}.first_video_last_error
                ELSE EXCLUDED.first_video_last_error
            END,
            first_video_claimed_at = CASE
                WHEN channels_raw_{language}.first_video_status IN ('success', 'no_public_videos')
                    THEN channels_raw_{language}.first_video_claimed_at
                ELSE EXCLUDED.first_video_claimed_at
            END,
            extracted_at = GREATEST(
                channels_raw_{language}.extracted_at,
                EXCLUDED.extracted_at
            )
    """, _json_payload(rows))


async def _upsert_videos(
    conn: asyncpg.Connection,
    language: str,
    rows: list[dict[str, Any]],
) -> None:
    if not rows:
        return
    await conn.execute(f"""
        WITH incoming AS (
            SELECT *
            FROM jsonb_to_recordset($1::JSONB) AS item(
                channel_url TEXT,
                video_id TEXT,
                upload_date DATE,
                duration_seconds INTEGER,
                view_count BIGINT,
                title TEXT
            )
        )
        INSERT INTO channel_videos_raw_{language} (
            video_id, channel_key, upload_date, duration_seconds, view_count, title
        )
        SELECT item.video_id, channel.id, item.upload_date,
               item.duration_seconds, item.view_count, item.title
        FROM incoming item
        JOIN channels_raw_{language} channel USING (channel_url)
        ON CONFLICT (video_id) DO UPDATE SET
            channel_key = EXCLUDED.channel_key,
            upload_date = COALESCE(EXCLUDED.upload_date, channel_videos_raw_{language}.upload_date),
            duration_seconds = COALESCE(EXCLUDED.duration_seconds, channel_videos_raw_{language}.duration_seconds),
            view_count = COALESCE(EXCLUDED.view_count, channel_videos_raw_{language}.view_count),
            title = COALESCE(EXCLUDED.title, channel_videos_raw_{language}.title)
    """, _json_payload(rows))


async def _upsert_relevance(
    conn: asyncpg.Connection,
    language: str,
    rows: list[dict[str, Any]],
) -> None:
    if not rows:
        return
    await conn.execute(f"""
        WITH incoming AS (
            SELECT *
            FROM jsonb_to_recordset($1::JSONB) AS item(
                channel_url TEXT,
                is_relevant BOOLEAN,
                notes TEXT,
                tags TEXT[],
                marked_at TIMESTAMPTZ
            )
        )
        INSERT INTO channel_relevance_{language} (
            channel_key, is_relevant, notes, tags, marked_at
        )
        SELECT channel.id, item.is_relevant, item.notes, item.tags, item.marked_at
        FROM incoming item
        JOIN channels_raw_{language} channel USING (channel_url)
        ON CONFLICT (channel_key) DO NOTHING
    """, _json_payload(rows))


async def _upsert_processed(
    conn: asyncpg.Connection,
    language: str,
    rows: list[dict[str, Any]],
) -> None:
    if not rows:
        return
    await conn.execute(f"""
        WITH incoming AS (
            SELECT *
            FROM jsonb_to_recordset($1::JSONB) AS item(
                channel_url TEXT,
                processed_at TIMESTAMPTZ,
                status TEXT
            )
        )
        INSERT INTO channels_processed_{language} (channel_url, processed_at, status)
        SELECT channel_url, processed_at, status FROM incoming
        ON CONFLICT (channel_url) DO UPDATE SET
            processed_at = GREATEST(
                channels_processed_{language}.processed_at,
                EXCLUDED.processed_at
            ),
            status = CASE
                WHEN EXCLUDED.processed_at >= channels_processed_{language}.processed_at
                    THEN EXCLUDED.status
                ELSE channels_processed_{language}.status
            END
    """, _json_payload(rows))


async def _upsert_search_runs(
    conn: asyncpg.Connection,
    language: str,
    rows: list[dict[str, Any]],
) -> None:
    if not rows:
        return
    await conn.execute(f"""
        WITH incoming AS (
            SELECT *
            FROM jsonb_to_recordset($1::JSONB) AS item(
                id UUID,
                query TEXT,
                mode TEXT,
                started_at TIMESTAMPTZ,
                finished_at TIMESTAMPTZ,
                status TEXT,
                result_count INTEGER,
                last_error TEXT
            )
        )
        INSERT INTO search_runs_{language} (
            id, query, mode, started_at, finished_at,
            status, result_count, last_error
        )
        SELECT id, query, mode, started_at, finished_at,
               status, result_count, last_error
        FROM incoming
        ON CONFLICT (id) DO UPDATE SET
            query = EXCLUDED.query,
            mode = EXCLUDED.mode,
            started_at = EXCLUDED.started_at,
            finished_at = EXCLUDED.finished_at,
            status = EXCLUDED.status,
            result_count = EXCLUDED.result_count,
            last_error = EXCLUDED.last_error
    """, _json_payload(rows))


async def _rebuild_stats(
    conn: asyncpg.Connection,
    language: str,
    channel_urls: list[str],
) -> None:
    if not channel_urls:
        return
    await conn.execute(f"""
        INSERT INTO channel_stats_{language} (
            channel_key, total_videos_tracked, avg_views_on_channel,
            max_views_on_channel, view_counts
        )
        SELECT channel.id,
               COUNT(video.video_id),
               COALESCE(ROUND(AVG(video.view_count), 2), 0),
               COALESCE(MAX(video.view_count), 0),
               COALESCE(
                   ARRAY_AGG(video.view_count ORDER BY video.view_count)
                       FILTER (WHERE video.view_count IS NOT NULL),
                   '{{}}'::BIGINT[]
               )
        FROM channels_raw_{language} channel
        LEFT JOIN channel_videos_raw_{language} video
               ON video.channel_key = channel.id
        WHERE channel.channel_url = ANY($1::TEXT[])
        GROUP BY channel.id
        ON CONFLICT (channel_key) DO UPDATE SET
            total_videos_tracked = EXCLUDED.total_videos_tracked,
            avg_views_on_channel = EXCLUDED.avg_views_on_channel,
            max_views_on_channel = EXCLUDED.max_views_on_channel,
            view_counts = EXCLUDED.view_counts
    """, channel_urls)


def _verify_preserved_rows(
    kind: str,
    source_rows: list[dict[str, Any]],
    destination_rows: list[dict[str, Any]],
    *,
    key_fields: Sequence[str],
    compare_fields: Sequence[str],
) -> None:
    def key(row: dict[str, Any]) -> tuple[Any, ...]:
        return tuple(_canonical(row.get(field)) for field in key_fields)

    destination = {key(row): row for row in destination_rows}
    for source in source_rows:
        row_key = key(source)
        target = destination.get(row_key)
        if target is None:
            raise ArchiveVerificationError(f"Missing {kind} row in local DB: {row_key}")
        for field in compare_fields:
            source_value = source.get(field)
            if source_value is None:
                continue
            if _canonical(source_value) != _canonical(target.get(field)):
                raise ArchiveVerificationError(
                    f"{kind} mismatch for {row_key}, field={field}: "
                    f"source={source_value!r} local={target.get(field)!r}"
                )


def _verify_first_video(
    source_rows: list[dict[str, Any]],
    destination_rows: list[dict[str, Any]],
) -> None:
    destination = {row["channel_url"]: row for row in destination_rows}
    for source in source_rows:
        if source.get("first_video_status") not in TERMINAL_FIRST_VIDEO_STATUSES:
            continue
        target = destination[source["channel_url"]]
        if target.get("first_video_status") not in TERMINAL_FIRST_VIDEO_STATUSES:
            raise ArchiveVerificationError(
                f"Terminal first-video state was not copied for {source['channel_url']}"
            )
        if target.get("first_video_status") != source.get("first_video_status"):
            raise ArchiveVerificationError(
                f"Conflicting terminal first-video state for {source['channel_url']}"
            )
        for field in ("first_video_id", "first_video_published_at"):
            if _canonical(source.get(field)) != _canonical(target.get(field)):
                raise ArchiveVerificationError(
                    f"First-video mismatch for {source['channel_url']}, field={field}"
                )


def _verify_rebuilt_stats(
    channels: list[dict[str, Any]],
    videos: list[dict[str, Any]],
    stats: list[dict[str, Any]],
) -> None:
    """Verify the local derived cache against the local video rows.

    Remote ``channel_stats`` can legitimately be stale when YouTube renames a
    channel: a later extraction moves globally unique video IDs to the canonical
    URL, while the old URL's cached aggregate may remain unchanged.  Raw video
    ownership is therefore the source of truth during archival.
    """
    videos_by_channel: dict[str, list[dict[str, Any]]] = {
        row["channel_url"]: [] for row in channels
    }
    for video in videos:
        videos_by_channel.setdefault(video["channel_url"], []).append(video)

    stats_by_channel = {row["channel_url"]: row for row in stats}
    for channel_url, channel_videos in videos_by_channel.items():
        target = stats_by_channel.get(channel_url)
        if target is None:
            raise ArchiveVerificationError(
                f"Missing rebuilt stats row in local DB: {(channel_url,)}"
            )

        view_counts = sorted(
            int(video["view_count"])
            for video in channel_videos
            if video.get("view_count") is not None
        )
        average_views = (
            (Decimal(sum(view_counts)) / Decimal(len(view_counts))).quantize(
                Decimal("0.01"),
                rounding=ROUND_HALF_UP,
            )
            if view_counts
            else Decimal(0)
        )
        expected = {
            "total_videos_tracked": len(channel_videos),
            "avg_views_on_channel": average_views,
            "max_views_on_channel": max(view_counts, default=0),
            "view_counts": view_counts,
        }
        for field, expected_value in expected.items():
            if _canonical(target.get(field)) != _canonical(expected_value):
                raise ArchiveVerificationError(
                    f"rebuilt stats mismatch for {(channel_url,)}, field={field}: "
                    f"expected={expected_value!r} local={target.get(field)!r}"
                )


async def _destination_batch_rows(
    conn: asyncpg.Connection,
    language: str,
    channel_urls: list[str],
) -> tuple[
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
]:
    channel_columns = ", ".join(CHANNEL_FIELDS)
    channels = _as_dicts(await conn.fetch(f"""
        SELECT {channel_columns}
        FROM channels_raw_{language}
        WHERE channel_url = ANY($1::TEXT[])
        ORDER BY channel_url
    """, channel_urls))
    videos = _as_dicts(await conn.fetch(f"""
        SELECT channel.channel_url, video.video_id, video.upload_date,
               video.duration_seconds, video.view_count, video.title
        FROM channel_videos_raw_{language} video
        JOIN channels_raw_{language} channel ON channel.id = video.channel_key
        WHERE channel.channel_url = ANY($1::TEXT[])
        ORDER BY channel.channel_url, video.video_id
    """, channel_urls))
    relevance = _as_dicts(await conn.fetch(f"""
        SELECT channel.channel_url, relevance.is_relevant, relevance.notes,
               relevance.tags, relevance.marked_at
        FROM channel_relevance_{language} relevance
        JOIN channels_raw_{language} channel ON channel.id = relevance.channel_key
        WHERE channel.channel_url = ANY($1::TEXT[])
        ORDER BY channel.channel_url
    """, channel_urls))
    stats = _as_dicts(await conn.fetch(f"""
        SELECT channel.channel_url, stats.total_videos_tracked,
               stats.avg_views_on_channel, stats.max_views_on_channel,
               stats.view_counts
        FROM channel_stats_{language} stats
        JOIN channels_raw_{language} channel ON channel.id = stats.channel_key
        WHERE channel.channel_url = ANY($1::TEXT[])
        ORDER BY channel.channel_url
    """, channel_urls))
    return channels, videos, relevance, stats


async def _sync_registry_table(
    source: asyncpg.Connection,
    destination: asyncpg.Connection,
    language: str,
    *,
    table: str,
    fields: Sequence[str],
    batch_size: int,
    upsert,
) -> int:
    columns = ", ".join(fields)
    order = "channel_url" if table == "channels_processed" else "started_at, id"
    records = await source.fetch(
        f"SELECT {columns} FROM {table}_{language} ORDER BY {order}"
    )
    rows = _as_dicts(records)
    for batch in _chunks(rows, batch_size):
        async with destination.transaction():
            await upsert(destination, language, batch)
    return len(rows)


async def _copy_heavy_snapshot(
    source: asyncpg.Connection,
    destination: asyncpg.Connection,
    language: str,
    *,
    batch_size: int,
    destructive: bool,
    claim_stale_minutes: int,
) -> tuple[ArchiveTotals, str, str, str]:
    totals = ArchiveTotals()
    channel_digest = CanonicalDigest()
    video_digest = CanonicalDigest()
    relevance_digest = CanonicalDigest()

    transaction = source.transaction(isolation="repeatable_read", readonly=not destructive)
    async with transaction:
        if destructive:
            active_claims = int(await source.fetchval(
                f"""
                    SELECT COUNT(*)
                    FROM channels_discovery_claims_{language}
                    WHERE claimed_at >= CURRENT_TIMESTAMP
                        - ($1::INTEGER * INTERVAL '1 minute')
                """,
                claim_stale_minutes,
            ) or 0)
            if active_claims:
                raise RuntimeError(
                    f"Cannot truncate {language.upper()}: {active_claims} active claims"
                )
            await source.execute("SET LOCAL lock_timeout = '5s'")
            await source.execute(f"""
                LOCK TABLE
                    channels_raw_{language},
                    channel_videos_raw_{language},
                    channel_stats_{language},
                    channel_relevance_{language}
                IN ACCESS EXCLUSIVE MODE NOWAIT
            """)
            await source.execute(f"""
                LOCK TABLE channels_discovery_claims_{language}
                IN SHARE MODE NOWAIT
            """)
            active_claims = int(await source.fetchval(
                f"""
                    SELECT COUNT(*)
                    FROM channels_discovery_claims_{language}
                    WHERE claimed_at >= CURRENT_TIMESTAMP
                        - ($1::INTEGER * INTERVAL '1 minute')
                """,
                claim_stale_minutes,
            ) or 0)
            if active_claims:
                raise RuntimeError(
                    f"Cannot truncate {language.upper()}: claims appeared during lock acquisition"
                )

        last_url: str | None = None
        channel_columns = ", ".join(f"channel.{field}" for field in CHANNEL_FIELDS)
        while True:
            source_channels = _as_dicts(await source.fetch(f"""
                SELECT channel.id AS source_channel_key, {channel_columns}
                FROM channels_raw_{language} channel
                WHERE ($1::TEXT IS NULL OR channel.channel_url > $1)
                ORDER BY channel.channel_url
                LIMIT $2
            """, last_url, batch_size))
            if not source_channels:
                break

            channel_urls = [row["channel_url"] for row in source_channels]
            channel_keys = [row["source_channel_key"] for row in source_channels]
            source_videos = _as_dicts(await source.fetch(f"""
                SELECT channel.channel_url, video.video_id, video.upload_date,
                       video.duration_seconds, video.view_count, video.title
                FROM channel_videos_raw_{language} video
                JOIN channels_raw_{language} channel ON channel.id = video.channel_key
                WHERE video.channel_key = ANY($1::BIGINT[])
                ORDER BY channel.channel_url, video.video_id
            """, channel_keys))
            source_relevance = _as_dicts(await source.fetch(f"""
                SELECT channel.channel_url, relevance.is_relevant, relevance.notes,
                       relevance.tags, relevance.marked_at
                FROM channel_relevance_{language} relevance
                JOIN channels_raw_{language} channel ON channel.id = relevance.channel_key
                WHERE relevance.channel_key = ANY($1::BIGINT[])
                ORDER BY channel.channel_url
            """, channel_keys))
            channel_payload = [
                {field: row.get(field) for field in CHANNEL_FIELDS}
                for row in source_channels
            ]
            async with destination.transaction():
                await _upsert_channels(destination, language, channel_payload)
                await _upsert_videos(destination, language, source_videos)
                await _upsert_relevance(destination, language, source_relevance)
                await _rebuild_stats(destination, language, channel_urls)

            local_channels, local_videos, local_relevance, local_stats = (
                await _destination_batch_rows(destination, language, channel_urls)
            )
            _verify_preserved_rows(
                "channel",
                channel_payload,
                local_channels,
                key_fields=("channel_url",),
                compare_fields=CHANNEL_COMPARE_FIELDS,
            )
            _verify_first_video(channel_payload, local_channels)
            _verify_preserved_rows(
                "video",
                source_videos,
                local_videos,
                key_fields=("video_id",),
                compare_fields=(
                    "channel_url", "upload_date", "duration_seconds", "view_count", "title"
                ),
            )
            _verify_preserved_rows(
                "relevance",
                source_relevance,
                local_relevance,
                key_fields=("channel_url",),
                compare_fields=(),  # Local relevance becomes authoritative after the first copy.
            )
            _verify_rebuilt_stats(local_channels, local_videos, local_stats)

            channel_digest.add("channel", channel_payload, CHANNEL_FIELDS)
            video_digest.add("video", source_videos, VIDEO_FIELDS)
            relevance_digest.add("relevance", source_relevance, RELEVANCE_FIELDS)
            totals.channels += len(channel_payload)
            totals.videos += len(source_videos)
            totals.relevance += len(source_relevance)
            last_url = channel_urls[-1]
            print(
                f"[archive][{language}] copied channels={totals.channels:,} "
                f"videos={totals.videos:,}"
            )

        if destructive:
            await source.execute(f"""
                TRUNCATE TABLE
                    channel_relevance_{language},
                    channel_stats_{language},
                    channel_videos_raw_{language},
                    channels_raw_{language}
                RESTART IDENTITY
            """)

    return (
        totals,
        channel_digest.hexdigest(),
        video_digest.hexdigest(),
        relevance_digest.hexdigest(),
    )


async def _archive_language(
    source: asyncpg.Connection,
    destination: asyncpg.Connection,
    language: str,
    *,
    batch_size: int,
    destructive: bool,
    source_identity: str,
    claim_stale_minutes: int,
) -> ArchiveTotals:
    batch_id = await _start_archive_batch(destination, language, source_identity)
    totals = ArchiveTotals()
    checksums: tuple[str | None, str | None, str | None] = (None, None, None)
    try:
        totals.processed = await _sync_registry_table(
            source,
            destination,
            language,
            table="channels_processed",
            fields=PROCESSED_FIELDS,
            batch_size=batch_size,
            upsert=_upsert_processed,
        )
        totals.search_runs = await _sync_registry_table(
            source,
            destination,
            language,
            table="search_runs",
            fields=SEARCH_RUN_FIELDS,
            batch_size=batch_size,
            upsert=_upsert_search_runs,
        )
        heavy_totals, *checksums = await _copy_heavy_snapshot(
            source,
            destination,
            language,
            batch_size=batch_size,
            destructive=destructive,
            claim_stale_minutes=claim_stale_minutes,
        )
        totals.channels = heavy_totals.channels
        totals.videos = heavy_totals.videos
        totals.relevance = heavy_totals.relevance
        await _finish_archive_batch(
            destination,
            batch_id,
            status="purged" if destructive else "verified",
            totals=totals,
            channel_checksum=checksums[0],
            video_checksum=checksums[1],
            relevance_checksum=checksums[2],
        )
        return totals
    except BaseException as exc:
        try:
            await _finish_archive_batch(
                destination,
                batch_id,
                status="failed",
                totals=totals,
                channel_checksum=checksums[0],
                video_checksum=checksums[1],
                relevance_checksum=checksums[2],
                error=str(exc),
            )
        except Exception as record_error:
            print(f"WARNING: could not record failed archive batch: {record_error}")
        raise


async def _show_storage(
    title: str,
    conn: asyncpg.Connection,
    languages: list[str],
) -> None:
    print(f"\n{title}")
    rows = await collect_report(conn, languages, exact_rows=True)
    print_report(rows, exact_rows=True)


async def run(args: argparse.Namespace) -> int:
    if args.truncate_after_verify and not args.copy:
        raise ValueError("--truncate-after-verify requires --copy")
    if not args.source_url:
        raise ValueError("SUPABASE_DATABASE_URL is not set")
    if not args.destination_url:
        raise ValueError("LOCAL_DATABASE_URL is not set")
    if args.batch_size <= 0:
        raise ValueError("--batch-size must be positive")
    if args.claim_stale_minutes <= 0:
        raise ValueError("--claim-stale-minutes must be positive")
    if args.copy and (urlsplit(args.source_url).port or 5432) == 6543:
        raise ValueError(
            "Copy mode requires a direct or session-pooler Supabase URL on port 5432; "
            "transaction-pooler port 6543 is only supported for dry-run"
        )

    print(f"Supabase source: {_safe_url_label(args.source_url)}")
    print(f"Local destination: {_safe_url_label(args.destination_url)}")

    source = await _connect(args.source_url, args.timeout_seconds)
    destination = await _connect(args.destination_url, args.timeout_seconds)
    try:
        source_identity = await _identity(source)
        destination_identity = await _identity(destination)
        if _same_database(source_identity, destination_identity):
            raise RuntimeError("Source and destination resolve to the same PostgreSQL database")
        if destination_identity.read_only and args.copy:
            raise RuntimeError("Local destination is read-only")
        if source_identity.read_only and args.truncate_after_verify:
            raise RuntimeError(
                "Supabase is read-only; copy is allowed but remote truncation is blocked"
            )
        active_languages: list[str] = []
        for language in args.languages:
            if not await _is_compact_schema(source, language):
                if len(args.languages) == 1:
                    raise RuntimeError(
                        f"Supabase does not have the compact {language.upper()} schema"
                    )
                print(
                    f"WARNING: Supabase has no compact {language.upper()} schema; skipping it"
                )
                continue
            active_languages.append(language)
            if await _has_schema(destination, language):
                if not await _is_compact_schema(destination, language):
                    raise RuntimeError(
                        f"Local {language.upper()} schema is legacy/incompatible; "
                        "use a fresh yt_archive database"
                    )
        if not active_languages:
            raise RuntimeError("Supabase has none of the requested compact schemas")
        if args.copy:
            await _ensure_local_schema(args.destination_url, active_languages)

        for language in active_languages:
            if args.copy and not await _has_schema(destination, language):
                raise RuntimeError(f"Local schema for {language.upper()} is missing")
            _print_counts(
                "Remote",
                language,
                await _language_counts(source, language),
            )
            if await _has_schema(destination, language):
                _print_counts(
                    "Local ",
                    language,
                    await _language_counts(destination, language),
                )

        await _show_storage("Remote storage before archive", source, active_languages)
        if not args.copy:
            print("\nDry-run complete. No database was modified.")
            return 0

        await _ensure_archive_batches(destination)
        for language in active_languages:
            totals = await _archive_language(
                source,
                destination,
                language,
                batch_size=args.batch_size,
                destructive=args.truncate_after_verify,
                source_identity=source_identity.label,
                claim_stale_minutes=args.claim_stale_minutes,
            )
            action = "copied and purged" if args.truncate_after_verify else "copied and verified"
            print(
                f"[archive][{language}] {action}: channels={totals.channels:,} "
                f"videos={totals.videos:,} relevance={totals.relevance:,}"
            )

        await _show_storage("Local storage after archive", destination, active_languages)
        await _show_storage("Remote storage after archive", source, active_languages)
        return 0
    finally:
        await destination.close()
        await source.close()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Archive Supabase channel data into local PostgreSQL"
    )
    language = parser.add_mutually_exclusive_group(required=True)
    language.add_argument("--ES", action="store_const", const=["es"], dest="languages")
    language.add_argument("--EN", action="store_const", const=["en"], dest="languages")
    language.add_argument("--all", action="store_const", const=["es", "en"], dest="languages")
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument(
        "--claim-stale-minutes",
        type=int,
        default=60,
        help="Only newer claims block truncation (default: 60)",
    )
    parser.add_argument("--copy", action="store_true", help="Copy and verify data locally")
    parser.add_argument(
        "--truncate-after-verify",
        action="store_true",
        help="Truncate heavy remote tables after a verified copy (requires --copy)",
    )
    parser.add_argument(
        "--source-url",
        default=os.getenv("SUPABASE_DATABASE_URL"),
        help="Defaults to SUPABASE_DATABASE_URL",
    )
    parser.add_argument(
        "--destination-url",
        default=os.getenv("LOCAL_DATABASE_URL"),
        help="Defaults to LOCAL_DATABASE_URL",
    )
    parser.add_argument("--timeout-seconds", type=float, default=120)
    return parser


def main() -> int:
    load_dotenv()
    args = _build_parser().parse_args()
    try:
        return asyncio.run(run(args))
    except KeyboardInterrupt:
        print("\nArchive stopped by user. Remote data was not truncated.", file=sys.stderr)
        return 130
    except Exception as exc:
        print(f"ERROR: archive failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
