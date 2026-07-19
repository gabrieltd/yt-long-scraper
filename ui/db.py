"""Database layer for the Channel Relevance UI."""

from __future__ import annotations

import os
import base64
import json
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from typing import Any

import asyncpg
from dotenv import load_dotenv

load_dotenv()

_pool: asyncpg.Pool | None = None

VALID_LANGUAGES = ("es", "en")
VALID_SORT_COLUMNS = {
    "channel_name",
    "subscriber_count",
    "total_videos_tracked",
    "hit_videos_count",
    "avg_views_on_channel",
    "max_views_on_channel",
    "is_relevant",
    "last_upload_date",
    "first_video_published_at",
}


async def init_pool() -> None:
    global _pool
    if _pool is not None:
        return
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        raise RuntimeError("DATABASE_URL environment variable not set")
    _pool = await asyncpg.create_pool(
        dsn,
        min_size=1,
        max_size=10,
        statement_cache_size=0,
        timeout=120,
        command_timeout=60,
    )


async def close_pool() -> None:
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None


def _require_pool() -> asyncpg.Pool:
    if _pool is None:
        raise RuntimeError("DB pool not initialized. Call init_pool() first.")
    return _pool


def _validate_lang(lang: str) -> str:
    lang = lang.lower().strip()
    if lang not in VALID_LANGUAGES:
        raise ValueError(f"Invalid language: {lang}")
    return lang


def _encode_cursor(sort_by: str, sort_order: str, row: asyncpg.Record) -> str:
    value = row[sort_by]
    if isinstance(value, Decimal):
        value = str(value)
    elif isinstance(value, (date, datetime)):
        value = value.isoformat()
    payload = {
        "sort_by": sort_by,
        "sort_order": sort_order,
        "value": value,
        "channel_url": row["channel_url"],
    }
    return base64.urlsafe_b64encode(json.dumps(payload, separators=(",", ":")).encode()).decode().rstrip("=")


def _decode_cursor(cursor: str, sort_by: str, sort_order: str) -> tuple[Any, str]:
    try:
        padded = cursor + "=" * (-len(cursor) % 4)
        payload = json.loads(base64.urlsafe_b64decode(padded).decode())
    except (ValueError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("Invalid pagination cursor") from exc

    if payload.get("sort_by") != sort_by or payload.get("sort_order") != sort_order:
        raise ValueError("Pagination cursor does not match the selected sort")
    channel_url = payload.get("channel_url")
    if not isinstance(channel_url, str) or not channel_url:
        raise ValueError("Invalid pagination cursor")
    value = payload.get("value")
    if sort_by == "first_video_published_at" and isinstance(value, str):
        try:
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError("Invalid pagination cursor timestamp") from exc
    return value, channel_url


def _utc_day_boundary(value: str, *, next_day: bool = False) -> datetime:
    try:
        parsed = date.fromisoformat(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid date: {value}") from exc
    if next_day:
        parsed += timedelta(days=1)
    return datetime.combine(parsed, time.min, tzinfo=timezone.utc)


async def get_filtered_channels(
    lang: str,
    *,
    min_views_individual: int = 0,
    max_views_individual: int | None = None,
    min_videos_total: int = 0,
    max_videos_total: int | None = None,
    min_hits_count: int = 0,
    min_avg_views: int = 0,
    min_subscribers: int | None = None,
    max_subscribers: int | None = None,
    is_verified: bool | None = None,
    channel_name_search: str | None = None,
    relevance_filter: str = "all",
    tag_filter: str | None = None,
    last_uploaded_after: str | None = None,
    last_uploaded_before: str | None = None,
    first_video_after: str | None = None,
    first_video_before: str | None = None,
    sort_by: str = "hit_videos_count",
    sort_order: str = "desc",
    cursor: str | None = None,
    page_size: int = 50,
) -> dict[str, Any]:
    """Return one keyset-paginated channel page backed by materialized stats."""
    lang = _validate_lang(lang)
    pool = _require_pool()
    page_size = max(1, min(page_size, 200))

    if sort_by not in VALID_SORT_COLUMNS:
        sort_by = "hit_videos_count"
    sort_order = "ASC" if sort_order.lower() == "asc" else "DESC"

    params: list[Any] = []
    index = 0

    def _next(value: Any) -> str:
        nonlocal index
        index += 1
        params.append(value)
        return f"${index}"

    min_views_param = _next(min_views_individual)
    max_views_param = _next(max_views_individual)
    hit_expression = (
        f"count_channel_views_in_range(stats.view_counts, {min_views_param}, {max_views_param})"
    )

    where_clauses = [
        f"total_videos_tracked >= {_next(min_videos_total)}",
        f"hit_videos_count >= {_next(min_hits_count)}",
        f"avg_views_on_channel >= {_next(min_avg_views)}",
    ]
    if max_videos_total is not None:
        where_clauses.append(f"total_videos_tracked <= {_next(max_videos_total)}")
    if min_subscribers is not None:
        where_clauses.append(f"subscriber_count >= {_next(min_subscribers)}")
    if max_subscribers is not None:
        where_clauses.append(f"subscriber_count <= {_next(max_subscribers)}")
    if is_verified is not None:
        where_clauses.append(f"is_verified = {_next(is_verified)}")
    if channel_name_search:
        where_clauses.append(f"channel_name ILIKE {_next(f'%{channel_name_search}%')}")
    if relevance_filter == "unmarked":
        where_clauses.append("is_relevant IS NULL")
    elif relevance_filter == "relevant":
        where_clauses.append("is_relevant IS TRUE")
    elif relevance_filter == "not_relevant":
        where_clauses.append("is_relevant IS FALSE")
    if tag_filter:
        where_clauses.append(f"tags @> {_next([tag_filter])}")
    if last_uploaded_after:
        where_clauses.append(f"last_upload_date >= {_next(last_uploaded_after)}")
    if last_uploaded_before:
        where_clauses.append(f"last_upload_date <= {_next(last_uploaded_before)}")
    if first_video_after:
        where_clauses.append(
            f"first_video_published_at >= {_next(_utc_day_boundary(first_video_after))}"
        )
    if first_video_before:
        where_clauses.append(
            f"first_video_published_at < {_next(_utc_day_boundary(first_video_before, next_day=True))}"
        )

    if cursor:
        cursor_value, cursor_channel_url = _decode_cursor(cursor, sort_by, sort_order)
        if cursor_value is None:
            where_clauses.append(f"({sort_by} IS NULL AND channel_url > {_next(cursor_channel_url)})")
        else:
            cursor_value_param = _next(cursor_value)
            if sort_by == "avg_views_on_channel":
                cursor_value_param = f"{cursor_value_param}::NUMERIC"
            cursor_url_param = _next(cursor_channel_url)
            comparator = "<" if sort_order == "DESC" else ">"
            where_clauses.append(
                f"(({sort_by} IS NOT NULL AND ({sort_by} {comparator} {cursor_value_param} "
                f"OR ({sort_by} = {cursor_value_param} AND channel_url > {cursor_url_param}))) "
                f"OR {sort_by} IS NULL)"
            )

    where_sql = " AND ".join(where_clauses) if where_clauses else "TRUE"
    query = f"""
        WITH channel_rows AS (
            SELECT
                cr.channel_name,
                cr.channel_url,
                cr.avatar_url,
                cr.subscriber_count,
                cr.is_verified,
                cr.last_upload_date,
                cr.first_video_id,
                cr.first_video_published_at,
                cr.first_video_status,
                stats.total_videos_tracked,
                {hit_expression} AS hit_videos_count,
                stats.avg_views_on_channel,
                stats.max_views_on_channel,
                rel.is_relevant,
                rel.notes,
                rel.tags,
                rel.marked_at
            FROM channels_raw_{lang} cr
            JOIN channel_stats_{lang} stats ON cr.channel_url = stats.channel_url
            LEFT JOIN channel_relevance_{lang} rel ON cr.channel_url = rel.channel_url
        )
        SELECT *
        FROM channel_rows
        WHERE {where_sql}
        ORDER BY {sort_by} {sort_order} NULLS LAST, channel_url ASC
        LIMIT {page_size + 1}
    """

    async with pool.acquire() as conn:
        rows = await conn.fetch(query, *params)

    has_next = len(rows) > page_size
    page_rows = rows[:page_size]
    channels = [
        {
            "channel_name": row["channel_name"],
            "channel_url": row["channel_url"],
            "avatar_url": row["avatar_url"],
            "subscriber_count": row["subscriber_count"],
            "is_verified": row["is_verified"],
            "last_upload_date": row["last_upload_date"],
            "first_video_id": row["first_video_id"],
            "first_video_published_at": (
                row["first_video_published_at"].isoformat()
                if row["first_video_published_at"] else None
            ),
            "first_video_status": row["first_video_status"],
            "total_videos_tracked": row["total_videos_tracked"],
            "hit_videos_count": row["hit_videos_count"],
            "avg_views_on_channel": float(row["avg_views_on_channel"]) if row["avg_views_on_channel"] is not None else 0,
            "max_views_on_channel": row["max_views_on_channel"],
            "is_relevant": row["is_relevant"],
            "notes": row["notes"],
            "tags": list(row["tags"]) if row["tags"] else [],
            "marked_at": row["marked_at"].isoformat() if row["marked_at"] else None,
        }
        for row in page_rows
    ]
    return {
        "channels": channels,
        "page_size": page_size,
        "has_next": has_next,
        "next_cursor": _encode_cursor(sort_by, sort_order, page_rows[-1]) if has_next and page_rows else None,
    }


async def get_channel_details(
    lang: str,
    channel_url: str,
    *,
    min_views_individual: int = 0,
    max_views_individual: int | None = None,
) -> dict[str, Any] | None:
    """Return persisted channel metadata, current stats, and tracked videos."""
    lang = _validate_lang(lang)
    pool = _require_pool()

    channel_sql = f"""
        SELECT
            cr.channel_url,
            cr.channel_id,
            cr.channel_name,
            cr.subscriber_count,
            cr.is_verified,
            cr.channel_description,
            cr.channel_tags,
            cr.avatar_url,
            cr.banner_url,
            cr.uploader_id,
            cr.uploader_url,
            cr.last_upload_date,
            cr.first_video_id,
            cr.first_video_published_at,
            cr.first_video_status,
            stats.total_videos_tracked,
            count_channel_views_in_range(stats.view_counts, $2, $3) AS hit_videos_count,
            stats.avg_views_on_channel,
            stats.max_views_on_channel,
            rel.is_relevant,
            rel.notes,
            rel.tags
        FROM channels_raw_{lang} cr
        JOIN channel_stats_{lang} stats ON stats.channel_url = cr.channel_url
        LEFT JOIN channel_relevance_{lang} rel ON rel.channel_url = cr.channel_url
        WHERE cr.channel_url = $1
    """
    videos_sql = f"""
        SELECT video_id, title, video_url, thumbnail_url, upload_date, duration_seconds, view_count
        FROM channel_videos_raw_{lang}
        WHERE channel_url = $1
        ORDER BY upload_date DESC NULLS LAST, view_count DESC NULLS LAST
    """

    async with pool.acquire() as conn:
        channel_row = await conn.fetchrow(
            channel_sql,
            channel_url,
            min_views_individual,
            max_views_individual,
        )
        if channel_row is None:
            return None
        video_rows = await conn.fetch(videos_sql, channel_url)

    channel = {
        "channel_url": channel_row["channel_url"],
        "channel_id": channel_row["channel_id"],
        "channel_name": channel_row["channel_name"],
        "subscriber_count": channel_row["subscriber_count"],
        "is_verified": channel_row["is_verified"],
        "channel_description": channel_row["channel_description"],
        "channel_tags": list(channel_row["channel_tags"]) if channel_row["channel_tags"] else [],
        "avatar_url": channel_row["avatar_url"],
        "banner_url": channel_row["banner_url"],
        "uploader_id": channel_row["uploader_id"],
        "uploader_url": channel_row["uploader_url"],
        "last_upload_date": channel_row["last_upload_date"],
        "first_video_id": channel_row["first_video_id"],
        "first_video_published_at": (
            channel_row["first_video_published_at"].isoformat()
            if channel_row["first_video_published_at"] else None
        ),
        "first_video_status": channel_row["first_video_status"],
        "total_videos_tracked": channel_row["total_videos_tracked"],
        "hit_videos_count": channel_row["hit_videos_count"],
        "avg_views_on_channel": float(channel_row["avg_views_on_channel"]) if channel_row["avg_views_on_channel"] is not None else 0,
        "max_views_on_channel": channel_row["max_views_on_channel"],
        "is_relevant": channel_row["is_relevant"],
        "notes": channel_row["notes"],
        "tags": list(channel_row["tags"]) if channel_row["tags"] else [],
    }
    videos = [
        {
            "video_id": row["video_id"],
            "title": row["title"],
            "video_url": row["video_url"],
            "thumbnail_url": row["thumbnail_url"],
            "upload_date": row["upload_date"],
            "duration_seconds": row["duration_seconds"],
            "view_count": row["view_count"],
        }
        for row in video_rows
    ]
    return {"channel": channel, "videos": videos}


# ─── Set relevance ─────────────────────────────────────────────────────────

async def set_channel_relevance(
    lang: str,
    channel_url: str,
    *,
    is_relevant: bool | None,
    notes: str | None = None,
    tags: list[str] | None = None,
) -> None:
    """Upsert channel relevance."""
    lang = _validate_lang(lang)
    pool = _require_pool()

    await pool.execute(
        f"""
        INSERT INTO channel_relevance_{lang} (channel_url, is_relevant, notes, tags, marked_at)
        VALUES ($1, $2, $3, $4, CURRENT_TIMESTAMP)
        ON CONFLICT (channel_url) DO UPDATE SET
            is_relevant = EXCLUDED.is_relevant,
            notes = EXCLUDED.notes,
            tags = EXCLUDED.tags,
            marked_at = CURRENT_TIMESTAMP
        """,
        channel_url,
        is_relevant,
        notes,
        tags or [],
    )


async def set_channels_relevance_bulk(
    lang: str,
    channel_urls: list[str],
    *,
    is_relevant: bool | None,
) -> None:
    """Bulk upsert channel relevance."""
    if not channel_urls:
        return
    lang = _validate_lang(lang)
    pool = _require_pool()

    tuples = [(url, is_relevant) for url in channel_urls]

    await pool.executemany(
        f"""
        INSERT INTO channel_relevance_{lang} (channel_url, is_relevant, notes, tags, marked_at)
        VALUES ($1, $2, NULL, NULL, CURRENT_TIMESTAMP)
        ON CONFLICT (channel_url) DO UPDATE SET
            is_relevant = EXCLUDED.is_relevant,
            marked_at = CURRENT_TIMESTAMP
        """,
        tuples
    )


# ─── Tags ──────────────────────────────────────────────────────────────────

async def get_distinct_tags(lang: str) -> list[str]:
    lang = _validate_lang(lang)
    pool = _require_pool()
    rows = await pool.fetch(
        f"SELECT DISTINCT UNNEST(tags) AS tag FROM channel_relevance_{lang} ORDER BY tag"
    )
    return [r["tag"] for r in rows]


# ─── Summary stats ─────────────────────────────────────────────────────────

async def get_summary_stats(lang: str) -> dict[str, int]:
    lang = _validate_lang(lang)
    pool = _require_pool()
    row = await pool.fetchrow(f"""
        SELECT
            (SELECT COUNT(*) FROM channels_raw_{lang}) AS total_channels,
            (SELECT COUNT(*) FROM channel_relevance_{lang} WHERE is_relevant = TRUE) AS relevant,
            (SELECT COUNT(*) FROM channel_relevance_{lang} WHERE is_relevant = FALSE) AS not_relevant
    """)
    total = row["total_channels"]
    relevant = row["relevant"]
    not_relevant = row["not_relevant"]
    return {
        "total_channels": total,
        "relevant": relevant,
        "not_relevant": not_relevant,
        "unmarked": total - relevant - not_relevant,
    }
