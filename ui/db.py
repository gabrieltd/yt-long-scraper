"""Database layer for the Channel Relevance UI."""

from __future__ import annotations

import os
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
    "first_upload_date",
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


# ─── Channel query with filters ────────────────────────────────────────────

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
    relevance_filter: str = "all",  # all | unmarked | relevant | not_relevant
    tag_filter: str | None = None,
    last_uploaded_after: str | None = None,   # YYYYMMDD
    last_uploaded_before: str | None = None,  # YYYYMMDD
    first_uploaded_after: str | None = None,  # YYYYMMDD
    first_uploaded_before: str | None = None, # YYYYMMDD
    sort_by: str = "hit_videos_count",
    sort_order: str = "desc",
    page: int = 1,
    page_size: int = 50,
) -> dict[str, Any]:
    """Return filtered channels with stats + relevance, paginated."""
    lang = _validate_lang(lang)
    pool = _require_pool()

    # Sanitize sort
    if sort_by not in VALID_SORT_COLUMNS:
        sort_by = "hit_videos_count"
    sort_order = "ASC" if sort_order.lower() == "asc" else "DESC"

    # Build parameterised query
    params: list[Any] = []
    idx = 0

    def _next() -> str:
        nonlocal idx
        idx += 1
        return f"${idx}"

    # ── Inner subquery (channel video stats) ──
    cv_conditions = []
    hit_view_min_param = _next()
    params.append(min_views_individual)

    if max_views_individual is not None:
        hit_view_max_param = _next()
        params.append(max_views_individual)
        hit_filter = f"WHERE cv.view_count >= {hit_view_min_param} AND cv.view_count <= {hit_view_max_param}"
    else:
        hit_filter = f"WHERE cv.view_count >= {hit_view_min_param}"

    stats_subquery = f"""
        SELECT
            cv.channel_url,
            COUNT(*) AS total_videos_tracked,
            COUNT(*) FILTER ({hit_filter}) AS hit_videos_count,
            ROUND(AVG(cv.view_count), 2) AS avg_views_on_channel,
            MAX(cv.view_count) AS max_views_on_channel
        FROM channel_videos_raw_{lang} cv
        GROUP BY cv.channel_url
    """

    # ── Outer WHERE conditions ──
    where_clauses = []

    # total_videos_tracked range
    p = _next(); params.append(min_videos_total)
    where_clauses.append(f"stats.total_videos_tracked >= {p}")
    if max_videos_total is not None:
        p = _next(); params.append(max_videos_total)
        where_clauses.append(f"stats.total_videos_tracked <= {p}")

    # min hits
    p = _next(); params.append(min_hits_count)
    where_clauses.append(f"stats.hit_videos_count >= {p}")

    # min avg views
    p = _next(); params.append(min_avg_views)
    where_clauses.append(f"stats.avg_views_on_channel >= {p}")

    # subscriber filters
    if min_subscribers is not None:
        p = _next(); params.append(min_subscribers)
        where_clauses.append(f"cr.subscriber_count >= {p}")
    if max_subscribers is not None:
        p = _next(); params.append(max_subscribers)
        where_clauses.append(f"cr.subscriber_count <= {p}")

    # verified filter
    if is_verified is not None:
        p = _next(); params.append(is_verified)
        where_clauses.append(f"cr.is_verified = {p}")

    # channel name search (ILIKE)
    if channel_name_search:
        p = _next(); params.append(f"%{channel_name_search}%")
        where_clauses.append(f"cr.channel_name ILIKE {p}")

    # relevance filter
    if relevance_filter == "unmarked":
        where_clauses.append("rel.is_relevant IS NULL")
    elif relevance_filter == "relevant":
        where_clauses.append("rel.is_relevant = TRUE")
    elif relevance_filter == "not_relevant":
        where_clauses.append("rel.is_relevant = FALSE")
    # "all" → no filter

    # tag filter
    if tag_filter:
        p = _next(); params.append([tag_filter])
        where_clauses.append(f"rel.tags @> {p}")

    # last_upload_date filters (YYYYMMDD string comparison)
    if last_uploaded_after:
        p = _next(); params.append(last_uploaded_after)
        where_clauses.append(f"cr.last_upload_date >= {p}")
    if last_uploaded_before:
        p = _next(); params.append(last_uploaded_before)
        where_clauses.append(f"cr.last_upload_date <= {p}")

    # first_upload_date filters (YYYYMMDD string comparison)
    if first_uploaded_after:
        p = _next(); params.append(first_uploaded_after)
        where_clauses.append(f"cr.first_upload_date >= {p}")
    if first_uploaded_before:
        p = _next(); params.append(first_uploaded_before)
        where_clauses.append(f"cr.first_upload_date <= {p}")

    where_sql = " AND ".join(where_clauses) if where_clauses else "TRUE"

    # ── Pagination ──
    offset = (max(page, 1) - 1) * page_size

    # ── Count query ──
    count_sql = f"""
        SELECT COUNT(*) AS total
        FROM channels_raw_{lang} cr
        JOIN ({stats_subquery}) stats ON cr.channel_url = stats.channel_url
        LEFT JOIN channel_relevance_{lang} rel ON cr.channel_url = rel.channel_url
        WHERE {where_sql}
    """

    # ── Data query ──
    data_sql = f"""
        SELECT
            cr.channel_name,
            cr.channel_url,
            cr.avatar_url,
            cr.subscriber_count,
            cr.is_verified,
            cr.last_upload_date,
            cr.first_upload_date,
            stats.total_videos_tracked,
            stats.hit_videos_count,
            stats.avg_views_on_channel,
            stats.max_views_on_channel,
            rel.is_relevant,
            rel.notes,
            rel.tags,
            rel.marked_at
        FROM channels_raw_{lang} cr
        JOIN ({stats_subquery}) stats ON cr.channel_url = stats.channel_url
        LEFT JOIN channel_relevance_{lang} rel ON cr.channel_url = rel.channel_url
        WHERE {where_sql}
        ORDER BY {sort_by} {sort_order}
        LIMIT {page_size} OFFSET {offset}
    """

    async with pool.acquire() as conn:
        count_row = await conn.fetchrow(count_sql, *params)
        total = count_row["total"] if count_row else 0
        rows = await conn.fetch(data_sql, *params)

    channels = []
    for r in rows:
        channels.append({
            "channel_name": r["channel_name"],
            "channel_url": r["channel_url"],
            "avatar_url": r["avatar_url"],
            "subscriber_count": r["subscriber_count"],
            "is_verified": r["is_verified"],
            "last_upload_date": r["last_upload_date"],
            "first_upload_date": r["first_upload_date"],
            "total_videos_tracked": r["total_videos_tracked"],
            "hit_videos_count": r["hit_videos_count"],
            "avg_views_on_channel": float(r["avg_views_on_channel"]) if r["avg_views_on_channel"] is not None else 0,
            "max_views_on_channel": r["max_views_on_channel"],
            "is_relevant": r["is_relevant"],
            "notes": r["notes"],
            "tags": list(r["tags"]) if r["tags"] else [],
            "marked_at": r["marked_at"].isoformat() if r["marked_at"] else None,
        })

    return {
        "channels": channels,
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": max(1, -(-total // page_size)),  # ceil division
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

    hit_filter = "cv.view_count >= $2"
    params: list[Any] = [channel_url, min_views_individual]
    if max_views_individual is not None:
        hit_filter += " AND cv.view_count <= $3"
        params.append(max_views_individual)

    channel_sql = f"""
        WITH stats AS (
            SELECT
                cv.channel_url,
                COUNT(*) AS total_videos_tracked,
                COUNT(*) FILTER (WHERE {hit_filter}) AS hit_videos_count,
                ROUND(AVG(cv.view_count), 2) AS avg_views_on_channel,
                MAX(cv.view_count) AS max_views_on_channel
            FROM channel_videos_raw_{lang} cv
            WHERE cv.channel_url = $1
            GROUP BY cv.channel_url
        )
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
            cr.first_upload_date,
            stats.total_videos_tracked,
            stats.hit_videos_count,
            stats.avg_views_on_channel,
            stats.max_views_on_channel,
            rel.is_relevant,
            rel.notes,
            rel.tags
        FROM channels_raw_{lang} cr
        JOIN stats ON stats.channel_url = cr.channel_url
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
        channel_row = await conn.fetchrow(channel_sql, *params)
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
        "first_upload_date": channel_row["first_upload_date"],
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
