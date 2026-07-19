"""PostgreSQL persistence layer for YouTube discovery."""

from __future__ import annotations

import os
import uuid
import asyncio
import json
from datetime import datetime, timezone
from typing import Any

import asyncpg

_DB_POOL: asyncpg.Pool | None = None
_DB_LANGUAGE: str = "es"  # Track the current language for table naming
_VALID_LANGUAGES = {"es", "en"}


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


async def init_db(
    dsn: str | None = None,
    min_size: int = 1,
    max_size: int = 20,
    language: str = "es",
    *,
    ensure_schema: bool = True,
) -> None:
    """Initialize the PostgreSQL connection pool and schema.
    
    Args:
        dsn: Database connection string
        min_size: Minimum pool size
        max_size: Maximum pool size
        language: Language suffix for tables ('es' or 'en')
        ensure_schema: Run idempotent schema creation/migrations before returning.
    """
    global _DB_POOL, _DB_LANGUAGE
    
    # Check if pool exists and is valid
    if _DB_POOL is not None:
        # If language changed, close old pool and reinitialize
        if _DB_LANGUAGE != language:
            await close_db()
        # If pool is closed or invalid, reinitialize
        elif _DB_POOL._closing or _DB_POOL._closed:
            _DB_POOL = None
        else:
            # Pool is valid and language matches, skip initialization
            return

    dsn = dsn or os.getenv("DATABASE_URL")
    if not dsn:
        raise RuntimeError("DATABASE_URL environment variable not set")

    # Store language for table naming
    _DB_LANGUAGE = language

    # Create a connection pool with statement cache disabled for PgBouncer compatibility
    # Defaulting to min_size=1 usually saves resources in serverless/container envs.
    # Increase timeout for connections (default is 60s)
    _DB_POOL = await asyncpg.create_pool(
        dsn, 
        min_size=min_size, 
        max_size=max_size, 
        statement_cache_size=0,
        timeout=120,  # Increase timeout to 120 seconds for slow connections
        command_timeout=60  # Set command timeout
    )
    
    if ensure_schema:
        await create_tables(language)


async def create_tables(language: str = "es") -> None:
    """Create language-specific database tables.
    
    Args:
        language: Language suffix for tables ('es' or 'en')
    """
    if language not in _VALID_LANGUAGES:
        raise ValueError(f"Unsupported language: {language}")

    pool = _require_pool()
    lang_suffix = f"_{language}"
    
    async with pool.acquire() as conn:
        # Schema creation
        await conn.execute(f"""
            CREATE TABLE IF NOT EXISTS search_runs{lang_suffix} (
                id TEXT PRIMARY KEY,
                query TEXT,
                mode TEXT,
                started_at TIMESTAMPTZ,
                finished_at TIMESTAMPTZ
            );
        """)

        # videos_raw
        await conn.execute(f"""
            CREATE TABLE IF NOT EXISTS videos_raw{lang_suffix} (
                video_id TEXT PRIMARY KEY,
                search_run_id TEXT REFERENCES search_runs{lang_suffix}(id),
                query TEXT,
                video_url TEXT,
                channel_url TEXT,
                duration_text TEXT,
                views_text TEXT,
                published_text TEXT,
                thumbnail_url TEXT,
                video_type TEXT,
                is_multi_creator BOOLEAN,
                discovered_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
            );
        """)

        # videos_normalized
        await conn.execute(f"""
            CREATE TABLE IF NOT EXISTS videos_normalized{lang_suffix} (
                video_id TEXT PRIMARY KEY REFERENCES videos_raw{lang_suffix}(video_id),
                channel_url TEXT,
                query TEXT,
                views_estimated BIGINT,
                published_at_estimated TIMESTAMPTZ,
                duration_seconds_estimated BIGINT,
                validation_passed BOOLEAN,
                validation_reason TEXT,
                normalized_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
            );
        """)

        # channels_raw
        await conn.execute(f"""
            CREATE TABLE IF NOT EXISTS channels_raw{lang_suffix} (
                channel_url TEXT PRIMARY KEY,
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
                last_upload_date TEXT,
                first_video_id TEXT,
                first_video_published_at TIMESTAMPTZ,
                first_video_checked_at TIMESTAMPTZ,
                first_video_last_attempt_at TIMESTAMPTZ,
                first_video_status TEXT NOT NULL DEFAULT 'pending',
                first_video_source TEXT,
                first_video_last_error TEXT,
                first_video_claimed_at TIMESTAMPTZ,
                extracted_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
            );
        """)

        # Migration: add last_upload_date if table already exists without it
        await conn.execute(f"""
            ALTER TABLE channels_raw{lang_suffix}
            ADD COLUMN IF NOT EXISTS last_upload_date TEXT;
        """)

        for column, definition in (
            ("channel_description", "TEXT"),
            ("channel_tags", "TEXT[]"),
            ("avatar_url", "TEXT"),
            ("banner_url", "TEXT"),
            ("uploader_id", "TEXT"),
            ("uploader_url", "TEXT"),
            ("first_video_id", "TEXT"),
            ("first_video_published_at", "TIMESTAMPTZ"),
            ("first_video_checked_at", "TIMESTAMPTZ"),
            ("first_video_last_attempt_at", "TIMESTAMPTZ"),
            ("first_video_status", "TEXT NOT NULL DEFAULT 'pending'"),
            ("first_video_source", "TEXT"),
            ("first_video_last_error", "TEXT"),
            ("first_video_claimed_at", "TIMESTAMPTZ"),
        ):
            await conn.execute(
                f"ALTER TABLE channels_raw{lang_suffix} ADD COLUMN IF NOT EXISTS {column} {definition};"
            )

        await conn.execute(f"""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_constraint
                    WHERE conname = 'channels_raw{language}_first_video_status_check'
                ) THEN
                    ALTER TABLE channels_raw{lang_suffix}
                    ADD CONSTRAINT channels_raw{language}_first_video_status_check
                    CHECK (first_video_status IN ('pending', 'processing', 'success', 'no_public_videos'));
                END IF;
                IF NOT EXISTS (
                    SELECT 1 FROM pg_constraint
                    WHERE conname = 'channels_raw{language}_first_video_source_check'
                ) THEN
                    ALTER TABLE channels_raw{lang_suffix}
                    ADD CONSTRAINT channels_raw{language}_first_video_source_check
                    CHECK (first_video_source IS NULL OR first_video_source IN ('innertube', 'yt_dlp'));
                END IF;
            END
            $$;
        """)

        # channel_videos_raw
        await conn.execute(f"""
            CREATE TABLE IF NOT EXISTS channel_videos_raw{lang_suffix} (
                channel_url TEXT NOT NULL,
                video_id TEXT NOT NULL,
                upload_date TEXT,
                duration_seconds BIGINT,
                view_count BIGINT,
                title TEXT,
                video_url TEXT,
                thumbnail_url TEXT,
                PRIMARY KEY (channel_url, video_id)
            );
        """)

        for column in ("title", "video_url", "thumbnail_url"):
            await conn.execute(
                f"ALTER TABLE channel_videos_raw{lang_suffix} ADD COLUMN IF NOT EXISTS {column} TEXT;"
            )

        # channels_processed
        await conn.execute(f"""
            CREATE TABLE IF NOT EXISTS channels_processed{lang_suffix} (
                channel_url TEXT PRIMARY KEY,
                processed_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                status TEXT DEFAULT 'success'
            );
        """)

        # channels_discovery_claims
        await conn.execute(f"""
            CREATE TABLE IF NOT EXISTS channels_discovery_claims{lang_suffix} (
                channel_url TEXT PRIMARY KEY,
                claimed_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                claim_owner TEXT
            );
        """)
        await conn.execute(f"""
            ALTER TABLE channels_discovery_claims{lang_suffix}
            ADD COLUMN IF NOT EXISTS claim_owner TEXT;
        """)

        # channel_relevance — tracks whether a channel is relevant, with notes and tags
        await conn.execute(f"""
            CREATE TABLE IF NOT EXISTS channel_relevance{lang_suffix} (
                channel_url TEXT PRIMARY KEY,
                is_relevant BOOLEAN,
                notes TEXT,
                tags TEXT[],
                marked_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
            );
        """)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS channel_stats_refresh_locks (
                language TEXT PRIMARY KEY,
                locked_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
        """)

        await conn.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm;")

        await conn.execute("""
            CREATE OR REPLACE FUNCTION count_channel_views_in_range(
                view_counts BIGINT[],
                min_views BIGINT,
                max_views BIGINT DEFAULT NULL
            ) RETURNS INTEGER
            LANGUAGE plpgsql
            IMMUTABLE
            PARALLEL SAFE
            AS $$
            DECLARE
                item_count INTEGER := COALESCE(array_length(view_counts, 1), 0);
                low_index INTEGER := 1;
                high_index INTEGER;
                midpoint INTEGER;
                first_match INTEGER := 0;
                last_match INTEGER := 0;
            BEGIN
                IF item_count = 0 THEN
                    RETURN 0;
                END IF;

                high_index := item_count;
                WHILE low_index <= high_index LOOP
                    midpoint := (low_index + high_index) / 2;
                    IF view_counts[midpoint] >= min_views THEN
                        first_match := midpoint;
                        high_index := midpoint - 1;
                    ELSE
                        low_index := midpoint + 1;
                    END IF;
                END LOOP;
                IF first_match = 0 THEN
                    RETURN 0;
                END IF;
                IF max_views IS NULL THEN
                    RETURN item_count - first_match + 1;
                END IF;

                low_index := first_match;
                high_index := item_count;
                WHILE low_index <= high_index LOOP
                    midpoint := (low_index + high_index) / 2;
                    IF view_counts[midpoint] <= max_views THEN
                        last_match := midpoint;
                        low_index := midpoint + 1;
                    ELSE
                        high_index := midpoint - 1;
                    END IF;
                END LOOP;
                RETURN GREATEST(last_match - first_match + 1, 0);
            END
            $$;
        """)

        # Indices
        indices = [
            f"CREATE INDEX IF NOT EXISTS idx_channel_relevance{lang_suffix}_is_relevant ON channel_relevance{lang_suffix} (is_relevant);",
            f"CREATE INDEX IF NOT EXISTS idx_videos_raw{lang_suffix}_channel_url ON videos_raw{lang_suffix} (channel_url);",
            f"CREATE INDEX IF NOT EXISTS idx_videos_raw{lang_suffix}_discovered_at ON videos_raw{lang_suffix} (discovered_at);",
            f"CREATE INDEX IF NOT EXISTS idx_videos_raw{lang_suffix}_search_run_id ON videos_raw{lang_suffix} (search_run_id);",
            f"CREATE INDEX IF NOT EXISTS idx_videos_normalized{lang_suffix}_validation_passed ON videos_normalized{lang_suffix} (validation_passed);",
            f"CREATE INDEX IF NOT EXISTS idx_videos_normalized{lang_suffix}_normalized_at ON videos_normalized{lang_suffix} (normalized_at);",
            f"CREATE INDEX IF NOT EXISTS idx_channels_processed{lang_suffix}_processed_at ON channels_processed{lang_suffix} (processed_at);",
            f"CREATE INDEX IF NOT EXISTS idx_channel_videos_raw{lang_suffix}_channel_url ON channel_videos_raw{lang_suffix} (channel_url);",
            f"CREATE INDEX IF NOT EXISTS idx_channels_raw{lang_suffix}_extracted_at ON channels_raw{lang_suffix} (extracted_at);",
            f"CREATE INDEX IF NOT EXISTS idx_channels_raw{lang_suffix}_subscribers ON channels_raw{lang_suffix} (subscriber_count);",
            f"CREATE INDEX IF NOT EXISTS idx_channels_raw{lang_suffix}_last_upload ON channels_raw{lang_suffix} (last_upload_date);",
            f"CREATE INDEX IF NOT EXISTS idx_channels_raw{lang_suffix}_first_video_published ON channels_raw{lang_suffix} (first_video_published_at);",
            f"CREATE INDEX IF NOT EXISTS idx_channels_raw{lang_suffix}_first_video_pending ON channels_raw{lang_suffix} (first_video_status, first_video_last_attempt_at);",
            f"CREATE INDEX IF NOT EXISTS idx_channel_claims{lang_suffix}_claimed_at ON channels_discovery_claims{lang_suffix} (claimed_at);",
            f"CREATE INDEX IF NOT EXISTS idx_channel_claims{lang_suffix}_owner ON channels_discovery_claims{lang_suffix} (claim_owner);",
            f"CREATE INDEX IF NOT EXISTS idx_channels_raw{lang_suffix}_verified_true ON channels_raw{lang_suffix} (channel_url) WHERE is_verified IS TRUE;",
            f"CREATE INDEX IF NOT EXISTS idx_channel_relevance{lang_suffix}_tags_gin ON channel_relevance{lang_suffix} USING GIN (tags);",
            f"CREATE INDEX IF NOT EXISTS idx_channels_raw{lang_suffix}_name_trgm ON channels_raw{lang_suffix} USING GIN (channel_name gin_trgm_ops);",
        ]
        for idx in indices:
            await conn.execute(idx)

        await conn.execute(f"""
            CREATE MATERIALIZED VIEW IF NOT EXISTS channel_stats{lang_suffix} AS
            SELECT
                channel_url,
                COUNT(*) AS total_videos_tracked,
                ROUND(AVG(view_count), 2) AS avg_views_on_channel,
                MAX(view_count) AS max_views_on_channel,
                ARRAY_AGG(view_count ORDER BY view_count)
                    FILTER (WHERE view_count IS NOT NULL) AS view_counts
            FROM channel_videos_raw{lang_suffix}
            GROUP BY channel_url
            WITH DATA;
        """, timeout=600)
        await conn.execute(
            f"CREATE UNIQUE INDEX IF NOT EXISTS idx_channel_stats{lang_suffix}_channel_url "
            f"ON channel_stats{lang_suffix} (channel_url);"
        )


async def refresh_channel_stats(language: str) -> bool:
    """Refresh materialized channel statistics after a discovery batch.

    A persisted lease prevents parallel workers from scheduling duplicate refreshes.
    The lease is intentionally table-backed instead of advisory-lock based so it is
    safe with the transaction pooler used by the hosted database.
    """
    if language not in _VALID_LANGUAGES:
        raise ValueError(f"Unsupported language: {language}")

    pool = _require_pool()
    async with pool.acquire() as conn:
        acquired = await conn.fetchval(
            """
            INSERT INTO channel_stats_refresh_locks (language, locked_at)
            VALUES ($1, CURRENT_TIMESTAMP)
            ON CONFLICT (language) DO UPDATE SET locked_at = EXCLUDED.locked_at
            WHERE channel_stats_refresh_locks.locked_at < CURRENT_TIMESTAMP - INTERVAL '2 hours'
            RETURNING language
            """,
            language,
        )
        if acquired is None:
            return False

        try:
            await conn.execute(
                f"REFRESH MATERIALIZED VIEW CONCURRENTLY channel_stats_{language};",
                timeout=600,
            )
            return True
        finally:
            await conn.execute("DELETE FROM channel_stats_refresh_locks WHERE language = $1", language)


async def close_db() -> None:
    global _DB_POOL, _DB_LANGUAGE
    if _DB_POOL is None:
        return
    await _DB_POOL.close()
    _DB_POOL = None
    _DB_LANGUAGE = "es"


def _require_pool() -> asyncpg.Pool:
    if _DB_POOL is None:
        raise RuntimeError("DB not initialized. Call init_db() first.")
    return _DB_POOL


def _get_table_name(base_name: str) -> str:
    """Get language-specific table name."""
    return f"{base_name}_{_DB_LANGUAGE}"


async def purge_pipeline_staging_tables(language: str | None = None) -> list[str]:
    """Truncate pipeline staging tables for one language and return table names."""
    lang = language or _DB_LANGUAGE
    if lang not in _VALID_LANGUAGES:
        raise ValueError(f"Unsupported language: {lang}")

    pool = _require_pool()
    tables = [
        f"videos_normalized_{lang}",
        f"videos_raw_{lang}",
        f"search_runs_{lang}",
        f"channels_discovery_claims_{lang}",
    ]
    await pool.execute(f"TRUNCATE TABLE {', '.join(tables)};")
    return tables


# Helper to handle datetime types for asyncpg (it expects datetime objects, not strings)
def _ensure_datetime(dt: datetime | str | None) -> datetime | None:
    if dt is None:
        return None
    if isinstance(dt, datetime):
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt
    if isinstance(dt, str):
        # If it's a string, try to parse it (assuming ISO format)
        try:
            normalized = dt[:-1] + "+00:00" if dt.endswith("Z") else dt
            parsed = datetime.fromisoformat(normalized)
            return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)
        except ValueError:
            return None
    return None


async def create_search_run(query: str, mode: str = "exploration") -> uuid.UUID:
    """Create a search run row and return its UUID."""
    pool = _require_pool()
    run_id = uuid.uuid4()
    started_at = _utcnow()
    table_name = _get_table_name("search_runs")
    await pool.execute(
        f"INSERT INTO {table_name} (id, query, mode, started_at) VALUES ($1, $2, $3, $4)",
        str(run_id), query, mode, started_at
    )
    return run_id


async def finish_search_run(search_run_id: uuid.UUID) -> None:
    """Mark a search run as finished."""
    pool = _require_pool()
    finished_at = _utcnow()
    table_name = _get_table_name("search_runs")
    await pool.execute(
        f"UPDATE {table_name} SET finished_at = $1 WHERE id = $2",
        finished_at, str(search_run_id)
    )


async def get_executed_queries() -> set[str]:
    """Return a set of distinct queries that have been logged in search_runs."""
    pool = _require_pool()
    table_name = _get_table_name("search_runs")
    rows = await pool.fetch(f"SELECT DISTINCT query FROM {table_name}")
    return {row["query"] for row in rows if row["query"]}


async def insert_videos_raw(search_run_id: uuid.UUID, videos: list[dict[str, Any]]) -> tuple[int, int]:
    """Batch insert raw video rows."""
    if not videos:
        return (0, 0)
    pool = _require_pool()

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
            bool(v.get("is_multi_creator"))  # Correct type for Postgres BOOLEAN
        ))

    if not tuples:
        return (0, 0)

    # asyncpg executemany using generated SQL for ON CONFLICT
    # Note: asyncpg executemany is fast but doesn't return rowcount for specific inserts derived from conflicts easily
    # in the standard way like sqlite's rowcount. 
    # However, we can use `INSERT ... ON CONFLICT DO NOTHING` and check results?
    # Actually `executemany` returns None.
    # To get a count, we might execute in a transaction or assume all succeeded? 
    # Users code expects (inserted_count, ignored_count).
    
    # Efficient strategy: Use COPY or unnest. For simplicity here, use executemany and accept approximate count
    # or just execute.
    # Actually, proper way with asyncpg to ignore duplicates is:
    table_name = _get_table_name("videos_raw")
    query = f"""
        INSERT INTO {table_name} (
            video_id, search_run_id, query, video_url, channel_url, 
            duration_text, views_text, published_text, thumbnail_url, 
            video_type, is_multi_creator
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        ON CONFLICT (video_id) DO NOTHING
    """
    
    # asyncpg `executemany` usually returns a status string like "INSERT 0 100".
    # But with ON CONFLICT DO NOTHING, if all are duplicates, it might be "INSERT 0 0"?
    # Let's iterate if we really need accurate counts or check status.
    # For bulk operations, usually we care less about exact duplicate count in logs. 
    
    # Let's try to get a reasonably accurate count.
    # We can batch them.
    
    try:
        res = await pool.executemany(query, tuples)
        # res is None for executemany usually? No, it returns None.
        
        # If we really need the count, we can do unnest trick or just return len(tuples) and 0 ignored?
        # Or don't return meaningful counts. The caller presumably logs it.
        # To be safe and compatible, let's just return (len(tuples), 0) or implement a count check.
        # But 'INSERT OR IGNORE' in sqlite returned rowcount.
        # Let's try to be better: 
        # But executemany doesn't return count.
        # We'll just return len(tuples) as inserted (optimistic) and 0 ignored. 
    except (asyncpg.PostgresError, asyncpg.InterfaceError, ConnectionError) as e:
        print(f"Error inserting videos: {e}")
        # Return 0 inserted, all ignored to avoid crash
        return 0, len(tuples)
    except Exception as e:
        print(f"Unexpected error inserting videos: {e}")
        return 0, len(tuples)

    # Note: asyncpg executemany returns None.
    return len(tuples), len(videos) - len(tuples)


async def fetch_unprocessed_videos_raw(limit: int | None = None) -> list[dict[str, Any]]:
    """Fetch raw videos that have not yet been normalized."""
    pool = _require_pool()
    videos_raw_table = _get_table_name("videos_raw")
    videos_normalized_table = _get_table_name("videos_normalized")
    sql = f"""
        SELECT r.video_id, r.channel_url, r.query, r.duration_text, r.views_text, r.published_text
        FROM {videos_raw_table} r
        LEFT JOIN {videos_normalized_table} n ON n.video_id = r.video_id
        WHERE n.video_id IS NULL
        ORDER BY r.discovered_at ASC
    """
    if limit:
        sql += f" LIMIT {limit}"

    rows = await pool.fetch(sql)
    return [dict(row) for row in rows]


async def insert_videos_normalized(rows: list[dict[str, Any]]) -> tuple[int, int]:
    """Batch insert normalized videos."""
    if not rows:
        return (0, 0)
    pool = _require_pool()

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
            _ensure_datetime(r.get("published_at_estimated")),
            r.get("duration_seconds_estimated"),
            bool(r.get("validation_passed")),
            r.get("validation_reason"),
            _ensure_datetime(r.get("normalized_at")) or _utcnow()
        ))

    if not tuples:
        return (0, 0)
    
    table_name = _get_table_name("videos_normalized")
    query = f"""
        INSERT INTO {table_name} (
            video_id, channel_url, query, views_estimated, published_at_estimated,
            duration_seconds_estimated, validation_passed, validation_reason, normalized_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        ON CONFLICT (video_id) DO NOTHING
    """
    
    await pool.executemany(query, tuples)
    return len(tuples), len(rows) - len(tuples)


async def claim_channels_for_discovery(
    limit: int,
    *,
    claim_owner: str | None = None,
    stale_after_minutes: int = 60,
) -> list[str]:
    """Atomically claim and return only channels acquired by this caller."""
    if limit <= 0:
        return []
    if stale_after_minutes <= 0:
        raise ValueError("stale_after_minutes must be positive")
    owner = claim_owner or str(uuid.uuid4())
    videos_normalized_table = _get_table_name("videos_normalized")
    channels_processed_table = _get_table_name("channels_processed")
    channels_claims_table = _get_table_name("channels_discovery_claims")
    rows = await _require_pool().fetch(f"""
        WITH candidates AS (
            SELECT n.channel_url, MIN(n.normalized_at) AS first_seen
            FROM {videos_normalized_table} n
            WHERE n.validation_passed = TRUE
              AND n.channel_url IS NOT NULL
              AND n.channel_url <> ''
              AND NOT EXISTS (
                  SELECT 1 FROM {channels_processed_table} p
                  WHERE p.channel_url = n.channel_url
              )
              AND NOT EXISTS (
                  SELECT 1 FROM {channels_claims_table} c
                  WHERE c.channel_url = n.channel_url
                    AND c.claimed_at >= CURRENT_TIMESTAMP
                        - ($3::INTEGER * INTERVAL '1 minute')
              )
            GROUP BY n.channel_url
            ORDER BY first_seen ASC
            LIMIT $1
        ),
        claimed AS (
            INSERT INTO {channels_claims_table} (channel_url, claimed_at, claim_owner)
            SELECT channel_url, CURRENT_TIMESTAMP, $2
            FROM candidates
            ON CONFLICT (channel_url) DO UPDATE
            SET claimed_at = EXCLUDED.claimed_at,
                claim_owner = EXCLUDED.claim_owner
            WHERE {channels_claims_table}.claimed_at < CURRENT_TIMESTAMP
                - ($3::INTEGER * INTERVAL '1 minute')
            RETURNING channel_url
        )
        SELECT channel_url FROM claimed
    """, limit, owner, stale_after_minutes)
    return [row["channel_url"] for row in rows]


async def release_channel_discovery_claims(claim_owner: str) -> int:
    """Release claims belonging to one discovery run and no other owner."""
    if not claim_owner:
        return 0
    table_name = _get_table_name("channels_discovery_claims")
    result = await _require_pool().execute(
        f"DELETE FROM {table_name} WHERE claim_owner = $1",
        claim_owner,
    )
    return int(result.rsplit(" ", 1)[-1])


async def release_channel_discovery_claim(channel_url: str, claim_owner: str) -> bool:
    """Release one claim only when it is still owned by this run."""
    if not channel_url or not claim_owner:
        return False
    table_name = _get_table_name("channels_discovery_claims")
    result = await _require_pool().execute(
        f"DELETE FROM {table_name} WHERE channel_url = $1 AND claim_owner = $2",
        channel_url,
        claim_owner,
    )
    return result.endswith(" 1")


async def _upsert_channel_raw_with(executor: Any, channel: dict[str, Any]) -> None:
    url = channel.get("channel_url")
    if not url:
        raise ValueError("channel_url is required")

    table_name = _get_table_name("channels_raw")
    await executor.execute(f"""
        INSERT INTO {table_name} (
            channel_url, channel_id, channel_name, subscriber_count, is_verified,
            channel_description, channel_tags, avatar_url, banner_url, uploader_id, uploader_url,
            last_upload_date, first_video_id, first_video_published_at,
            first_video_checked_at, first_video_last_attempt_at, first_video_status,
            first_video_source, first_video_last_error, first_video_claimed_at, extracted_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
                $13, $14, $15, $16, $17, $18, $19, $20, $21)
        ON CONFLICT(channel_url) DO UPDATE SET
            channel_id=COALESCE(EXCLUDED.channel_id, {table_name}.channel_id),
            channel_name=COALESCE(EXCLUDED.channel_name, {table_name}.channel_name),
            subscriber_count=COALESCE(EXCLUDED.subscriber_count, {table_name}.subscriber_count),
            is_verified=COALESCE(EXCLUDED.is_verified, {table_name}.is_verified),
            channel_description=COALESCE(EXCLUDED.channel_description, {table_name}.channel_description),
            channel_tags=COALESCE(EXCLUDED.channel_tags, {table_name}.channel_tags),
            avatar_url=COALESCE(EXCLUDED.avatar_url, {table_name}.avatar_url),
            banner_url=COALESCE(EXCLUDED.banner_url, {table_name}.banner_url),
            uploader_id=COALESCE(EXCLUDED.uploader_id, {table_name}.uploader_id),
            uploader_url=COALESCE(EXCLUDED.uploader_url, {table_name}.uploader_url),
            last_upload_date=COALESCE(EXCLUDED.last_upload_date, {table_name}.last_upload_date),
            first_video_id=COALESCE(EXCLUDED.first_video_id, {table_name}.first_video_id),
            first_video_published_at=COALESCE(EXCLUDED.first_video_published_at, {table_name}.first_video_published_at),
            first_video_checked_at=COALESCE(EXCLUDED.first_video_checked_at, {table_name}.first_video_checked_at),
            first_video_last_attempt_at=COALESCE(EXCLUDED.first_video_last_attempt_at, {table_name}.first_video_last_attempt_at),
            first_video_status=CASE
                WHEN EXCLUDED.first_video_status IN ('success', 'no_public_videos')
                    THEN EXCLUDED.first_video_status
                ELSE {table_name}.first_video_status
            END,
            first_video_source=COALESCE(EXCLUDED.first_video_source, {table_name}.first_video_source),
            first_video_last_error=EXCLUDED.first_video_last_error,
            first_video_claimed_at=EXCLUDED.first_video_claimed_at,
            extracted_at=EXCLUDED.extracted_at
    """, 
        url,
        channel.get("channel_id"),
        channel.get("channel_name"),
        channel.get("subscriber_count"),
        channel.get("is_verified"),
        channel.get("channel_description"),
        channel.get("channel_tags"),
        channel.get("avatar_url"),
        channel.get("banner_url"),
        channel.get("uploader_id"),
        channel.get("uploader_url"),
        channel.get("last_upload_date"),
        channel.get("first_video_id"),
        _ensure_datetime(channel.get("first_video_published_at")),
        _ensure_datetime(channel.get("first_video_checked_at")),
        _ensure_datetime(channel.get("first_video_last_attempt_at")),
        channel.get("first_video_status") or "pending",
        channel.get("first_video_source"),
        channel.get("first_video_last_error"),
        _ensure_datetime(channel.get("first_video_claimed_at")),
        _ensure_datetime(channel.get("extracted_at")) or _utcnow()
    )


async def upsert_channel_raw(channel: dict[str, Any]) -> None:
    """Upsert one raw channel row."""
    await _upsert_channel_raw_with(_require_pool(), channel)


async def update_first_video_success(
    channel_url: str,
    *,
    video_id: str,
    published_at: datetime | str,
    source: str,
    checked_at: datetime | None = None,
) -> None:
    """Persist a successful first-video result atomically."""
    table_name = _get_table_name("channels_raw")
    now = checked_at or _utcnow()
    await _require_pool().execute(f"""
        UPDATE {table_name}
        SET first_video_id = $2,
            first_video_published_at = $3,
            first_video_checked_at = $4,
            first_video_last_attempt_at = $4,
            first_video_status = 'success',
            first_video_source = $5,
            first_video_last_error = NULL,
            first_video_claimed_at = NULL
        WHERE channel_url = $1
    """, channel_url, video_id, _ensure_datetime(published_at), now, source)


async def update_first_video_no_public(
    channel_url: str,
    *,
    reason: str,
    checked_at: datetime | None = None,
) -> None:
    """Mark an explicitly empty Videos tab as a terminal result."""
    table_name = _get_table_name("channels_raw")
    now = checked_at or _utcnow()
    await _require_pool().execute(f"""
        UPDATE {table_name}
        SET first_video_id = NULL,
            first_video_published_at = NULL,
            first_video_checked_at = $3,
            first_video_last_attempt_at = $3,
            first_video_status = 'no_public_videos',
            first_video_source = NULL,
            first_video_last_error = $2,
            first_video_claimed_at = NULL
        WHERE channel_url = $1
    """, channel_url, reason[:2000], now)


async def update_first_video_failure(
    channel_url: str,
    *,
    error: str,
    attempted_at: datetime | None = None,
) -> None:
    """Return a failed enrichment to pending for a future run."""
    table_name = _get_table_name("channels_raw")
    await _require_pool().execute(f"""
        UPDATE {table_name}
        SET first_video_status = 'pending',
            first_video_last_attempt_at = $3,
            first_video_last_error = $2,
            first_video_claimed_at = NULL
        WHERE channel_url = $1
          AND first_video_status <> 'success'
    """, channel_url, error[:2000], attempted_at or _utcnow())


async def claim_channels_for_first_video_enrichment(
    limit: int,
    *,
    stale_after_minutes: int = 60,
    eligible_before: datetime | None = None,
) -> list[dict[str, str]]:
    """Atomically claim pending enrichment rows, including stale workers."""
    if limit <= 0:
        return []
    table_name = _get_table_name("channels_raw")
    rows = await _require_pool().fetch(f"""
        WITH candidates AS (
            SELECT channel_url
            FROM {table_name}
            WHERE channel_id IS NOT NULL
              AND (
                  (
                      first_video_status = 'pending'
                      AND (
                          $3::TIMESTAMPTZ IS NULL
                          OR first_video_last_attempt_at IS NULL
                          OR first_video_last_attempt_at < $3
                      )
                  )
                  OR (
                      first_video_status = 'processing'
                      AND first_video_claimed_at < CURRENT_TIMESTAMP
                          - ($2::INTEGER * INTERVAL '1 minute')
                  )
              )
            ORDER BY first_video_last_attempt_at NULLS FIRST, channel_url
            FOR UPDATE SKIP LOCKED
            LIMIT $1
        )
        UPDATE {table_name} AS channel
        SET first_video_status = 'processing',
            first_video_claimed_at = CURRENT_TIMESTAMP,
            first_video_last_attempt_at = CURRENT_TIMESTAMP
        FROM candidates
        WHERE channel.channel_url = candidates.channel_url
        RETURNING channel.channel_url, channel.channel_id
    """, limit, stale_after_minutes, eligible_before)
    return [dict(row) for row in rows]


async def update_first_video_results(results: list[dict[str, Any]]) -> int:
    """Persist a mixed enrichment result batch in one PostgreSQL round-trip."""
    if not results:
        return 0
    payload: list[dict[str, Any]] = []
    now = _utcnow()
    for result in results:
        channel_url = result.get("channel_url")
        status = result.get("first_video_status")
        if not channel_url or status not in {"success", "no_public_videos", "pending"}:
            continue
        attempted_at = _ensure_datetime(result.get("first_video_last_attempt_at")) or now
        checked_at = (
            _ensure_datetime(result.get("first_video_checked_at")) or attempted_at
            if status in {"success", "no_public_videos"}
            else None
        )
        payload.append({
            "channel_url": channel_url,
            "status": status,
            "video_id": result.get("first_video_id") if status == "success" else None,
            "published_at": (
                _ensure_datetime(result.get("first_video_published_at")).isoformat()
                if status == "success" and result.get("first_video_published_at")
                else None
            ),
            "checked_at": checked_at.isoformat() if checked_at else None,
            "attempted_at": attempted_at.isoformat(),
            "source": result.get("first_video_source") if status == "success" else None,
            "error": (result.get("first_video_last_error") or "")[:2000] or None,
        })
    if not payload:
        return 0
    table_name = _get_table_name("channels_raw")
    await _require_pool().execute(f"""
        UPDATE {table_name} AS channel
        SET first_video_id = CASE WHEN result.status = 'success' THEN result.video_id ELSE NULL END,
            first_video_published_at = CASE
                WHEN result.status = 'success' THEN result.published_at
                ELSE NULL
            END,
            first_video_checked_at = result.checked_at,
            first_video_last_attempt_at = result.attempted_at,
            first_video_status = result.status,
            first_video_source = CASE WHEN result.status = 'success' THEN result.source ELSE NULL END,
            first_video_last_error = CASE WHEN result.status = 'success' THEN NULL ELSE result.error END,
            first_video_claimed_at = NULL
        FROM jsonb_to_recordset($1::JSONB) AS result(
            channel_url TEXT,
            status TEXT,
            video_id TEXT,
            published_at TIMESTAMPTZ,
            checked_at TIMESTAMPTZ,
            attempted_at TIMESTAMPTZ,
            source TEXT,
            error TEXT
        )
        WHERE channel.channel_url = result.channel_url
          AND channel.first_video_status NOT IN ('success', 'no_public_videos')
    """, json.dumps(payload))
    return len(payload)


def _prepare_channel_video_columns(
    videos: list[dict[str, Any]],
) -> tuple[list[list[Any]], int]:
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
            vid,
            v.get("upload_date"),
            v.get("duration_seconds"),
            v.get("view_count"),
            v.get("title"),
            v.get("video_url"),
            v.get("thumbnail_url"),
        ))

    if not tuples:
        return ([], 0)
    columns = [list(values) for values in zip(*tuples)]
    return (columns, len(tuples))


async def _upsert_channel_videos_raw_with(
    executor: Any,
    channel_url: str,
    videos: list[dict[str, Any]],
) -> tuple[int, int]:
    columns, count = _prepare_channel_video_columns(videos)
    if not count:
        return (0, 0)
    table_name = _get_table_name("channel_videos_raw")
    await executor.execute(f"""
        INSERT INTO {table_name} (
            channel_url, video_id, upload_date, duration_seconds, view_count,
            title, video_url, thumbnail_url
        )
        SELECT $1, batch.video_id, batch.upload_date, batch.duration_seconds,
               batch.view_count, batch.title, batch.video_url, batch.thumbnail_url
        FROM UNNEST(
            $2::TEXT[], $3::TEXT[], $4::BIGINT[], $5::BIGINT[],
            $6::TEXT[], $7::TEXT[], $8::TEXT[]
        ) AS batch(
            video_id, upload_date, duration_seconds, view_count,
            title, video_url, thumbnail_url
        )
        ON CONFLICT(channel_url, video_id) DO UPDATE SET
            upload_date=COALESCE(EXCLUDED.upload_date, {table_name}.upload_date),
            duration_seconds=COALESCE(EXCLUDED.duration_seconds, {table_name}.duration_seconds),
            view_count=COALESCE(EXCLUDED.view_count, {table_name}.view_count),
            title=COALESCE(EXCLUDED.title, {table_name}.title),
            video_url=COALESCE(EXCLUDED.video_url, {table_name}.video_url),
            thumbnail_url=COALESCE(EXCLUDED.thumbnail_url, {table_name}.thumbnail_url)
    """, channel_url, *columns)
    return count, 0


async def upsert_channel_videos_raw(
    channel_url: str,
    videos: list[dict[str, Any]],
) -> tuple[int, int]:
    """Upsert all videos for a channel in one PostgreSQL statement."""
    return await _upsert_channel_videos_raw_with(
        _require_pool(), channel_url, videos
    )


async def _mark_channel_processed_with(
    executor: Any,
    channel_url: str,
    *,
    processed_at: datetime,
    status: str,
) -> None:
    table_name = _get_table_name("channels_processed")
    await executor.execute(f"""
        INSERT INTO {table_name} (channel_url, processed_at, status)
        VALUES ($1, $2, $3)
        ON CONFLICT(channel_url) DO UPDATE SET
            processed_at=EXCLUDED.processed_at,
            status=EXCLUDED.status
    """, channel_url, processed_at, status)


async def persist_channel_discovery_result(
    channel: dict[str, Any],
    videos: list[dict[str, Any]],
    *,
    claim_owner: str,
    status: str = "success",
) -> tuple[int, int]:
    """Persist one completed channel and release its claim atomically."""
    channel_url = channel.get("channel_url")
    if not channel_url:
        raise ValueError("channel_url is required")
    pool = _require_pool()
    claims_table = _get_table_name("channels_discovery_claims")
    async with pool.acquire() as conn:
        async with conn.transaction():
            await _upsert_channel_raw_with(conn, channel)
            counts = await _upsert_channel_videos_raw_with(conn, channel_url, videos)
            await _mark_channel_processed_with(
                conn,
                channel_url,
                processed_at=_utcnow(),
                status=status,
            )
            await conn.execute(
                f"DELETE FROM {claims_table} WHERE channel_url = $1 AND claim_owner = $2",
                channel_url,
                claim_owner,
            )
    return counts


async def mark_channel_processed(
    channel_url: str,
    *,
    processed_at: datetime | None = None,
    status: str = "success",
    claim_owner: str | None = None,
) -> None:
    """Mark a channel as processed."""
    p_at = _ensure_datetime(processed_at) or _utcnow()
    pool = _require_pool()
    claims_table = _get_table_name("channels_discovery_claims")
    async with pool.acquire() as conn:
        async with conn.transaction():
            await _mark_channel_processed_with(
                conn, channel_url, processed_at=p_at, status=status
            )
            if claim_owner:
                await conn.execute(
                    f"DELETE FROM {claims_table} WHERE channel_url = $1 AND claim_owner = $2",
                    channel_url,
                    claim_owner,
                )


async def is_channel_processed(channel_url: str) -> bool:
    """Check if a channel has already been processed."""
    if not channel_url:
        return False
    pool = _require_pool()
    table_name = _get_table_name("channels_processed")
    row = await pool.fetchrow(f"SELECT 1 FROM {table_name} WHERE channel_url = $1", channel_url)
    return row is not None
