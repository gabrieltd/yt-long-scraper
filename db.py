"""PostgreSQL persistence layer for YouTube discovery."""

from __future__ import annotations

import os
import uuid
import asyncio
import json
from datetime import date, datetime, timezone
from typing import Any

import asyncpg

_DB_POOL: asyncpg.Pool | None = None
_DB_LANGUAGE: str = "es"  # Track the current language for table naming
_VALID_LANGUAGES = {"es", "en"}


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


async def _setup_db_connection(connection: asyncpg.Connection) -> None:
    """Ensure pooled Supabase sessions can execute the pipeline's writes."""
    await connection.execute("SET default_transaction_read_only = off")


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
            # A worker may have initialized only the pool. Honor a later schema
            # request instead of silently returning with missing relations.
            if ensure_schema:
                try:
                    await create_tables(language)
                except Exception:
                    await close_db()
                    raise
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
        # Supabase can apply a read-only role default after startup parameters.
        # Run the override whenever asyncpg acquires a pooled connection.
        setup=_setup_db_connection,
        timeout=120,  # Increase timeout to 120 seconds for slow connections
        command_timeout=60  # Set command timeout
    )
    
    if ensure_schema:
        try:
            await create_tables(language)
        except Exception:
            # Do not retain a pool that points at a partially prepared schema.
            # A later invocation can then initialize and retry cleanly.
            await close_db()
            raise


async def create_tables(language: str = "es") -> None:
    """Create the compact, fresh-start schema for one language."""
    if language not in _VALID_LANGUAGES:
        raise ValueError(f"Unsupported language: {language}")

    pool = _require_pool()
    lang_suffix = f"_{language}"
    
    async with pool.acquire() as conn:
        await conn.execute(f"""
            CREATE TABLE IF NOT EXISTS search_runs{lang_suffix} (
                id UUID PRIMARY KEY,
                query TEXT NOT NULL,
                mode TEXT NOT NULL,
                started_at TIMESTAMPTZ NOT NULL,
                finished_at TIMESTAMPTZ,
                status TEXT NOT NULL DEFAULT 'running'
                    CHECK (status IN ('running', 'success', 'failed')),
                result_count INTEGER NOT NULL DEFAULT 0,
                last_error TEXT
            );
        """)

        await conn.execute(f"""
            CREATE TABLE IF NOT EXISTS discovery_videos_staging{lang_suffix} (
                video_id TEXT PRIMARY KEY,
                search_run_id UUID NOT NULL REFERENCES search_runs{lang_suffix}(id),
                channel_url TEXT,
                duration_text TEXT,
                views_text TEXT,
                published_text TEXT,
                discovered_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                views_estimated BIGINT,
                published_at_estimated TIMESTAMPTZ,
                duration_seconds_estimated INTEGER,
                validation_passed BOOLEAN,
                validation_reason TEXT,
                normalized_at TIMESTAMPTZ
            );
        """)

        await conn.execute(f"""
            CREATE TABLE IF NOT EXISTS channel_candidates{lang_suffix} (
                channel_url TEXT PRIMARY KEY,
                first_seen TIMESTAMPTZ NOT NULL
            );
        """)

        await conn.execute(f"""
            CREATE TABLE IF NOT EXISTS channels_raw{lang_suffix} (
                id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                channel_url TEXT NOT NULL UNIQUE,
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
                first_video_status TEXT NOT NULL DEFAULT 'pending'
                    CHECK (first_video_status IN ('pending', 'processing', 'success', 'no_public_videos')),
                first_video_source TEXT
                    CHECK (first_video_source IS NULL OR first_video_source IN ('innertube', 'yt_dlp')),
                first_video_last_error TEXT,
                first_video_claimed_at TIMESTAMPTZ,
                extracted_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
            );
        """)

        await conn.execute(f"""
            CREATE TABLE IF NOT EXISTS channel_videos_raw{lang_suffix} (
                video_id TEXT PRIMARY KEY,
                channel_key BIGINT NOT NULL REFERENCES channels_raw{lang_suffix}(id)
                    ON DELETE CASCADE,
                upload_date DATE,
                duration_seconds INTEGER,
                view_count BIGINT,
                title TEXT
            );
        """)

        await conn.execute(f"""
            CREATE TABLE IF NOT EXISTS channels_processed{lang_suffix} (
                channel_url TEXT PRIMARY KEY,
                processed_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                status TEXT DEFAULT 'success'
            );
        """)

        await conn.execute(f"""
            CREATE TABLE IF NOT EXISTS channels_discovery_claims{lang_suffix} (
                channel_url TEXT PRIMARY KEY,
                claimed_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                claim_owner TEXT
            );
        """)

        await conn.execute(f"""
            CREATE TABLE IF NOT EXISTS channel_relevance{lang_suffix} (
                channel_key BIGINT PRIMARY KEY REFERENCES channels_raw{lang_suffix}(id)
                    ON DELETE CASCADE,
                is_relevant BOOLEAN,
                notes TEXT,
                tags TEXT[],
                marked_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
            );
        """)

        await conn.execute(f"""
            CREATE TABLE IF NOT EXISTS channel_stats{lang_suffix} (
                channel_key BIGINT PRIMARY KEY REFERENCES channels_raw{lang_suffix}(id)
                    ON DELETE CASCADE,
                total_videos_tracked BIGINT NOT NULL,
                avg_views_on_channel NUMERIC,
                max_views_on_channel BIGINT,
                view_counts BIGINT[] NOT NULL DEFAULT '{{}}'::BIGINT[]
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

        indices = [
            f"CREATE INDEX IF NOT EXISTS idx_search_runs{lang_suffix}_successful_query ON search_runs{lang_suffix} (query) WHERE status = 'success';",
            f"CREATE INDEX IF NOT EXISTS idx_discovery_staging{lang_suffix}_pending ON discovery_videos_staging{lang_suffix} (discovered_at) WHERE normalized_at IS NULL;",
            f"CREATE INDEX IF NOT EXISTS idx_channel_candidates{lang_suffix}_first_seen ON channel_candidates{lang_suffix} (first_seen, channel_url);",
            f"CREATE INDEX IF NOT EXISTS idx_channel_videos_raw{lang_suffix}_detail ON channel_videos_raw{lang_suffix} (channel_key, upload_date DESC NULLS LAST, view_count DESC NULLS LAST);",
            f"CREATE INDEX IF NOT EXISTS idx_channels_raw{lang_suffix}_subscribers ON channels_raw{lang_suffix} (subscriber_count);",
            f"CREATE INDEX IF NOT EXISTS idx_channels_raw{lang_suffix}_last_upload ON channels_raw{lang_suffix} (last_upload_date);",
            f"CREATE INDEX IF NOT EXISTS idx_channels_raw{lang_suffix}_first_video_published ON channels_raw{lang_suffix} (first_video_published_at);",
            f"CREATE INDEX IF NOT EXISTS idx_channels_raw{lang_suffix}_first_video_pending ON channels_raw{lang_suffix} (first_video_status, first_video_last_attempt_at);",
            f"CREATE INDEX IF NOT EXISTS idx_channel_claims{lang_suffix}_owner ON channels_discovery_claims{lang_suffix} (claim_owner);",
            f"CREATE INDEX IF NOT EXISTS idx_channels_raw{lang_suffix}_verified_true ON channels_raw{lang_suffix} (channel_url) WHERE is_verified IS TRUE;",
            f"CREATE INDEX IF NOT EXISTS idx_channel_relevance{lang_suffix}_tags_gin ON channel_relevance{lang_suffix} USING GIN (tags);",
            f"CREATE INDEX IF NOT EXISTS idx_channels_raw{lang_suffix}_name_trgm ON channels_raw{lang_suffix} USING GIN (channel_name gin_trgm_ops);",
        ]
        for idx in indices:
            await conn.execute(idx)

        await conn.execute(f"""
            CREATE OR REPLACE VIEW videos_raw{lang_suffix} AS
            SELECT s.video_id,
                   s.search_run_id::TEXT AS search_run_id,
                   r.query,
                   'https://www.youtube.com/watch?v=' || s.video_id AS video_url,
                   s.channel_url,
                   s.duration_text,
                   s.views_text,
                   s.published_text,
                   'https://i.ytimg.com/vi/' || s.video_id || '/hqdefault.jpg' AS thumbnail_url,
                   NULL::TEXT AS video_type,
                   FALSE AS is_multi_creator,
                   s.discovered_at
            FROM discovery_videos_staging{lang_suffix} s
            JOIN search_runs{lang_suffix} r ON r.id = s.search_run_id;
        """)
        await conn.execute(f"""
            CREATE OR REPLACE VIEW videos_normalized{lang_suffix} AS
            SELECT s.video_id,
                   s.channel_url,
                   r.query,
                   s.views_estimated,
                   s.published_at_estimated,
                   s.duration_seconds_estimated::BIGINT AS duration_seconds_estimated,
                   s.validation_passed,
                   s.validation_reason,
                   s.normalized_at
            FROM discovery_videos_staging{lang_suffix} s
            JOIN search_runs{lang_suffix} r ON r.id = s.search_run_id
            WHERE s.normalized_at IS NOT NULL;
        """)


async def refresh_channel_stats(language: str) -> bool:
    """Rebuild the compact statistics table as an explicit repair operation."""
    if language not in _VALID_LANGUAGES:
        raise ValueError(f"Unsupported language: {language}")

    stats = f"channel_stats_{language}"
    videos = f"channel_videos_raw_{language}"
    channels = f"channels_raw_{language}"
    pool = _require_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(f"DELETE FROM {stats}")
            await conn.execute(f"""
                INSERT INTO {stats} (
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
                FROM {channels} channel
                LEFT JOIN {videos} video ON video.channel_key = channel.id
                GROUP BY channel.id
            """)
    return True


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
    """Release heavy staging and make unfinished searches eligible for retry."""
    lang = language or _DB_LANGUAGE
    if lang not in _VALID_LANGUAGES:
        raise ValueError(f"Unsupported language: {lang}")

    pool = _require_pool()
    staging = f"discovery_videos_staging_{lang}"
    claims = f"channels_discovery_claims_{lang}"
    processed = f"channels_processed_{lang}"
    runs = f"search_runs_{lang}"
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(f"""
                DELETE FROM {claims} AS claim
                WHERE EXISTS (
                    SELECT 1 FROM {processed} AS done
                    WHERE done.channel_url = claim.channel_url
                )
            """)
            # Discovery marks a search successful once its raw result is stored.
            # If normalization never completed, demote that run before removing
            # the heavy rows so the same query remains eligible for retry.
            await conn.execute(f"""
                UPDATE {runs} AS run
                SET status = 'failed',
                    finished_at = COALESCE(run.finished_at, NOW()),
                    last_error = COALESCE(
                        run.last_error,
                        'Normalization did not complete before finalization'
                    )
                WHERE run.status = 'success'
                  AND EXISTS (
                      SELECT 1
                      FROM {staging} AS staged
                      WHERE staged.search_run_id = run.id
                        AND staged.normalized_at IS NULL
                  )
            """)
            await conn.execute(f"TRUNCATE TABLE {staging}")
    return [staging]


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


def _ensure_date(value: date | datetime | str | None) -> date | None:
    """Coerce yt-dlp's YYYYMMDD value (or ISO date) to a PostgreSQL DATE."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        cleaned = value.strip()
        try:
            if len(cleaned) == 8 and cleaned.isdigit():
                return datetime.strptime(cleaned, "%Y%m%d").date()
            return date.fromisoformat(cleaned)
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
        run_id, query, mode, started_at
    )
    return run_id


async def finish_search_run(
    search_run_id: uuid.UUID,
    *,
    status: str = "success",
    result_count: int = 0,
    error: str | None = None,
) -> None:
    """Finish a search run; only successful runs enter query history."""
    if status not in {"success", "failed"}:
        raise ValueError("status must be 'success' or 'failed'")
    pool = _require_pool()
    table_name = _get_table_name("search_runs")
    await pool.execute(
        f"""
        UPDATE {table_name}
        SET finished_at = $2,
            status = $3,
            result_count = $4,
            last_error = $5
        WHERE id = $1
        """,
        search_run_id,
        _utcnow(),
        status,
        max(0, int(result_count)),
        error[:2000] if error else None,
    )


async def get_executed_queries() -> set[str]:
    """Return a set of distinct queries that have been logged in search_runs."""
    pool = _require_pool()
    table_name = _get_table_name("search_runs")
    rows = await pool.fetch(
        f"SELECT DISTINCT query FROM {table_name} WHERE status = 'success'"
    )
    return {row["query"] for row in rows if row["query"]}


async def insert_videos_raw(search_run_id: uuid.UUID, videos: list[dict[str, Any]]) -> tuple[int, int]:
    """Batch insert raw video rows."""
    if not videos:
        return (0, 0)
    pool = _require_pool()

    tuples: list[tuple[Any, ...]] = []
    seen = set()
    for v in videos:
        vid = v.get("video_id")
        if not vid or not isinstance(vid, str):
            continue
        if vid in seen:
            continue
        seen.add(vid)

        channel_url = v.get("channel_url")
        if not channel_url and v.get("channels") and isinstance(v.get("channels"), list):
            # Extract from channels list if needed
            try:
                channel_url = v.get("channels")[0].get("url")
            except (IndexError, AttributeError):
                pass
        
        tuples.append((
            vid,
            channel_url,
            v.get("duration"),
            v.get("views_text"),
            v.get("published_text"),
        ))

    if not tuples:
        return (0, 0)

    columns = [list(values) for values in zip(*tuples)]
    table_name = _get_table_name("discovery_videos_staging")
    query = f"""
        WITH batch AS (
            SELECT * FROM UNNEST(
                $2::TEXT[], $3::TEXT[], $4::TEXT[], $5::TEXT[], $6::TEXT[]
            ) AS item(video_id, channel_url, duration_text, views_text, published_text)
        ), inserted AS (
            INSERT INTO {table_name} (
                video_id, search_run_id, channel_url, duration_text,
                views_text, published_text
            )
            SELECT video_id, $1, channel_url, duration_text, views_text, published_text
            FROM batch
            ON CONFLICT (video_id) DO NOTHING
            RETURNING 1
        )
        SELECT COUNT(*) FROM inserted
    """
    try:
        inserted = int(await pool.fetchval(query, search_run_id, *columns) or 0)
    except (asyncpg.PostgresError, asyncpg.InterfaceError, ConnectionError) as e:
        raise RuntimeError(f"Error inserting discovery videos: {e}") from e
    except Exception as e:
        raise RuntimeError(f"Unexpected error inserting discovery videos: {e}") from e

    return inserted, len(videos) - inserted


async def fetch_unprocessed_videos_raw(limit: int | None = None) -> list[dict[str, Any]]:
    """Fetch raw videos that have not yet been normalized."""
    pool = _require_pool()
    staging = _get_table_name("discovery_videos_staging")
    search_runs = _get_table_name("search_runs")
    sql = f"""
        SELECT s.video_id, s.channel_url, run.query,
               s.duration_text, s.views_text, s.published_text
        FROM {staging} s
        JOIN {search_runs} run ON run.id = s.search_run_id
        WHERE s.normalized_at IS NULL
        ORDER BY s.discovered_at ASC
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

    tuples: list[tuple[Any, ...]] = []
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
            r.get("views_estimated"),
            _ensure_datetime(r.get("published_at_estimated")),
            r.get("duration_seconds_estimated"),
            bool(r.get("validation_passed")),
            r.get("validation_reason"),
            _ensure_datetime(r.get("normalized_at")) or _utcnow()
        ))

    if not tuples:
        return (0, 0)
    
    columns = [list(values) for values in zip(*tuples)]
    staging = _get_table_name("discovery_videos_staging")
    candidates = _get_table_name("channel_candidates")
    processed = _get_table_name("channels_processed")
    query = f"""
        WITH input AS (
            SELECT * FROM UNNEST(
                $1::TEXT[], $2::BIGINT[], $3::TIMESTAMPTZ[], $4::INTEGER[],
                $5::BOOLEAN[], $6::TEXT[], $7::TIMESTAMPTZ[]
            ) AS item(
                video_id, views_estimated, published_at_estimated,
                duration_seconds_estimated, validation_passed,
                validation_reason, normalized_at
            )
        ), updated AS (
            UPDATE {staging} AS target
            SET views_estimated = input.views_estimated,
                published_at_estimated = input.published_at_estimated,
                duration_seconds_estimated = input.duration_seconds_estimated,
                validation_passed = input.validation_passed,
                validation_reason = input.validation_reason,
                normalized_at = input.normalized_at
            FROM input
            WHERE target.video_id = input.video_id
              AND target.normalized_at IS NULL
            RETURNING target.channel_url, target.normalized_at, target.validation_passed
        ), candidate_rows AS (
            SELECT channel_url, MIN(normalized_at) AS first_seen
            FROM updated
            WHERE validation_passed IS TRUE
              AND channel_url IS NOT NULL
              AND channel_url <> ''
            GROUP BY channel_url
        ), inserted_candidates AS (
            INSERT INTO {candidates} (channel_url, first_seen)
            SELECT row.channel_url, row.first_seen
            FROM candidate_rows row
            WHERE NOT EXISTS (
                SELECT 1 FROM {processed} done
                WHERE done.channel_url = row.channel_url
            )
            ON CONFLICT (channel_url) DO UPDATE
            SET first_seen = LEAST({candidates}.first_seen, EXCLUDED.first_seen)
            RETURNING 1
        )
        SELECT COUNT(*) FROM updated
    """
    updated = int(await pool.fetchval(query, *columns) or 0)
    return updated, len(rows) - updated


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
    candidates_table = _get_table_name("channel_candidates")
    channels_processed_table = _get_table_name("channels_processed")
    channels_claims_table = _get_table_name("channels_discovery_claims")
    pool = _require_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            # Candidate selection must also be serialized. Without this short
            # transaction-level lock, parallel GitHub jobs can select the same
            # batch; only one inserts it and the others receive an empty result,
            # incorrectly concluding that the shared queue is exhausted.
            # Transaction-level advisory locks are safe with transaction poolers
            # because PostgreSQL releases the lock on commit.
            await conn.execute(
                "SELECT pg_advisory_xact_lock(hashtext($1))",
                f"youtube-long-channel-claims:{_DB_LANGUAGE}",
            )
            rows = await conn.fetch(f"""
                WITH candidates AS (
                    SELECT queued.channel_url, queued.first_seen
                    FROM {candidates_table} queued
                    WHERE NOT EXISTS (
                          SELECT 1 FROM {channels_processed_table} p
                          WHERE p.channel_url = queued.channel_url
                      )
                      AND NOT EXISTS (
                          SELECT 1 FROM {channels_claims_table} c
                          WHERE c.channel_url = queued.channel_url
                            AND c.claimed_at >= CURRENT_TIMESTAMP
                                - ($3::INTEGER * INTERVAL '1 minute')
                      )
                    ORDER BY queued.first_seen ASC, queued.channel_url
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


async def count_pending_channels_for_discovery() -> int:
    """Count valid candidate channels that have not reached a terminal status."""
    candidates_table = _get_table_name("channel_candidates")
    channels_processed_table = _get_table_name("channels_processed")
    value = await _require_pool().fetchval(f"""
        SELECT COUNT(*)
        FROM {candidates_table} candidate
        WHERE NOT EXISTS (
              SELECT 1 FROM {channels_processed_table} p
              WHERE p.channel_url = candidate.channel_url
          )
    """)
    return int(value or 0)


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


async def _upsert_channel_raw_with(executor: Any, channel: dict[str, Any]) -> int:
    url = channel.get("channel_url")
    if not url:
        raise ValueError("channel_url is required")

    table_name = _get_table_name("channels_raw")
    channel_key = await executor.fetchval(f"""
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
        RETURNING id
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
        _ensure_date(channel.get("last_upload_date")),
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
    if channel_key is None:
        raise RuntimeError(f"Channel upsert did not return an id for {url}")
    return int(channel_key)


async def upsert_channel_raw(channel: dict[str, Any]) -> int:
    """Upsert one raw channel row."""
    return await _upsert_channel_raw_with(_require_pool(), channel)


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
            _ensure_date(v.get("upload_date")),
            v.get("duration_seconds"),
            v.get("view_count"),
            v.get("title"),
        ))

    if not tuples:
        return ([], 0)
    columns = [list(values) for values in zip(*tuples)]
    return (columns, len(tuples))


async def _upsert_channel_videos_raw_with(
    executor: Any,
    channel_key: int,
    videos: list[dict[str, Any]],
) -> tuple[int, int]:
    columns, count = _prepare_channel_video_columns(videos)
    if not count:
        return (0, 0)
    table_name = _get_table_name("channel_videos_raw")
    await executor.execute(f"""
        INSERT INTO {table_name} (
            channel_key, video_id, upload_date, duration_seconds, view_count, title
        )
        SELECT $1, batch.video_id, batch.upload_date, batch.duration_seconds,
               batch.view_count, batch.title
        FROM UNNEST(
            $2::TEXT[], $3::DATE[], $4::INTEGER[], $5::BIGINT[], $6::TEXT[]
        ) AS batch(
            video_id, upload_date, duration_seconds, view_count, title
        )
        ON CONFLICT(video_id) DO UPDATE SET
            channel_key=EXCLUDED.channel_key,
            upload_date=COALESCE(EXCLUDED.upload_date, {table_name}.upload_date),
            duration_seconds=COALESCE(EXCLUDED.duration_seconds, {table_name}.duration_seconds),
            view_count=COALESCE(EXCLUDED.view_count, {table_name}.view_count),
            title=COALESCE(EXCLUDED.title, {table_name}.title)
    """, channel_key, *columns)
    return count, 0


async def upsert_channel_videos_raw(
    channel_url: str,
    videos: list[dict[str, Any]],
) -> tuple[int, int]:
    """Upsert all videos for a channel in one PostgreSQL statement."""
    channels = _get_table_name("channels_raw")
    pool = _require_pool()
    channel_key = await pool.fetchval(
        f"SELECT id FROM {channels} WHERE channel_url = $1", channel_url
    )
    if channel_key is None:
        raise ValueError(f"Unknown channel_url: {channel_url}")
    return await _upsert_channel_videos_raw_with(
        pool, int(channel_key), videos
    )


async def _update_channel_stats_with(executor: Any, channel_key: int) -> None:
    """Refresh one channel's exact statistics inside its persistence transaction."""
    stats = _get_table_name("channel_stats")
    videos = _get_table_name("channel_videos_raw")
    await executor.execute(f"""
        INSERT INTO {stats} (
            channel_key, total_videos_tracked, avg_views_on_channel,
            max_views_on_channel, view_counts
        )
        SELECT $1,
               COUNT(video_id),
               COALESCE(ROUND(AVG(view_count), 2), 0),
               COALESCE(MAX(view_count), 0),
               COALESCE(
                   ARRAY_AGG(view_count ORDER BY view_count)
                       FILTER (WHERE view_count IS NOT NULL),
                   '{{}}'::BIGINT[]
               )
        FROM {videos}
        WHERE channel_key = $1
        ON CONFLICT (channel_key) DO UPDATE SET
            total_videos_tracked = EXCLUDED.total_videos_tracked,
            avg_views_on_channel = EXCLUDED.avg_views_on_channel,
            max_views_on_channel = EXCLUDED.max_views_on_channel,
            view_counts = EXCLUDED.view_counts
    """, channel_key)


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
    candidates_table = _get_table_name("channel_candidates")
    async with pool.acquire() as conn:
        async with conn.transaction():
            owned_claim = await conn.fetchval(
                f"""
                    DELETE FROM {claims_table}
                    WHERE channel_url = $1 AND claim_owner = $2
                    RETURNING channel_url
                """,
                channel_url,
                claim_owner,
            )
            if owned_claim is None:
                raise RuntimeError(
                    f"Discovery claim is no longer owned by {claim_owner}: "
                    f"{channel_url}"
                )
            channel_key = await _upsert_channel_raw_with(conn, channel)
            counts = await _upsert_channel_videos_raw_with(conn, channel_key, videos)
            await _update_channel_stats_with(conn, channel_key)
            await _mark_channel_processed_with(
                conn,
                channel_url,
                processed_at=_utcnow(),
                status=status,
            )
            await conn.execute(
                f"DELETE FROM {candidates_table} WHERE channel_url = $1",
                channel_url,
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
    candidates_table = _get_table_name("channel_candidates")
    async with pool.acquire() as conn:
        async with conn.transaction():
            if claim_owner:
                owned_claim = await conn.fetchval(
                    f"""
                        DELETE FROM {claims_table}
                        WHERE channel_url = $1 AND claim_owner = $2
                        RETURNING channel_url
                    """,
                    channel_url,
                    claim_owner,
                )
                if owned_claim is None:
                    raise RuntimeError(
                        f"Discovery claim is no longer owned by {claim_owner}: "
                        f"{channel_url}"
                    )
            await _mark_channel_processed_with(
                conn, channel_url, processed_at=p_at, status=status
            )
            await conn.execute(
                f"DELETE FROM {candidates_table} WHERE channel_url = $1",
                channel_url,
            )


async def is_channel_processed(channel_url: str) -> bool:
    """Check if a channel has already been processed."""
    if not channel_url:
        return False
    pool = _require_pool()
    table_name = _get_table_name("channels_processed")
    row = await pool.fetchrow(f"SELECT 1 FROM {table_name} WHERE channel_url = $1", channel_url)
    return row is not None
