from __future__ import annotations

import asyncio
import uuid
import unittest
from pathlib import Path
from unittest import mock

import db
from scripts import report_db_storage


def _async_context(value):
    context = mock.MagicMock()
    context.__aenter__ = mock.AsyncMock(return_value=value)
    context.__aexit__ = mock.AsyncMock(return_value=False)
    return context


class CompactSchemaContractTests(unittest.TestCase):
    def tearDown(self) -> None:
        db._DB_POOL = None
        db._DB_LANGUAGE = "es"

    def _schema_sql(self, language: str) -> str:
        connection = mock.AsyncMock()
        pool = mock.Mock()
        pool.acquire.return_value = _async_context(connection)
        db._DB_POOL = pool

        # A clean schema must be safe to initialize repeatedly. Running the
        # builder twice also catches non-idempotent CREATE statements in the
        # recording without requiring a writable external PostgreSQL service.
        asyncio.run(db.create_tables(language))
        asyncio.run(db.create_tables(language))
        return "\n".join(
            call.args[0] for call in connection.execute.await_args_list
        ).lower()

    def test_clean_schema_is_compact_idempotent_and_bilingual(self) -> None:
        for language in ("es", "en"):
            with self.subTest(language=language):
                sql = self._schema_sql(language)
                suffix = f"_{language}"

                self.assertIn(
                    f"create table if not exists discovery_videos_staging{suffix}",
                    sql,
                )
                self.assertIn(
                    f"create table if not exists channel_candidates{suffix}", sql
                )
                self.assertIn(
                    f"create or replace view videos_raw{suffix}", sql
                )
                self.assertIn(
                    f"create or replace view videos_normalized{suffix}", sql
                )
                self.assertIn(
                    f"create table if not exists channel_stats{suffix}", sql
                )
                self.assertNotIn("create materialized view", sql)
                self.assertNotIn("channel_stats_refresh_locks", sql)
                self.assertNotIn("alter table", sql)

                channels_start = sql.index(
                    f"create table if not exists channels_raw{suffix}"
                )
                channels_end = sql.index(");", channels_start)
                channels_ddl = sql[channels_start:channels_end]
                self.assertIn(
                    "id bigint generated always as identity primary key", channels_ddl
                )
                self.assertIn("channel_url text not null unique", channels_ddl)
                self.assertIn("last_upload_date date", channels_ddl)

                videos_start = sql.index(
                    f"create table if not exists channel_videos_raw{suffix}"
                )
                videos_end = sql.index(");", videos_start)
                videos_ddl = sql[videos_start:videos_end]
                self.assertIn("video_id text primary key", videos_ddl)
                self.assertIn("channel_key bigint not null", videos_ddl)
                self.assertIn("upload_date date", videos_ddl)
                self.assertIn("duration_seconds integer", videos_ddl)
                self.assertNotIn("channel_url", videos_ddl)
                self.assertNotIn("video_url", videos_ddl)
                self.assertNotIn("thumbnail_url", videos_ddl)

    def test_compatibility_views_derive_urls_instead_of_storing_them(self) -> None:
        sql = self._schema_sql("es")
        self.assertIn(
            "'https://www.youtube.com/watch?v=' || s.video_id as video_url", sql
        )
        self.assertIn(
            "'https://i.ytimg.com/vi/' || s.video_id || '/hqdefault.jpg' as thumbnail_url",
            sql,
        )


class CompactStagingAndHistoryTests(unittest.TestCase):
    def tearDown(self) -> None:
        db._DB_POOL = None
        db._DB_LANGUAGE = "es"

    def test_search_history_uses_native_uuid_and_only_successful_queries(self) -> None:
        pool = mock.AsyncMock()
        pool.fetch.return_value = [{"query": "history"}]
        db._DB_POOL = pool

        run_id = asyncio.run(db.create_search_run("history"))
        self.assertIsInstance(run_id, uuid.UUID)
        self.assertIsInstance(pool.execute.await_args.args[1], uuid.UUID)

        asyncio.run(
            db.finish_search_run(run_id, status="success", result_count=17)
        )
        finish_args = pool.execute.await_args.args
        self.assertIs(finish_args[1], run_id)
        self.assertEqual(finish_args[3], "success")
        self.assertEqual(finish_args[4], 17)

        executed = asyncio.run(db.get_executed_queries())
        self.assertEqual(executed, {"history"})
        self.assertIn("where status = 'success'", pool.fetch.await_args.args[0].lower())

    def test_raw_and_normalized_writes_share_one_physical_staging_table(self) -> None:
        pool = mock.AsyncMock()
        pool.fetchval.side_effect = [2, 2]
        db._DB_POOL = pool
        run_id = uuid.uuid4()

        raw = [
            {
                "video_id": "abcdefghijk",
                "channel_url": "https://www.youtube.com/@a",
                "duration": "10:00",
                "views_text": "1K views",
                "published_text": "1 day ago",
            },
            {
                "video_id": "lmnopqrstuv",
                "channel_url": "https://www.youtube.com/@a",
                "duration": "11:00",
                "views_text": "2K views",
                "published_text": "2 days ago",
            },
        ]
        self.assertEqual(asyncio.run(db.insert_videos_raw(run_id, raw)), (2, 0))
        raw_sql = pool.fetchval.await_args_list[0].args[0].lower()
        self.assertIn("insert into discovery_videos_staging_es", raw_sql)
        self.assertIn("from unnest", raw_sql)
        self.assertNotIn("video_url", raw_sql)
        self.assertNotIn("thumbnail_url", raw_sql)
        self.assertIs(pool.fetchval.await_args_list[0].args[1], run_id)

        normalized = [
            {
                "video_id": row["video_id"],
                "views_estimated": 1000,
                "duration_seconds_estimated": 600,
                "validation_passed": True,
            }
            for row in raw
        ]
        self.assertEqual(
            asyncio.run(db.insert_videos_normalized(normalized)), (2, 0)
        )
        normalized_sql = pool.fetchval.await_args_list[1].args[0].lower()
        self.assertIn("update discovery_videos_staging_es", normalized_sql)
        self.assertIn("insert into channel_candidates_es", normalized_sql)
        self.assertIn("not exists", normalized_sql)
        self.assertIn("channels_processed_es", normalized_sql)

    def test_claims_read_the_candidate_queue_not_the_compatibility_view(self) -> None:
        connection = mock.AsyncMock()
        connection.fetch.return_value = [{"channel_url": "channel-a"}]
        connection.transaction = mock.Mock(return_value=_async_context(None))
        pool = mock.Mock()
        pool.acquire.return_value = _async_context(connection)
        db._DB_POOL = pool

        claimed = asyncio.run(
            db.claim_channels_for_discovery(
                3, claim_owner="owner-a", stale_after_minutes=60
            )
        )
        sql = connection.fetch.await_args.args[0].lower()
        self.assertEqual(claimed, ["channel-a"])
        self.assertIn("from channel_candidates_es", sql)
        self.assertNotIn("videos_normalized_es", sql)

    def test_purge_releases_heavy_staging_but_preserves_history_and_candidates(self) -> None:
        connection = mock.AsyncMock()
        connection.transaction = mock.Mock(return_value=_async_context(None))
        pool = mock.Mock()
        pool.acquire.return_value = _async_context(connection)
        db._DB_POOL = pool

        purged = asyncio.run(db.purge_pipeline_staging_tables("es"))
        sql = "\n".join(
            call.args[0] for call in connection.execute.await_args_list
        ).lower()
        self.assertEqual(purged, ["discovery_videos_staging_es"])
        self.assertIn("truncate table discovery_videos_staging_es", sql)
        self.assertIn("update search_runs_es", sql)
        self.assertIn("normalized_at is null", sql)
        self.assertNotIn("truncate table channel_candidates_es", sql)
        self.assertNotIn("truncate table search_runs_es", sql)


class TransactionalChannelStatsTests(unittest.TestCase):
    def tearDown(self) -> None:
        db._DB_POOL = None
        db._DB_LANGUAGE = "es"

    def test_channel_videos_stats_processed_and_queue_cleanup_are_atomic(self) -> None:
        connection = mock.AsyncMock()
        connection.fetchval.side_effect = [
            "https://www.youtube.com/@channel",
            42,
        ]
        transaction = _async_context(None)
        connection.transaction = mock.Mock(return_value=transaction)
        pool = mock.Mock()
        pool.acquire.return_value = _async_context(connection)
        db._DB_POOL = pool

        result = asyncio.run(
            db.persist_channel_discovery_result(
                {
                    "channel_url": "https://www.youtube.com/@channel",
                    "channel_name": "Channel",
                    "last_upload_date": "20240102",
                },
                [
                    {
                        "video_id": "abcdefghijk",
                        "upload_date": "20240101",
                        "duration_seconds": 600,
                        "view_count": 1000,
                        "title": "Video",
                        # These compatibility fields must never be persisted.
                        "video_url": "https://example.invalid/watch",
                        "thumbnail_url": "https://example.invalid/image",
                    }
                ],
                claim_owner="owner-a",
            )
        )
        self.assertEqual(result, (1, 0))
        transaction.__aenter__.assert_awaited_once()
        transaction.__aexit__.assert_awaited_once()

        fetch_sql = "\n".join(
            call.args[0] for call in connection.fetchval.await_args_list
        ).lower()
        execute_sql = "\n".join(
            call.args[0] for call in connection.execute.await_args_list
        ).lower()
        self.assertIn("returning id", fetch_sql)
        self.assertIn("insert into channel_videos_raw_es", execute_sql)
        self.assertIn("$3::date[]", execute_sql)
        self.assertNotIn("video_url", execute_sql)
        self.assertNotIn("thumbnail_url", execute_sql)
        self.assertIn("insert into channel_stats_es", execute_sql)
        self.assertIn("array_agg(view_count order by view_count)", execute_sql)
        self.assertIn("insert into channels_processed_es", execute_sql)
        self.assertIn("delete from channels_discovery_claims_es", fetch_sql)
        self.assertIn("delete from channel_candidates_es", execute_sql)

    def test_stats_include_channels_with_zero_videos(self) -> None:
        connection = mock.AsyncMock()
        connection.fetchval.side_effect = [
            "https://www.youtube.com/@empty",
            43,
        ]
        connection.transaction = mock.Mock(return_value=_async_context(None))
        pool = mock.Mock()
        pool.acquire.return_value = _async_context(connection)
        db._DB_POOL = pool

        self.assertEqual(
            asyncio.run(
                db.persist_channel_discovery_result(
                    {"channel_url": "https://www.youtube.com/@empty"},
                    [],
                    claim_owner="owner-a",
                )
            ),
            (0, 0),
        )
        sql = "\n".join(
            call.args[0] for call in connection.execute.await_args_list
        ).lower()
        self.assertIn("count(video_id)", sql)
        self.assertIn("coalesce(max(view_count), 0)", sql)
        self.assertNotIn("having count", sql)

    def test_explicit_stats_rebuild_is_transactional_repair_not_a_refresh(self) -> None:
        connection = mock.AsyncMock()
        transaction = _async_context(None)
        connection.transaction = mock.Mock(return_value=transaction)
        pool = mock.Mock()
        pool.acquire.return_value = _async_context(connection)
        db._DB_POOL = pool

        self.assertTrue(asyncio.run(db.refresh_channel_stats("es")))
        sql = "\n".join(
            call.args[0] for call in connection.execute.await_args_list
        ).lower()
        self.assertIn("delete from channel_stats_es", sql)
        self.assertIn("insert into channel_stats_es", sql)
        self.assertNotIn("refresh materialized view", sql)
        transaction.__aenter__.assert_awaited_once()

    def test_terminal_failure_removes_candidate_but_transient_release_does_not(self) -> None:
        connection = mock.AsyncMock()
        connection.execute.return_value = "DELETE 1"
        connection.fetchval.return_value = "https://www.youtube.com/@terminal"
        connection.transaction = mock.Mock(return_value=_async_context(None))
        pool = mock.Mock()
        pool.acquire.return_value = _async_context(connection)
        db._DB_POOL = pool

        asyncio.run(
            db.mark_channel_processed(
                "https://www.youtube.com/@terminal",
                status="failed",
                claim_owner="owner-a",
            )
        )
        terminal_sql = "\n".join(
            [call.args[0] for call in connection.fetchval.await_args_list]
            + [call.args[0] for call in connection.execute.await_args_list]
        ).lower()
        self.assertIn("insert into channels_processed_es", terminal_sql)
        self.assertIn("delete from channels_discovery_claims_es", terminal_sql)
        self.assertIn("delete from channel_candidates_es", terminal_sql)

        pool.execute = mock.AsyncMock(return_value="DELETE 1")
        self.assertTrue(
            asyncio.run(
                db.release_channel_discovery_claim(
                    "https://www.youtube.com/@transient", "owner-a"
                )
            )
        )
        transient_sql = pool.execute.await_args.args[0].lower()
        self.assertIn("delete from channels_discovery_claims_es", transient_sql)
        self.assertNotIn("channel_candidates_es", transient_sql)

    def test_intermediate_stats_failure_leaves_cleanup_to_transaction_rollback(self) -> None:
        connection = mock.AsyncMock()
        connection.fetchval.side_effect = [
            "https://www.youtube.com/@rollback",
            42,
        ]

        async def execute(sql, *_args, **_kwargs):
            if "INSERT INTO channel_stats_es" in sql:
                raise RuntimeError("stats write failed")
            return "OK"

        connection.execute.side_effect = execute
        transaction = _async_context(None)
        connection.transaction = mock.Mock(return_value=transaction)
        pool = mock.Mock()
        pool.acquire.return_value = _async_context(connection)
        db._DB_POOL = pool

        with self.assertRaisesRegex(RuntimeError, "stats write failed"):
            asyncio.run(
                db.persist_channel_discovery_result(
                    {"channel_url": "https://www.youtube.com/@rollback"},
                    [{"video_id": "abcdefghijk", "view_count": 1}],
                    claim_owner="owner-a",
                )
            )

        sql_before_failure = "\n".join(
            call.args[0] for call in connection.execute.await_args_list
        ).lower()
        self.assertNotIn("insert into channels_processed_es", sql_before_failure)
        # The claim is consumed first but the surrounding transaction restores
        # it when the later statistics write fails.
        claim_sql = connection.fetchval.await_args_list[0].args[0].lower()
        self.assertIn("delete from channels_discovery_claims_es", claim_sql)
        self.assertNotIn("delete from channel_candidates_es", sql_before_failure)
        transaction.__aexit__.assert_awaited_once()
        self.assertIs(transaction.__aexit__.await_args.args[0], RuntimeError)

    def test_stale_claim_owner_cannot_persist_or_remove_candidate(self) -> None:
        connection = mock.AsyncMock()
        connection.fetchval.return_value = None
        transaction = _async_context(None)
        connection.transaction = mock.Mock(return_value=transaction)
        pool = mock.Mock()
        pool.acquire.return_value = _async_context(connection)
        db._DB_POOL = pool

        with self.assertRaisesRegex(RuntimeError, "no longer owned"):
            asyncio.run(
                db.persist_channel_discovery_result(
                    {"channel_url": "https://www.youtube.com/@stale"},
                    [{"video_id": "abcdefghijk"}],
                    claim_owner="old-owner",
                )
            )

        connection.execute.assert_not_awaited()
        claim_sql = connection.fetchval.await_args.args[0].lower()
        self.assertIn("claim_owner = $2", claim_sql)
        self.assertIn("returning channel_url", claim_sql)

    def test_stale_claim_owner_cannot_mark_terminal_failure(self) -> None:
        connection = mock.AsyncMock()
        connection.fetchval.return_value = None
        connection.transaction = mock.Mock(return_value=_async_context(None))
        pool = mock.Mock()
        pool.acquire.return_value = _async_context(connection)
        db._DB_POOL = pool

        with self.assertRaisesRegex(RuntimeError, "no longer owned"):
            asyncio.run(
                db.mark_channel_processed(
                    "https://www.youtube.com/@stale",
                    status="failed",
                    claim_owner="old-owner",
                )
            )
        connection.execute.assert_not_awaited()


class StorageBudgetAndReportTests(unittest.TestCase):
    def test_representative_video_layout_meets_the_35_percent_budget(self) -> None:
        # Conservative byte model for one representative row plus its indexes.
        # It intentionally ignores PostgreSQL TOAST savings, so passing this
        # estimate is stricter than typical real-world channel/video data.
        row_header = 24
        video_id = 12
        channel_url = 44
        title = 81
        watch_url = 44
        thumbnail_url = 55
        old_heap = (
            row_header
            + channel_url
            + video_id
            + 9  # YYYYMMDD text
            + 8  # BIGINT duration
            + 8  # BIGINT views
            + title
            + watch_url
            + thumbnail_url
        )
        old_indexes = (channel_url + video_id + 16) + (channel_url + 16)

        compact_heap = (
            row_header
            + video_id
            + 8  # channel_key
            + 4  # DATE
            + 4  # INTEGER duration
            + 8  # BIGINT views
            + title
        )
        compact_indexes = (video_id + 16) + (8 + 4 + 8 + 16)

        ratio = (compact_heap + compact_indexes) / (old_heap + old_indexes)
        self.assertLessEqual(ratio, 0.65)

    def test_storage_report_covers_heap_indexes_toast_total_and_languages(self) -> None:
        sql = report_db_storage.REPORT_SQL.lower()
        self.assertIn("pg_relation_size", sql)
        self.assertIn("pg_indexes_size", sql)
        self.assertIn("reltoastrelid", sql)
        self.assertIn("pg_total_relation_size", sql)
        self.assertIn("n.nspname = current_schema()", sql)

        parser = report_db_storage._build_parser()
        self.assertEqual(parser.parse_args([]).languages, ["es", "en"])
        self.assertEqual(parser.parse_args(["--ES"]).languages, ["es"])
        self.assertEqual(parser.parse_args(["--EN"]).languages, ["en"])

    def test_storage_report_script_is_read_only(self) -> None:
        source = (
            Path(__file__).resolve().parents[1] / "scripts" / "report_db_storage.py"
        ).read_text(encoding="utf-8").lower()
        for mutation in ("insert into", "update ", "delete from", "truncate", "drop "):
            self.assertNotIn(mutation, source)


if __name__ == "__main__":
    unittest.main()
