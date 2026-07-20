from __future__ import annotations

import asyncio
import sys
import types
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

import db
import yt_channel_discovery as discovery
import yt_channel_finalize as channel_finalize
import yt_first_video_enrichment as enrichment
from scripts import benchmark_channel_ytdlp_modes as channel_benchmark


CHANNEL_ID = "UCaaaaaaaaaaaaaaaaaaaaaa"


class DatabaseOptimizationTests(unittest.TestCase):
    def tearDown(self) -> None:
        db._DB_POOL = None
        db._DB_LANGUAGE = "es"

    def test_init_db_can_skip_schema(self) -> None:
        pool = mock.Mock()
        with (
            mock.patch.object(db.asyncpg, "create_pool", new=mock.AsyncMock(return_value=pool)),
            mock.patch.object(db, "create_tables", new=mock.AsyncMock()) as create_mock,
        ):
            asyncio.run(db.init_db("postgresql://test", ensure_schema=False))
        create_mock.assert_not_awaited()

    def test_discovery_claim_returns_only_atomic_insert_results(self) -> None:
        pool = mock.Mock()
        connection = mock.AsyncMock()
        connection.fetch.return_value = [{"channel_url": "channel-a"}]
        acquire_context = mock.MagicMock()
        acquire_context.__aenter__ = mock.AsyncMock(return_value=connection)
        acquire_context.__aexit__ = mock.AsyncMock(return_value=False)
        transaction_context = mock.MagicMock()
        transaction_context.__aenter__ = mock.AsyncMock(return_value=None)
        transaction_context.__aexit__ = mock.AsyncMock(return_value=False)
        pool.acquire.return_value = acquire_context
        connection.transaction = mock.Mock(return_value=transaction_context)
        db._DB_POOL = pool
        result = asyncio.run(db.claim_channels_for_discovery(
            5, claim_owner="owner-a", stale_after_minutes=60
        ))
        lock_sql = connection.execute.await_args.args[0]
        sql = connection.fetch.await_args.args[0]
        self.assertIn("pg_advisory_xact_lock", lock_sql)
        self.assertIn("ON CONFLICT (channel_url) DO UPDATE", sql)
        self.assertIn("RETURNING channel_url", sql)
        self.assertEqual(connection.fetch.await_args.args[2], "owner-a")
        self.assertEqual(result, ["channel-a"])

    def test_pending_channel_count_ignores_claims(self) -> None:
        pool = mock.AsyncMock()
        pool.fetchval.return_value = 7
        db._DB_POOL = pool
        result = asyncio.run(db.count_pending_channels_for_discovery())
        sql = pool.fetchval.await_args.args[0]
        self.assertNotIn("channels_discovery_claims", sql)
        self.assertEqual(result, 7)

    def test_release_claims_is_scoped_to_owner(self) -> None:
        pool = mock.AsyncMock()
        pool.execute.return_value = "DELETE 2"
        db._DB_POOL = pool
        deleted = asyncio.run(db.release_channel_discovery_claims("owner-a"))
        self.assertEqual(deleted, 2)
        self.assertIn("claim_owner = $1", pool.execute.await_args.args[0])
        self.assertEqual(pool.execute.await_args.args[1], "owner-a")

    def test_video_upsert_uses_one_unnest_statement(self) -> None:
        pool = mock.AsyncMock()
        db._DB_POOL = pool
        videos = [
            {"video_id": "abcdefghijk", "upload_date": "20240101", "view_count": 1},
            {"video_id": "lmnopqrstuv", "upload_date": "20240102", "view_count": 2},
        ]
        result = asyncio.run(db.upsert_channel_videos_raw("channel", videos))
        sql = pool.execute.await_args.args[0]
        self.assertIn("FROM UNNEST", sql)
        self.assertEqual(pool.execute.await_count, 1)
        self.assertEqual(pool.execute.await_args.args[2], ["abcdefghijk", "lmnopqrstuv"])
        self.assertEqual(result, (2, 0))

    def test_first_video_batch_uses_json_recordset(self) -> None:
        pool = mock.AsyncMock()
        db._DB_POOL = pool
        asyncio.run(db.update_first_video_results([{
            "channel_url": "channel",
            "first_video_status": "pending",
            "first_video_last_error": "temporary",
        }]))
        sql = pool.execute.await_args.args[0]
        self.assertIn("jsonb_to_recordset", sql)
        self.assertIn("NOT IN ('success', 'no_public_videos')", sql)

    def test_channel_result_is_persisted_inside_one_transaction(self) -> None:
        pool = mock.Mock()
        connection = mock.AsyncMock()
        acquire_context = mock.MagicMock()
        acquire_context.__aenter__ = mock.AsyncMock(return_value=connection)
        acquire_context.__aexit__ = mock.AsyncMock(return_value=False)
        transaction_context = mock.MagicMock()
        transaction_context.__aenter__ = mock.AsyncMock(return_value=None)
        transaction_context.__aexit__ = mock.AsyncMock(return_value=False)
        pool.acquire.return_value = acquire_context
        connection.transaction = mock.Mock(return_value=transaction_context)
        db._DB_POOL = pool
        asyncio.run(db.persist_channel_discovery_result(
            {"channel_url": "channel", "first_video_status": "pending"},
            [{"video_id": "abcdefghijk"}],
            claim_owner="owner",
        ))
        statements = [call.args[0] for call in connection.execute.await_args_list]
        self.assertTrue(any("INSERT INTO channels_raw_es" in sql for sql in statements))
        self.assertTrue(any("FROM UNNEST" in sql for sql in statements))
        self.assertTrue(any("INSERT INTO channels_processed_es" in sql for sql in statements))
        self.assertTrue(any("claim_owner = $2" in sql for sql in statements))
        transaction_context.__aenter__.assert_awaited_once()


class DiscoveryOptimizationTests(unittest.TestCase):

    def test_claim_batch_retries_a_transient_empty_result(self) -> None:
        runner = mock.Mock()
        results = [[], ["channel-a"]]

        def run_claim(coroutine):
            coroutine.close()
            return results.pop(0)

        runner.run.side_effect = run_claim
        discovery._STOP_EVENT.clear()
        with mock.patch.object(discovery._STOP_EVENT, "wait", return_value=False) as wait_mock:
            claimed = discovery._claim_channel_batch(
                runner,
                limit=12,
                claim_owner="owner-a",
                stale_after_minutes=60,
            )

        self.assertEqual(claimed, ["channel-a"])
        self.assertEqual(runner.run.call_count, 2)
        wait_mock.assert_called_once_with(discovery.EMPTY_CLAIM_RETRY_SECONDS)

    def test_benchmark_ignores_localized_titles_but_not_structural_fields(self) -> None:
        base = {
            "channel_id": CHANNEL_ID,
            "channel_name": "Original",
            "videos": [("abcdefghijk", "Original title", "20240101", 120)],
        }
        translated = {
            "channel_id": CHANNEL_ID,
            "channel_name": "Translated",
            "videos": [("abcdefghijk", "Translated title", "20240101", 120)],
        }
        changed_id = {
            **translated,
            "videos": [("zyxwvutsrqp", "Translated title", "20240101", 120)],
        }

        self.assertEqual(
            channel_benchmark._structural_signature(base),
            channel_benchmark._structural_signature(translated),
        )
        self.assertNotEqual(
            channel_benchmark._structural_signature(base),
            channel_benchmark._structural_signature(changed_id),
        )
    def _dump(self) -> dict:
        return {
            "channel_id": CHANNEL_ID,
            "channel": "Channel",
            "entries": [{
                "id": "abcdefghijk",
                "title": "Video",
                "upload_date": "20240102",
            }],
        }

    def test_rss_excludes_shorts_and_returns_exact_long_date(self) -> None:
        xml = b"""<?xml version='1.0' encoding='UTF-8'?>
        <feed xmlns='http://www.w3.org/2005/Atom'
              xmlns:yt='http://www.youtube.com/xml/schemas/2015'>
          <entry><yt:videoId>abcdefghijk</yt:videoId>
            <published>2024-01-01T23:30:00+00:00</published>
            <link href='https://www.youtube.com/watch?v=abcdefghijk'/></entry>
          <entry><yt:videoId>lmnopqrstuv</yt:videoId>
            <published>2024-01-02T01:00:00+00:00</published>
            <link href='https://www.youtube.com/shorts/lmnopqrstuv'/></entry>
        </feed>"""
        response = mock.Mock(content=xml)
        session = mock.Mock()
        session.get.return_value = response
        with mock.patch.object(discovery, "_rss_session", return_value=session):
            dates, latest = discovery.fetch_rss_dates(CHANNEL_ID)
        self.assertEqual(dates, {"abcdefghijk": "20240101"})
        self.assertEqual(latest, "20240101")

    def test_exact_rss_date_replaces_approximate_date(self) -> None:
        runner = mock.Mock()
        runner.run.side_effect = lambda value: False if value == "check" else None
        pending = {"first_video_status": "pending", "first_video_last_error": "later"}
        with (
            mock.patch.object(
                discovery, "is_channel_processed", new=mock.Mock(return_value="check")
            ),
            mock.patch.object(discovery, "run_ytdlp_channel_dump", return_value=self._dump()),
            mock.patch.object(discovery, "fetch_rss_dates", return_value=({"abcdefghijk": "20240101"}, "20240101")),
            mock.patch.object(discovery, "resolve_first_video", return_value=pending),
            mock.patch.object(
                discovery,
                "persist_channel_discovery_result",
                new=mock.Mock(return_value="persist"),
            ) as persist_mock,
            mock.patch("builtins.print"),
        ):
            result = discovery.process_one_channel(
                "https://www.youtube.com/@channel",
                runner,
                claim_owner="owner",
                oldest_video_client=mock.Mock(),
            )
        channel, videos = persist_mock.call_args.args[:2]
        self.assertEqual(channel["last_upload_date"], "20240101")
        self.assertEqual(videos[0]["upload_date"], "20240101")
        self.assertEqual(result[1], "processed")

    def test_process_pool_api_sanitizes_the_same_dump_shape(self) -> None:
        payload = self._dump()

        class FakeYoutubeDL:
            def __init__(self, options):
                self.options = options

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return None

            def extract_info(self, url, download=False):
                self.url = url
                return payload

            def sanitize_info(self, info):
                return info

        fake_module = types.SimpleNamespace(
            YoutubeDL=FakeYoutubeDL,
            parse_options=lambda _args: types.SimpleNamespace(ydl_opts={"quiet": True}),
        )
        discovery._ytdlp_api_options.cache_clear()
        with mock.patch.dict(sys.modules, {"yt_dlp": fake_module}):
            result = discovery.run_ytdlp_channel_dump_api(
                "https://www.youtube.com/@channel", max_videos=60
            )
        discovery._ytdlp_api_options.cache_clear()
        self.assertEqual(result, payload)

    def test_last_upload_keeps_approximate_date_outside_rss_window(self) -> None:
        runner = mock.Mock()
        runner.run.side_effect = lambda value: False if value == "check" else None
        with (
            mock.patch.object(
                discovery, "is_channel_processed", new=mock.Mock(return_value="check")
            ),
            mock.patch.object(discovery, "run_ytdlp_channel_dump", return_value=self._dump()),
            mock.patch.object(discovery, "fetch_rss_dates", return_value=({}, None)),
            mock.patch.object(
                discovery,
                "persist_channel_discovery_result",
                new=mock.Mock(return_value="persist"),
            ) as persist_mock,
            mock.patch("builtins.print"),
        ):
            discovery.process_one_channel(
                "https://www.youtube.com/@channel",
                runner,
                claim_owner="owner",
            )
        channel = persist_mock.call_args.args[0]
        self.assertEqual(channel["last_upload_date"], "20240102")

    def test_preclaimed_channel_skips_redundant_processed_lookup(self) -> None:
        runner = mock.Mock()
        pending = {"first_video_status": "pending", "first_video_last_error": "later"}
        with (
            mock.patch.object(discovery, "is_channel_processed") as processed_mock,
            mock.patch.object(discovery, "run_ytdlp_channel_dump", return_value=self._dump()),
            mock.patch.object(discovery, "fetch_rss_dates", return_value=({}, None)),
            mock.patch.object(discovery, "resolve_first_video", return_value=pending),
            mock.patch.object(
                discovery,
                "persist_channel_discovery_result",
                new=mock.Mock(return_value="persist"),
            ),
            mock.patch("builtins.print"),
        ):
            result = discovery.process_one_channel(
                "https://www.youtube.com/@channel",
                runner,
                claim_owner="owner",
                preclaimed=True,
            )

        self.assertEqual(result[1], "processed")
        processed_mock.assert_not_called()

    def test_process_pool_is_the_default_with_subprocess_rollback(self) -> None:
        parser = discovery._build_arg_parser()
        self.assertEqual(parser.parse_args([]).ytdlp_mode, "process-pool")
        self.assertEqual(
            parser.parse_args(["--ytdlp-mode", "subprocess"]).ytdlp_mode,
            "subprocess",
        )

    def test_stop_terminates_registered_subprocesses(self) -> None:
        process = mock.Mock()
        discovery._ACTIVE_YTDLP_PROCESSES.add(process)
        try:
            with (
                mock.patch.object(discovery, "_terminate_process_tree") as terminate_mock,
                mock.patch.object(discovery, "terminate_active_fallback_processes") as fallback_mock,
            ):
                discovery._request_stop()
            terminate_mock.assert_called_once_with(process)
            fallback_mock.assert_called_once_with()
            self.assertTrue(discovery._STOP_EVENT.is_set())
        finally:
            discovery._ACTIVE_YTDLP_PROCESSES.discard(process)
            discovery._STOP_EVENT.clear()


class EnrichmentBatchTests(unittest.TestCase):
    def test_enrichment_limits_active_window_and_does_not_reclaim_same_cycle(self) -> None:
        profiles = [
            {"channel_url": f"channel-{index}", "channel_id": CHANNEL_ID}
            for index in range(8)
        ]
        claims = [profiles, []]
        writes: list[list[dict]] = []

        class FakeRunner:
            def start(self):
                pass

            def stop(self):
                pass

            def run(self, value):
                if isinstance(value, tuple) and value[0] == "claim":
                    return claims.pop(0)
                if isinstance(value, tuple) and value[0] == "write":
                    writes.append(value[1])
                    return len(value[1])
                return None

        claim_mock = mock.Mock(side_effect=lambda *args, **kwargs: ("claim", args, kwargs))
        write_mock = mock.Mock(side_effect=lambda results: ("write", results))
        result = {
            "first_video_status": "success",
            "first_video_id": "abcdefghijk",
            "first_video_published_at": "2024-01-01T00:00:00Z",
            "first_video_source": "innertube",
            "first_video_last_attempt_at": datetime.now(timezone.utc),
        }
        with (
            mock.patch.object(enrichment, "_DBRunner", return_value=FakeRunner()),
            mock.patch.object(enrichment, "init_db", new=mock.Mock(return_value=("init",))),
            mock.patch.object(enrichment, "close_db", new=mock.Mock(return_value=("close",))),
            mock.patch.object(enrichment, "claim_channels_for_first_video_enrichment", new=claim_mock),
            mock.patch.object(enrichment, "update_first_video_results", new=write_mock),
            mock.patch.object(enrichment.YouTubeOldestVideoClient, "initialize", return_value=mock.Mock()),
            mock.patch.object(
                enrichment, "resolve_first_video", side_effect=lambda *_args, **_kwargs: result.copy()
            ),
            mock.patch("builtins.print"),
        ):
            enrichment.run(workers=2, limit=20, batch_size=50)
        self.assertEqual([len(batch) for batch in writes], [4, 4])
        self.assertIsInstance(
            claim_mock.call_args_list[0].kwargs["eligible_before"], datetime
        )


class WorkflowTests(unittest.TestCase):

    def test_finalizer_preserves_staging_while_channels_are_pending(self) -> None:
        with (
            mock.patch.object(channel_finalize, "init_db", new=mock.AsyncMock()),
            mock.patch.object(channel_finalize, "close_db", new=mock.AsyncMock()),
            mock.patch.object(
                channel_finalize, "refresh_channel_stats", new=mock.AsyncMock(return_value=True)
            ),
            mock.patch.object(
                channel_finalize,
                "count_pending_channels_for_discovery",
                new=mock.AsyncMock(return_value=21_311),
            ),
            mock.patch.object(
                channel_finalize, "purge_pipeline_staging_tables", new=mock.AsyncMock()
            ) as purge_mock,
        ):
            asyncio.run(channel_finalize.finalize(ensure_schema=False))

        purge_mock.assert_not_awaited()

    def test_finalizer_purges_staging_after_queue_is_drained(self) -> None:
        with (
            mock.patch.object(channel_finalize, "init_db", new=mock.AsyncMock()),
            mock.patch.object(channel_finalize, "close_db", new=mock.AsyncMock()),
            mock.patch.object(
                channel_finalize, "refresh_channel_stats", new=mock.AsyncMock(return_value=True)
            ),
            mock.patch.object(
                channel_finalize,
                "count_pending_channels_for_discovery",
                new=mock.AsyncMock(return_value=0),
            ),
            mock.patch.object(
                channel_finalize,
                "purge_pipeline_staging_tables",
                new=mock.AsyncMock(return_value=["videos_raw_es"]),
            ) as purge_mock,
        ):
            asyncio.run(channel_finalize.finalize(ensure_schema=False))

        purge_mock.assert_awaited_once_with("es")
    def test_parallel_workers_skip_schema_and_finalize_once(self) -> None:
        root = Path(__file__).resolve().parents[1]
        for filename in ("full-pipeline.yml", "parallel-channel-discovery.yml"):
            text = (root / ".github" / "workflows" / filename).read_text(encoding="utf-8")
            self.assertIn("--skip-schema --skip-finalize", text)
            self.assertIn("--claim-batch-size 120", text)
            self.assertIn("yt_channel_finalize.py --skip-schema", text)
            self.assertEqual(text.count("yt_channel_finalize.py --skip-schema"), 1)
            self.assertIn("group: youtube-long-channel-pipeline-${{ inputs.language }}", text)
        parallel = (
            root / ".github" / "workflows" / "parallel-channel-discovery.yml"
        ).read_text(encoding="utf-8")
        self.assertIn("prepare-schema:", parallel)
        self.assertIn("needs: [prepare-matrix, prepare-schema]", parallel)


if __name__ == "__main__":
    unittest.main()
