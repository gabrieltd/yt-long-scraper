from __future__ import annotations

import argparse
import asyncio
import unittest
from datetime import datetime, timezone
from decimal import Decimal
from unittest import mock

from scripts import archive_supabase_to_local as archive


def _async_context(value=None):
    context = mock.MagicMock()
    context.__aenter__ = mock.AsyncMock(return_value=value)
    context.__aexit__ = mock.AsyncMock(return_value=False)
    return context


def _identity(
    database: str,
    address: str,
    *,
    read_only: bool = False,
) -> archive.ConnectionIdentity:
    return archive.ConnectionIdentity(
        database=database,
        server_address=address,
        server_port=5432,
        postmaster_started_at=datetime(2026, 7, 21, tzinfo=timezone.utc),
        read_only=read_only,
    )


def _args(**overrides):
    values = {
        "source_url": "postgresql://source/db",
        "destination_url": "postgresql://local/db",
        "languages": ["es"],
        "batch_size": 500,
        "claim_stale_minutes": 60,
        "copy": False,
        "truncate_after_verify": False,
        "timeout_seconds": 30,
    }
    values.update(overrides)
    return argparse.Namespace(**values)


class ArchiveCliTests(unittest.TestCase):
    def test_parser_requires_explicit_language_and_defaults_to_dry_run(self) -> None:
        parser = archive._build_parser()
        with self.assertRaises(SystemExit):
            parser.parse_args([])
        args = parser.parse_args(["--all"])
        self.assertEqual(args.languages, ["es", "en"])
        self.assertFalse(args.copy)
        self.assertFalse(args.truncate_after_verify)
        self.assertEqual(args.batch_size, 500)

    def test_truncate_requires_copy_before_connecting(self) -> None:
        with (
            mock.patch.object(archive, "_connect", new=mock.AsyncMock()) as connect,
            self.assertRaisesRegex(ValueError, "requires --copy"),
        ):
            asyncio.run(archive.run(_args(truncate_after_verify=True)))
        connect.assert_not_awaited()

    def test_transaction_pooler_is_dry_run_only(self) -> None:
        with (
            mock.patch.object(archive, "_connect", new=mock.AsyncMock()) as connect,
            self.assertRaisesRegex(ValueError, "port 5432"),
        ):
            asyncio.run(archive.run(_args(
                source_url="postgresql://source.example:6543/postgres",
                copy=True,
            )))
        connect.assert_not_awaited()

    def test_same_database_identity_uses_server_instance_and_database(self) -> None:
        source = _identity("postgres", "10.0.0.1")
        self.assertTrue(archive._same_database(source, source))
        self.assertFalse(
            archive._same_database(source, _identity("archive", "10.0.0.1"))
        )
        self.assertFalse(
            archive._same_database(source, _identity("postgres", "127.0.0.1"))
        )

    def test_dry_run_never_initializes_or_writes(self) -> None:
        source = mock.AsyncMock()
        destination = mock.AsyncMock()
        zero = archive.LanguageCounts(0, 0, 0, 0, 0, 0, 0, 0)
        with (
            mock.patch.object(
                archive,
                "_connect",
                new=mock.AsyncMock(side_effect=[source, destination]),
            ),
            mock.patch.object(
                archive,
                "_identity",
                new=mock.AsyncMock(side_effect=[
                    _identity("remote", "10.0.0.1"),
                    _identity("local", "127.0.0.1"),
                ]),
            ),
            mock.patch.object(
                archive, "_has_schema", new=mock.AsyncMock(return_value=True)
            ),
            mock.patch.object(
                archive, "_is_compact_schema", new=mock.AsyncMock(return_value=True)
            ),
            mock.patch.object(
                archive, "_language_counts", new=mock.AsyncMock(return_value=zero)
            ),
            mock.patch.object(archive, "_show_storage", new=mock.AsyncMock()),
            mock.patch.object(
                archive, "_ensure_local_schema", new=mock.AsyncMock()
            ) as ensure_schema,
            mock.patch.object(
                archive, "_ensure_archive_batches", new=mock.AsyncMock()
            ) as ensure_batches,
        ):
            self.assertEqual(asyncio.run(archive.run(_args())), 0)
        ensure_schema.assert_not_awaited()
        ensure_batches.assert_not_awaited()
        source.execute.assert_not_awaited()
        destination.execute.assert_not_awaited()
        source.close.assert_awaited_once()
        destination.close.assert_awaited_once()

    def test_legacy_destination_is_rejected_without_schema_mutation(self) -> None:
        source = mock.AsyncMock()
        destination = mock.AsyncMock()
        with (
            mock.patch.object(
                archive,
                "_connect",
                new=mock.AsyncMock(side_effect=[source, destination]),
            ),
            mock.patch.object(
                archive,
                "_identity",
                new=mock.AsyncMock(side_effect=[
                    _identity("remote", "10.0.0.1"),
                    _identity("legacy", "127.0.0.1"),
                ]),
            ),
            mock.patch.object(
                archive, "_has_schema", new=mock.AsyncMock(return_value=True)
            ),
            mock.patch.object(
                archive,
                "_is_compact_schema",
                new=mock.AsyncMock(side_effect=[True, False]),
            ),
            mock.patch.object(
                archive, "_ensure_local_schema", new=mock.AsyncMock()
            ) as ensure_schema,
            self.assertRaisesRegex(RuntimeError, "legacy/incompatible"),
        ):
            asyncio.run(archive.run(_args(copy=True)))
        ensure_schema.assert_not_awaited()

    def test_all_languages_skips_a_schema_that_does_not_exist_remotely(self) -> None:
        source = mock.AsyncMock()
        destination = mock.AsyncMock()
        zero = archive.LanguageCounts(0, 0, 0, 0, 0, 0, 0, 0)
        with (
            mock.patch.object(
                archive,
                "_connect",
                new=mock.AsyncMock(side_effect=[source, destination]),
            ),
            mock.patch.object(
                archive,
                "_identity",
                new=mock.AsyncMock(side_effect=[
                    _identity("remote", "10.0.0.1"),
                    _identity("local", "127.0.0.1"),
                ]),
            ),
            mock.patch.object(
                archive,
                "_is_compact_schema",
                new=mock.AsyncMock(side_effect=[False, True, True]),
            ),
            mock.patch.object(
                archive, "_has_schema", new=mock.AsyncMock(return_value=True)
            ),
            mock.patch.object(
                archive, "_language_counts", new=mock.AsyncMock(return_value=zero)
            ) as counts,
            mock.patch.object(archive, "_show_storage", new=mock.AsyncMock()) as storage,
        ):
            self.assertEqual(
                asyncio.run(archive.run(_args(languages=["es", "en"]))),
                0,
            )
        self.assertEqual(counts.await_count, 2)
        self.assertEqual(storage.await_args.args[2], ["en"])

    def test_same_database_is_rejected_before_schema_creation(self) -> None:
        source = mock.AsyncMock()
        destination = mock.AsyncMock()
        same = _identity("postgres", "10.0.0.1")
        with (
            mock.patch.object(
                archive,
                "_connect",
                new=mock.AsyncMock(side_effect=[source, destination]),
            ),
            mock.patch.object(
                archive, "_identity", new=mock.AsyncMock(side_effect=[same, same])
            ),
            mock.patch.object(
                archive, "_ensure_local_schema", new=mock.AsyncMock()
            ) as ensure_schema,
            self.assertRaisesRegex(RuntimeError, "same PostgreSQL database"),
        ):
            asyncio.run(archive.run(_args(copy=True)))
        ensure_schema.assert_not_awaited()

    def test_read_only_source_allows_copy_but_blocks_truncate(self) -> None:
        source = mock.AsyncMock()
        destination = mock.AsyncMock()
        with (
            mock.patch.object(
                archive,
                "_connect",
                new=mock.AsyncMock(side_effect=[source, destination]),
            ),
            mock.patch.object(
                archive,
                "_identity",
                new=mock.AsyncMock(side_effect=[
                    _identity("remote", "10.0.0.1", read_only=True),
                    _identity("local", "127.0.0.1"),
                ]),
            ),
            mock.patch.object(
                archive, "_ensure_local_schema", new=mock.AsyncMock()
            ) as ensure_schema,
            self.assertRaisesRegex(RuntimeError, "read-only"),
        ):
            asyncio.run(
                archive.run(_args(copy=True, truncate_after_verify=True))
            )
        ensure_schema.assert_not_awaited()


class ArchiveVerificationTests(unittest.TestCase):
    def test_canonical_digest_is_stable_for_dates_arrays_and_decimals(self) -> None:
        row = {
            "channel_url": "channel",
            "when": datetime(2026, 7, 21, tzinfo=timezone.utc),
            "tags": ["a", "b"],
        }
        left = archive.CanonicalDigest()
        right = archive.CanonicalDigest()
        left.add("channel", [row], tuple(row))
        right.add("channel", [dict(row)], tuple(row))
        self.assertEqual(left.hexdigest(), right.hexdigest())

    def test_verification_accepts_local_values_when_source_is_null(self) -> None:
        archive._verify_preserved_rows(
            "video",
            [{"video_id": "abcdefghijk", "title": None}],
            [{"video_id": "abcdefghijk", "title": "Local title"}],
            key_fields=("video_id",),
            compare_fields=("title",),
        )

    def test_verification_rejects_missing_or_changed_rows(self) -> None:
        with self.assertRaisesRegex(archive.ArchiveVerificationError, "Missing video"):
            archive._verify_preserved_rows(
                "video",
                [{"video_id": "abcdefghijk", "view_count": 5}],
                [],
                key_fields=("video_id",),
                compare_fields=("view_count",),
            )
        with self.assertRaisesRegex(archive.ArchiveVerificationError, "mismatch"):
            archive._verify_preserved_rows(
                "video",
                [{"video_id": "abcdefghijk", "view_count": 5}],
                [{"video_id": "abcdefghijk", "view_count": 4}],
                key_fields=("video_id",),
                compare_fields=("view_count",),
            )

    def test_pending_first_video_can_be_enriched_locally(self) -> None:
        archive._verify_first_video(
            [{"channel_url": "channel", "first_video_status": "pending"}],
            [{
                "channel_url": "channel",
                "first_video_status": "success",
                "first_video_id": "abcdefghijk",
            }],
        )

    def test_terminal_first_video_must_match(self) -> None:
        source = [{
            "channel_url": "channel",
            "first_video_status": "success",
            "first_video_id": "abcdefghijk",
            "first_video_published_at": datetime(2024, 1, 1, tzinfo=timezone.utc),
        }]
        destination = [{
            "channel_url": "channel",
            "first_video_status": "success",
            "first_video_id": "lmnopqrstuv",
            "first_video_published_at": datetime(2024, 1, 1, tzinfo=timezone.utc),
        }]
        with self.assertRaisesRegex(archive.ArchiveVerificationError, "mismatch"):
            archive._verify_first_video(source, destination)

    def test_rebuilt_stats_use_video_rows_not_stale_remote_cache(self) -> None:
        channels = [
            {"channel_url": "old-alias"},
            {"channel_url": "canonical"},
        ]
        videos = [
            {"channel_url": "canonical", "video_id": "abcdefghijk", "view_count": 8},
            {"channel_url": "canonical", "video_id": "lmnopqrstuv", "view_count": 3},
        ]
        stats = [
            {
                "channel_url": "old-alias",
                "total_videos_tracked": 0,
                "avg_views_on_channel": Decimal(0),
                "max_views_on_channel": 0,
                "view_counts": [],
            },
            {
                "channel_url": "canonical",
                "total_videos_tracked": 2,
                "avg_views_on_channel": Decimal("5.50"),
                "max_views_on_channel": 8,
                "view_counts": [3, 8],
            },
        ]

        archive._verify_rebuilt_stats(channels, videos, stats)

    def test_rebuilt_stats_reject_incorrect_local_aggregate(self) -> None:
        with self.assertRaisesRegex(
            archive.ArchiveVerificationError,
            "rebuilt stats mismatch",
        ):
            archive._verify_rebuilt_stats(
                [{"channel_url": "channel"}],
                [{
                    "channel_url": "channel",
                    "video_id": "abcdefghijk",
                    "view_count": 10,
                }],
                [{
                    "channel_url": "channel",
                    "total_videos_tracked": 0,
                    "avg_views_on_channel": Decimal(0),
                    "max_views_on_channel": 0,
                    "view_counts": [],
                }],
            )


class ArchiveSqlSafetyTests(unittest.TestCase):
    def _source(self, *, claims: int = 0):
        source = mock.AsyncMock()
        source.transaction = mock.Mock(return_value=_async_context(None))
        source.fetchval.return_value = claims
        source.fetch.return_value = []
        return source

    def test_destructive_snapshot_locks_and_truncates_expected_tables(self) -> None:
        source = self._source()
        destination = mock.AsyncMock()
        asyncio.run(
            archive._copy_heavy_snapshot(
                source,
                destination,
                "es",
                batch_size=50,
                destructive=True,
                claim_stale_minutes=60,
            )
        )
        sql = "\n".join(
            call.args[0] for call in source.execute.await_args_list
        ).lower()
        self.assertIn("access exclusive mode nowait", sql)
        self.assertIn("channels_discovery_claims_es", sql)
        self.assertIn("truncate table", sql)
        self.assertIn("channels_raw_es", sql)
        self.assertIn("restart identity", sql)
        self.assertNotIn("channels_processed_es", sql)
        self.assertNotIn("search_runs_es", sql)
        self.assertNotIn("channel_candidates_es", sql)
        self.assertNotIn("discovery_videos_staging_es", sql)

    def test_active_claim_prevents_lock_copy_and_truncate(self) -> None:
        source = self._source(claims=2)
        destination = mock.AsyncMock()
        with self.assertRaisesRegex(RuntimeError, "2 active claims"):
            asyncio.run(
                archive._copy_heavy_snapshot(
                    source,
                    destination,
                    "en",
                    batch_size=50,
                    destructive=True,
                    claim_stale_minutes=60,
                )
            )
        source.execute.assert_not_awaited()

    def test_verification_failure_never_reaches_truncate(self) -> None:
        source = self._source()
        source.fetch.side_effect = [
            [{
                "source_channel_key": 1,
                "channel_url": "channel",
                "first_video_status": "pending",
            }],
            [],
            [],
            [],
        ]
        destination = mock.AsyncMock()
        destination.transaction = mock.Mock(return_value=_async_context(None))
        with (
            mock.patch.object(
                archive,
                "_destination_batch_rows",
                new=mock.AsyncMock(return_value=([], [], [], [])),
            ),
            self.assertRaises(archive.ArchiveVerificationError),
        ):
            asyncio.run(
                archive._copy_heavy_snapshot(
                    source,
                    destination,
                    "es",
                    batch_size=50,
                    destructive=True,
                    claim_stale_minutes=60,
                )
            )
        sql = "\n".join(
            call.args[0] for call in source.execute.await_args_list
        ).lower()
        self.assertNotIn("truncate table", sql)

    def test_channel_and_video_upserts_remap_numeric_identity(self) -> None:
        connection = mock.AsyncMock()
        asyncio.run(archive._upsert_channels(
            connection,
            "es",
            [{"channel_url": "channel", "first_video_status": "pending"}],
        ))
        channel_sql = connection.execute.await_args.args[0].lower()
        self.assertIn("on conflict (channel_url)", channel_sql)
        self.assertNotIn("insert into channels_raw_es (\n            id", channel_sql)

        connection.reset_mock()
        asyncio.run(archive._upsert_videos(
            connection,
            "es",
            [{"channel_url": "channel", "video_id": "abcdefghijk"}],
        ))
        video_sql = connection.execute.await_args.args[0].lower()
        self.assertIn("join channels_raw_es channel using (channel_url)", video_sql)
        self.assertIn("channel.id", video_sql)


if __name__ == "__main__":
    unittest.main()
