from __future__ import annotations

import asyncio
import unittest
from datetime import datetime, timezone
from unittest import mock

import db
import ui.db as ui_db
import yt_channel_discovery as discovery
from youtube_oldest_video import FirstVideoMetadata, OldestVideoError


CHANNEL_ID = "UCaaaaaaaaaaaaaaaaaaaaaa"
VIDEO_ID = "abcdefghijk"


class ResolveFirstVideoTests(unittest.TestCase):
    def test_individual_failure_uses_known_id_fallback(self) -> None:
        client = mock.Mock()
        client.fetch_first_video.side_effect = OldestVideoError(
            "player blocked", video_id=VIDEO_ID
        )
        fallback = FirstVideoMetadata(
            VIDEO_ID, "2024-01-01T00:00:00Z", "yt_dlp"
        )
        with mock.patch.object(
            discovery, "fetch_first_video_with_ytdlp", return_value=fallback
        ) as fallback_mock:
            result = discovery.resolve_first_video(
                "https://www.youtube.com/@channel", CHANNEL_ID, client
            )
        self.assertEqual(result["first_video_status"], "success")
        self.assertEqual(result["first_video_source"], "yt_dlp")
        self.assertEqual(fallback_mock.call_args.kwargs["known_video_id"], VIDEO_ID)

    def test_both_failures_remain_pending(self) -> None:
        client = mock.Mock()
        client.fetch_first_video.side_effect = OldestVideoError("browse failed")
        with mock.patch.object(
            discovery,
            "fetch_first_video_with_ytdlp",
            side_effect=OldestVideoError("playlist failed"),
        ):
            result = discovery.resolve_first_video(
                "https://www.youtube.com/@channel", CHANNEL_ID, client
            )
        self.assertEqual(result["first_video_status"], "pending")
        self.assertIn("browse failed", result["first_video_last_error"])
        self.assertIn("playlist failed", result["first_video_last_error"])

    def test_pending_enrichment_does_not_fail_channel_discovery(self) -> None:
        runner = mock.Mock()
        runner.run.side_effect = lambda value: value == "not-processed" and False
        pending = {
            "first_video_status": "pending",
            "first_video_last_error": "temporary error",
        }
        with (
            mock.patch.object(
                discovery,
                "is_channel_processed",
                new=mock.Mock(return_value="not-processed"),
            ),
            mock.patch.object(discovery, "run_ytdlp_channel_dump", return_value={
                "channel_id": CHANNEL_ID,
                "entries": [],
            }),
            mock.patch.object(discovery, "fetch_rss_dates", return_value=({}, None)),
            mock.patch.object(discovery, "resolve_first_video", return_value=pending),
            mock.patch.object(
                discovery,
                "upsert_channel_raw",
                new=mock.Mock(return_value="upsert-channel"),
            ),
            mock.patch.object(
                discovery,
                "upsert_channel_videos_raw",
                new=mock.Mock(return_value="upsert-videos"),
            ),
            mock.patch.object(
                discovery,
                "mark_channel_processed",
                new=mock.Mock(return_value="mark"),
            ),
            mock.patch("builtins.print"),
        ):
            result = discovery.process_one_channel(
                "https://www.youtube.com/@channel",
                runner,
                oldest_video_client=mock.Mock(),
            )
        self.assertEqual(result[1], "processed")
        self.assertEqual(result[2], "pending")


class PersistenceTests(unittest.TestCase):
    def test_success_update_is_atomic_and_clears_claim(self) -> None:
        pool = mock.AsyncMock()
        original = db._DB_POOL
        db._DB_POOL = pool
        try:
            asyncio.run(db.update_first_video_success(
                "channel",
                video_id=VIDEO_ID,
                published_at="2024-01-01T00:00:00Z",
                source="innertube",
            ))
        finally:
            db._DB_POOL = original
        sql = pool.execute.await_args.args[0]
        self.assertIn("first_video_status = 'success'", sql)
        self.assertIn("first_video_claimed_at = NULL", sql)

    def test_claim_query_uses_skip_locked_and_stale_recovery(self) -> None:
        pool = mock.AsyncMock()
        pool.fetch.return_value = [
            {"channel_url": "channel", "channel_id": CHANNEL_ID}
        ]
        original = db._DB_POOL
        db._DB_POOL = pool
        try:
            result = asyncio.run(
                db.claim_channels_for_first_video_enrichment(5)
            )
        finally:
            db._DB_POOL = original
        sql = pool.fetch.await_args.args[0]
        self.assertIn("FOR UPDATE SKIP LOCKED", sql)
        self.assertIn("first_video_status = 'processing'", sql)
        self.assertEqual(result[0]["channel_id"], CHANNEL_ID)

    def test_channel_upsert_writes_new_first_video_columns(self) -> None:
        pool = mock.AsyncMock()
        original = db._DB_POOL
        db._DB_POOL = pool
        try:
            asyncio.run(db.upsert_channel_raw({
                "channel_url": "channel",
                "channel_id": CHANNEL_ID,
                "first_video_id": VIDEO_ID,
                "first_video_published_at": "2024-01-01T00:00:00Z",
                "first_video_status": "success",
                "first_video_source": "innertube",
            }))
        finally:
            db._DB_POOL = original
        args = pool.execute.await_args.args
        self.assertIn("first_video_published_at", args[0])
        self.assertEqual(len(args) - 1, 21)
        self.assertEqual(args[13], VIDEO_ID)
        self.assertIsInstance(args[14], datetime)


class UiDateTests(unittest.TestCase):
    def test_day_boundaries_are_utc_and_before_is_inclusive(self) -> None:
        start = ui_db._utc_day_boundary("2024-01-02")
        end = ui_db._utc_day_boundary("2024-01-02", next_day=True)
        self.assertEqual(start, datetime(2024, 1, 2, tzinfo=timezone.utc))
        self.assertEqual(end, datetime(2024, 1, 3, tzinfo=timezone.utc))

    def test_cursor_round_trips_timestamp(self) -> None:
        row = {
            "channel_url": "channel",
            "first_video_published_at": datetime(
                2024, 1, 2, 3, 4, tzinfo=timezone.utc
            ),
        }
        cursor = ui_db._encode_cursor(
            "first_video_published_at", "ASC", row
        )
        value, channel_url = ui_db._decode_cursor(
            cursor, "first_video_published_at", "ASC"
        )
        self.assertEqual(value, row["first_video_published_at"])
        self.assertEqual(channel_url, "channel")


if __name__ == "__main__":
    unittest.main()
