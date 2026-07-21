from __future__ import annotations

import asyncio
import contextlib
import io
import unittest
from datetime import date
from unittest import mock

from scripts import purge_unmarked_channel_data as purge
from scripts import report_db_storage as storage
import ui.db as ui_db


class _Acquire:
    def __init__(self, connection):
        self.connection = connection

    async def __aenter__(self):
        return self.connection

    async def __aexit__(self, *_args):
        return None


class _Pool:
    def __init__(self, connection):
        self.connection = connection

    def acquire(self):
        return _Acquire(self.connection)


class CompactUiTests(unittest.TestCase):
    def test_detail_derives_urls_and_compact_dates(self) -> None:
        connection = mock.AsyncMock()
        connection.fetchrow.return_value = {
            "channel_key": 42,
            "channel_url": "https://www.youtube.com/@example",
            "channel_id": "UCaaaaaaaaaaaaaaaaaaaaaa",
            "channel_name": "Example",
            "subscriber_count": 10,
            "is_verified": False,
            "channel_description": None,
            "channel_tags": None,
            "avatar_url": None,
            "banner_url": None,
            "uploader_id": None,
            "uploader_url": None,
            "last_upload_date": date(2026, 7, 20),
            "first_video_id": "abcdefghijk",
            "first_video_published_at": None,
            "first_video_status": "pending",
            "total_videos_tracked": 1,
            "hit_videos_count": 1,
            "avg_views_on_channel": 25,
            "max_views_on_channel": 25,
            "is_relevant": None,
            "notes": None,
            "tags": None,
        }
        connection.fetch.return_value = [{
            "video_id": "abcdefghijk",
            "title": "Video",
            "upload_date": date(2026, 7, 19),
            "duration_seconds": 60,
            "view_count": 25,
        }]
        original = ui_db._pool
        ui_db._pool = _Pool(connection)
        try:
            result = asyncio.run(ui_db.get_channel_details(
                "es", "https://www.youtube.com/@example"
            ))
        finally:
            ui_db._pool = original

        self.assertEqual(result["channel"]["last_upload_date"], "20260720")
        self.assertEqual(result["videos"][0]["upload_date"], "20260719")
        self.assertEqual(
            result["videos"][0]["video_url"],
            "https://www.youtube.com/watch?v=abcdefghijk",
        )
        self.assertEqual(
            result["videos"][0]["thumbnail_url"],
            "https://i.ytimg.com/vi/abcdefghijk/hqdefault.jpg",
        )
        channel_sql = connection.fetchrow.await_args.args[0]
        videos_sql, channel_key = connection.fetch.await_args.args
        self.assertIn("stats.channel_key = cr.id", channel_sql)
        self.assertIn("rel.channel_key = cr.id", channel_sql)
        self.assertIn("WHERE channel_key = $1", videos_sql)
        self.assertNotIn("video_url", videos_sql)
        self.assertEqual(channel_key, 42)

    def test_last_upload_cursor_round_trips_as_date(self) -> None:
        row = {
            "channel_url": "channel",
            "last_upload_date": date(2026, 7, 20),
        }
        cursor = ui_db._encode_cursor("last_upload_date", "DESC", row)
        value, channel_url = ui_db._decode_cursor(
            cursor, "last_upload_date", "DESC"
        )
        self.assertEqual(value, date(2026, 7, 20))
        self.assertEqual(channel_url, "channel")

    def test_last_upload_filter_accepts_compact_and_iso_dates(self) -> None:
        expected = date(2026, 7, 20)
        self.assertEqual(ui_db._parse_date("20260720"), expected)
        self.assertEqual(ui_db._parse_date("2026-07-20"), expected)

    def test_bulk_relevance_resolves_numeric_channel_keys(self) -> None:
        pool = mock.AsyncMock()
        original = ui_db._pool
        ui_db._pool = pool
        try:
            asyncio.run(ui_db.set_channels_relevance_bulk(
                "en", ["channel-a", "channel-b"], is_relevant=True
            ))
        finally:
            ui_db._pool = original
        sql, urls, relevance = pool.execute.await_args.args
        self.assertIn("channel_key", sql)
        self.assertIn("JOIN channels_raw_en", sql)
        self.assertEqual(urls, ["channel-a", "channel-b"])
        self.assertIs(relevance, True)


class CompactPurgeTests(unittest.TestCase):
    def test_purge_uses_numeric_relevance_and_physical_staging(self) -> None:
        connection = mock.AsyncMock()
        connection.transaction = mock.Mock(return_value=mock.MagicMock(
            __aenter__=mock.AsyncMock(return_value=None),
            __aexit__=mock.AsyncMock(return_value=None),
        ))
        asyncio.run(purge.purge_unmarked_data(connection, "es"))
        statements = "\n".join(call.args[0] for call in connection.execute.await_args_list)
        self.assertIn("rel.channel_key = cr.id", statements)
        self.assertIn("discovery_videos_staging_es", statements)
        self.assertIn("channel_candidates_es", statements)
        self.assertNotIn("TRUNCATE TABLE videos_normalized_es", statements)
        self.assertNotIn("TRUNCATE TABLE videos_raw_es", statements)


class StorageReportTests(unittest.TestCase):
    def test_storage_report_can_replace_estimates_with_exact_counts(self) -> None:
        connection = mock.AsyncMock()
        connection.fetch.return_value = [{
            "language": "es",
            "table_name": "channels_raw_es",
            "rows": 7,
            "heap_bytes": 8192,
            "index_bytes": 16384,
            "toast_bytes": 0,
            "total_bytes": 24576,
        }]
        connection.fetchval.return_value = 9
        rows = asyncio.run(storage.collect_report(
            connection, ["es"], exact_rows=True
        ))
        self.assertEqual(rows[0].rows, 9)
        self.assertIn('"channels_raw_es"', connection.fetchval.await_args.args[0])

        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            storage.print_report(rows, exact_rows=True)
        self.assertIn("heap", output.getvalue())
        self.assertIn("ES total", output.getvalue())
        self.assertEqual(storage._human_size(1024), "1.0 KiB")


if __name__ == "__main__":
    unittest.main()
