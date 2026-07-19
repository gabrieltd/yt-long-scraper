from __future__ import annotations

import asyncio
import unittest
from unittest import mock

import yt_discovery as discovery


class DiscoveryOptimizationTests(unittest.TestCase):
    def test_limit_stops_before_any_scroll(self) -> None:
        page = mock.Mock()
        page.evaluate = mock.AsyncMock()

        with mock.patch.object(
            discovery,
            "_result_state",
            new=mock.AsyncMock(return_value={"count": 20, "noMore": False}),
        ):
            count = asyncio.run(discovery._scroll_until_complete(
                page,
                limit=20,
                no_more_message="No more results",
            ))

        self.assertEqual(count, 20)
        page.evaluate.assert_not_awaited()

    def test_scroll_waits_for_growth_and_stops_at_limit(self) -> None:
        page = mock.Mock()
        page.evaluate = mock.AsyncMock()

        with (
            mock.patch.object(
                discovery,
                "_result_state",
                new=mock.AsyncMock(return_value={"count": 10, "noMore": False}),
            ),
            mock.patch.object(
                discovery,
                "_wait_for_result_change",
                new=mock.AsyncMock(return_value={"count": 25, "noMore": False}),
            ) as wait_mock,
        ):
            count = asyncio.run(discovery._scroll_until_complete(
                page,
                limit=20,
                no_more_message="No more results",
            ))

        self.assertEqual(count, 25)
        page.evaluate.assert_awaited_once()
        wait_mock.assert_awaited_once_with(
            page,
            previous_count=10,
            no_more_message="No more results",
        )

    def test_cli_schema_setup_is_enabled_unless_explicitly_skipped(self) -> None:
        self.assertTrue(discovery.parse_args([]).ensure_schema)
        self.assertFalse(discovery.parse_args(["--skip-schema"]).ensure_schema)

    def test_debug_artifacts_are_opt_in(self) -> None:
        self.assertFalse(discovery.parse_args([]).debug_artifacts)
        self.assertTrue(discovery.parse_args(["--debug-artifacts"]).debug_artifacts)


if __name__ == "__main__":
    unittest.main()
