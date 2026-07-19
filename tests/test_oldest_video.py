from __future__ import annotations

import json
import subprocess
import unittest
from pathlib import Path
from unittest import mock

import youtube_oldest_video as oldest


CHANNEL_ID = "UCaaaaaaaaaaaaaaaaaaaaaa"
VIDEO_ID = "abcdefghijk"


def _lockup(video_id: str) -> dict:
    return {
        "richItemRenderer": {
            "content": {
                "lockupViewModel": {
                    "rendererContext": {
                        "commandContext": {
                            "onTap": {
                                "innertubeCommand": {
                                    "watchEndpoint": {"videoId": video_id}
                                }
                            }
                        }
                    }
                }
            }
        }
    }


class OldestVideoClientTests(unittest.TestCase):
    def setUp(self) -> None:
        self.client = oldest.YouTubeOldestVideoClient(
            api_key="key",
            client_version="1.0",
        )

    def test_extract_web_config_and_normalize_offset(self) -> None:
        config = oldest.extract_web_config(
            '<script>ytcfg.set({"INNERTUBE_API_KEY":"k",'
            '"INNERTUBE_CLIENT_VERSION":"v"});</script>'
        )
        self.assertEqual(config["INNERTUBE_API_KEY"], "k")
        self.assertEqual(
            oldest.normalize_published_at("2024-03-25T13:34:15-07:00"),
            "2024-03-25T20:34:15Z",
        )

    def test_direct_and_nested_oldest_tokens(self) -> None:
        direct = {
            "chipViewModel": {
                "text": "Oldest",
                "continuationCommand": {"token": "direct-token"},
            }
        }
        nested = {
            "listItemViewModel": {
                "title": {"content": "Oldest"},
                "continuationCommand": {"token": "nested-token"},
            }
        }
        self.assertEqual(self.client._oldest_token(direct), "direct-token")
        self.assertEqual(self.client._oldest_token(nested), "nested-token")

    def test_fetch_uses_oldest_token_and_lockup_renderer(self) -> None:
        tab = {
            "chipViewModel": {
                "text": "Oldest",
                "continuationCommand": {"token": "token"},
            }
        }
        ordered = {"continuationItems": [_lockup(VIDEO_ID)]}
        with (
            mock.patch.object(self.client, "_browse", side_effect=[tab, ordered]),
            mock.patch.object(
                self.client,
                "_published_at",
                return_value="2024-01-01T00:00:00Z",
            ),
        ):
            result = self.client.fetch_first_video(CHANNEL_ID)
        self.assertEqual(result.video_id, VIDEO_ID)
        self.assertEqual(result.source, "innertube")

    def test_small_channel_uses_last_initial_video(self) -> None:
        tab = {"contents": [_lockup("12345678901"), _lockup(VIDEO_ID)]}
        with (
            mock.patch.object(self.client, "_browse", return_value=tab),
            mock.patch.object(
                self.client,
                "_published_at",
                return_value="2024-01-01T00:00:00Z",
            ),
        ):
            result = self.client.fetch_first_video(CHANNEL_ID)
        self.assertEqual(result.video_id, VIDEO_ID)

    def test_missing_oldest_with_pagination_is_error(self) -> None:
        tab = {
            "contents": [
                _lockup(VIDEO_ID),
                {"continuationItemRenderer": {"continuationCommand": {"token": "x"}}},
            ]
        }
        with mock.patch.object(self.client, "_browse", return_value=tab):
            with self.assertRaises(oldest.OldestVideoError):
                self.client.fetch_first_video(CHANNEL_ID)

    def test_explicit_empty_state_is_terminal(self) -> None:
        with mock.patch.object(
            self.client,
            "_browse",
            return_value={"alertRenderer": {"text": "No videos"}},
        ):
            with self.assertRaises(oldest.NoPublicVideosError):
                self.client.fetch_first_video(CHANNEL_ID)


class YtDlpFallbackTests(unittest.TestCase):
    def _completed(self, payload: dict) -> subprocess.CompletedProcess:
        return subprocess.CompletedProcess(
            args=[], returncode=0, stdout=json.dumps(payload), stderr=""
        )

    def test_known_id_fallback_extracts_only_video(self) -> None:
        payload = {
            "id": VIDEO_ID,
            "channel_id": CHANNEL_ID,
            "timestamp": 1704067200,
        }
        with (
            mock.patch.object(oldest, "_local_ytdlp_env", return_value={}),
            mock.patch.object(
                oldest.subprocess, "run", return_value=self._completed(payload)
            ) as run_mock,
        ):
            result = oldest.fetch_first_video_with_ytdlp(
                "https://www.youtube.com/@channel",
                expected_channel_id=CHANNEL_ID,
                known_video_id=VIDEO_ID,
                project_root=Path("."),
            )
        command = run_mock.call_args.args[0]
        self.assertNotIn("--playlist-items", command)
        self.assertEqual(result.published_at, "2024-01-01T00:00:00Z")

    def test_unknown_id_fallback_selects_last_playlist_item(self) -> None:
        payload = {
            "channel_id": CHANNEL_ID,
            "entries": [{
                "id": VIDEO_ID,
                "channel_id": CHANNEL_ID,
                "upload_date": "20200102",
            }],
        }
        with (
            mock.patch.object(oldest, "_local_ytdlp_env", return_value={}),
            mock.patch.object(
                oldest.subprocess, "run", return_value=self._completed(payload)
            ) as run_mock,
        ):
            result = oldest.fetch_first_video_with_ytdlp(
                "https://www.youtube.com/@channel",
                expected_channel_id=CHANNEL_ID,
                project_root=Path("."),
            )
        command = run_mock.call_args.args[0]
        self.assertEqual(command[command.index("--playlist-items") + 1], "-1")
        self.assertEqual(result.published_at, "2020-01-02T00:00:00Z")


if __name__ == "__main__":
    unittest.main()
