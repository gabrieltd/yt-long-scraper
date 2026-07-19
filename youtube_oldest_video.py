"""Resolve a channel's oldest public long-form video."""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterator

import requests


YOUTUBE_HOME_URL = "https://www.youtube.com"
YOUTUBE_BROWSE_URL = "https://www.youtube.com/youtubei/v1/browse"
YOUTUBE_PLAYER_URL = "https://www.youtube.com/youtubei/v1/player"
VIDEOS_TAB_PARAMS = "EgZ2aWRlb3PyBgQKAjoA"
DEFAULT_TIMEOUT_SECONDS = 20
VIDEO_ID_RE = re.compile(r"^[A-Za-z0-9_-]{11}$")
CHANNEL_ID_RE = re.compile(r"^UC[A-Za-z0-9_-]{22}$")

_GRID_BASE = (
    "contents.twoColumnBrowseResultsRenderer.tabs.tabRenderer.content."
    "richGridRenderer"
)
_CHIP_BASE = f"{_GRID_BASE}.header.chipBarViewModel.chips.chipViewModel"
_DROPDOWN_ITEM_BASE = (
    f"{_CHIP_BASE}.tapCommand.innertubeCommand.showSheetCommand."
    "panelLoadingStrategy.inlineContent.sheetViewModel.content.listViewModel."
    "listItems.listItemViewModel"
)
_LOCKUP_ID_PATH = (
    "richItemRenderer.content.lockupViewModel.rendererContext.commandContext."
    "onTap.innertubeCommand.watchEndpoint.videoId"
)
_VIDEO_RENDERER_ID_PATH = "richItemRenderer.content.videoRenderer.videoId"
_CONTINUATION_PATH = (
    "continuationItemRenderer.continuationEndpoint.continuationCommand.token"
)
TAB_FIELD_MASK = (
    f"{_CHIP_BASE}.text,"
    f"{_CHIP_BASE}.tapCommand.innertubeCommand.continuationCommand.token,"
    f"{_DROPDOWN_ITEM_BASE}.title.content,"
    f"{_DROPDOWN_ITEM_BASE}.rendererContext.commandContext.onTap."
    "innertubeCommand.commandExecutorCommand.commands.continuationCommand.token,"
    f"{_GRID_BASE}.contents.{_LOCKUP_ID_PATH},"
    f"{_GRID_BASE}.contents.{_VIDEO_RENDERER_ID_PATH},"
    f"{_GRID_BASE}.contents.{_CONTINUATION_PATH},"
    "alerts.alertRenderer.text.runs.text,"
    "contents.twoColumnBrowseResultsRenderer.tabs.tabRenderer.content."
    "sectionListRenderer.contents.itemSectionRenderer.contents."
    "messageRenderer.text.runs.text"
)
OLDEST_VIDEO_FIELD_MASK = (
    "onResponseReceivedActions.reloadContinuationItemsCommand."
    f"continuationItems.{_LOCKUP_ID_PATH},"
    "onResponseReceivedActions.reloadContinuationItemsCommand."
    f"continuationItems.{_VIDEO_RENDERER_ID_PATH},"
    "onResponseReceivedActions.appendContinuationItemsAction."
    f"continuationItems.{_LOCKUP_ID_PATH},"
    "onResponseReceivedActions.appendContinuationItemsAction."
    f"continuationItems.{_VIDEO_RENDERER_ID_PATH}"
)
DATE_FIELD_MASK = (
    "microformat.playerMicroformatRenderer.publishDate,"
    "microformat.playerMicroformatRenderer.uploadDate,"
    "playabilityStatus.status,playabilityStatus.reason"
)


class OldestVideoError(RuntimeError):
    """A transient or structural error while resolving the oldest video."""

    def __init__(self, message: str, *, video_id: str | None = None) -> None:
        super().__init__(message)
        self.video_id = video_id


class NoPublicVideosError(OldestVideoError):
    """The channel explicitly has no public entries in its Videos tab."""


@dataclass(frozen=True)
class FirstVideoMetadata:
    video_id: str
    published_at: str
    source: str


def _iter_named_values(value: Any, key: str) -> Iterator[Any]:
    if isinstance(value, dict):
        for current_key, child in value.items():
            if current_key == key:
                yield child
            yield from _iter_named_values(child, key)
    elif isinstance(value, list):
        for child in value:
            yield from _iter_named_values(child, key)


def extract_web_config(html: str) -> dict[str, Any]:
    decoder = json.JSONDecoder()
    config: dict[str, Any] = {}
    position = 0
    marker = "ytcfg.set("
    while True:
        marker_index = html.find(marker, position)
        if marker_index < 0:
            break
        json_start = marker_index + len(marker)
        try:
            parsed, length = decoder.raw_decode(html[json_start:])
        except json.JSONDecodeError:
            position = json_start
            continue
        if isinstance(parsed, dict):
            config.update(parsed)
        position = json_start + length

    missing = [
        key for key in ("INNERTUBE_API_KEY", "INNERTUBE_CLIENT_VERSION")
        if not config.get(key)
    ]
    if missing:
        raise OldestVideoError(
            "YouTube did not provide the required web configuration: "
            + ", ".join(missing)
        )
    return config


def normalize_published_at(value: str | int | float) -> str:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        parsed = datetime.fromtimestamp(value, tz=timezone.utc)
    elif isinstance(value, str) and value.strip():
        normalized = value.strip()
        if normalized.isdigit():
            parsed = datetime.fromtimestamp(int(normalized), tz=timezone.utc)
        else:
            if normalized.endswith("Z"):
                normalized = normalized[:-1] + "+00:00"
            try:
                parsed = datetime.fromisoformat(normalized)
            except ValueError as exc:
                raise OldestVideoError(
                    f"YouTube returned an invalid publication date: {value}"
                ) from exc
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
    else:
        raise OldestVideoError("YouTube did not provide a valid publication date")
    return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


class YouTubeOldestVideoClient:
    """Use YouTube's web client to select the Videos tab's Oldest ordering."""

    def __init__(
        self,
        *,
        api_key: str,
        client_version: str,
        visitor_data: str | None = None,
        timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
        session_factory: Callable[[], requests.Session] = requests.Session,
    ) -> None:
        self.api_key = api_key
        self.client_version = client_version
        self.visitor_data = visitor_data
        self.timeout_seconds = timeout_seconds
        self._session_factory = session_factory
        self._thread_local = threading.local()

    @classmethod
    def initialize(
        cls,
        *,
        timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
        session_factory: Callable[[], requests.Session] = requests.Session,
    ) -> "YouTubeOldestVideoClient":
        session = session_factory()
        cls._configure_session(session)
        try:
            response = session.get(YOUTUBE_HOME_URL, timeout=timeout_seconds)
            response.raise_for_status()
        except requests.RequestException as exc:
            raise OldestVideoError(
                f"Could not initialize the YouTube web client: {exc}"
            ) from exc
        config = extract_web_config(response.text)
        return cls(
            api_key=str(config["INNERTUBE_API_KEY"]),
            client_version=str(config["INNERTUBE_CLIENT_VERSION"]),
            visitor_data=config.get("VISITOR_DATA"),
            timeout_seconds=timeout_seconds,
            session_factory=session_factory,
        )

    @staticmethod
    def _configure_session(session: requests.Session) -> None:
        session.headers.update({
            "User-Agent": "Mozilla/5.0",
            "Accept-Language": "en-US,en;q=0.9",
        })

    def _session(self) -> requests.Session:
        session = getattr(self._thread_local, "session", None)
        if session is None:
            session = self._session_factory()
            self._configure_session(session)
            self._thread_local.session = session
        return session

    def _client_context(self) -> dict[str, str]:
        client = {"clientName": "WEB", "clientVersion": self.client_version}
        if self.visitor_data:
            client["visitorData"] = self.visitor_data
        return client

    def _post(
        self,
        url: str,
        *,
        payload: dict[str, Any],
        field_mask: str,
        stage: str,
    ) -> dict[str, Any]:
        try:
            response = self._session().post(
                url,
                params={"key": self.api_key, "prettyPrint": "false"},
                headers={"X-Goog-FieldMask": field_mask},
                json=payload,
                timeout=self.timeout_seconds,
            )
            response.raise_for_status()
            data = response.json()
        except (requests.RequestException, ValueError) as exc:
            raise OldestVideoError(f"Failed during {stage}: {exc}") from exc
        if not isinstance(data, dict):
            raise OldestVideoError(f"Invalid response during {stage}")
        return data

    def _browse(self, body: dict[str, Any], *, field_mask: str, stage: str) -> dict[str, Any]:
        return self._post(
            YOUTUBE_BROWSE_URL,
            payload={"context": {"client": self._client_context()}, **body},
            field_mask=field_mask,
            stage=stage,
        )

    @staticmethod
    def _oldest_token(data: dict[str, Any]) -> str | None:
        for chip in _iter_named_values(data, "chipViewModel"):
            if isinstance(chip, dict) and chip.get("text") == "Oldest":
                token = next(_iter_named_values(chip, "token"), None)
                if isinstance(token, str) and token:
                    return token
        for item in _iter_named_values(data, "listItemViewModel"):
            if not isinstance(item, dict):
                continue
            title = item.get("title")
            if not isinstance(title, dict) or title.get("content") != "Oldest":
                continue
            token = next(_iter_named_values(item, "token"), None)
            if isinstance(token, str) and token:
                return token
        return None

    @staticmethod
    def _video_ids(data: dict[str, Any]) -> list[str]:
        result: list[str] = []
        for video_id in _iter_named_values(data, "videoId"):
            if isinstance(video_id, str) and VIDEO_ID_RE.fullmatch(video_id):
                if video_id not in result:
                    result.append(video_id)
        return result

    @staticmethod
    def _explicitly_empty(data: dict[str, Any]) -> bool:
        return any(
            list(_iter_named_values(data, key))
            for key in ("alertRenderer", "messageRenderer", "emptyStateViewModel")
        )

    def _oldest_video_id(self, channel_id: str) -> str:
        data = self._browse(
            {"browseId": channel_id, "params": VIDEOS_TAB_PARAMS},
            field_mask=TAB_FIELD_MASK,
            stage="the Videos tab lookup",
        )
        token = self._oldest_token(data)
        if token:
            ordered = self._browse(
                {"continuation": token},
                field_mask=OLDEST_VIDEO_FIELD_MASK,
                stage="the oldest video lookup",
            )
            ids = self._video_ids(ordered)
            if ids:
                return ids[0]
            raise OldestVideoError("YouTube did not provide the oldest video ID")

        initial_ids = self._video_ids(data)
        has_continuation = any(_iter_named_values(data, "continuationItemRenderer"))
        if initial_ids and not has_continuation:
            return initial_ids[-1]
        if has_continuation:
            raise OldestVideoError(
                "YouTube did not provide Oldest and the initial list is paginated"
            )
        if self._explicitly_empty(data):
            raise NoPublicVideosError("The channel has no public videos")
        raise OldestVideoError("YouTube returned no recognizable Videos tab content")

    def _published_at(self, video_id: str) -> str:
        data = self._post(
            YOUTUBE_PLAYER_URL,
            payload={
                "context": {"client": self._client_context()},
                "videoId": video_id,
                "contentCheckOk": True,
                "racyCheckOk": True,
            },
            field_mask=DATE_FIELD_MASK,
            stage="the first video publication date lookup",
        )
        value = next(_iter_named_values(data, "publishDate"), None)
        if value is None:
            value = next(_iter_named_values(data, "uploadDate"), None)
        if value is None:
            reason = next(_iter_named_values(data, "reason"), None)
            suffix = f": {reason}" if isinstance(reason, str) and reason else ""
            raise OldestVideoError(
                "YouTube did not provide the first video publication date" + suffix,
                video_id=video_id,
            )
        try:
            return normalize_published_at(value)
        except OldestVideoError as exc:
            raise OldestVideoError(str(exc), video_id=video_id) from exc

    def fetch_first_video(self, channel_id: str) -> FirstVideoMetadata:
        if not isinstance(channel_id, str) or not CHANNEL_ID_RE.fullmatch(channel_id):
            raise OldestVideoError(f"Invalid channel ID: {channel_id!r}")
        video_id = self._oldest_video_id(channel_id)
        try:
            published_at = self._published_at(video_id)
        except OldestVideoError as exc:
            if exc.video_id is None:
                exc.video_id = video_id
            raise
        return FirstVideoMetadata(video_id, published_at, "innertube")


def _local_ytdlp_env(project_root: Path) -> dict[str, str]:
    local_root = project_root / "yt-dlp" / "yt-dlp"
    if not (local_root / "yt_dlp" / "__main__.py").is_file():
        raise OldestVideoError(f"Vendored yt-dlp is missing under {local_root}")
    env = os.environ.copy()
    existing = env.get("PYTHONPATH")
    env["PYTHONPATH"] = (
        str(local_root) if not existing else os.pathsep.join([str(local_root), existing])
    )
    return env


def fetch_first_video_with_ytdlp(
    channel_url: str,
    *,
    expected_channel_id: str,
    known_video_id: str | None = None,
    timeout_seconds: int = 180,
    project_root: Path | None = None,
) -> FirstVideoMetadata:
    """Fallback lookup. A known ID avoids scanning the full Videos playlist."""
    if not CHANNEL_ID_RE.fullmatch(expected_channel_id or ""):
        raise OldestVideoError(f"Invalid expected channel ID: {expected_channel_id!r}")
    if known_video_id is not None and not VIDEO_ID_RE.fullmatch(known_video_id):
        raise OldestVideoError(f"Invalid known video ID: {known_video_id!r}")

    root = project_root or Path(__file__).resolve().parent
    target = (
        f"https://www.youtube.com/watch?v={known_video_id}"
        if known_video_id
        else channel_url.rstrip("/") + "/videos"
    )
    cmd = [
        sys.executable,
        "-m",
        "yt_dlp",
        "--dump-single-json",
        "--skip-download",
        "--no-warnings",
    ]
    if not known_video_id:
        cmd.extend(["--playlist-items", "-1"])
    cmd.append(target)
    try:
        process = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
            env=_local_ytdlp_env(root),
        )
    except subprocess.TimeoutExpired as exc:
        raise OldestVideoError(f"yt-dlp fallback timed out for {channel_url}") from exc
    if process.returncode != 0:
        detail = (process.stderr or process.stdout or "").strip()
        lowered = detail.lower()
        if "no uploads" in lowered or "does not have a videos tab" in lowered:
            raise NoPublicVideosError("The channel has no public videos")
        raise OldestVideoError(
            f"yt-dlp fallback failed for {channel_url}: {detail[:1000]}"
        )
    try:
        data = json.loads(process.stdout)
    except json.JSONDecodeError as exc:
        raise OldestVideoError("yt-dlp fallback returned invalid JSON") from exc

    entry = data
    entries = data.get("entries") if isinstance(data, dict) else None
    if isinstance(entries, list):
        entry = next((item for item in entries if isinstance(item, dict)), None)
    if not isinstance(entry, dict):
        raise NoPublicVideosError("The channel has no public videos")
    video_id = entry.get("id")
    if not isinstance(video_id, str) or not VIDEO_ID_RE.fullmatch(video_id):
        raise OldestVideoError("yt-dlp fallback did not provide a valid video ID")
    if known_video_id and video_id != known_video_id:
        raise OldestVideoError("yt-dlp fallback returned an unexpected video ID")
    channel_id = entry.get("channel_id") or data.get("channel_id")
    if channel_id and channel_id != expected_channel_id:
        raise OldestVideoError(
            f"yt-dlp fallback returned video {video_id} from another channel"
        )
    published = entry.get("timestamp") or entry.get("upload_date")
    if published is None:
        raise OldestVideoError("yt-dlp fallback did not provide a publication date")
    if isinstance(published, str) and re.fullmatch(r"\d{8}", published):
        published = datetime.strptime(published, "%Y%m%d").replace(
            tzinfo=timezone.utc
        ).isoformat()
    return FirstVideoMetadata(video_id, normalize_published_at(published), "yt_dlp")
