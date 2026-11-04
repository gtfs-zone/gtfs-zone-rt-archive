"""Recorder health: last good poll per feed and kind, pushed to Gatus once a minute."""

from __future__ import annotations

import logging
import threading
import time
from typing import TYPE_CHECKING

import httpx

if TYPE_CHECKING:
    from collections.abc import Iterable

    from gtfs_zone_rt_archive.config import GatusSettings

log = logging.getLogger(__name__)

PUSH_INTERVAL = 60.0
# A feed is healthy when both of these had a 200 within MAX_AGE seconds.
REQUIRED = ("vp", "tu")
MAX_AGE = 120.0


class Health:
    def __init__(self, feeds: Iterable[str]) -> None:
        self.feeds = list(feeds)
        self._ok: dict[tuple[str, str], float] = {}
        self._lock = threading.Lock()

    def ok(self, feed: str, kind: str, t: float) -> None:
        with self._lock:
            self._ok[(feed, kind)] = t

    def failing(self, now: float) -> list[str]:
        """`feed:kind` for every required poll without a recent 200."""
        with self._lock:
            return [
                f"{feed}:{kind}"
                for feed in self.feeds
                for kind in REQUIRED
                if now - self._ok.get((feed, kind), float("-inf")) > MAX_AGE
            ]


class Heartbeat:
    """Pushes Health to a Gatus external endpoint. Never raises."""

    def __init__(self, gatus: GatusSettings, health: Health) -> None:
        self.health = health
        self._token = gatus.token
        self._url = (
            f"{gatus.url.rstrip('/')}/api/v1/endpoints/{gatus.endpoint_key}/external"
            if gatus.url and gatus.endpoint_key
            else None
        )
        if self._url is None:
            log.info("GATUS_URL or GATUS_ENDPOINT_KEY unset; not pushing heartbeats")

    def push(self, http: httpx.Client, now: float) -> None:
        if self._url is None:
            return
        failing = self.health.failing(now)
        params = {"success": "false" if failing else "true"}
        if failing:
            params["error"] = ("no recent 200: " + ", ".join(failing))[:500]
        try:
            http.post(
                self._url,
                params=params,
                headers={"Authorization": f"Bearer {self._token}"},
            ).raise_for_status()
        except httpx.HTTPError as exc:
            log.warning("gatus heartbeat push failed: %r", exc)

    def run(self, stop: threading.Event) -> None:
        with httpx.Client(timeout=10) as http:
            while not stop.wait(PUSH_INTERVAL):
                self.push(http, time.time())
