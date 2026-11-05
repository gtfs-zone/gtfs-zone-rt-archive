"""Reads one feed's raw recordings for a time window back from the raw bucket."""

from __future__ import annotations

import base64
import io
import logging
import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import obstore
from google.protobuf.message import DecodeError
from google.transit import gtfs_realtime_pb2

from gtfs_zone_rt_archive.store import parse_segment
from gtfs_zone_rt_archive.upload import PREFIX, STATIC_LOG

if TYPE_CHECKING:
    from collections.abc import Iterator
    from typing import Any

    from obstore.store import ObjectStore

log = logging.getLogger(__name__)

# Good polls further apart than this split the coverage.
MAX_GAP = 120.0
_SEGMENT = re.compile(r"/(\d{4}-\d{2}-\d{2})/(\d{2})((?:\.[b-z])?)\.jsonl\.zst$")

FeedMessage = gtfs_realtime_pb2.FeedMessage


@dataclass
class Poll:
    c: float
    vp: FeedMessage | None = None
    tu: FeedMessage | None = None
    # End of the previous coverage interval, when a gap or a restart came first.
    resumed_after: float | None = None

    @property
    def good(self) -> bool:
        return self.vp is not None and self.tu is not None


def get_bytes(store: ObjectStore, key: str) -> bytes | None:
    try:
        return bytes(obstore.get(store, key).bytes())
    except FileNotFoundError:
        return None


def static_log(store: ObjectStore, feed: str) -> bytes:
    return get_bytes(store, f"{PREFIX}/{feed}/{STATIC_LOG}") or b""


def static_zip(store: ObjectStore, feed: str, sha8: str) -> bytes | None:
    return get_bytes(store, f"{PREFIX}/{feed}/gtfs-{sha8}.zip")


def segment_keys(store: ObjectStore, feed: str, start: float, end: float) -> list[str]:
    """Keys of the hour segments overlapping [start, end), in time order."""
    found: list[tuple[str, str, str, str]] = []
    day = datetime.fromtimestamp(start, UTC).date()
    last = datetime.fromtimestamp(end, UTC).date()
    while day <= last:
        prefix = f"{PREFIX}/{feed}/{day.isoformat()}/"
        for batch in obstore.list(store, prefix=prefix):
            for meta in batch:
                m = _SEGMENT.search(meta["path"])
                if not m:
                    continue
                hour = datetime.fromisoformat(f"{m[1]}T{m[2]}:00:00+00:00").timestamp()
                if hour < end and hour + 3600 > start:
                    found.append((m[1], m[2], m[3], meta["path"]))
        day += timedelta(days=1)
    return [key for *_, key in sorted(found)]


class Recording:
    """Polls of one feed in [start, end) and the intervals it was recorded.

    `coverage` is filled while `polls()` is consumed: runs of good polls (VP
    and TU both decoded) with no gap over MAX_GAP and no restart in between.
    """

    def __init__(self, store: ObjectStore, feed: str, start: float, end: float) -> None:
        self.store = store
        self.feed = feed
        self.start = start
        self.end = end
        self.coverage: list[tuple[float, float]] = []
        self._decoded: dict[str, tuple[str, FeedMessage]] = {}

    def _decode(self, line: dict[str, Any]) -> FeedMessage | None:
        if line.get("status") != 200 or "b64" not in line:
            return None
        kind = line["feed"]
        cached = self._decoded.get(kind)
        if cached and cached[0] == line.get("sha"):
            return cached[1]
        msg = FeedMessage()
        try:
            msg.ParseFromString(base64.b64decode(line["b64"]))
        except DecodeError:
            log.warning("%s: undecodable %s body at %s", self.feed, kind, line["c"])
            return None
        self._decoded[kind] = (line.get("sha", ""), msg)
        return msg

    def _lines(self) -> Iterator[dict[str, Any]]:
        for key in segment_keys(self.store, self.feed, self.start, self.end):
            data = get_bytes(self.store, key)
            if data is not None:
                yield from parse_segment(io.BytesIO(data))

    def polls(self) -> Iterator[Poll]:
        poll: Poll | None = None
        interval: list[float] | None = None
        restarted = False

        def flush(p: Poll) -> Poll:
            nonlocal interval, restarted
            if not p.good:
                return p
            if interval and not restarted and p.c - interval[1] <= MAX_GAP:
                interval[1] = p.c
            else:
                if interval:
                    self._close(interval)
                    p.resumed_after = interval[1]
                interval = [p.c, p.c]
            restarted = False
            return p

        for line in self._lines():
            kind = line.get("k")
            if kind == "start":
                if poll is not None:
                    yield flush(poll)
                    poll = None
                restarted = True
                continue
            if kind != "raw" or not (self.start <= line["c"] < self.end):
                continue
            if poll is None or line["c"] != poll.c:
                if poll is not None:
                    yield flush(poll)
                poll = Poll(line["c"])
            if line["feed"] in ("vp", "tu"):
                setattr(poll, line["feed"], self._decode(line))
        if poll is not None:
            yield flush(poll)
        if interval:
            self._close(interval)

    def _close(self, interval: list[float]) -> None:
        self.coverage.append((max(interval[0], self.start), min(interval[1], self.end)))
