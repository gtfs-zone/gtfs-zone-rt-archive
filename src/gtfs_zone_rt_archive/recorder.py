"""Polls every enabled feed, one thread each, into its store.Writer."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import signal
import threading
import time
from importlib.metadata import version
from typing import TYPE_CHECKING, Any

import httpx

from gtfs_zone_rt_archive.config import FeedsFile, RecorderSettings
from gtfs_zone_rt_archive.heartbeat import Health, Heartbeat
from gtfs_zone_rt_archive.store import Writer
from gtfs_zone_rt_archive.upload import STATIC_LOG, Uploader, s3_store

if TYPE_CHECKING:
    from pathlib import Path
    from types import FrameType

    from gtfs_zone_rt_archive.config import Feed

log = logging.getLogger(__name__)

VERSION = version("gtfs-zone-rt-archive")
USER_AGENT = f"gtfs-zone-rt-archive/{VERSION} (+https://github.com/gtfs-zone/gtfs-zone-rt-archive)"
STATIC_EVERY = 3600.0
HTTP_TIMEOUT = 30.0


def _write_atomic(path: Path, data: bytes) -> None:
    tmp = path.with_name(f".{path.name}.tmp")
    tmp.write_bytes(data)
    os.replace(tmp, path)


def _known_statics(log_path: Path) -> set[str]:
    if not log_path.exists():
        return set()
    known = set()
    for text in log_path.read_text().splitlines():
        try:
            sha8 = json.loads(text).get("sha8")
        except ValueError:
            continue
        if sha8:
            known.add(sha8)
    return known


class FeedRecorder:
    def __init__(
        self,
        name: str,
        feed: Feed,
        data_dir: Path,
        uploader: Uploader,
        health: Health,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self.name = name
        self.feed = feed
        self.dir = data_dir / name
        self.dir.mkdir(parents=True, exist_ok=True)
        self.uploader = uploader
        self.health = health
        self.transport = transport
        self.static_log = self.dir / STATIC_LOG
        self.statics = _known_statics(self.static_log)

    def check_static(self, http: httpx.Client, writer: Writer) -> dict[str, Any]:
        t = time.time()
        try:
            r = http.get(self.feed.static)
            r.raise_for_status()
        except httpx.HTTPError as exc:
            log.error("%s: static failed: %r", self.name, exc)
            entry: dict[str, Any] = {"k": "static", "t": t, "error": repr(exc)}
        else:
            sha8 = hashlib.sha256(r.content).hexdigest()[:8]
            new = sha8 not in self.statics
            if new:
                _write_atomic(self.dir / f"gtfs-{sha8}.zip", r.content)
                self.statics.add(sha8)
                log.info(
                    "%s: new static %s (%d bytes)", self.name, sha8, len(r.content)
                )
            entry = {
                "k": "static",
                "t": t,
                "sha8": sha8,
                "new": new,
                "last_modified": r.headers.get("last-modified"),
            }
        writer.write(entry)
        old = self.static_log.read_bytes() if self.static_log.exists() else b""
        _write_atomic(self.static_log, old + json.dumps(entry).encode() + b"\n")
        self.uploader.wake()
        return entry

    def poll(self, http: httpx.Client, writer: Writer) -> None:
        c = time.time()
        for kind, url in self.feed.rt_urls().items():
            rec: dict[str, Any] = {
                "t": time.time(),
                "feed": kind,
                "method": "GET",
                "url": url,
            }
            try:
                r = http.get(url)
            except httpx.HTTPError as exc:
                log.warning("%s: %s failed: %r", self.name, kind, exc)
                writer.write(
                    writer.raw_line({**rec, "c": c, "error": repr(exc)}, None), at=c
                )
                continue
            writer.write(
                writer.raw_line({**rec, "c": c, "status": r.status_code}, r.content),
                at=c,
            )
            if r.status_code == 200:
                self.health.ok(self.name, kind, rec["t"])
            else:
                log.warning("%s: %s returned %d", self.name, kind, r.status_code)

    def run(self, stop: threading.Event) -> None:
        writer = Writer(self.dir, on_compressed=self.uploader.wake)
        writer.write(
            {
                "k": "start",
                "t": time.time(),
                "feed": self.name,
                "version": VERSION,
                "poll": self.feed.poll,
            }
        )
        next_static = 0.0
        try:
            with httpx.Client(
                timeout=HTTP_TIMEOUT,
                follow_redirects=True,
                headers={"User-Agent": USER_AGENT},
                transport=self.transport,
            ) as http:
                while not stop.is_set():
                    t0 = time.time()
                    try:
                        if t0 >= next_static:
                            self.check_static(http, writer)
                            next_static = t0 + STATIC_EVERY
                        self.poll(http, writer)
                    except Exception:
                        log.exception("%s: poll failed", self.name)
                    stop.wait(max(0.0, self.feed.poll - (time.time() - t0)))
        finally:
            writer.close()


def record() -> None:
    settings = RecorderSettings.from_env()
    feeds = FeedsFile.load(settings.feeds).enabled()
    settings.data_dir.mkdir(parents=True, exist_ok=True)
    uploader = Uploader(s3_store(settings.s3), settings.data_dir)
    for name in feeds:
        uploader.restore_static_log(name)
    health = Health(feeds)
    stop = threading.Event()

    def on_signal(signum: int, _frame: FrameType | None) -> None:
        log.info("signal %d, stopping", signum)
        stop.set()

    signal.signal(signal.SIGTERM, on_signal)
    signal.signal(signal.SIGINT, on_signal)

    log.info("recording %s", ", ".join(feeds) or "no feeds")
    threads = [
        threading.Thread(
            target=FeedRecorder(name, feed, settings.data_dir, uploader, health).run,
            args=(stop,),
            name=f"feed-{name}",
        )
        for name, feed in feeds.items()
    ]
    for th in threads:
        th.start()
    side = [
        threading.Thread(target=uploader.run, args=(stop,), name="upload", daemon=True),
        threading.Thread(
            target=Heartbeat(settings.gatus, health).run,
            args=(stop,),
            name="heartbeat",
            daemon=True,
        ),
    ]
    for th in side:
        th.start()
    for th in threads:
        th.join()
    # Segments closed during shutdown; the open hour stays plain on the volume.
    uploader.sweep()
    log.info("stopped")
