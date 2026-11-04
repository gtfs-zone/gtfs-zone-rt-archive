"""Uploads closed segments, static zips and static.jsonl to the raw bucket.

The local data dir is the queue: every sweep uploads each feed's
YYYY-MM-DD/*.jsonl.zst and gtfs-*.zip, then deletes the local copy once the
upload is verified. Failures stay on disk and the next sweep retries them.
static.jsonl is kept locally and re-uploaded whole whenever it changed.
"""

from __future__ import annotations

import hashlib
import logging
import re
import threading
from typing import TYPE_CHECKING

import obstore

if TYPE_CHECKING:
    from pathlib import Path

    from obstore.store import ObjectStore

    from gtfs_zone_rt_archive.config import S3Settings

log = logging.getLogger(__name__)

PREFIX = "raw"
RETRY_EVERY = 60.0
STATIC_LOG = "static.jsonl"
_MD5 = re.compile(r"^[0-9a-f]{32}$")
_SUFFIXES = ["", *(f".{c}" for c in "bcdefghijklmnopqrstuvwxyz")]
_TYPES = {
    ".zst": "application/zstd",
    ".zip": "application/zip",
    ".jsonl": "application/x-ndjson",
}


def s3_store(s3: S3Settings) -> ObjectStore:
    from obstore.store import S3Store

    return S3Store(
        s3.bucket,
        endpoint=s3.endpoint,
        region=s3.region,
        access_key_id=s3.access_key,
        secret_access_key=s3.secret_key,
        virtual_hosted_style_request=False,
        client_options={
            "allow_http": s3.endpoint.startswith("http://"),
            "timeout": "120s",
        },
    )


def _md5(data: bytes) -> str:
    return hashlib.md5(data, usedforsecurity=False).hexdigest()


def _remote_equals(store: ObjectStore, key: str, data: bytes) -> bool | None:
    """Whether `key` holds `data`: True, False (other bytes) or None (absent)."""
    try:
        meta = obstore.head(store, key)
    except FileNotFoundError:
        return None
    if meta["size"] != len(data):
        return False
    etag = (meta.get("e_tag") or "").strip('"')
    if _MD5.match(etag):
        return etag == _md5(data)
    return bytes(obstore.get(store, key).bytes()) == data


def _segment_keys(key: str) -> list[str]:
    """`key` then its .b, .c, ... twins, for a segment whose name is already taken."""
    head, name = key.rsplit("/", 1)
    stem = name.removesuffix(".jsonl.zst")
    hour, _, suffix = stem.partition(".")
    start = _SUFFIXES.index(f".{suffix}" if suffix else "")
    return [f"{head}/{hour}{s}.jsonl.zst" for s in _SUFFIXES[start:]]


class Uploader:
    def __init__(self, store: ObjectStore, data_dir: Path) -> None:
        self.store = store
        self.data_dir = data_dir
        self._wake = threading.Event()
        self._lock = threading.Lock()
        self._static_md5: dict[str, str] = {}

    def key(self, path: Path) -> str:
        return f"{PREFIX}/{path.relative_to(self.data_dir).as_posix()}"

    def wake(self, _path: Path | None = None) -> None:
        """Start a sweep now; usable as a Writer's on_compressed hook."""
        self._wake.set()

    def _put(self, key: str, data: bytes) -> None:
        content_type = next(
            (t for ext, t in _TYPES.items() if key.endswith(ext)),
            "application/octet-stream",
        )
        obstore.put(
            self.store,
            key,
            data,
            use_multipart=False,
            attributes={"Content-Type": content_type},
        )
        if not _remote_equals(self.store, key, data):
            raise RuntimeError(f"upload of {key} did not verify")

    def upload_segment(self, path: Path) -> str:
        """Upload a closed segment and delete it locally; returns the key used.

        A key already holding the same bytes (a crash after upload) counts as
        done. One holding other bytes is never overwritten: the segment goes to
        the next free HH.b, HH.c, ... name.
        """
        data = path.read_bytes()
        for key in _segment_keys(self.key(path)):
            same = _remote_equals(self.store, key, data)
            if same is False:
                continue
            if same is None:
                self._put(key, data)
            path.unlink()
            log.info("uploaded %s (%d bytes)", key, len(data))
            return key
        raise RuntimeError(f"no free key for {path}")

    def upload_static_zip(self, path: Path) -> None:
        """Upload gtfs-<sha8>.zip (content-addressed, so an existing key is kept)."""
        key = self.key(path)
        data = path.read_bytes()
        if _remote_equals(self.store, key, data) is None:
            self._put(key, data)
            log.info("uploaded %s (%d bytes)", key, len(data))
        path.unlink()

    def upload_static_log(self, path: Path) -> None:
        data = path.read_bytes()
        md5 = _md5(data)
        if self._static_md5.get(str(path)) == md5:
            return
        self._put(self.key(path), data)
        self._static_md5[str(path)] = md5

    def restore_static_log(self, feed: str) -> None:
        """Fetch the remote static.jsonl when the local one is missing (new volume)."""
        path = self.data_dir / feed / STATIC_LOG
        if path.exists():
            return
        try:
            data = bytes(obstore.get(self.store, self.key(path)).bytes())
        except FileNotFoundError:
            return
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
        self._static_md5[str(path)] = _md5(data)
        log.info("restored %s from the bucket", path)

    def sweep(self) -> int:
        """One pass over every feed dir; returns the number of failures."""
        failures = 0
        with self._lock:
            jobs = [
                *(
                    (self.upload_segment, p)
                    for p in self.data_dir.glob("*/*/*.jsonl.zst")
                ),
                *(
                    (self.upload_static_zip, p)
                    for p in self.data_dir.glob("*/gtfs-*.zip")
                ),
                *(
                    (self.upload_static_log, p)
                    for p in self.data_dir.glob(f"*/{STATIC_LOG}")
                ),
            ]
            for fn, path in sorted(jobs, key=lambda j: str(j[1])):
                try:
                    fn(path)
                except Exception as exc:
                    failures += 1
                    log.warning("upload of %s failed, will retry: %r", path, exc)
        return failures

    def run(self, stop: threading.Event) -> None:
        """Sweep on each wake() and every RETRY_EVERY seconds until `stop`."""
        while not stop.is_set():
            self._wake.clear()
            self.sweep()
            self._wake.wait(RETRY_EVERY)
