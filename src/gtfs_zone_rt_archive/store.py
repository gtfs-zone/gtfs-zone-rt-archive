"""Hourly zstd JSONL store for one feed's raw recordings.

Layout: <feed_dir>/YYYY-MM-DD/HH.jsonl.zst (UTC). The open segment is plain
HH.jsonl, flushed per line; it is compressed when the next segment starts (or
on the next startup after a crash). A segment whose hour was already
compressed continues as HH.b.jsonl, HH.c.jsonl, ...

Line kinds (`k`):
- start:  {t, feed, version, poll}
- raw:    {t, c, feed, method, url, status, sha, b64 | same} or {..., error}
          b64 is the response body; same means the body equals this URL's
          previous one in the segment. `feed` is vp, tu or al.
- static: {t, sha8, new, last_modified} or {t, error}
`c` is the poll start time; a poll's raw lines share it and its segment.
"""

from __future__ import annotations

import base64
import hashlib
import io
import json
import logging
import threading
import time
from datetime import UTC, datetime
from typing import IO, TYPE_CHECKING, Any

import zstandard

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator
    from pathlib import Path

log = logging.getLogger(__name__)

SEGMENT = 3600
_PARAMS = zstandard.ZstdCompressionParameters.from_level(
    19, window_log=27, enable_ldm=True, write_checksum=True
)
_MAX_WINDOW = 2**27


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()[:16]


def _segment_path(feed_dir: Path, t: float, segment: int = SEGMENT) -> Path:
    start = datetime.fromtimestamp(t - t % segment, UTC)
    name = start.strftime("%H" if segment >= 3600 else "%H%M%S")
    return feed_dir / start.strftime("%Y-%m-%d") / f"{name}.jsonl"


def _stem(p: Path) -> str:
    """HH, or HH.b for a second segment of the same hour."""
    return p.name.removesuffix(".zst").removesuffix(".jsonl")


def _free_plain(base: Path) -> Path:
    """The plain file to append to for segment `base`, skipping compressed ones."""
    for suffix in ["", *(f".{c}" for c in "bcdefghijklmnopqrstuvwxyz")]:
        p = base.with_name(f"{_stem(base)}{suffix}.jsonl")
        if p.exists() or not p.with_suffix(".jsonl.zst").exists():
            return p
    raise RuntimeError(f"no free segment name for {base}")


def _ends_newline(p: Path) -> bool:
    with p.open("rb") as fh:
        fh.seek(-1, 2)
        return fh.read(1) == b"\n"


def compress(plain: Path) -> Path:
    """Plain .jsonl to .jsonl.zst, verified by read-back; the plain file is removed."""
    data = plain.read_bytes()
    out = plain.with_suffix(".jsonl.zst")
    tmp = plain.with_suffix(".jsonl.zst.tmp")
    tmp.write_bytes(zstandard.ZstdCompressor(compression_params=_PARAMS).compress(data))
    back = zstandard.ZstdDecompressor(max_window_size=_MAX_WINDOW).decompress(
        tmp.read_bytes()
    )
    if back != data:
        tmp.unlink()
        raise RuntimeError(f"zstd read-back mismatch for {plain}")
    tmp.rename(out)
    plain.unlink()
    log.info("compressed %s: %d -> %d bytes", plain, len(data), out.stat().st_size)
    return out


class Writer:
    """Appends lines to the open segment and compresses each closed one.

    `on_compressed` is called with each new .jsonl.zst, from the thread that
    compressed it.
    """

    def __init__(
        self,
        feed_dir: Path,
        segment: int = SEGMENT,
        background: bool = True,
        on_compressed: Callable[[Path], None] | None = None,
    ) -> None:
        self.dir = feed_dir
        self.segment = segment
        self.background = background
        self.on_compressed = on_compressed
        self._path: Path | None = None
        self._start: float | None = None
        self._fh: IO[str] | None = None
        self._last_raw: dict[str, str] = {}
        self._threads: list[threading.Thread] = []
        self.compress_stale()

    def _compress(self, plain: Path) -> None:
        try:
            out = compress(plain)
        except Exception:
            # The plain file stays and the next startup retries it.
            log.exception("compressing %s failed", plain)
            return
        if self.on_compressed:
            self.on_compressed(out)

    def compress_stale(self) -> None:
        """Compress every plain segment except those of the current segment."""
        current = _segment_path(self.dir, time.time(), self.segment)
        for plain in sorted(self.dir.glob("*/*.jsonl")):
            if plain.parent == current.parent and _stem(plain).split(".")[0] == _stem(
                current
            ):
                continue
            if plain.with_suffix(".jsonl.zst").exists():
                plain.unlink()  # crashed between rename and unlink
            else:
                self._compress(plain)
        for tmp in self.dir.glob("*/*.zst.tmp"):
            tmp.unlink()

    def _open(self, t: float) -> IO[str]:
        # Segments only move forward, so a line timed just before the open
        # segment (e.g. a poll's start) lands in it rather than reopening the last.
        start = t - t % self.segment
        if self._fh is not None and self._start is not None and start <= self._start:
            return self._fh
        self._start = start
        path = _free_plain(_segment_path(self.dir, t, self.segment))
        old = self._path
        if self._fh:
            self._fh.close()
        path.parent.mkdir(parents=True, exist_ok=True)
        torn = path.exists() and path.stat().st_size and not _ends_newline(path)
        self._path = path
        self._fh = fh = path.open("a")
        if torn:
            fh.write("\n")  # end a line cut off by a crash
        self._last_raw = {}
        if old is not None and old.exists():
            if self.background:
                th = threading.Thread(target=self._compress, args=(old,))
                th.start()
                self._threads.append(th)
            else:
                self._compress(old)
        return fh

    def write(self, line: dict[str, Any], at: float | None = None) -> None:
        """One line, in the segment of `at` (default line["t"])."""
        fh = self._open(line["t"] if at is None else at)
        fh.write(json.dumps(line, separators=(",", ":"), default=str) + "\n")
        fh.flush()

    def raw_line(self, rec: dict[str, Any], content: bytes | None) -> dict[str, Any]:
        """Fill sha + b64/same on a raw record, deduping per URL within its segment.

        Opens the segment of rec["c"] (else rec["t"]) first, so write the line
        right after with the same `at`.
        """
        self._open(rec.get("c", rec["t"]))
        line = {"k": "raw", **rec}
        if content is None:
            return line
        sha = _sha(content)
        line["sha"] = sha
        if self._last_raw.get(rec["url"]) == sha:
            line["same"] = True
        else:
            line["b64"] = base64.b64encode(content).decode()
        self._last_raw[rec["url"]] = sha
        return line

    def close(self, compress_open: bool = False) -> None:
        """Close the open segment, left plain so a restart in the hour resumes it."""
        if self._fh:
            self._fh.close()
            self._fh = None
            if compress_open and self._path is not None:
                self._compress(self._path)
        for th in self._threads:
            th.join()
        self._threads = []


def segments(feed_dir: Path) -> list[Path]:
    """Every segment file in time order, .zst preferred over a leftover plain twin."""
    out: dict[Path, Path] = {}
    for p in sorted(feed_dir.glob("*/*.jsonl*")):
        if p.name.endswith(".tmp"):
            continue
        stem = p.parent / _stem(p)
        if stem not in out or p.suffix == ".zst":
            out[stem] = p
    return [out[k] for k in sorted(out)]


def _read(path: Path) -> Iterator[str]:
    if path.suffix == ".zst":
        dctx = zstandard.ZstdDecompressor(max_window_size=_MAX_WINDOW)
        with path.open("rb") as fh, dctx.stream_reader(fh) as r:
            yield from io.TextIOWrapper(r, encoding="utf-8")
    else:
        with path.open() as fh:
            yield from fh


def read_segment(path: Path) -> Iterator[dict[str, Any]]:
    """A segment's lines with deduped raw bodies filled back in."""
    last_b64: dict[str, str] = {}
    for text in _read(path):
        try:
            line = json.loads(text)
        except ValueError:
            continue  # a line cut off by a crash
        if line.get("k") == "raw":
            if line.pop("same", False):
                line["b64"] = last_b64[line["url"]]
            elif "b64" in line:
                last_b64[line["url"]] = line["b64"]
        yield line


def lines(feed_dir: Path) -> Iterator[dict[str, Any]]:
    """Every line of every segment, in time order."""
    for path in segments(feed_dir):
        yield from read_segment(path)
