"""Builds one feed's service day and publishes it to the TIDES bucket.

Layout: tides/<feed>/<date>/{datapackage.json, <table>.csv} with the CSVs
stored gzipped (Content-Encoding: gzip), gtfs/<feed>/<sha8>.zip for the
snapshot a package was built against, and index.json listing every package.
"""

from __future__ import annotations

import csv
import gzip
import io
import json
import logging
import time
from datetime import date, timedelta
from importlib.metadata import version
from typing import TYPE_CHECKING, Any
from zoneinfo import ZoneInfo

import obstore

from gtfs_zone_rt_archive import raw
from gtfs_zone_rt_archive.gtfs import Gtfs, local_noon, pick_static
from gtfs_zone_rt_archive.tides import TABLES, DayBuilder, columns, iso

if TYPE_CHECKING:
    from obstore.store import ObjectStore

    from gtfs_zone_rt_archive.config import Feed

log = logging.getLogger(__name__)

VERSION = version("gtfs-zone-rt-archive")
BASE_URL = "https://archive.rt.gtfs.zone"
# The gtfs-zone/predictions commit the vendored schemas were copied from.
TIDES_REF = "5a28cac520840de06f59479f8f098825b4f348e5"
TIDES_SPEC = f"https://raw.githubusercontent.com/gtfs-zone/TIDES/{TIDES_REF}/spec"
INDEX = "index.json"
PACKAGE_CACHE = "public, max-age=86400"
INDEX_CACHE = "public, max-age=600"


def package_prefix(feed: str, d: date) -> str:
    return f"tides/{feed}/{d.isoformat()}"


def csv_bytes(table: str, rows: list[dict[str, Any]]) -> bytes:
    out = io.StringIO()
    w = csv.DictWriter(out, columns(table), extrasaction="raise", lineterminator="\n")
    w.writeheader()
    for row in rows:
        w.writerow(
            {
                k: ("true" if v else "false") if isinstance(v, bool) else v
                for k, v in row.items()
            }
        )
    return out.getvalue().encode()


def _put(store: ObjectStore, key: str, data: bytes, **attributes: str) -> None:
    obstore.put(store, key, data, use_multipart=False, attributes=attributes)


def load_gtfs(store: ObjectStore, feed: str, d: date) -> tuple[str, Gtfs] | None:
    """The snapshot live at local noon of `d`, as (sha8, Gtfs)."""
    log_bytes = raw.static_log(store, feed)
    # The zone comes from the snapshot itself: guess with UTC noon, then redo.
    sha8 = pick_static(log_bytes, local_noon(d, ZoneInfo("UTC")))
    gtfs = None
    for _ in range(2):
        if sha8 is None:
            return None
        data = raw.static_zip(store, feed, sha8)
        if data is None:
            log.warning("%s: static log names missing gtfs-%s.zip", feed, sha8)
            return None
        gtfs = Gtfs(data)
        local = pick_static(log_bytes, local_noon(d, gtfs.tz))
        if local == sha8:
            break
        sha8 = local
    assert gtfs is not None and sha8 is not None
    return sha8, gtfs


def build_day(
    raw_store: ObjectStore,
    tides_store: ObjectStore,
    name: str,
    feed: Feed,
    d: date,
) -> dict[str, Any] | None:
    """Build and publish feed `name`'s service day `d`.

    Returns the index.json entry, or None when nothing was recorded (any
    earlier package for the day is then removed).
    """
    loaded = load_gtfs(raw_store, name, d)
    if loaded is None:
        log.info("%s %s: no static GTFS", name, d)
        _unpublish(tides_store, name, d)
        return None
    sha8, gtfs = loaded
    start = gtfs.midnight(d)
    end = gtfs.midnight(d + timedelta(days=feed.build_lag_days + 1))
    rec = raw.Recording(raw_store, name, start, end)
    builder = DayBuilder(gtfs, d, feed.producer)
    t0 = time.monotonic()
    for poll in rec.polls():
        builder.add(poll)
    if rec.coverage and rec.coverage[-1][1] < end - raw.MAX_GAP:
        builder.withdraw_all(rec.coverage[-1][1])
    if not rec.coverage:
        log.info("%s %s: no recording", name, d)
        _unpublish(tides_store, name, d)
        return None
    package = builder.finish()
    log.info(
        "%s %s: built in %.0fs: %s", name, d, time.monotonic() - t0, package.counts
    )

    gtfs_key = f"gtfs/{name}/{sha8}.zip"
    if raw.get_bytes(tides_store, gtfs_key) is None:
        data = raw.static_zip(raw_store, name, sha8)
        assert data is not None
        _put(
            tides_store,
            gtfs_key,
            data,
            **{"Content-Type": "application/zip", "Cache-Control": PACKAGE_CACHE},
        )

    prefix = package_prefix(name, d)
    for table in TABLES:
        _put(
            tides_store,
            f"{prefix}/{table}.csv",
            gzip.compress(csv_bytes(table, package.tables[table]), mtime=0),
            **{
                "Content-Type": "text/csv; charset=utf-8",
                "Content-Encoding": "gzip",
                "Cache-Control": PACKAGE_CACHE,
            },
        )
    coverage = [{"start": iso(a), "end": iso(b)} for a, b in rec.coverage]
    descriptor = datapackage(name, d, gtfs.agency, coverage, gtfs_key, package.counts)
    _put(
        tides_store,
        f"{prefix}/datapackage.json",
        json.dumps(descriptor, indent=1).encode(),
        **{"Content-Type": "application/json", "Cache-Control": PACKAGE_CACHE},
    )
    day = (gtfs.midnight(d), gtfs.midnight(d + timedelta(days=1)))
    covered = sum(max(0.0, min(b, day[1]) - max(a, day[0])) for a, b in rec.coverage)
    entry = {
        "trips": package.counts["trips_performed"],
        "predictions": package.counts["stop_time_predictions"],
        "coverage": round(covered / (day[1] - day[0]), 3),
    }
    update_index(tides_store, name, feed.producer, d, entry)
    return entry


def datapackage(
    name: str,
    d: date,
    agency: str | None,
    coverage: list[dict[str, str | None]],
    gtfs_key: str,
    counts: dict[str, int],
) -> dict[str, Any]:
    return {
        "profile": f"{TIDES_SPEC}/tides-datapackage-profile.json",
        "name": f"{name}-{d.isoformat()}",
        "title": f"{agency or name} realtime, service date {d.isoformat()}",
        "description": (
            "Vehicle locations, GPS-derived stop visits and recorded GTFS-realtime"
            f" stop time predictions for {name}, recorded by gtfs.zone."
        ),
        **({"agency": agency} if agency else {}),
        "resources": [
            {
                "name": table,
                "path": f"{table}.csv",
                "profile": "tabular-data-resource",
                "tides-table": table,
                "schema": f"{TIDES_SPEC}/{table}.schema.json",
                "format": "csv",
                "mediatype": "text/csv",
                "encoding": "utf-8",
            }
            for table in TABLES
        ],
        "gtfs_zone": {
            "coverage": coverage,
            "gtfs": f"{BASE_URL}/{gtfs_key}",
            "counts": counts,
            "generator": {"name": "gtfs-zone-rt-archive", "version": VERSION},
        },
    }


def read_index(store: ObjectStore) -> dict[str, Any]:
    data = raw.get_bytes(store, INDEX)
    return json.loads(data) if data else {"feeds": {}}


def update_index(
    store: ObjectStore,
    name: str,
    producer: str,
    d: date,
    entry: dict[str, Any] | None,
) -> None:
    """Set (or with None remove) one date of index.json.

    Read-modify-write: safe because the Dagster instance runs one run at a time.
    """
    index = read_index(store)
    feed = index["feeds"].setdefault(name, {"producer": producer, "dates": {}})
    feed["producer"] = producer
    if entry is None:
        feed["dates"].pop(d.isoformat(), None)
    else:
        feed["dates"][d.isoformat()] = entry
    feed["dates"] = dict(sorted(feed["dates"].items()))
    index["feeds"] = dict(sorted(index["feeds"].items()))
    index["generated"] = iso(time.time())
    _put(
        store,
        INDEX,
        json.dumps(index, separators=(",", ":")).encode(),
        **{"Content-Type": "application/json", "Cache-Control": INDEX_CACHE},
    )


def _unpublish(store: ObjectStore, name: str, d: date) -> None:
    index = read_index(store)
    dates = index["feeds"].get(name, {}).get("dates", {})
    if d.isoformat() not in dates:
        return
    prefix = package_prefix(name, d)
    keys = [
        m["path"] for batch in obstore.list(store, prefix=f"{prefix}/") for m in batch
    ]
    if keys:
        obstore.delete(store, keys)
    update_index(store, name, index["feeds"][name]["producer"], d, None)
