"""publish: a recorded day end to end, validated against the TIDES schemas."""

import gzip
import json
from datetime import date
from pathlib import Path

import jsonschema
import obstore
import pytest
from frictionless import Package
from helpers import (
    gtfs_zip,
    local,
    lon_at,
    put_segment,
    put_static,
    raw_lines,
    tu_message,
    vp_message,
)
from obstore.store import MemoryStore

from gtfs_zone_rt_archive import publish
from gtfs_zone_rt_archive.config import Feed
from gtfs_zone_rt_archive.gtfs import Gtfs
from gtfs_zone_rt_archive.tides import TABLES

D = date(2026, 11, 5)
SD = "20261105"
SCHEMAS = Path(publish.__file__).parent / "schemas"
PROFILE = Path(__file__).parent / "fixtures" / "tides-datapackage-profile.json"
FEED = Feed(
    enabled=True,
    producer="test",
    static="https://example.com/gtfs.zip",
    vehicle_positions="https://example.com/vp.pb",
    trip_updates="https://example.com/tu.pb",
)


@pytest.fixture
def stores():
    raw_store, tides_store = MemoryStore(), MemoryStore()
    data = gtfs_zip()
    put_static(raw_store, "f", data, local("2026-11-04", "20:00:00"))
    line = Gtfs(data).trips["T1"].line
    a = local("2026-11-05", "08:00:00")
    by_hour: dict[str, list] = {}
    for c in range(a - 120, a + 1500, 15):
        m = max(0.0, (c - a) / 600 * line.dists[1])
        vp = vp_message(c, [("bus1", "T1", SD, lon_at(m), c)])
        tu = tu_message(c, [("T1", SD, [(2, 60 if c < a + 300 else 90)])])
        hour = publish.iso(c)[:13].replace("T", "/")
        by_hour.setdefault(hour, []).extend(raw_lines(c, vp, tu))
    for hour, lines in by_hour.items():
        put_segment(raw_store, "f", f"{hour}.jsonl.zst", lines)
    return raw_store, tides_store


def _get(store, key):
    return bytes(obstore.get(store, key).bytes())


def test_a_recorded_day_is_published_and_valid(stores, tmp_path):
    raw_store, tides_store = stores
    entry = publish.build_day(raw_store, tides_store, "f", FEED, D)
    assert entry["trips"] == 1
    assert entry["predictions"] > 0
    assert 0 < entry["coverage"] < 0.05

    prefix = "tides/f/2026-11-05"
    descriptor = json.loads(_get(tides_store, f"{prefix}/datapackage.json"))
    assert descriptor["gtfs_zone"]["counts"]["trips_performed"] == 1
    assert descriptor["gtfs_zone"]["generator"]["name"] == "gtfs-zone-rt-archive"
    gtfs_url = descriptor["gtfs_zone"]["gtfs"]
    assert _get(tides_store, gtfs_url.removeprefix(f"{publish.BASE_URL}/"))
    jsonschema.validate(descriptor, json.loads(PROFILE.read_text()))

    # Validate the tables locally: vendored schemas instead of the pinned URLs.
    for table in TABLES:
        meta = obstore.head(tides_store, f"{prefix}/{table}.csv")
        assert meta["size"] > 0
        schema = f"{table}.schema.json"
        (tmp_path / schema).write_bytes((SCHEMAS / schema).read_bytes())
        (tmp_path / f"{table}.csv").write_bytes(
            gzip.decompress(_get(tides_store, f"{prefix}/{table}.csv"))
        )
    local_descriptor = {
        "name": descriptor["name"],
        "resources": [
            {**r, "schema": f"{r['name']}.schema.json"} for r in descriptor["resources"]
        ],
    }
    for r in local_descriptor["resources"]:
        r.pop("tides-table")
        r.pop("profile")
    report = Package(local_descriptor, basepath=str(tmp_path)).validate()
    assert report.valid, report.flatten(["rowNumber", "fieldName", "type", "note"])

    index = json.loads(_get(tides_store, "index.json"))
    assert index["feeds"]["f"] == {"producer": "test", "dates": {"2026-11-05": entry}}


def test_csv_upload_carries_gzip_encoding(stores):
    raw_store, tides_store = stores
    publish.build_day(raw_store, tides_store, "f", FEED, D)
    result = obstore.get(tides_store, "tides/f/2026-11-05/stop_visits.csv")
    assert result.attributes["Content-Encoding"] == "gzip"
    text = gzip.decompress(bytes(result.bytes())).decode()
    assert text.splitlines()[0].startswith("service_date,trip_id_performed,")


def test_a_day_without_recording_removes_its_package(stores):
    raw_store, tides_store = stores
    publish.build_day(raw_store, tides_store, "f", FEED, D)
    for batch in obstore.list(raw_store, prefix="raw/f/2026-11-05/"):
        obstore.delete(raw_store, [m["path"] for m in batch])
    assert publish.build_day(raw_store, tides_store, "f", FEED, D) is None
    keys = [m["path"] for b in obstore.list(tides_store, prefix="tides/") for m in b]
    assert keys == []
    index = json.loads(_get(tides_store, "index.json"))
    assert index["feeds"]["f"]["dates"] == {}
