"""recorder: one feed's polls, static checks and shutdown."""

import base64
import json
import threading

import httpx
from obstore.store import MemoryStore

from gtfs_zone_rt_archive.config import Feed
from gtfs_zone_rt_archive.heartbeat import Health
from gtfs_zone_rt_archive.recorder import FeedRecorder
from gtfs_zone_rt_archive.store import lines
from gtfs_zone_rt_archive.upload import Uploader

FEED = Feed(
    enabled=True,
    producer="rt.gtfs.zone",
    static="https://example.com/gtfs.zip",
    vehicle_positions="https://example.com/vp.pb",
    trip_updates="https://example.com/tu.pb",
    service_alerts="https://example.com/al.pb",
)


def _recorder(tmp_path, handler):
    health = Health(["amtrak"])
    rec = FeedRecorder(
        "amtrak",
        FEED,
        tmp_path,
        Uploader(MemoryStore(), tmp_path),
        health,
        transport=httpx.MockTransport(handler),
    )
    return rec, health


def _handler(request):
    path = request.url.path
    if path == "/gtfs.zip":
        return httpx.Response(200, content=b"zip", headers={"last-modified": "x"})
    if path == "/tu.pb":
        return httpx.Response(503, content=b"down")
    if path == "/al.pb":
        raise httpx.ConnectError("refused")
    return httpx.Response(200, content=b"vp-bytes")


def _run_once(rec):
    stop = threading.Event()

    def stop_after_first_wait(timeout=None):
        stop.set()
        return True

    stop.wait = stop_after_first_wait
    rec.run(stop)


def test_one_poll_records_every_url_and_the_static(tmp_path):
    rec, health = _recorder(tmp_path, _handler)
    _run_once(rec)

    recorded = list(lines(tmp_path / "amtrak"))
    assert [line["k"] for line in recorded] == ["start", "static", "raw", "raw", "raw"]
    start, static, vp, tu, al = recorded
    assert start["feed"] == "amtrak"
    assert start["poll"] == 15
    assert static["new"] is True
    assert static["last_modified"] == "x"
    assert (tmp_path / "amtrak" / f"gtfs-{static['sha8']}.zip").read_bytes() == b"zip"
    assert vp["feed"] == "vp"
    assert vp["status"] == 200
    assert base64.b64decode(vp["b64"]) == b"vp-bytes"
    assert tu["status"] == 503
    assert "error" in al
    assert vp["c"] == tu["c"] == al["c"]
    assert health.failing(vp["t"]) == ["amtrak:tu"]


def test_a_known_static_is_not_saved_again(tmp_path):
    rec, _ = _recorder(tmp_path, _handler)
    _run_once(rec)
    zips = list((tmp_path / "amtrak").glob("gtfs-*.zip"))
    for z in zips:
        z.unlink()

    rec, _ = _recorder(tmp_path, _handler)
    _run_once(rec)
    assert not list((tmp_path / "amtrak").glob("gtfs-*.zip"))
    log = [
        json.loads(s)
        for s in (tmp_path / "amtrak" / "static.jsonl").read_text().splitlines()
    ]
    assert [entry["new"] for entry in log] == [True, False]


def test_a_static_error_is_logged_and_polling_goes_on(tmp_path):
    def handler(request):
        if request.url.path == "/gtfs.zip":
            return httpx.Response(404)
        return httpx.Response(200, content=b"ok")

    rec, health = _recorder(tmp_path, handler)
    _run_once(rec)
    static = [line for line in lines(tmp_path / "amtrak") if line["k"] == "static"]
    assert "error" in static[0]
    assert health.failing(static[0]["t"]) == []
