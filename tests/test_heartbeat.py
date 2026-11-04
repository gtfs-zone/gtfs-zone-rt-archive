"""heartbeat: health per feed and the Gatus push."""

import httpx

from gtfs_zone_rt_archive.config import GatusSettings
from gtfs_zone_rt_archive.heartbeat import Health, Heartbeat

PUSH = "http://gatus:8080/api/v1/endpoints/pollers_archive-recorder/external"


def test_a_feed_needs_recent_vp_and_tu():
    h = Health(["amtrak", "suntran"])
    h.ok("amtrak", "vp", 1000)
    h.ok("amtrak", "tu", 1000)
    h.ok("suntran", "vp", 1000)
    h.ok("suntran", "al", 1000)
    assert h.failing(1100) == ["suntran:tu"]
    assert h.failing(1121) == ["amtrak:vp", "amtrak:tu", "suntran:vp", "suntran:tu"]


def _push(health, gatus):
    requests = []
    transport = httpx.MockTransport(lambda r: requests.append(r) or httpx.Response(200))
    with httpx.Client(transport=transport) as http:
        Heartbeat(gatus, health).push(http, 1000)
    return requests


def test_push_shape():
    gatus = GatusSettings("http://gatus:8080/", "pollers_archive-recorder", "tok")
    [req] = _push(Health(["amtrak"]), gatus)
    assert str(req.url).startswith(PUSH)
    assert req.url.params["success"] == "false"
    assert req.url.params["error"] == "no recent 200: amtrak:vp, amtrak:tu"
    assert req.headers["authorization"] == "Bearer tok"

    h = Health(["amtrak"])
    h.ok("amtrak", "vp", 990)
    h.ok("amtrak", "tu", 990)
    [req] = _push(h, gatus)
    assert req.url.params["success"] == "true"
    assert "error" not in req.url.params


def test_unset_is_a_no_op():
    assert _push(Health(["amtrak"]), GatusSettings(None, None, "")) == []


def test_push_failures_do_not_raise():
    gatus = GatusSettings("http://gatus:8080", "k", "")
    transport = httpx.MockTransport(lambda r: httpx.Response(503))
    with httpx.Client(transport=transport) as http:
        Heartbeat(gatus, Health([])).push(http, 1000)
