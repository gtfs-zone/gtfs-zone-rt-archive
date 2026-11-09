"""config: feeds.yaml model, defaults and validation."""

import pytest
from pydantic import ValidationError

from gtfs_zone_rt_archive.config import FeedsFile, S3Settings

FEEDS = """
feeds:
  amtrak:
    enabled: true
    producer: rt.gtfs.zone
    static: https://example.com/gtfs.zip
    vehicle_positions: http://gtfs-api:8000/amtrak/vehicle_positions.pb
    trip_updates: http://gtfs-api:8000/amtrak/trip_updates.pb
    service_alerts: http://gtfs-api:8000/amtrak/service_alerts.pb
    build_lag_days: 3
  suntran:
    enabled: false
    producer: suntran
    static: https://example.com/s.zip
    vehicle_positions: https://example.com/vp.pb
    trip_updates: https://example.com/tu.pb
    poll: 30
"""


def _load(tmp_path, text=FEEDS):
    path = tmp_path / "feeds.yaml"
    path.write_text(text)
    return FeedsFile.load(path)


def test_defaults_and_overrides(tmp_path):
    cfg = _load(tmp_path)
    amtrak, suntran = cfg.feeds["amtrak"], cfg.feeds["suntran"]
    assert (amtrak.poll, amtrak.build_lag_days) == (15, 3)
    assert (suntran.poll, suntran.build_lag_days) == (30, 1)


def test_enabled_skips_disabled_feeds(tmp_path):
    assert list(_load(tmp_path).enabled()) == ["amtrak"]


def test_rt_urls_include_alerts_only_when_set(tmp_path):
    cfg = _load(tmp_path)
    assert list(cfg.feeds["amtrak"].rt_urls()) == ["vp", "tu", "al"]
    assert list(cfg.feeds["suntran"].rt_urls()) == ["vp", "tu"]


@pytest.mark.parametrize(
    ("old", "new"),
    [
        ("  amtrak:", "  Amtrak:"),  # names are bucket paths and partition keys
        ("    build_lag_days: 3", "    build_lag_dayz: 3"),  # unknown key
        ("    poll: 30", "    poll: 1"),  # too fast
        ("https://example.com/vp.pb", "example.com/vp.pb"),  # not a URL
        ("    enabled: false\n", ""),  # enabled is required
    ],
)
def test_invalid_files_are_rejected(tmp_path, old, new):
    assert old in FEEDS
    with pytest.raises(ValidationError):
        _load(tmp_path, FEEDS.replace(old, new))


def test_s3_endpoint_and_region_fall_back_to_shared(monkeypatch):
    for k, v in {
        "S3_ENDPOINT": "http://garage:3900",
        "RAW_S3_BUCKET": "rt-archive",
        "RAW_S3_ACCESS_KEY": "ra",
        "RAW_S3_SECRET_KEY": "rs",
        "TIDES_S3_ENDPOINT": "https://s3.us-east-005.backblazeb2.com",
        "TIDES_S3_REGION": "us-east-005",
        "TIDES_S3_BUCKET": "gtfs-zone-rt-archive",
        "TIDES_S3_ACCESS_KEY": "ta",
        "TIDES_S3_SECRET_KEY": "ts",
    }.items():
        monkeypatch.setenv(k, v)
    monkeypatch.delenv("S3_REGION", raising=False)
    raw, tides = S3Settings.from_env("RAW_"), S3Settings.from_env("TIDES_")
    assert (raw.endpoint, raw.region) == ("http://garage:3900", "garage")
    assert (tides.endpoint, tides.region) == (
        "https://s3.us-east-005.backblazeb2.com",
        "us-east-005",
    )


def test_s3_endpoint_is_required(monkeypatch):
    monkeypatch.delenv("S3_ENDPOINT", raising=False)
    monkeypatch.delenv("X_S3_ENDPOINT", raising=False)
    with pytest.raises(KeyError):
        S3Settings.from_env("X_")
