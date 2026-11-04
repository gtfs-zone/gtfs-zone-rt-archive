"""config: feeds.yaml model, defaults and validation."""

import pytest
from pydantic import ValidationError

from gtfs_zone_rt_archive.config import FeedsFile

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
