"""pipeline: partitions from feeds.yaml and the daily schedule."""

import importlib
from datetime import UTC, datetime

import pytest
from dagster import (
    DagsterInstance,
    MultiPartitionKey,
    build_schedule_context,
    materialize,
)

FEEDS = """
feeds:
  amtrak:
    enabled: true
    producer: rt.gtfs.zone
    static: https://example.com/a.zip
    vehicle_positions: https://example.com/vp
    trip_updates: https://example.com/tu
    build_lag_days: 3
  hudsonlink:
    enabled: true
    producer: rt.gtfs.zone
    static: https://example.com/h.zip
    vehicle_positions: https://example.com/vp
    trip_updates: https://example.com/tu
  old:
    enabled: false
    producer: x
    static: https://example.com/o.zip
    vehicle_positions: https://example.com/vp
    trip_updates: https://example.com/tu
"""


@pytest.fixture
def defs(tmp_path, monkeypatch):
    feeds = tmp_path / "feeds.yaml"
    feeds.write_text(FEEDS)
    monkeypatch.setenv("RT_ARCHIVE_FEEDS", str(feeds))
    monkeypatch.setenv("S3_ENDPOINT", "http://garage:3900")
    for prefix in ("RAW_", "TIDES_"):
        for name in ("S3_BUCKET", "S3_ACCESS_KEY", "S3_SECRET_KEY"):
            monkeypatch.setenv(f"{prefix}{name}", "x")
    from gtfs_zone_rt_archive.pipeline import definitions

    return importlib.reload(definitions)


def test_partitions_include_disabled_feeds(defs):
    feeds = defs.partitions.get_partitions_def_for_dimension("feed")
    assert feeds.get_partition_keys() == ["amtrak", "hudsonlink", "old"]


def test_schedule_builds_each_enabled_feed_after_its_lag(defs):
    ctx = build_schedule_context(
        scheduled_execution_time=datetime(2026, 11, 10, 16, tzinfo=UTC)
    )
    requests = defs.daily_tides(ctx)
    assert [(r.run_key, r.partition_key) for r in requests] == [
        (
            "amtrak-2026-11-07",
            MultiPartitionKey({"feed": "amtrak", "date": "2026-11-07"}),
        ),
        (
            "hudsonlink-2026-11-09",
            MultiPartitionKey({"feed": "hudsonlink", "date": "2026-11-09"}),
        ),
    ]


def test_definitions_load(defs):
    defs.defs.resolve_all_job_defs()
    assert defs.defs.resolve_job_def("tides").partitions_def == defs.partitions


def test_heartbeat_waits_for_every_scheduled_feed(defs, monkeypatch):
    pushed = []
    monkeypatch.setattr(defs, "build_day", lambda *a: None)
    monkeypatch.setattr(defs, "push_success", lambda gatus: pushed.append(True))
    due = defs.scheduled_dates(datetime.now(UTC).date())
    instance = DagsterInstance.ephemeral()
    for feed in ("amtrak", "hudsonlink"):
        materialize(
            [defs.tides_package],
            partition_key=defs.partition_key(feed, due[feed]),
            instance=instance,
        )
        assert pushed == ([] if feed == "amtrak" else [True])


def test_run_storage_url_uses_psycopg2():
    from sqlalchemy.engine import make_url

    # The infra dagster.yaml passes a bare postgresql:// URL.
    assert make_url("postgresql://u:p@h/db").get_dialect().driver == "psycopg2"
