"""Dagster entrypoint: one tides_package partition per feed and service date.

The 16:00 UTC schedule builds each enabled feed's service date
`build_lag_days` before today. Any other range is rebuilt with a partition
backfill; raw recordings are kept forever. The Gatus heartbeat is pushed after
a run once every enabled feed's scheduled date has been built.
"""

from datetime import UTC, date, datetime, timedelta

from dagster import (
    AssetExecutionContext,
    DailyPartitionsDefinition,
    DefaultScheduleStatus,
    Definitions,
    MaterializeResult,
    MetadataValue,
    MultiPartitionKey,
    MultiPartitionsDefinition,
    RunRequest,
    ScheduleEvaluationContext,
    StaticPartitionsDefinition,
    asset,
    define_asset_job,
    schedule,
)

from gtfs_zone_rt_archive.config import BuildSettings, FeedsFile
from gtfs_zone_rt_archive.heartbeat import push_success
from gtfs_zone_rt_archive.publish import BASE_URL, build_day, package_prefix
from gtfs_zone_rt_archive.upload import s3_store

SETTINGS = BuildSettings.from_env()
FEEDS = FeedsFile.load(SETTINGS.feeds)
# The first day the kcfam recordings cover.
FIRST_DATE = "2026-10-12"

partitions = MultiPartitionsDefinition(
    {
        "feed": StaticPartitionsDefinition(sorted(FEEDS.feeds)),
        "date": DailyPartitionsDefinition(start_date=FIRST_DATE),
    }
)


def partition_key(feed: str, day: str) -> MultiPartitionKey:
    return MultiPartitionKey({"feed": feed, "date": day})


def scheduled_dates(today: date) -> dict[str, str]:
    """The service date the schedule builds today, per enabled feed."""
    return {
        name: (today - timedelta(days=feed.build_lag_days)).isoformat()
        for name, feed in FEEDS.enabled().items()
    }


@asset(partitions_def=partitions)
def tides_package(context: AssetExecutionContext) -> MaterializeResult:
    """Build one feed's service day from the raw bucket and publish it."""
    keys = context.partition_key.keys_by_dimension
    name, d = keys["feed"], date.fromisoformat(keys["date"])
    tides = s3_store(SETTINGS.tides)
    entry = build_day(s3_store(SETTINGS.raw), tides, name, FEEDS.feeds[name], d)

    # A day without recording still counts: the recorder has its own heartbeat.
    done = context.instance.get_materialized_partitions(context.asset_key)
    done.add(context.partition_key)
    missing = [
        f"{feed}/{day}"
        for feed, day in scheduled_dates(datetime.now(UTC).date()).items()
        if partition_key(feed, day) not in done
    ]
    if missing:
        context.log.info("not pushing the heartbeat, not built: %s", ", ".join(missing))
    else:
        push_success(SETTINGS.gatus)

    url = f"{BASE_URL}/{package_prefix(name, d)}/datapackage.json"
    return MaterializeResult(
        metadata={
            "published": entry is not None,
            **(
                {
                    **entry,
                    "package": MetadataValue.url(url),
                }
                if entry
                else {}
            ),
        }
    )


tides_job = define_asset_job("tides", selection=[tides_package])


# After feed-catalog (09:00), timetable-sites (11:00) and crimes (14:00), which
# share the instance's single-run queue.
@schedule(
    job=tides_job,
    cron_schedule="0 16 * * *",
    execution_timezone="UTC",
    default_status=DefaultScheduleStatus.RUNNING,
)
def daily_tides(context: ScheduleEvaluationContext) -> list[RunRequest]:
    today = context.scheduled_execution_time.date()
    return [
        RunRequest(
            run_key=f"{name}-{day}",
            partition_key=partition_key(name, day),
        )
        for name, day in sorted(scheduled_dates(today).items())
    ]


defs = Definitions(assets=[tides_package], jobs=[tides_job], schedules=[daily_tides])
