# gtfs-zone-rt-archive

Records GTFS-realtime feeds and publishes them as
[TIDES](https://github.com/TIDES-transit/TIDES) data packages.

- The recorder polls each enabled feed and stores raw responses as hourly
  zstd JSONL files in object storage.
- A daily Dagster job builds one TIDES data package per feed and service day:
  `trips_performed`, `stop_visits` (actual times from GPS),
  `vehicle_locations` and `stop_time_predictions` (the feed's own
  predictions, defined in the
  [gtfs-zone/TIDES](https://github.com/gtfs-zone/TIDES/tree/gtfs-zone/predictions)
  fork).

Published packages are browsable at https://accuracy.rt.gtfs.zone.

## Recorder

```bash
uv run gtfs-zone-rt-archive record
```

| Env | Default | |
| --- | --- | --- |
| `RT_ARCHIVE_FEEDS` | `feeds.yaml` | feed list, see gtfs-zone-infra `gtfs/rt-archive/feeds.yaml` |
| `RT_ARCHIVE_DATA_DIR` | `data` | open hour and upload queue |
| `S3_ENDPOINT`, `S3_REGION` | -, `garage` | S3-compatible storage |
| `S3_BUCKET`, `S3_ACCESS_KEY`, `S3_SECRET_KEY` | - | raw bucket |
| `GATUS_URL`, `GATUS_ENDPOINT_KEY`, `GATUS_TOKEN` | unset | heartbeat; off when unset |

## TIDES build

A Dagster code location in feed-catalog's instance, one `tides_package`
partition per feed and service date, scheduled daily at 16:00 UTC for each
enabled feed's date `build_lag_days` back.

```bash
uv run --with dagster-webserver==1.13.24 dagster dev -m gtfs_zone_rt_archive.pipeline.definitions
```

| Env | Default | |
| --- | --- | --- |
| `RT_ARCHIVE_FEEDS` | `feeds.yaml` | feed list, also the partition keys |
| `S3_ENDPOINT`, `S3_REGION` | -, `garage` | S3-compatible storage |
| `RAW_S3_BUCKET`, `RAW_S3_ACCESS_KEY`, `RAW_S3_SECRET_KEY` | - | raw bucket (read) |
| `TIDES_S3_BUCKET`, `TIDES_S3_ACCESS_KEY`, `TIDES_S3_SECRET_KEY` | - | public bucket |
| `GATUS_URL`, `GATUS_ENDPOINT_KEY`, `GATUS_TOKEN` | unset | publish heartbeat |

Public layout: `tides/<feed>/<date>/{datapackage.json, <table>.csv}` (CSVs
gzipped with `Content-Encoding: gzip`), `gtfs/<feed>/<sha8>.zip` and
`index.json`.

## License

AGPL-3.0-or-later. See [LICENSE.txt](LICENSE.txt).
