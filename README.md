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

## License

AGPL-3.0-or-later. See [LICENSE.txt](LICENSE.txt).
