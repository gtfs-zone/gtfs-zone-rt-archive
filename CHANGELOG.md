# Changelog

## v0.2.1 (2026-11-05)

### Fix

- **pipeline**: pin sqlalchemy below 2.1 for the psycopg2 run storage

## v0.2.0 (2026-11-05)

### Feat

- **pipeline**: add the Dagster code location with a daily schedule
- **tides**: build and publish TIDES packages from the raw bucket

### Refactor

- **store**: parse segments from any binary stream

## v0.1.0 (2026-11-05)

### Feat

- **recorder**: poll enabled feeds into the raw bucket
- **heartbeat**: push recorder health to Gatus once a minute
- **upload**: sweep closed segments and static files to the raw bucket
- **config**: add feeds.yaml model and env settings
- **store**: add hourly zstd JSONL store

### Fix

- **store**: compress one segment at a time
