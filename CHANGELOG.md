# Changelog

## v0.1.0 (2026-11-05)

### Feat

- **recorder**: poll enabled feeds into the raw bucket
- **heartbeat**: push recorder health to Gatus once a minute
- **upload**: sweep closed segments and static files to the raw bucket
- **config**: add feeds.yaml model and env settings
- **store**: add hourly zstd JSONL store

### Fix

- **store**: compress one segment at a time
