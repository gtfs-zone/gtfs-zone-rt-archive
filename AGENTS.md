# AGENTS.md

Records GTFS-realtime feeds and publishes them as TIDES data packages for
prediction accuracy checks (consumed by gtfs-zone-rt-accuracy at
accuracy.rt.gtfs.zone).

## Layout

- **Recorder**: long-running process that polls the feeds listed in
  gtfs-zone-infra and writes raw hourly files to the private Garage bucket
  `rt-archive`.
- **TIDES build**: Dagster code location in feed-catalog's Dagster instance,
  one daily partition per feed, writing to the public Garage website bucket
  `archive.rt.gtfs.zone`.
- **Handoff format**: TIDES CSV plus the `stop_time_predictions` table from
  the `gtfs-zone/predictions` branch of
  [gtfs-zone/TIDES](https://github.com/gtfs-zone/TIDES).

## Commands

```bash
scripts/check.sh                 # ruff, vulture, deptry, pytest (pre-commit and CI)
uv run gtfs-zone-rt-archive record
```

## Recorder

`recorder.py` runs one thread per enabled feed in `feeds.yaml`
(`config.py`). Each poll writes raw lines through `store.Writer`; a closed
hour is compressed to `.jsonl.zst` and `upload.Uploader` sweeps the data dir
into `raw/<feed>/` of the raw bucket, deleting local files once verified.
`heartbeat.py` pushes per-feed health to Gatus once a minute. The raw line
format is in the `store.py` docstring.

## Conventions

- **Commits**: Conventional Commits, enforced by the `commit-msg` hook. Never add
  Co-Authored-By trailers. Setup is in [CONTRIBUTING.md](CONTRIBUTING.md).
- **Logging**: module loggers are named `log`, never `logger`.
- **Cross-repo work**: sibling gtfs.zone repos live under the same parent
  directory and may be read and edited when a change spans repos.
- **Plans**: write plans to `CURRENT_PLAN.md` at the repo root as a
  checklist (`- [ ]`), ticked off as work lands. It is neither tracked nor
  gitignored: never stage or commit it. This repo's plan is the central plan
  for the recorder, TIDES fork, accuracy frontend, infra and homepage work.
