"""Command line: `gtfs-zone-rt-archive record`."""

import argparse
import logging


def main() -> None:
    parser = argparse.ArgumentParser(prog="gtfs-zone-rt-archive")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("record", help="poll the enabled feeds into the raw bucket")
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s"
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    if args.command == "record":
        from gtfs_zone_rt_archive.recorder import record

        record()
