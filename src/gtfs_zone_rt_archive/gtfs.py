"""Static GTFS snapshots: which one was live on a date, schedules and stop polylines."""

from __future__ import annotations

import csv
import io
import json
import math
import zipfile
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from functools import cached_property
from typing import TYPE_CHECKING, Any
from zoneinfo import ZoneInfo

from shapely.geometry import LineString, Point

if TYPE_CHECKING:
    from collections.abc import Iterator

# GTFS route_type to the TIDES trips_performed.route_type label.
ROUTE_TYPES = {
    "0": "Tram / Streetcar / Light rail",
    "1": "Subway / Metro",
    "2": "Rail",
    "3": "Bus",
    "4": "Ferry",
    "5": "Cable tram",
    "6": "Aerial lift",
    "7": "Funicular",
    "11": "Trolleybus",
    "12": "Monorail",
}
# A trip instance whose window holds an observation +- this many seconds is
# preferred over one that merely starts closer to it.
SLACK = 3600


def seconds(t: str) -> int | None:
    """GTFS HH:MM:SS (hours may pass 24) to seconds after midnight."""
    if not t:
        return None
    h, m, s = t.strip().split(":")
    return int(h) * 3600 + int(m) * 60 + int(s)


def metric(lat: float, lon: float, cos_lat: float) -> tuple[float, float]:
    """Equirectangular metres, good enough over one trip."""
    return lon * cos_lat * 111320, lat * 110540


@dataclass(frozen=True)
class StopTime:
    stop_id: str
    stop_sequence: int
    arrival: int | None
    departure: int | None
    timepoint: bool | None
    lat: float
    lon: float


@dataclass(frozen=True)
class Trip:
    trip_id: str
    route_id: str
    shape_id: str | None
    direction_id: int | None
    block_id: str | None
    stop_times: tuple[StopTime, ...]
    frequency: bool

    @cached_property
    def line(self) -> TripLine | None:
        return TripLine(self.stop_times) if len(self.stop_times) > 1 else None

    @cached_property
    def offsets(self) -> list[tuple[int, int]] | None:
        """(arrival, departure) seconds per stop, gaps interpolated by distance."""
        known = [
            (i, st.arrival if st.arrival is not None else st.departure)
            for i, st in enumerate(self.stop_times)
            if st.arrival is not None or st.departure is not None
        ]
        if not known:
            return None
        dist = self.line.dists if self.line else [0.0] * len(self.stop_times)
        out: list[tuple[int, int]] = []
        for i, st in enumerate(self.stop_times):
            if st.arrival is not None or st.departure is not None:
                arr = st.arrival if st.arrival is not None else st.departure
                dep = st.departure if st.departure is not None else st.arrival
                assert arr is not None and dep is not None
                out.append((arr, dep))
                continue
            before = [k for k in known if k[0] < i]
            after = [k for k in known if k[0] > i]
            if not before or not after:
                edge = (before or after)[-1 if before else 0][1]
                out.append((edge, edge))
                continue
            (i0, t0), (i1, t1) = before[-1], after[0]
            span = dist[i1] - dist[i0]
            share = (dist[i] - dist[i0]) / span if span > 0 else (i - i0) / (i1 - i0)
            t = round(t0 + share * (t1 - t0))
            out.append((t, t))
        return out

    @cached_property
    def index_of_sequence(self) -> dict[int, int]:
        return {st.stop_sequence: i for i, st in enumerate(self.stop_times)}


class TripLine:
    """A trip's stop polyline in metres, as the delay estimator draws it."""

    def __init__(self, stops: tuple[StopTime, ...]) -> None:
        self.cos_lat = math.cos(math.radians(stops[0].lat))
        self.xy = [metric(s.lat, s.lon, self.cos_lat) for s in stops]
        self.line = LineString(self.xy)
        # project() maps a repeated point to its first occurrence; keep stops ordered.
        self.dists = [self.line.project(Point(c)) for c in self.xy]
        for i in range(1, len(self.dists)):
            self.dists[i] = max(self.dists[i], self.dists[i - 1])

    def fix(
        self, ts: int, lat: float, lon: float
    ) -> tuple[int, float, tuple[float, float]]:
        """(ts, distance along the line, metric xy) of one GPS fix."""
        xy = metric(lat, lon, self.cos_lat)
        return ts, self.line.project(Point(xy)), xy


def _rows(z: zipfile.ZipFile, name: str) -> Iterator[dict[str, str]]:
    if name not in z.namelist():
        return
    with z.open(name) as f:
        yield from csv.DictReader(io.TextIOWrapper(f, "utf-8-sig"))


def _int(v: str | None) -> int | None:
    return int(v) if v not in (None, "") else None


class Gtfs:
    def __init__(self, data: bytes) -> None:
        z = zipfile.ZipFile(io.BytesIO(data))
        agencies = list(_rows(z, "agency.txt"))
        self.tz = ZoneInfo(agencies[0]["agency_timezone"])
        self.agency = agencies[0].get("agency_name") or None
        self.route_type = {
            r["route_id"]: ROUTE_TYPES.get(r.get("route_type", ""))
            for r in _rows(z, "routes.txt")
        }
        stops = {
            r["stop_id"]: (float(r["stop_lat"]), float(r["stop_lon"]))
            for r in _rows(z, "stops.txt")
            if r.get("stop_lat") and r.get("stop_lon")
        }
        frequency = {r["trip_id"] for r in _rows(z, "frequencies.txt")}
        by_trip: dict[str, list[StopTime]] = {}
        for r in _rows(z, "stop_times.txt"):
            pos = stops.get(r["stop_id"])
            if pos is None:
                continue
            tp = r.get("timepoint", "")
            by_trip.setdefault(r["trip_id"], []).append(
                StopTime(
                    stop_id=r["stop_id"],
                    stop_sequence=int(r["stop_sequence"]),
                    arrival=seconds(r.get("arrival_time", "")),
                    departure=seconds(r.get("departure_time", "")),
                    timepoint={"0": False, "1": True}.get(tp.strip()),
                    lat=pos[0],
                    lon=pos[1],
                )
            )
        self.trips: dict[str, Trip] = {}
        self._dates: dict[tuple[str, str | None, int], date | None] = {}
        for r in _rows(z, "trips.txt"):
            sts = by_trip.get(r["trip_id"])
            if not sts:
                continue
            sts.sort(key=lambda s: s.stop_sequence)
            self.trips[r["trip_id"]] = Trip(
                trip_id=r["trip_id"],
                route_id=r["route_id"],
                shape_id=r.get("shape_id") or None,
                direction_id=_int(r.get("direction_id")),
                block_id=r.get("block_id") or None,
                stop_times=tuple(sts),
                frequency=r["trip_id"] in frequency,
            )

    def midnight(self, d: date) -> int:
        """Epoch of GTFS service day `d`'s 00:00:00 (noon minus 12 h)."""
        noon = datetime(d.year, d.month, d.day, 12, tzinfo=self.tz)
        return int((noon - timedelta(hours=12)).timestamp())

    def schedule(
        self, trip: Trip, d: date, start_time: str | None = None
    ) -> list[tuple[int, int]] | None:
        """(arrival, departure) epochs per stop of one trip instance.

        A frequency-based trip is shifted so its first departure is `start_time`.
        """
        offsets = trip.offsets
        if offsets is None:
            return None
        base = self.midnight(d)
        if trip.frequency:
            start = seconds(start_time or "")
            if start is None:
                return None
            base += start - offsets[0][1]
        return [(base + a, base + dep) for a, dep in offsets]

    def service_date(self, trip: Trip, t: float, start_time: str | None) -> date | None:
        """The service date of the instance of `trip` observed at epoch `t`.

        Prefers instances whose scheduled span (+- SLACK) holds `t`, then the
        one whose first departure is closest to it. Memoized per 5 minutes.
        """
        memo = (trip.trip_id, start_time, int(t) // 300)
        if memo not in self._dates:
            self._dates[memo] = self._service_date(trip, t, start_time)
        return self._dates[memo]

    def _service_date(
        self, trip: Trip, t: float, start_time: str | None
    ) -> date | None:
        local = datetime.fromtimestamp(t, self.tz).date()
        best: tuple[bool, float, date] | None = None
        for back in range(-1, 4):
            d = local - timedelta(days=back)
            sched = self.schedule(trip, d, start_time)
            if sched is None:
                return None
            start, end = sched[0][1], sched[-1][0]
            key = (not (start - SLACK <= t <= end + SLACK), abs(start - t), d)
            if best is None or key < best:
                best = key
        return best[2] if best else None


def pick_static(static_log: bytes, at: float) -> str | None:
    """sha8 of the snapshot live at epoch `at`: the last check at or before it,
    else the first one after."""
    checks: list[tuple[float, str]] = []
    for text in static_log.decode().splitlines():
        try:
            line: dict[str, Any] = json.loads(text)
        except ValueError:
            continue
        if line.get("sha8"):
            checks.append((float(line["t"]), line["sha8"]))
    checks.sort()
    before = [sha8 for t, sha8 in checks if t <= at]
    if before:
        return before[-1]
    return checks[0][1] if checks else None


def local_noon(d: date, tz: ZoneInfo) -> float:
    return datetime(d.year, d.month, d.day, 12, tzinfo=tz).timestamp()
