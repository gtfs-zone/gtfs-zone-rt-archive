"""TIDES tables for one feed and service date from its recorded polls.

Feed polls to DayBuilder.add() in time order, then finish(). Only trips of
the builder's service date are kept; a trip is performed when it has at least
one VehiclePosition fix, and its stop_visits actuals come from that GPS.
stop_time_predictions holds a row per change of a stop's predicted times, a
withdrawal row (null times) when the stop leaves the feed before its actual
time or the recording has a gap, and delays propagated to later stops with no
update, as GTFS-RT consumers do.
"""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from importlib import resources
from typing import TYPE_CHECKING, Any
from zoneinfo import ZoneInfo

from google.transit import gtfs_realtime_pb2
from tzfpy import get_tz

from gtfs_zone_rt_archive.actuals import actual_times

if TYPE_CHECKING:
    from gtfs_zone_rt_archive.actuals import Actual
    from gtfs_zone_rt_archive.gtfs import Gtfs, Trip
    from gtfs_zone_rt_archive.raw import FeedMessage, Poll

TABLES = (
    "trips_performed",
    "stop_visits",
    "vehicle_locations",
    "stop_time_predictions",
)

_STU = gtfs_realtime_pb2.TripUpdate.StopTimeUpdate
_TRIP = gtfs_realtime_pb2.TripDescriptor
_VP = gtfs_realtime_pb2.VehiclePosition
_STATUS = {
    _VP.INCOMING_AT: "Incoming at",
    _VP.STOPPED_AT: "Stopped at",
    _VP.IN_TRANSIT_TO: "In transit to",
}

# (predicted arrival, predicted departure, schedule_relationship); all None
# is a withdrawal.
State = tuple[int | None, int | None, str | None]
WITHDRAWN: State = (None, None, None)


def columns(table: str) -> list[str]:
    """Field names of a vendored TIDES schema, in schema order."""
    text = resources.files("gtfs_zone_rt_archive").joinpath(
        f"schemas/{table}.schema.json"
    )
    return [f["name"] for f in json.loads(text.read_text())["fields"]]


def iso(t: float | None) -> str | None:
    if t is None:
        return None
    return datetime.fromtimestamp(t, UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _start_date(desc: Any) -> date | None:  # noqa: ANN401 - protobuf message
    if not desc.start_date:
        return None
    try:
        return datetime.strptime(desc.start_date, "%Y%m%d").date()
    except ValueError:
        return None


def predict(
    trip: Trip,
    sched: list[tuple[int, int]],
    tu: Any,  # noqa: ANN401 - protobuf message
) -> dict[int, State]:
    """Predicted state per stop index for one TripUpdate.

    StopTimeUpdates match by stop_sequence, else by stop_id after the
    previous match. Delay-only events become scheduled + delay, and the last
    delay carries on to later stops until the next update. Stops before the
    first update get no prediction; NO_DATA clears the carried delay.
    """
    if tu.trip.schedule_relationship == _TRIP.CANCELED:
        return {i: (None, None, "Skipped") for i in range(len(sched))}
    by_index: dict[int, Any] = {}
    after = -1
    for stu in tu.stop_time_update:
        idx = None
        if stu.HasField("stop_sequence"):
            idx = trip.index_of_sequence.get(stu.stop_sequence)
        elif stu.stop_id:
            idx = next(
                (
                    i
                    for i in range(after + 1, len(trip.stop_times))
                    if trip.stop_times[i].stop_id == stu.stop_id
                ),
                None,
            )
        if idx is not None and idx not in by_index:
            by_index[idx] = stu
            after = max(after, idx)
    delay: int | None = None
    if not by_index and tu.HasField("delay"):
        delay = tu.delay
    first = min(by_index) if by_index else 0
    out: dict[int, State] = {}
    for i in range(first, len(sched)):
        s_arr, s_dep = sched[i]
        stu = by_index.get(i)
        if stu is not None:
            if stu.schedule_relationship == _STU.SKIPPED:
                out[i] = (None, None, "Skipped")
                continue
            if stu.schedule_relationship == _STU.NO_DATA:
                delay = None
                continue
            arr = _event(stu, "arrival", s_arr, delay)
            if arr is not None:
                delay = arr - s_arr
            dep = _event(stu, "departure", s_dep, delay)
            if dep is not None:
                delay = dep - s_dep
            if arr is None and dep is not None:
                arr = s_arr + (dep - s_dep)
        elif delay is None:
            continue
        else:
            arr, dep = s_arr + delay, s_dep + delay
        if arr is None or dep is None:
            continue
        out[i] = (arr, max(arr, dep), "Scheduled")
    return out


def _event(stu: Any, name: str, scheduled: int, delay: int | None) -> int | None:  # noqa: ANN401
    if stu.HasField(name):
        ev = getattr(stu, name)
        if ev.HasField("time") and ev.time:
            return ev.time
        if ev.HasField("delay"):
            return scheduled + ev.delay
    return None if delay is None else scheduled + delay


def _state(row: dict[str, Any]) -> State:
    return (
        row["predicted_arrival_time"],
        row["predicted_departure_time"],
        row["schedule_relationship"],
    )


@dataclass
class _Performed:
    trip: Trip
    start_time: str | None
    sched: list[tuple[int, int]]
    # vehicle id -> {timestamp: (lat, lon)}
    fixes: dict[str, dict[int, tuple[float, float]]] = field(
        default_factory=lambda: defaultdict(dict)
    )
    # stop index -> rows, in time order
    rows: dict[int, list[dict[str, Any]]] = field(
        default_factory=lambda: defaultdict(list)
    )
    last: dict[int, State] = field(default_factory=dict)
    # Timestamp of the last TripUpdate used; older ones are skipped.
    last_tu_ts: int = 0


@dataclass
class Package:
    tables: dict[str, list[dict[str, Any]]]
    counts: dict[str, int]


_EASTERN = "America/New_York"
# Slack for clock skew when rejecting re-read timestamps that land after the poll.
_FUTURE_SLACK = 120


def eastern_wall_clock_fix(ts: int, lon: float, lat: float, c: float) -> int:
    """Re-read `ts`, a local wall clock taken as Eastern, in its real zone.

    The zone is the one at the fix, or Eastern; the latest reading not after
    the poll at `c` wins, else the reading in the zone at the fix.
    """
    wall = datetime.fromtimestamp(ts, ZoneInfo(_EASTERN)).replace(tzinfo=None)
    geo = get_tz(lon, lat)
    if not geo or geo.startswith("Etc/"):
        geo = _EASTERN
    readings = [
        int(wall.replace(tzinfo=ZoneInfo(z)).timestamp())
        for z in dict.fromkeys([geo, _EASTERN])
    ]
    past = [r for r in readings if r <= c + _FUTURE_SLACK]
    return max(past) if past else readings[0]


class DayBuilder:
    def __init__(
        self,
        gtfs: Gtfs,
        service_date: date,
        producer: str,
        eastern_wall_clock_until: float | None = None,
    ) -> None:
        self.gtfs = gtfs
        self.date = service_date
        self.producer = producer
        self.eastern_wall_clock_until = eastern_wall_clock_until
        # (vehicle id, recorded timestamp) to its corrected value, this poll only
        self.fixed_ts: dict[tuple[str, int], int] = {}
        self.trips: dict[str, _Performed] = {}
        self.pings: dict[str, dict[str, Any]] = {}
        self.skipped: Counter[str] = Counter()

    def trip_id_performed(self, trip: Trip, start_time: str | None) -> str:
        return f"{trip.trip_id}@{start_time}" if trip.frequency else trip.trip_id

    def _resolve(
        self,
        desc: Any,  # noqa: ANN401 - protobuf TripDescriptor
        t: float,
    ) -> tuple[_Performed | None, date | None]:
        """The performed trip of service date D a descriptor names, if any."""
        if not desc.trip_id:
            return None, None
        trip = self.gtfs.trips.get(desc.trip_id)
        if trip is None:
            self.skipped["unknown_trip"] += 1
            return None, None
        start_time = desc.start_time or None
        d = _start_date(desc) or self.gtfs.service_date(trip, t, start_time)
        if d != self.date:
            return None, d
        key = self.trip_id_performed(trip, start_time)
        perf = self.trips.get(key)
        if perf is None:
            sched = self.gtfs.schedule(trip, d, start_time)
            if sched is None:
                self.skipped["no_schedule"] += 1
                return None, d
            perf = self.trips[key] = _Performed(trip, start_time, sched)
        return perf, d

    def add(self, poll: Poll) -> None:
        if poll.resumed_after is not None:
            self.withdraw_all(poll.resumed_after)
        self.fixed_ts.clear()
        if poll.vp is not None:
            self._add_vp(poll.vp, poll.c)
        if poll.tu is not None:
            self._add_tu(poll.tu, poll.c)

    def _add_vp(self, msg: FeedMessage, c: float) -> None:
        header_ts = msg.header.timestamp or int(c)
        for ent in msg.entity:
            if not ent.HasField("vehicle"):
                continue
            vp = ent.vehicle
            if not vp.HasField("position"):
                continue
            vid = vp.vehicle.id or vp.vehicle.label or ent.id
            ts = vp.timestamp or header_ts
            until = self.eastern_wall_clock_until
            if vp.timestamp and until is not None and c < until:
                pos = vp.position
                fixed = eastern_wall_clock_fix(ts, pos.longitude, pos.latitude, c)
                self.fixed_ts[(vid, ts)] = fixed
                ts = fixed
            ping_id = f"{vid}:{ts}"
            if ping_id in self.pings:
                continue
            perf, d = (
                self._resolve(vp.trip, ts) if vp.HasField("trip") else (None, None)
            )
            if perf is None and d is None:
                d = datetime.fromtimestamp(ts, self.gtfs.tz).date()
            if d != self.date:
                continue
            pos = vp.position
            row: dict[str, Any] = {
                "location_ping_id": ping_id,
                "service_date": self.date.isoformat(),
                "event_timestamp": iso(ts),
                "vehicle_id": vid,
                "latitude": round(pos.latitude, 6),
                "longitude": round(pos.longitude, 6),
                "heading": round(pos.bearing, 1) if pos.HasField("bearing") else None,
                "speed": round(pos.speed, 2) if pos.HasField("speed") else None,
                "current_status": _STATUS.get(vp.current_status)
                if vp.HasField("current_status")
                else None,
                "stop_id": vp.stop_id or None,
            }
            if perf is not None:
                key = self.trip_id_performed(perf.trip, perf.start_time)
                row["_trip"] = key
                row["trip_id_scheduled"] = perf.trip.trip_id
                if vp.HasField("current_stop_sequence"):
                    idx = perf.trip.index_of_sequence.get(vp.current_stop_sequence)
                    row["scheduled_stop_sequence"] = vp.current_stop_sequence
                    row["trip_stop_sequence"] = idx + 1 if idx is not None else None
                perf.fixes[vid][ts] = (pos.latitude, pos.longitude)
            self.pings[ping_id] = row

    def _add_tu(self, msg: FeedMessage, c: float) -> None:
        header_ts = msg.header.timestamp or int(c)
        seen: set[str] = set()
        for ent in msg.entity:
            if not ent.HasField("trip_update"):
                continue
            tu = ent.trip_update
            ts = tu.timestamp or header_ts
            vid = (tu.vehicle.id or None) if tu.HasField("vehicle") else None
            if vid is not None:
                ts = self.fixed_ts.get((vid, ts), ts)
            perf, _ = self._resolve(tu.trip, ts)
            if perf is None:
                continue
            key = self.trip_id_performed(perf.trip, perf.start_time)
            if key in seen or ts < perf.last_tu_ts:
                continue  # a second update for the trip, or an older one
            seen.add(key)
            states = predict(perf.trip, perf.sched, tu)
            for idx in set(perf.last) | set(states):
                self._emit(perf, idx, ts, states.get(idx, WITHDRAWN), vid)
            perf.last_tu_ts = ts
        for key, perf in self.trips.items():
            if key not in seen and perf.last:
                self._withdraw(perf, header_ts)

    def _emit(
        self, perf: _Performed, idx: int, ts: int, state: State, vid: str | None
    ) -> None:
        """Append a row when the stop's state changed.

        Rows of one stop stay in time order, each after the previous one. A
        prediction with the same timestamp as a previous prediction replaces
        it; anything else is moved past the previous row.
        """
        if state == perf.last.get(idx, WITHDRAWN):
            return
        rows = perf.rows[idx]
        if rows:
            prev = rows[-1]["prediction_timestamp"]
            both = state != WITHDRAWN and _state(rows[-1]) != WITHDRAWN
            ts = max(ts, prev) if both else max(ts, prev + 1)
        row = {
            "prediction_timestamp": ts,
            "vehicle_id": vid,
            "predicted_arrival_time": state[0],
            "predicted_departure_time": state[1],
            "schedule_relationship": state[2],
        }
        if rows and rows[-1]["prediction_timestamp"] == ts:
            rows.pop()  # same key as the previous row: the newer values win
            if rows and _state(rows[-1]) == state:
                row = None  # back to what the row before said
        if row is not None:
            rows.append(row)
        if state == WITHDRAWN:
            perf.last.pop(idx, None)
        else:
            perf.last[idx] = state

    def _withdraw(self, perf: _Performed, ts: float) -> None:
        for idx in list(perf.last):
            self._emit(perf, idx, int(ts), WITHDRAWN, None)

    def withdraw_all(self, ts: float) -> None:
        """Close every open prediction at `ts` (a recording gap began)."""
        for perf in self.trips.values():
            if perf.last:
                self._withdraw(perf, ts)

    def finish(self) -> Package:
        d = self.date.isoformat()
        tables: dict[str, list[dict[str, Any]]] = {t: [] for t in TABLES}
        dropped = 0
        owners: dict[str, str] = {}
        for key, perf in sorted(self.trips.items()):
            if not perf.fixes:
                dropped += bool(perf.rows)
                continue
            owner = max(perf.fixes, key=lambda v: (len(perf.fixes[v]), v))
            owners[key] = owner
            acts: dict[int, Actual] = {}
            if perf.trip.line is not None:
                line = perf.trip.line
                acts = actual_times(
                    line,
                    [
                        line.fix(ts, lat, lon)
                        for ts, (lat, lon) in perf.fixes[owner].items()
                    ],
                )
            tables["trips_performed"].append(self._trip_row(key, perf, owner, acts))
            tables["stop_visits"].extend(self._visit_rows(key, perf, owner, acts))
            tables["stop_time_predictions"].extend(
                self._prediction_rows(key, perf, owner, acts)
            )
        for row in sorted(self.pings.values(), key=lambda r: r["location_ping_id"]):
            key = row.pop("_trip", None)
            if key is not None and owners.get(key) == row["vehicle_id"]:
                row["trip_id_performed"] = key
            tables["vehicle_locations"].append(row)
        for row in tables["trips_performed"] + tables["stop_visits"]:
            row["service_date"] = d
        counts = {t: len(rows) for t, rows in tables.items()}
        counts["trips_without_vehicle_locations"] = dropped
        counts.update({f"skipped_{k}": v for k, v in sorted(self.skipped.items())})
        return Package(tables, counts)

    def _trip_row(
        self, key: str, perf: _Performed, owner: str, acts: dict[int, Actual]
    ) -> dict[str, Any]:
        trip, sts = perf.trip, perf.trip.stop_times
        times = [
            t for a in acts.values() for t in (a.arrival, a.departure) if t is not None
        ]
        return {
            "trip_id_performed": key,
            "vehicle_id": owner,
            "trip_id_scheduled": trip.trip_id,
            "route_id": trip.route_id,
            "route_type": self.gtfs.route_type.get(trip.route_id),
            "shape_id": trip.shape_id,
            "direction_id": trip.direction_id,
            "block_id": trip.block_id,
            "trip_start_stop_id": sts[0].stop_id,
            "trip_end_stop_id": sts[-1].stop_id,
            "schedule_trip_start": iso(perf.sched[0][1]),
            "schedule_trip_end": iso(perf.sched[-1][0]),
            "actual_trip_start": iso(min(times)) if times else None,
            "actual_trip_end": iso(max(times)) if times else None,
            "trip_type": "In service",
            "schedule_relationship": "Scheduled",
        }

    def _visit_rows(
        self, key: str, perf: _Performed, owner: str, acts: dict[int, Actual]
    ) -> list[dict[str, Any]]:
        out = []
        for i, st in enumerate(perf.trip.stop_times):
            a = acts.get(i)
            dwell = None
            if a is not None and not a.approx and a.arrival is not None:
                dwell = a.departure - a.arrival
            out.append(
                {
                    "trip_id_performed": key,
                    "trip_stop_sequence": i + 1,
                    "scheduled_stop_sequence": st.stop_sequence,
                    "vehicle_id": owner,
                    "dwell": dwell,
                    "stop_id": st.stop_id,
                    "timepoint": st.timepoint,
                    "schedule_arrival_time": iso(perf.sched[i][0]),
                    "schedule_departure_time": iso(perf.sched[i][1]),
                    "actual_arrival_time": iso(a.arrival) if a else None,
                    "actual_departure_time": iso(a.departure) if a else None,
                    "schedule_relationship": "Scheduled",
                }
            )
        return out

    def _prediction_rows(
        self, key: str, perf: _Performed, owner: str, acts: dict[int, Actual]
    ) -> list[dict[str, Any]]:
        out = []
        for i, rows in sorted(perf.rows.items()):
            act = acts.get(i)
            st = perf.trip.stop_times[i]
            for row in rows:
                withdrawn = (
                    row["predicted_arrival_time"] is None
                    and row["schedule_relationship"] is None
                )
                if (
                    withdrawn
                    and act is not None
                    and row["prediction_timestamp"] >= act.time
                ):
                    continue  # the stop left the feed because it was served
                out.append(
                    {
                        "service_date": self.date.isoformat(),
                        "trip_id_performed": key,
                        "trip_stop_sequence": i + 1,
                        "producer": self.producer,
                        "prediction_timestamp": iso(row["prediction_timestamp"]),
                        "trip_id_scheduled": perf.trip.trip_id,
                        "scheduled_stop_sequence": st.stop_sequence,
                        "vehicle_id": row["vehicle_id"] or owner,
                        "stop_id": st.stop_id,
                        "predicted_arrival_time": iso(row["predicted_arrival_time"]),
                        "predicted_departure_time": iso(
                            row["predicted_departure_time"]
                        ),
                        "schedule_relationship": row["schedule_relationship"],
                    }
                )
        return out
