"""Builders for tiny GTFS zips, GTFS-RT messages and raw segments."""

import base64
import hashlib
import io
import json
import zipfile
from datetime import datetime
from zoneinfo import ZoneInfo

import obstore
import zstandard
from google.transit import gtfs_realtime_pb2

TZ = ZoneInfo("America/New_York")
# Three stops 0.01 deg of longitude apart (about 830 m) at 42 N.
STOPS = [("A", 42.0, -73.80), ("B", 42.0, -73.79), ("C", 42.0, -73.78)]
LON_M = 111320 * 0.7431448  # metres per degree of longitude at 42 N


def local(day, hms):
    """Epoch of HH:MM:SS local time on a date, as an int."""
    return int(datetime.fromisoformat(f"{day}T{hms}").replace(tzinfo=TZ).timestamp())


def csv_text(header, rows):
    return "\n".join([",".join(header), *(",".join(map(str, r)) for r in rows)]) + "\n"


def gtfs_zip(stop_times=None, frequencies=None):
    """Trip T1 (route R1) A 08:00, B 08:10, C 08:20; T2 a second trip at 09:00."""
    if stop_times is None:
        stop_times = [
            ("T1", "08:00:00", "08:00:00", "A", 1),
            ("T1", "08:10:00", "08:10:00", "B", 2),
            ("T1", "08:20:00", "08:20:00", "C", 3),
            ("T2", "09:00:00", "09:00:00", "A", 1),
            ("T2", "09:10:00", "09:10:00", "B", 2),
            ("T2", "09:20:00", "09:20:00", "C", 3),
        ]
    trips = sorted({st[0] for st in stop_times})
    files = {
        "agency.txt": csv_text(
            ["agency_id", "agency_name", "agency_url", "agency_timezone"],
            [("X", "Test Transit", "https://example.com", "America/New_York")],
        ),
        "routes.txt": csv_text(
            ["route_id", "agency_id", "route_short_name", "route_type"],
            [("R1", "X", "1", 3)],
        ),
        "stops.txt": csv_text(
            ["stop_id", "stop_name", "stop_lat", "stop_lon"],
            [(s, s, lat, lon) for s, lat, lon in STOPS],
        ),
        "trips.txt": csv_text(
            ["route_id", "service_id", "trip_id", "direction_id", "shape_id"],
            [("R1", "S", t, 0, "SH1") for t in trips],
        ),
        "stop_times.txt": csv_text(
            ["trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence"],
            stop_times,
        ),
    }
    if frequencies:
        files["frequencies.txt"] = csv_text(
            ["trip_id", "start_time", "end_time", "headway_secs"], frequencies
        )
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w") as z:
        for name, text in files.items():
            z.writestr(name, text)
    return out.getvalue()


def lon_at(metres):
    """Longitude `metres` east of stop A along the line."""
    return STOPS[0][2] + metres / LON_M


def vp_message(ts, vehicles):
    """vehicles: [(vehicle_id, trip_id, start_date | None, lon, timestamp)]."""
    msg = gtfs_realtime_pb2.FeedMessage()
    msg.header.gtfs_realtime_version = "2.0"
    msg.header.timestamp = ts
    for vid, trip_id, start_date, lon, vts in vehicles:
        ent = msg.entity.add(id=vid)
        if trip_id:
            ent.vehicle.trip.trip_id = trip_id
            if start_date:
                ent.vehicle.trip.start_date = start_date
        ent.vehicle.vehicle.id = vid
        ent.vehicle.position.latitude = 42.0
        ent.vehicle.position.longitude = lon
        ent.vehicle.timestamp = vts
    return msg


def tu_message(ts, updates):
    """updates: [(trip_id, start_date, [(stop_sequence, arrival_delay)])]."""
    msg = gtfs_realtime_pb2.FeedMessage()
    msg.header.gtfs_realtime_version = "2.0"
    msg.header.timestamp = ts
    for trip_id, start_date, stus in updates:
        ent = msg.entity.add(id=trip_id)
        ent.trip_update.trip.trip_id = trip_id
        ent.trip_update.trip.start_date = start_date
        for seq, delay in stus:
            stu = ent.trip_update.stop_time_update.add(stop_sequence=seq)
            stu.arrival.delay = delay
    return msg


def raw_lines(c, vp=None, tu=None, status=200):
    out = []
    for kind, msg in (("vp", vp), ("tu", tu)):
        if msg is None:
            continue
        body = msg.SerializeToString()
        out.append(
            {
                "k": "raw",
                "t": c,
                "c": c,
                "feed": kind,
                "method": "GET",
                "url": f"http://x/{kind}",
                "status": status,
                "sha": hashlib.sha256(body).hexdigest()[:16],
                "b64": base64.b64encode(body).decode(),
            }
        )
    return out


def put_segment(store, feed, key_name, lines):
    """Store lines as raw/<feed>/<key_name> (e.g. 2026-11-05/13.jsonl.zst)."""
    text = "".join(json.dumps(line) + "\n" for line in lines).encode()
    obstore.put(
        store, f"raw/{feed}/{key_name}", zstandard.ZstdCompressor().compress(text)
    )


def put_static(store, feed, data, t):
    sha8 = hashlib.sha256(data).hexdigest()[:8]
    obstore.put(store, f"raw/{feed}/gtfs-{sha8}.zip", data)
    line = {"k": "static", "t": t, "sha8": sha8, "new": True, "last_modified": None}
    obstore.put(store, f"raw/{feed}/static.jsonl", (json.dumps(line) + "\n").encode())
    return sha8
