"""tides: delay propagation, prediction dedup and withdrawal, performed trips."""

from datetime import date

from google.transit import gtfs_realtime_pb2
from helpers import gtfs_zip, local, lon_at, tu_message, vp_message

from gtfs_zone_rt_archive.gtfs import Gtfs
from gtfs_zone_rt_archive.raw import Poll
from gtfs_zone_rt_archive.tides import DayBuilder, iso, predict

D = date(2026, 11, 5)
SD = "20261105"
G = Gtfs(gtfs_zip())
T1 = G.trips["T1"]
SCHED = G.schedule(T1, D)
A, B, C = (SCHED[i][0] for i in range(3))
B_M = T1.line.dists[1]
C_M = T1.line.dists[2]
STU = gtfs_realtime_pb2.TripUpdate.StopTimeUpdate


def _tu(build):
    tu = gtfs_realtime_pb2.TripUpdate()
    tu.trip.trip_id = "T1"
    build(tu)
    return tu


def test_delay_propagates_to_later_stops_and_not_before_the_first_update():
    def build(tu):
        tu.stop_time_update.add(stop_sequence=2).arrival.delay = 120

    states = predict(T1, SCHED, _tu(build))
    assert 0 not in states
    assert states[1] == (B + 120, B + 120, "Scheduled")
    assert states[2] == (C + 120, C + 120, "Scheduled")


def test_absolute_times_set_the_delay_and_match_by_stop_id():
    def build(tu):
        stu = tu.stop_time_update.add(stop_id="B")
        stu.arrival.time = B + 60
        stu.departure.time = B + 90

    states = predict(T1, SCHED, _tu(build))
    assert states[1] == (B + 60, B + 90, "Scheduled")
    assert states[2] == (C + 90, C + 90, "Scheduled")


def test_skipped_and_no_data_stops():
    def build(tu):
        tu.stop_time_update.add(stop_sequence=1).departure.delay = 60
        tu.stop_time_update.add(stop_sequence=2, schedule_relationship=STU.SKIPPED)
        tu.stop_time_update.add(stop_sequence=3, schedule_relationship=STU.NO_DATA)

    states = predict(T1, SCHED, _tu(build))
    assert states[0] == (A + 60, A + 60, "Scheduled")
    assert states[1] == (None, None, "Skipped")
    assert 2 not in states


def test_canceled_trip_skips_every_stop():
    def build(tu):
        tu.trip.schedule_relationship = gtfs_realtime_pb2.TripDescriptor.CANCELED

    assert set(predict(T1, SCHED, _tu(build)).values()) == {(None, None, "Skipped")}


def _builder():
    return DayBuilder(G, D, "test")


def _poll(c, vehicles=(), updates=(), resumed_after=None):
    return Poll(
        c,
        vp_message(c, list(vehicles)),
        tu_message(c, list(updates)),
        resumed_after,
    )


def _drive(b, start, until, step=30, vid="bus1", trip="T1", updates=None):
    """One poll every `step` s, the bus moving 830 m per 10 min from A."""
    for c in range(start, until, step):
        m = (c - A) / 600 * B_M
        ups = updates(c) if updates else []
        b.add(_poll(c, [(vid, trip, SD, lon_at(m), c)], ups))


def _predictions(pkg, seq):
    return [
        (r["prediction_timestamp"], r["predicted_arrival_time"])
        for r in pkg.tables["stop_time_predictions"]
        if r["trip_stop_sequence"] == seq
    ]


def test_unchanged_predictions_are_written_once():
    b = _builder()
    _drive(b, A - 60, C + 60, updates=lambda c: [("T1", SD, [(3, 60)])])
    pkg = b.finish()
    assert _predictions(pkg, 3) == [(iso(A - 60), iso(C + 60))]


def test_a_stop_leaving_the_feed_before_its_actual_is_withdrawn():
    b = _builder()

    def ups(c):
        return [("T1", SD, [(3, 60)])] if c < A + 300 else [("T1", SD, [])]

    _drive(b, A - 60, C + 60, updates=ups)
    assert _predictions(b.finish(), 3) == [
        (iso(A - 60), iso(C + 60)),
        (iso(A + 300), None),
    ]


def test_a_recording_gap_withdraws_open_predictions():
    b = _builder()
    b.add(_poll(A, [("bus1", "T1", SD, lon_at(0), A)], [("T1", SD, [(3, 0)])]))
    b.add(
        _poll(
            A + 400,
            [("bus1", "T1", SD, lon_at(B_M), A + 400)],
            [("T1", SD, [(3, 0)])],
            resumed_after=A,
        )
    )
    assert _predictions(b.finish(), 3) == [
        (iso(A), iso(C)),
        (iso(A + 1), None),
        (iso(A + 400), iso(C)),
    ]


def test_stop_visits_and_trips_performed_use_the_gps():
    b = _builder()
    _drive(b, A - 60, C + 120)
    pkg = b.finish()
    (trip,) = pkg.tables["trips_performed"]
    assert trip["trip_id_performed"] == "T1"
    assert trip["vehicle_id"] == "bus1"
    assert trip["route_type"] == "Bus"
    assert trip["schedule_trip_start"] == iso(A)
    visits = pkg.tables["stop_visits"]
    assert [v["trip_stop_sequence"] for v in visits] == [1, 2, 3]
    assert visits[0]["actual_arrival_time"] is None
    assert visits[1]["schedule_arrival_time"] == iso(B)
    assert visits[1]["actual_arrival_time"] is not None
    assert all(v["service_date"] == "2026-11-05" for v in visits)


def test_two_vehicles_on_a_trip_the_one_with_more_fixes_owns_it():
    b = _builder()
    _drive(b, A - 60, B, vid="bus1")
    b.add(_poll(A + 30, [("bus2", "T1", SD, lon_at(C_M), A + 30)]))
    pkg = b.finish()
    assert pkg.tables["trips_performed"][0]["vehicle_id"] == "bus1"
    pings = {r["vehicle_id"]: r for r in pkg.tables["vehicle_locations"]}
    assert pings["bus1"]["trip_id_performed"] == "T1"
    assert "trip_id_performed" not in pings["bus2"]


def test_trips_of_other_dates_and_without_gps_are_left_out():
    b = _builder()
    b.add(
        _poll(
            A,
            [("bus9", "T1", "20261104", lon_at(0), A)],
            [("T2", SD, [(1, 0)]), ("T1", "20261104", [(1, 0)])],
        )
    )
    pkg = b.finish()
    assert pkg.tables["trips_performed"] == []
    assert pkg.tables["vehicle_locations"] == []
    assert pkg.counts["trips_without_vehicle_locations"] == 1


def test_vehicle_locations_are_deduped_by_vehicle_and_timestamp():
    b = _builder()
    for c in (A, A + 15):
        b.add(_poll(c, [("bus1", "T1", SD, lon_at(0), A)]))
    (ping,) = b.finish().tables["vehicle_locations"]
    assert ping["location_ping_id"] == f"bus1:{A}"


def test_unassigned_fixes_are_kept_on_their_local_date():
    b = _builder()
    noon = local("2026-11-05", "12:00:00")
    late = local("2026-11-06", "01:00:00")
    b.add(_poll(noon, [("bus1", None, None, lon_at(0), noon)]))
    b.add(_poll(late, [("bus1", None, None, lon_at(0), late)]))
    (ping,) = b.finish().tables["vehicle_locations"]
    assert ping["event_timestamp"] == iso(noon)


def test_an_update_older_than_a_withdrawal_comes_back_after_it():
    b = _builder()
    fix = [("bus1", "T1", SD, lon_at(0), A)]
    tu = tu_message(A, [("T1", SD, [(3, 0)])])
    tu.entity[0].trip_update.timestamp = A - 5
    b.add(Poll(A, vp_message(A, fix), tu))
    b.add(Poll(A + 15, vp_message(A + 15, fix), tu_message(A + 15, [])))
    late = tu_message(A + 30, [("T1", SD, [(3, 60)])])
    late.entity[0].trip_update.timestamp = A + 10  # stamped before the withdrawal
    b.add(Poll(A + 30, vp_message(A + 30, fix), late))
    assert _predictions(b.finish(), 3) == [
        (iso(A - 5), iso(C)),
        (iso(A + 15), None),
        (iso(A + 16), iso(C + 60)),
    ]


def test_two_values_at_one_timestamp_keep_the_newer():
    b = _builder()
    fix = [("bus1", "T1", SD, lon_at(0), A)]
    for c, delay in ((A, 0), (A + 15, 60)):
        tu = tu_message(c, [("T1", SD, [(3, delay)])])
        tu.entity[0].trip_update.timestamp = A
        b.add(Poll(c, vp_message(c, fix), tu))
    assert _predictions(b.finish(), 3) == [(iso(A), iso(C + 60))]
