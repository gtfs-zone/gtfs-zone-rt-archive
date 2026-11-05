"""gtfs: schedules, service dates of observations and the live snapshot."""

import json
from datetime import date

from helpers import gtfs_zip, local

from gtfs_zone_rt_archive.gtfs import Gtfs, pick_static, seconds

D = date(2026, 11, 5)


def test_seconds_allows_hours_past_24():
    assert seconds("25:01:02") == 90062
    assert seconds("") is None


def test_schedule_is_relative_to_noon_minus_12h_on_dst_days():
    g = Gtfs(gtfs_zip())
    # 2026-11-01 is the US fall-back day: 08:00 is 9 h after noon - 12 h.
    sched = g.schedule(g.trips["T1"], date(2026, 11, 1))
    assert sched[0][1] == local("2026-11-01", "07:00:00")


def test_blank_times_are_interpolated_by_distance():
    g = Gtfs(
        gtfs_zip(
            [
                ("T1", "08:00:00", "08:00:00", "A", 1),
                ("T1", "", "", "B", 2),
                ("T1", "08:20:00", "08:20:00", "C", 3),
            ]
        )
    )
    sched = g.schedule(g.trips["T1"], D)
    assert sched[1] == (local("2026-11-05", "08:10:00"),) * 2


def test_frequency_trips_shift_to_the_start_time():
    g = Gtfs(gtfs_zip(frequencies=[("T1", "06:00:00", "10:00:00", 600)]))
    trip = g.trips["T1"]
    assert trip.frequency
    sched = g.schedule(trip, D, "06:30:00")
    assert sched[0][1] == local("2026-11-05", "06:30:00")
    assert sched[2][0] == local("2026-11-05", "06:50:00")
    assert g.schedule(trip, D) is None


def test_service_date_prefers_the_instance_running_at_the_observation():
    g = Gtfs(
        gtfs_zip(
            [
                ("T1", "23:30:00", "23:30:00", "A", 1),
                ("T1", "24:10:00", "24:10:00", "B", 2),
                ("T1", "24:40:00", "24:40:00", "C", 3),
            ]
        )
    )
    trip = g.trips["T1"]
    # 00:20 on the 6th is inside the 5th's instance.
    assert g.service_date(trip, local("2026-11-06", "00:20:00"), None) == D
    assert g.service_date(trip, local("2026-11-05", "22:00:00"), None) == D
    assert g.service_date(trip, local("2026-11-06", "12:00:00"), None) == date(
        2026, 11, 6
    )


def test_pick_static_takes_the_last_check_before_else_the_first_after():
    log = "".join(
        json.dumps(line) + "\n"
        for line in [
            {"k": "static", "t": 100, "sha8": "aaaa0000"},
            {"k": "static", "t": 150, "error": "boom"},
            {"k": "static", "t": 200, "sha8": "bbbb0000"},
        ]
    ).encode()
    assert pick_static(log, 199) == "aaaa0000"
    assert pick_static(log, 250) == "bbbb0000"
    assert pick_static(log, 50) == "aaaa0000"
    assert pick_static(b"", 50) is None
