"""actuals: near-stop and interpolated actual times, as build.py computed them."""

from helpers import gtfs_zip, lon_at

from gtfs_zone_rt_archive.actuals import actual_times
from gtfs_zone_rt_archive.gtfs import Gtfs

LINE = Gtfs(gtfs_zip()).trips["T1"].line
B = LINE.dists[1]  # about 830 m


def _fixes(points):
    return [LINE.fix(ts, 42.0, lon_at(m)) for ts, m in points]


def test_fixes_near_a_stop_give_first_and_last_as_arrival_and_departure():
    acts = actual_times(
        LINE,
        _fixes([(0, 0), (30, 10), (60, 200), (100, B - 50), (130, B), (160, B + 40)]),
    )
    assert acts[0].arrival is None  # the origin has departure only
    assert acts[0].departure == 30
    assert (acts[1].arrival, acts[1].departure, acts[1].approx) == (100, 160, False)


def test_stop_without_a_near_fix_gets_the_interpolated_crossing():
    acts = actual_times(LINE, _fixes([(0, 0), (60, B - 200), (100, B + 200)]))
    assert acts[1].approx
    assert acts[1].arrival == acts[1].departure == 80


def test_origin_departs_after_the_offset_when_no_fix_is_near():
    # The first fix projects onto A itself; 50 m past it is a fifth of the way.
    acts = actual_times(LINE, _fixes([(0, -150), (100, 250)]))
    assert (acts[0].arrival, acts[0].departure, acts[0].approx) == (None, 20, True)


def test_no_interpolation_across_a_gap():
    acts = actual_times(LINE, _fixes([(0, 0), (60, B - 200), (400, B + 200)]))
    assert 1 not in acts


def test_projection_jumps_faster_than_max_speed_are_dropped():
    # 2 km in 10 s is dropped; the vehicle then reaches B at 200 s.
    acts = actual_times(
        LINE, _fixes([(0, 0), (10, 2 * B + 300), (150, B - 300), (250, B + 200)])
    )
    assert 2 not in acts
    assert acts[1].approx and acts[1].arrival == 210


def test_backwards_projection_does_not_reach_a_stop_early():
    # The backwards fix holds the vehicle at B - 300 until 90 s.
    acts = actual_times(
        LINE, _fixes([(0, 0), (60, B - 300), (90, B - 400), (150, B + 300)])
    )
    assert acts[1].arrival == 120


def test_unreached_stops_are_absent():
    acts = actual_times(LINE, _fixes([(0, 0), (60, 300)]))
    assert set(acts) == {0}
