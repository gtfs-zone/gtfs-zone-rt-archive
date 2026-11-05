"""raw: segment selection, poll grouping and coverage intervals."""

from datetime import UTC, datetime

import pytest
from helpers import put_segment, raw_lines, tu_message, vp_message
from obstore.store import MemoryStore

from gtfs_zone_rt_archive.raw import Recording, segment_keys

H13 = 1793970000 - 1793970000 % 3600  # an hour boundary on 2026-11-06 UTC


@pytest.fixture
def store():
    return MemoryStore()


def _poll(c, ok=True):
    return raw_lines(c, vp_message(c, []), tu_message(c, []), 200 if ok else 503)


def _hour(t):
    return datetime.fromtimestamp(t, UTC).strftime("%Y-%m-%d/%H")


def test_segment_keys_cover_the_window_in_order(store):
    for h, suffix in [(-1, ""), (0, ""), (0, ".b"), (1, ""), (2, "")]:
        put_segment(store, "f", f"{_hour(H13 + h * 3600)}{suffix}.jsonl.zst", [])
    keys = segment_keys(store, "f", H13 + 10, H13 + 3600 + 10)
    assert keys == [
        f"raw/f/{_hour(H13)}.jsonl.zst",
        f"raw/f/{_hour(H13)}.b.jsonl.zst",
        f"raw/f/{_hour(H13 + 3600)}.jsonl.zst",
    ]


def test_polls_group_lines_and_coverage_splits_on_gaps_and_restarts(store):
    lines = [{"k": "start", "t": H13, "feed": "f"}]
    for c in (H13 + 0, H13 + 15, H13 + 30):
        lines += _poll(c)
    lines += _poll(H13 + 45, ok=False)
    lines += _poll(H13 + 300)  # after a 270 s gap
    lines += [{"k": "start", "t": H13 + 310, "feed": "f"}]
    lines += _poll(H13 + 315)  # restart, 15 s later
    put_segment(store, "f", f"{_hour(H13)}.jsonl.zst", lines)

    rec = Recording(store, "f", H13, H13 + 3600)
    polls = list(rec.polls())
    assert [p.c for p in polls] == [H13 + s for s in (0, 15, 30, 45, 300, 315)]
    assert [p.good for p in polls] == [True, True, True, False, True, True]
    assert polls[4].resumed_after == H13 + 30
    assert polls[5].resumed_after == H13 + 300
    assert rec.coverage == [
        (H13, H13 + 30),
        (H13 + 300, H13 + 300),
        (H13 + 315, H13 + 315),
    ]


def test_polls_outside_the_window_are_skipped(store):
    put_segment(
        store, "f", f"{_hour(H13)}.jsonl.zst", _poll(H13 + 10) + _poll(H13 + 3000)
    )
    rec = Recording(store, "f", H13 + 20, H13 + 3600)
    assert [p.c for p in rec.polls()] == [H13 + 3000]
