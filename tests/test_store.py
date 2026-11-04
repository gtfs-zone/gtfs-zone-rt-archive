"""store: segment rollover, raw dedup, crash recovery and HH.b segments."""

import base64

from gtfs_zone_rt_archive import store
from gtfs_zone_rt_archive.store import Writer, read_segment, segments

# 2026-11-05 10:00:00 UTC
T = 1_793_872_800.0


def _raw(w, t, url, body):
    rec = {"t": t, "c": t, "feed": "vp", "url": url, "status": 200}
    w.write(w.raw_line(rec, body), at=t)


def _all(feed_dir):
    return [line for p in segments(feed_dir) for line in read_segment(p)]


def test_rollover_compresses_the_closed_hour(tmp_path):
    done = []
    w = Writer(tmp_path, background=False, on_compressed=done.append)
    _raw(w, T, "u", b"a")
    _raw(w, T + 3600, "u", b"b")
    w.close()

    day = tmp_path / "2026-11-05"
    assert sorted(p.name for p in day.iterdir()) == ["10.jsonl.zst", "11.jsonl"]
    assert done == [day / "10.jsonl.zst"]
    assert [base64.b64decode(line["b64"]) for line in _all(tmp_path)] == [b"a", b"b"]


def test_same_body_is_deduped_within_a_segment_and_filled_back(tmp_path):
    w = Writer(tmp_path, background=False)
    _raw(w, T, "u", b"a")
    _raw(w, T + 15, "u", b"a")
    _raw(w, T + 3600, "u", b"a")  # new segment: stored whole again
    w.close()

    plain = (tmp_path / "2026-11-05" / "11.jsonl").read_text()
    assert '"b64"' in plain
    stored = [line for p in segments(tmp_path) for line in store._read(p)]
    assert sum('"same":true' in s for s in stored) == 1
    assert all(base64.b64decode(line["b64"]) == b"a" for line in _all(tmp_path))


def test_error_lines_have_no_body(tmp_path):
    w = Writer(tmp_path, background=False)
    rec = {"t": T, "c": T, "feed": "tu", "url": "u", "error": "ConnectError()"}
    w.write(w.raw_line(rec, None), at=T)
    w.close()
    [line] = _all(tmp_path)
    assert "b64" not in line
    assert "sha" not in line
    assert line["error"] == "ConnectError()"


def test_a_line_timed_before_the_open_segment_stays_in_it(tmp_path):
    w = Writer(tmp_path, background=False)
    _raw(w, T + 3600, "u", b"a")
    _raw(w, T + 3599, "u", b"b")
    w.close()
    assert [p.name for p in segments(tmp_path)] == ["11.jsonl"]


def test_startup_compresses_stale_plain_segments(tmp_path):
    day = tmp_path / "2026-11-05"
    day.mkdir()
    (day / "10.jsonl").write_text('{"k":"raw","t":1,"url":"u"}\n')
    done = []
    Writer(tmp_path, background=False, on_compressed=done.append).close()
    assert done == [day / "10.jsonl.zst"]
    assert not (day / "10.jsonl").exists()


def test_startup_drops_a_plain_twin_of_a_compressed_segment(tmp_path):
    day = tmp_path / "2026-11-05"
    day.mkdir()
    (day / "10.jsonl").write_text('{"k":"raw","t":1}\n')
    store.compress(day / "10.jsonl")
    (day / "10.jsonl").write_text('{"k":"raw","t":1}\n')
    (day / "10.jsonl.zst.tmp").write_bytes(b"partial")
    Writer(tmp_path, background=False).close()
    assert sorted(p.name for p in day.iterdir()) == ["10.jsonl.zst"]


def test_a_torn_last_line_is_ended_and_skipped_on_read(tmp_path, monkeypatch):
    monkeypatch.setattr(store.time, "time", lambda: T + 60)
    day = tmp_path / "2026-11-05"
    day.mkdir()
    (day / "10.jsonl").write_text('{"k":"raw","t":1,"url":"u"}\n{"k":"ra')
    w = Writer(tmp_path, background=False)
    _raw(w, T + 60, "u", b"a")
    w.close()
    lines = _all(tmp_path)
    assert [line["t"] for line in lines] == [1, T + 60]


def test_an_hour_already_compressed_continues_as_b(tmp_path):
    w = Writer(tmp_path, background=False)
    _raw(w, T, "u", b"a")
    w.close(compress_open=True)
    w = Writer(tmp_path, background=False)
    _raw(w, T + 60, "u", b"b")
    w.close(compress_open=True)

    day = tmp_path / "2026-11-05"
    assert sorted(p.name for p in day.iterdir()) == ["10.b.jsonl.zst", "10.jsonl.zst"]
    assert [base64.b64decode(line["b64"]) for line in _all(tmp_path)] == [b"a", b"b"]


def test_a_restart_within_the_hour_appends_to_the_open_segment(tmp_path, monkeypatch):
    monkeypatch.setattr(store.time, "time", lambda: T + 120)
    w = Writer(tmp_path, background=False)
    _raw(w, T + 60, "u", b"a")
    w.close()
    w = Writer(tmp_path, background=False)
    _raw(w, T + 120, "u", b"a")
    w.close()
    assert [p.name for p in segments(tmp_path)] == ["10.jsonl"]
    assert len(_all(tmp_path)) == 2


def test_background_compression_runs_the_hook(tmp_path):
    done = []
    w = Writer(tmp_path, on_compressed=done.append)
    _raw(w, T, "u", b"a")
    _raw(w, T + 3600, "u", b"b")
    w.close()
    assert done == [tmp_path / "2026-11-05" / "10.jsonl.zst"]
