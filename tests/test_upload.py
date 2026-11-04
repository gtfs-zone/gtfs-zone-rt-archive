"""upload: segment keys, verification, retries and static.jsonl."""

import obstore
import pytest
from obstore.store import MemoryStore

from gtfs_zone_rt_archive import upload
from gtfs_zone_rt_archive.upload import Uploader


@pytest.fixture
def up(tmp_path):
    return Uploader(MemoryStore(), tmp_path)


def _get(up, key):
    return bytes(obstore.get(up.store, key).bytes())


def _keys(up):
    return sorted(m["path"] for batch in obstore.list(up.store) for m in batch)


def _segment(tmp_path, name="10.jsonl.zst", data=b"seg"):
    path = tmp_path / "amtrak" / "2026-11-05" / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    return path


def test_sweep_uploads_and_deletes_closed_segments(up, tmp_path):
    path = _segment(tmp_path)
    open_hour = _segment(tmp_path, "11.jsonl", b"open")
    assert up.sweep() == 0
    assert _keys(up) == ["raw/amtrak/2026-11-05/10.jsonl.zst"]
    assert _get(up, "raw/amtrak/2026-11-05/10.jsonl.zst") == b"seg"
    assert not path.exists()
    assert open_hour.exists()


def test_a_key_with_the_same_bytes_counts_as_uploaded(up, tmp_path):
    obstore.put(up.store, "raw/amtrak/2026-11-05/10.jsonl.zst", b"seg")
    path = _segment(tmp_path)
    up.sweep()
    assert _keys(up) == ["raw/amtrak/2026-11-05/10.jsonl.zst"]
    assert not path.exists()


def test_a_taken_key_is_never_overwritten(up, tmp_path):
    obstore.put(up.store, "raw/amtrak/2026-11-05/10.jsonl.zst", b"other")
    obstore.put(up.store, "raw/amtrak/2026-11-05/10.b.jsonl.zst", b"other b")
    _segment(tmp_path)
    up.sweep()
    assert _get(up, "raw/amtrak/2026-11-05/10.jsonl.zst") == b"other"
    assert _get(up, "raw/amtrak/2026-11-05/10.c.jsonl.zst") == b"seg"


def test_a_b_segment_keeps_its_name(up, tmp_path):
    _segment(tmp_path, "10.b.jsonl.zst")
    up.sweep()
    assert _keys(up) == ["raw/amtrak/2026-11-05/10.b.jsonl.zst"]


def test_failed_uploads_stay_local_for_the_next_sweep(up, tmp_path, monkeypatch):
    path = _segment(tmp_path)
    real_put = obstore.put

    def down(*args, **kwargs):
        raise OSError("garage down")

    monkeypatch.setattr(upload.obstore, "put", down)
    assert up.sweep() == 1
    assert path.exists()
    monkeypatch.setattr(upload.obstore, "put", real_put)
    assert up.sweep() == 0
    assert not path.exists()


def test_static_zip_is_uploaded_once_and_deleted(up, tmp_path):
    zip_path = tmp_path / "amtrak" / "gtfs-abcd1234.zip"
    zip_path.parent.mkdir()
    zip_path.write_bytes(b"zip")
    up.sweep()
    assert _get(up, "raw/amtrak/gtfs-abcd1234.zip") == b"zip"
    assert not zip_path.exists()


def test_static_log_is_reuploaded_only_when_it_changes(up, tmp_path, monkeypatch):
    log_path = tmp_path / "amtrak" / "static.jsonl"
    log_path.parent.mkdir()
    log_path.write_bytes(b'{"t":1}\n')
    puts = []
    real_put = obstore.put

    def counting_put(store, key, data, **kwargs):
        puts.append(key)
        return real_put(store, key, data, **kwargs)

    monkeypatch.setattr(upload.obstore, "put", counting_put)
    up.sweep()
    up.sweep()
    log_path.write_bytes(b'{"t":1}\n{"t":2}\n')
    up.sweep()
    assert puts == ["raw/amtrak/static.jsonl"] * 2
    assert log_path.exists()
    assert _get(up, "raw/amtrak/static.jsonl") == b'{"t":1}\n{"t":2}\n'


def test_restore_static_log_only_when_missing(up, tmp_path):
    obstore.put(up.store, "raw/amtrak/static.jsonl", b"remote\n")
    up.restore_static_log("amtrak")
    assert (tmp_path / "amtrak" / "static.jsonl").read_bytes() == b"remote\n"
    (tmp_path / "amtrak" / "static.jsonl").write_bytes(b"local\n")
    up.restore_static_log("amtrak")
    assert (tmp_path / "amtrak" / "static.jsonl").read_bytes() == b"local\n"
    up.restore_static_log("suntran")
    assert not (tmp_path / "suntran").exists()


def test_md5_etag_is_compared_without_a_download(monkeypatch):
    head = {"size": 3, "e_tag": '"' + upload._md5(b"abc") + '"'}
    monkeypatch.setattr(upload.obstore, "head", lambda s, k: head)
    monkeypatch.setattr(upload.obstore, "get", lambda s, k: pytest.fail("downloaded"))
    assert upload._remote_equals(None, "k", b"abc") is True
    assert upload._remote_equals(None, "k", b"abd") is False
