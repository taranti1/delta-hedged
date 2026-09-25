"""Recorder: format, rotation, crash safety (truncated frames), no rewrites, thread safety."""

from __future__ import annotations

import hashlib
import shutil
import threading

import orjson
import pytest
import zstandard as zstd

from dh.core.events import FeedStatus
from dh.store.codec import decode_event
from dh.store.recorder import HOUR_NS, ClockSampler, Recorder, encode_record, utc_day_hour
from dh.store.replay import ReadStats, iter_raw, read_index, read_segment

T0 = 1790337600 * 10**9  # 2026-09-25T12:00:00Z


def _files(root, stream):
    return sorted((root / "raw" / stream).rglob("*.jsonl.zst"))


def test_record_format_roundtrip_and_index(tmp_path):
    rec = Recorder(tmp_path, start=False)
    frames = [b'{"a":1}', "café ☃".encode(), b"\xff\xfe\x00binary", b'{"q":"\\"quoted\\""}']
    for i, f in enumerate(frames):
        rec.write("x.ws", T0 + i, f)
    rec.write("y.ws", T0 + 10, b"{}")
    rec.flush()
    seg = tmp_path / "raw" / "x.ws" / "2026-09-25" / "12.jsonl.zst"
    assert seg.exists() and read_index(seg) is None  # still open: no sidecar yet
    rec.close()
    idx = read_index(seg)
    assert idx["count"] == 4 and idx["first_t"] == T0 and idx["last_t"] == T0 + 3 and idx["frames"] == 1
    assert idx["min_q"] == 0 and idx["max_q"] == 3 and idx["stream"] == "x.ws"
    got = list(read_segment(seg))
    assert [r.data for r in got] == frames and [r.q for r in got] == [0, 1, 2, 3]
    assert all(r.stream == "x.ws" for r in got)
    # plain zstd tooling reads it: one JSON object per line with t/s/q and d (or b64 'b')
    with open(seg, "rb") as fh:
        text = zstd.ZstdDecompressor().stream_reader(fh, read_across_frames=True).read()
    lines = [orjson.loads(line) for line in text.splitlines()]
    assert list(lines[0]) == ["t", "s", "q", "d"] and "b" in lines[2] and "d" not in lines[2]
    assert encode_record(5, "s", 6, b"{}") == b'{"t":5,"s":"s","q":6,"d":"{}"}\n'


def test_each_flush_is_an_independent_frame(tmp_path):
    rec = Recorder(tmp_path, start=False)
    for i in range(5):
        rec.write("x.ws", T0 + i, b'{"i":%d}' % i)
        rec.flush()
    rec.close()
    seg = _files(tmp_path, "x.ws")[0]
    assert read_index(seg)["frames"] == 5
    data = seg.read_bytes()
    assert data.count(b"\x28\xb5\x2f\xfd") == 5  # zstd frame magic, one per flush


def test_hourly_rotation_utc(tmp_path):
    rec = Recorder(tmp_path, start=False)
    rec.write("x.ws", T0 + HOUR_NS - 1, b"late")
    rec.write("x.ws", T0 + HOUR_NS + 1, b"next hour")
    rec.write("x.ws", T0 + 12 * HOUR_NS, b"next day")  # 2026-09-26T00
    rec.flush()
    files = [p.relative_to(tmp_path / "raw").as_posix() for p in _files(tmp_path, "x.ws")]
    assert files == ["x.ws/2026-09-25/12.jsonl.zst", "x.ws/2026-09-25/13.jsonl.zst", "x.ws/2026-09-26/00.jsonl.zst"]
    # closed segments already have their index; the open one does not
    assert read_index(_files(tmp_path, "x.ws")[0])["count"] == 1
    assert read_index(_files(tmp_path, "x.ws")[2]) is None
    rec.close()
    assert utc_day_hour(T0 + HOUR_NS) == ("2026-09-25", "13")


def test_truncated_segment_recovers_earlier_records(tmp_path):
    rec = Recorder(tmp_path, start=False)
    n = 0
    for _batch in range(4):
        for _ in range(50):
            rec.write("x.ws", T0 + n, orjson.dumps({"n": n, "pad": "x" * 40}))
            n += 1
        rec.flush()  # one frame per batch of 50
    seg = _files(tmp_path, "x.ws")[0]
    # the process "crashes" here: the segment is open, never closed, no index
    crashed = tmp_path / "crash" / "raw" / "x.ws" / "2026-09-25"
    crashed.mkdir(parents=True)
    full = seg.read_bytes()
    frames = full.split(b"\x28\xb5\x2f\xfd")[1:]
    last_frame_start = len(full) - len(frames[-1]) - 4
    for cut in (last_frame_start + 3, last_frame_start + len(frames[-1]) // 2, len(full) - 1):
        shutil.rmtree(crashed)
        crashed.mkdir(parents=True)
        (crashed / "12.jsonl.zst").write_bytes(full[:cut])
        st = ReadStats()
        got = list(read_segment(crashed / "12.jsonl.zst", st))
        ns = [orjson.loads(r.data)["n"] for r in got]
        assert ns[:150] == list(range(150))  # all complete frames recovered
        assert ns == list(range(len(ns))) and len(ns) <= 200  # partial tail only adds whole records
        assert st.truncated_files == [str(crashed / "12.jsonl.zst")]
    # iter_raw over the crashed store works too
    assert len(list(iter_raw(tmp_path / "crash", None, 0, 2**63 - 1))) >= 150
    rec.close()


def test_garbage_tail_is_tolerated(tmp_path):
    rec = Recorder(tmp_path, start=False)
    for i in range(10):
        rec.write("x.ws", T0 + i, b"{}")
    rec.close()
    seg = _files(tmp_path, "x.ws")[0]
    bad = tmp_path / "bad.jsonl.zst"
    bad.write_bytes(seg.read_bytes() + b"\x00garbage-not-zstd")
    st = ReadStats()
    assert len(list(read_segment(bad, st))) == 10 and st.corrupt_files


def test_never_rewrites_existing_files(tmp_path):
    rec = Recorder(tmp_path, start=False)
    rec.write("x.ws", T0 + 1, b"first process")
    rec.close()
    seg = _files(tmp_path, "x.ws")[0]
    digest = hashlib.sha256(seg.read_bytes()).hexdigest()
    rec2 = Recorder(tmp_path, start=False)  # restart within the same hour
    rec2.write("x.ws", T0 + 2, b"second process")
    rec2.close()
    files = [p.name for p in _files(tmp_path, "x.ws")]
    assert files == ["12.jsonl.zst", "12.p1.jsonl.zst"]
    assert hashlib.sha256(seg.read_bytes()).hexdigest() == digest
    assert [r.data for r in iter_raw(tmp_path, ["x.ws"], 0, 2**63 - 1)] == [b"first process", b"second process"]


def test_close_idle_segments_and_late_record(tmp_path):
    rec = Recorder(tmp_path, start=False, clock_ns=lambda: T0 + HOUR_NS + 10 * 10**9)
    rec.write("x.ws", T0 + 5, b"a")
    rec.flush()
    rec.close_idle_segments()
    assert read_index(_files(tmp_path, "x.ws")[0])["count"] == 1 and rec.open_segments() == {}
    rec.write("x.ws", T0 + 6, b"late record after close")  # clock step / straggler
    rec.close()
    assert [p.name for p in _files(tmp_path, "x.ws")] == ["12.jsonl.zst", "12.p1.jsonl.zst"]


def test_concurrent_writers_with_background_flusher(tmp_path):
    rec = Recorder(tmp_path, flush_interval_s=0.005)
    per_thread = 500

    def worker(k: int) -> None:
        for i in range(per_thread):
            rec.write(f"s{k % 3}.ws", T0 + i, orjson.dumps({"k": k, "i": i}))

    threads = [threading.Thread(target=worker, args=(k,)) for k in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    rec.close()
    got = list(iter_raw(tmp_path, None, 0, 2**63 - 1))
    assert len(got) == 8 * per_thread
    assert sorted(r.q for r in got) == list(range(8 * per_thread))
    by_thread: dict[int, list[int]] = {}
    for r in sorted(got, key=lambda r: r.q):
        o = orjson.loads(r.data)
        by_thread.setdefault(o["k"], []).append(o["i"])
    assert all(v == list(range(per_thread)) for v in by_thread.values())
    assert rec.stats.records == 8 * per_thread and rec.stats.write_errors == 0
    with pytest.raises(RuntimeError):
        rec.write("x.ws", T0, b"after close")


def test_validation_and_events(tmp_path):
    rec = Recorder(tmp_path, start=False)
    for bad in ("", "../x", "a/b", "x..y", " x"):
        with pytest.raises(ValueError):
            rec.write(bad, T0, b"{}")
    with pytest.raises(TypeError):
        rec.write("x.ws", T0, "text")  # type: ignore[arg-type]
    st = FeedStatus(T0 + 7, 0, "kalshi.ws", "disconnected", "1006")
    rec.write_event("status", st)
    sampler = ClockSampler(rec)
    sample = sampler.sample_once()
    assert sample["src"] in ("chronyc", "timedatectl", "adjtimex", "unknown")
    rec.close()
    got = {r.stream: r for r in iter_raw(tmp_path, None, 0, 2**63 - 1)}
    assert decode_event(got["status"].data) == st
    assert orjson.loads(got["clock"].data)["src"] == sample["src"]


def test_frames_straddling_read_chunks(tmp_path, monkeypatch):
    import dh.store.replay as replay

    rec = Recorder(tmp_path, start=False)
    n = 0
    for _ in range(7):
        for _ in range(13):
            rec.write("x.ws", T0 + n, orjson.dumps({"n": n, "pad": "y" * (n % 17)}))
            n += 1
        rec.flush()
    rec.close()
    seg = _files(tmp_path, "x.ws")[0]
    for chunk in (1, 7, 64, 1 << 20):
        monkeypatch.setattr(replay, "READ_CHUNK", chunk)
        st = ReadStats()
        got = [orjson.loads(r.data)["n"] for r in read_segment(seg, st)]
        assert got == list(range(n)) and not st.truncated_files and not st.bad_lines, chunk
