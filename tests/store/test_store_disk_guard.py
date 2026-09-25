"""Low-disk guard: shedding, hysteresis, clean stop, start-up refusal, meta records, Kalshi never
shed, and a full disk never leaving a partial frame in a segment (monkeypatched disk_usage)."""

from __future__ import annotations

import argparse
import asyncio
import errno
import importlib.util
import os
import sys
import time
from pathlib import Path
from types import SimpleNamespace

import orjson
import pytest
import yaml

import dh.store.recorder as recmod
from dh.feeds.base import FeedMetrics, make_marker
from dh.store.recorder import (
    DEFAULT_SHED_STREAMS,
    EXIT_LOW_DISK,
    GB,
    DiskGuard,
    Recorder,
    never_shed,
)
from dh.store.replay import ReadStats, iter_raw, list_streams, read_index, read_segment, segment_files

REPO = Path(__file__).resolve().parents[2]
T0 = 1790337600 * 10**9  # 2026-09-25T12:00:00Z
END = 2**63 - 1
KEPT = ("kalshi.ws", "kalshi.rest.orderbooks", "coinbase.ws", "kraken.ws", "bitstamp.ws", "clock", "status")


class FakeDisk:
    """Stands in for shutil.disk_usage: free space is whatever the test sets."""

    def __init__(self, free_gb: float = 100.0, total_gb: float = 500.0) -> None:
        self.free_gb, self.total_gb = free_gb, total_gb
        self.paths: list[Path] = []

    def __call__(self, path):
        self.paths.append(Path(path))
        return SimpleNamespace(total=int(self.total_gb * GB), used=0, free=int(self.free_gb * GB))


@pytest.fixture
def disk(monkeypatch):
    d = FakeDisk()
    monkeypatch.setattr(recmod.shutil, "disk_usage", d)
    return d


def _clock():
    n = [T0 + 10**9]

    def now() -> int:
        n[0] += 1000
        return n[0]

    return now


def _write_all(rec: Recorder, t: int) -> None:
    for s in DEFAULT_SHED_STREAMS + KEPT:
        rec.write(s, t, orjson.dumps({"s": s, "t": t}))


def _metas(root) -> list[dict]:
    return [orjson.loads(r.data) for r in iter_raw(root, ["meta"], 0, END)]


def _times(root, stream) -> list[int]:
    return [r.t for r in iter_raw(root, [stream], 0, END)]


def _assert_clean_store(root) -> None:
    """Every segment closed with an index, nothing truncated or corrupt."""
    st = ReadStats()
    for s in list_streams(root):
        for _h, _p, seg in segment_files(root, s):
            assert read_index(seg) is not None, seg
            list(read_segment(seg, st))
    assert not st.truncated_files and not st.corrupt_files and not st.bad_lines


# ============================================================================ DiskGuard + Recorder
def test_shed_resume_with_hysteresis_and_meta_records(tmp_path, disk):
    rec = Recorder(tmp_path, start=False)
    guard = DiskGuard(tmp_path, rec, clock_ns=_clock())  # defaults: shed < 30, resume >= 35, stop < 8
    calls: list[tuple[str, tuple[str, ...]]] = []
    guard.before_shed = lambda s, info: calls.append(("before_shed", s))
    guard.after_unshed = lambda s, info: calls.append(("after_unshed", s))

    assert guard.check().action == "ok" and rec.stats.free_gb == pytest.approx(100.0)
    _write_all(rec, T0)
    disk.free_gb = 29.9
    c = guard.check()
    assert c.action == "shed" and c.shed and rec.shed_streams == frozenset(DEFAULT_SHED_STREAMS)
    _write_all(rec, T0 + 1)
    for free in (30.0, 34.99):  # back above the threshold but inside the 5 GB hysteresis: still shed
        disk.free_gb = free
        assert guard.check().action == "ok" and guard.shed
        _write_all(rec, T0 + 2)
    disk.free_gb = 35.0
    c = guard.check()
    assert c.action == "unshed" and not c.shed and rec.shed_streams == frozenset()
    _write_all(rec, T0 + 5)
    assert rec.stats.shed_records == 3 * len(DEFAULT_SHED_STREAMS)
    assert rec.stream_stats()["deribit.options"].shed == 3 and rec.stream_stats()["kalshi.ws"].shed == 0
    rec.close()

    for s in DEFAULT_SHED_STREAMS:
        assert _times(tmp_path, s) == [T0, T0 + 5], s  # nothing recorded while shed
    for s in KEPT:
        assert _times(tmp_path, s) == [T0, T0 + 1, T0 + 2, T0 + 2, T0 + 5], s
    metas = _metas(tmp_path)
    assert [m["kind"] for m in metas] == ["disk_shed", "disk_unshed"]
    shed, unshed = metas
    assert shed["streams"] == list(DEFAULT_SHED_STREAMS) and shed["free_gb"] == pytest.approx(29.9)
    assert shed["min_free_gb_shed"] == 30 and shed["resume_above_gb"] == 35 and shed["min_free_gb_stop"] == 8
    assert shed["last_t"] == {s: T0 for s in DEFAULT_SHED_STREAMS}  # where each stream stopped
    assert unshed["not_recorded"] == 3 * len(DEFAULT_SHED_STREAMS) and unshed["shed_since_ns"] > 0
    assert calls == [("before_shed", DEFAULT_SHED_STREAMS), ("after_unshed", DEFAULT_SHED_STREAMS)]
    # shed records consume no sequence number: q stays contiguous
    qs = sorted(r.q for r in iter_raw(tmp_path, None, 0, END))
    assert qs == list(range(len(qs)))
    _assert_clean_store(tmp_path)


def test_kalshi_and_core_streams_are_never_shed(tmp_path, disk):
    assert not any(never_shed(s) for s in DEFAULT_SHED_STREAMS)
    assert all(never_shed(s) for s in ("kalshi.ws", "kalshi.rest.series", "meta", "clock", "status"))
    assert not {"coinbase.ws", "kraken.ws", "bitstamp.ws"} & set(DEFAULT_SHED_STREAMS)
    # the committed config sheds exactly the default list
    cfg = yaml.safe_load((REPO / "config" / "feeds.yaml").read_text())
    assert DiskGuard.from_config(cfg["recorder"], tmp_path).shed_streams == DEFAULT_SHED_STREAMS

    rec = Recorder(tmp_path, start=False)
    guard = DiskGuard(tmp_path, rec, shed_streams=["kalshi.ws", "kalshi.rest.orderbooks", "meta", "clock",
                                                   "status", "gemini.ws"])
    assert guard.shed_streams == ("gemini.ws",)  # protected streams dropped from the list
    disk.free_gb = 10.0
    assert guard.check().action == "shed"
    assert rec.set_shed(["kalshi.ws", "kalshi.rest.orderbooks", "gemini.ws"]) == frozenset({"gemini.ws"})
    for s in ("kalshi.ws", "kalshi.rest.orderbooks", "gemini.ws", "clock"):
        rec.write(s, T0, b"{}")
    rec.close()
    assert _times(tmp_path, "kalshi.ws") == [T0] and _times(tmp_path, "kalshi.rest.orderbooks") == [T0]
    assert _times(tmp_path, "clock") == [T0] and "gemini.ws" not in list_streams(tmp_path)


def test_stop_below_min_free_is_terminal_and_closes_cleanly(tmp_path, disk):
    rec = Recorder(tmp_path, start=False)
    guard = DiskGuard(tmp_path, rec, clock_ns=_clock())
    disk.free_gb = 20.0
    assert guard.check().action == "shed"
    _write_all(rec, T0)
    disk.free_gb = 7.9
    c = guard.check()
    assert c.action == "stop" and guard.stopped
    disk.free_gb = 100.0
    assert guard.check().action == "stop"  # terminal: the collector exits and restarts fresh
    rec.close()  # what the collector does next: flush + fsync + indexes
    metas = _metas(tmp_path)
    assert [m["kind"] for m in metas] == ["disk_shed", "disk_stop"]
    assert metas[-1]["exit_code"] == EXIT_LOW_DISK == 5 and metas[-1]["free_gb"] == pytest.approx(7.9)
    assert metas[-1]["shed"] is True
    _assert_clean_store(tmp_path)


def test_disk_guard_config_validation(tmp_path, disk):
    with pytest.raises(ValueError):
        DiskGuard(tmp_path, min_free_gb_shed=8, min_free_gb_stop=8)
    with pytest.raises(ValueError):
        DiskGuard(tmp_path, shed_streams="deribit.ws")  # a string, not a list
    with pytest.raises(ValueError):
        DiskGuard(tmp_path, shed_streams=["../x"])
    with pytest.raises(ValueError):
        DiskGuard(tmp_path, hysteresis_gb=-1)
    with pytest.raises(ValueError):
        DiskGuard.from_config({"disk_check_interval_s": 0}, tmp_path)
    g = DiskGuard.from_config({}, tmp_path)  # every key defaults
    assert (g.min_free_gb_shed, g.min_free_gb_stop, g.hysteresis_gb, g.interval_s) == (30, 8, 5, 60)
    assert g.shed_streams == DEFAULT_SHED_STREAMS
    # 0 disables a level
    rec = Recorder(tmp_path, start=False)
    off = DiskGuard(tmp_path, rec, min_free_gb_shed=0, min_free_gb_stop=0)
    disk.free_gb = 0.5
    assert off.check().action == "ok" and not off.shed
    rec.close()
    # measured on the store's filesystem: <root>/raw (may be a symlink to another volume)
    assert disk.paths[-1] == (tmp_path / "raw").absolute()
    assert DiskGuard(tmp_path / "not" / "yet").path() == tmp_path.absolute()
    with pytest.raises(RuntimeError):
        DiskGuard(tmp_path).check()  # no recorder attached


def test_measure_failure_keeps_state(tmp_path, monkeypatch):
    def boom(path):
        raise OSError(errno.EIO, "statfs failed")

    monkeypatch.setattr(recmod.shutil, "disk_usage", boom)
    rec = Recorder(tmp_path, start=False)
    c = DiskGuard(tmp_path, rec).check()
    assert c.action == "error" and c.free_gb is None and not c.shed
    rec.close()


# ============================================================================ full disk: no partial frames
class _FullDiskFile:
    """Wraps a segment's raw file: in 'partial' mode the next write lands half the bytes and the
    one after raises ENOSPC (what a filling disk does)."""

    def __init__(self, f) -> None:
        self.f, self.mode = f, "ok"

    def write(self, b):
        if self.mode == "partial":
            self.mode = "enospc"
            return self.f.write(bytes(b[: len(b) // 2]))
        if self.mode == "enospc":
            raise OSError(errno.ENOSPC, "No space left on device")
        return self.f.write(b)

    def __getattr__(self, name):
        return getattr(self.f, name)


def test_disk_full_write_is_cut_back_to_a_frame_boundary(tmp_path):
    rec = Recorder(tmp_path, start=False, disk_full_backoff_s=3600)
    rec.write("x.ws", T0, b'{"n":0}')
    rec.flush()
    seg = rec._segments["x.ws"]
    good = seg.path.stat().st_size
    seg.f = wrapper = _FullDiskFile(seg.f)
    wrapper.mode = "partial"
    rec.write("x.ws", T0 + 1, b'{"n":1,"pad":"' + b"x" * 500 + b'"}')
    rec.flush()
    assert seg.path.stat().st_size == good  # the half-written frame was cut off again
    assert rec.stats.disk_full_errors == 1 and rec.stats.dropped_records == 1 and not seg.closed
    rec.write("x.ws", T0 + 2, b'{"n":2}')
    rec.flush()  # inside the back-off: stays queued, nothing is attempted
    assert seg.path.stat().st_size == good and rec.stats.disk_full_errors == 1
    wrapper.mode = "ok"  # space freed
    rec.close()  # close retries at once, even inside the back-off
    assert [orjson.loads(r.data)["n"] for r in read_segment(seg.path)] == [0, 2]
    assert read_index(seg.path)["count"] == 2
    _assert_clean_store(tmp_path)


def test_disk_full_write_that_cannot_be_cut_back_abandons_the_segment(tmp_path, monkeypatch):
    rec = Recorder(tmp_path, start=False, disk_full_backoff_s=0)
    rec.write("x.ws", T0, b'{"n":0}')
    rec.flush()
    seg = rec._segments["x.ws"]
    seg.f = wrapper = _FullDiskFile(seg.f)
    wrapper.mode = "partial"

    def no_truncate(fd, n):
        raise OSError(errno.EIO, "I/O error")

    rec.write("x.ws", T0 + 1, b'{"n":1,"pad":"' + b"x" * 500 + b'"}')
    with monkeypatch.context() as m:
        m.setattr(recmod.os, "ftruncate", no_truncate)
        rec.flush()
    assert seg.abandoned and rec.stats.segments_abandoned == 1 and rec.open_segments() == {}
    rec.write("x.ws", T0 + 2, b'{"n":2}')  # the hour continues in a new part
    rec.close()
    files = [p.name for _h, _p, p in segment_files(tmp_path, "x.ws")]
    assert files == ["12.jsonl.zst", "12.p1.jsonl.zst"]
    st = ReadStats()
    got = [orjson.loads(r.data)["n"] for r in iter_raw(tmp_path, ["x.ws"], 0, END, st)]
    assert got == [0, 2] and st.truncated_files == [str(seg.path)]  # reader flags the cut tail
    assert read_index(seg.path) is None  # abandoned: no index claims the cut tail


# ============================================================================ collector (scripts/record.py)
def _load_record():
    spec = importlib.util.spec_from_file_location("_script_record_disk", REPO / "scripts" / "record.py")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


record = _load_record()


class _FakeFeed:
    """Duck-typed FeedClient: a connected marker, then a frame every 5 ms until stopped."""

    implemented = True

    def __init__(self, name: str) -> None:
        self.name = name
        self.metrics = FeedMetrics()
        self.conn_id = 0
        self.reconnect_requests: list[str] = []
        self._stopping = False

    def describe(self) -> str:
        return self.name

    async def run(self, emit) -> None:
        self.conn_id += 1
        self.metrics.connects += 1
        emit(self.name, time.time_ns(), make_marker("status", self.conn_id, status="connected", url="fake"))
        i = 0
        while not self._stopping:
            emit(self.name, time.time_ns(), orjson.dumps({"i": i}))
            i += 1
            await asyncio.sleep(0.005)
        self.metrics.disconnects += 1
        emit(self.name, time.time_ns(), make_marker("status", self.conn_id, status="disconnected", detail="stop"))

    def stop(self) -> None:
        self._stopping = True

    def request_reconnect(self, reason: str) -> None:
        self.reconnect_requests.append(reason)


def _collector(tmp_path, monkeypatch, duration: float = 30.0):
    root = tmp_path / "store"
    cfg = {"root": str(root), "recorder": {"flush_interval_s": 0.02, "disk_check_interval_s": 0.05},
           "clock": {"sample_interval_s": 60}, "feeds": {}, "kalshi": {"enabled": False}}
    cfg_path = tmp_path / "feeds.yaml"
    cfg_path.write_text(yaml.safe_dump(cfg))
    feeds = {"deribit": _FakeFeed("deribit.ws"), "coinbase": _FakeFeed("coinbase.ws")}
    monkeypatch.setattr(record, "build_feeds", lambda cfg, only=None, status_cb=None: feeds)
    guards: list[DiskGuard] = []

    class _Guard(DiskGuard):
        def __init__(self, *a, **kw) -> None:
            super().__init__(*a, **kw)
            guards.append(self)

    monkeypatch.setattr(record, "DiskGuard", _Guard)
    args = argparse.Namespace(config=str(cfg_path), root=None, only="", no_kalshi=False, duration=duration,
                              status_interval=0.05)
    return root, args, feeds, guards


async def _until(cond, timeout: float = 10.0) -> None:
    t_end = time.monotonic() + timeout
    while not cond():
        assert time.monotonic() < t_end, "condition not reached"
        await asyncio.sleep(0.01)


def test_collector_refuses_to_start_below_min_free(tmp_path, monkeypatch, disk, caplog):
    root, args, _feeds, _guards = _collector(tmp_path, monkeypatch)
    disk.free_gb = 7.5
    with caplog.at_level("ERROR"):
        assert asyncio.run(record.amain(args)) == EXIT_LOW_DISK == 5
    assert not root.exists()  # nothing created: no store, no lock file
    assert "refusing to start" in caplog.text and "7.5 GB free" in caplog.text


async def test_collector_sheds_resumes_and_stops_cleanly(tmp_path, monkeypatch, disk):
    root, args, feeds, guards = _collector(tmp_path, monkeypatch)
    deribit, coinbase = feeds["deribit"], feeds["coinbase"]
    disk.free_gb = 100.0
    task = asyncio.create_task(record.amain(args))
    await _until(lambda: guards and guards[0].recorder is not None
                 and guards[0].recorder.stream_stats().get("deribit.ws", recmod.StreamStats()).count > 5)
    guard = guards[0]
    disk.free_gb = 25.0
    await _until(lambda: guard.shed)
    await asyncio.sleep(0.15)
    assert guard.recorder.stream_stats()["deribit.ws"].shed > 0
    disk.free_gb = 33.0  # inside the hysteresis band
    await asyncio.sleep(0.2)
    assert guard.shed and not deribit.reconnect_requests
    disk.free_gb = 40.0
    await _until(lambda: not guard.shed)
    assert len(deribit.reconnect_requests) == 1 and not coinbase.reconnect_requests  # fresh snapshot
    await asyncio.sleep(0.15)
    disk.free_gb = 5.0
    assert await asyncio.wait_for(task, 10) == EXIT_LOW_DISK

    metas = _metas(root)
    kinds = [m["kind"] for m in metas]
    assert kinds == ["session_start", "disk_shed", "disk_unshed", "disk_stop", "session_end"]
    assert metas[0]["disk"]["free_gb"] == pytest.approx(100.0)
    assert metas[-1]["reason"] == "low_disk" and metas[-1]["exit_code"] == 5
    assert metas[1]["streams"] == list(DEFAULT_SHED_STREAMS)
    t_shed, t_unshed = (next(r.t for r in iter_raw(root, ["meta"], 0, END) if orjson.loads(r.data)["kind"] == k)
                        for k in ("disk_shed", "disk_unshed"))
    # deribit.ws: a 'disconnected' marker at the shed, then nothing until the unshed
    der = list(iter_raw(root, ["deribit.ws"], 0, END))
    marks = [orjson.loads(r.data) for r in der if r.data.startswith(b'{"_dh":')]
    shed_marks = [m for m in marks if "recorder shed stream" in m.get("detail", "")]
    assert len(shed_marks) == 1 and shed_marks[0]["status"] == "disconnected"
    assert not [r for r in der if t_shed < r.t < t_unshed and not r.data.startswith(b'{"_dh":')]
    assert any(r.t > t_unshed for r in der)  # resumed
    # coinbase.ws was recorded throughout
    cb = [r.t for r in iter_raw(root, ["coinbase.ws"], 0, END)]
    assert any(t_shed < t < t_unshed for t in cb)
    assert orjson.loads(list(iter_raw(root, ["coinbase.ws"], 0, END))[-1].data)["status"] == "disconnected"
    _assert_clean_store(root)
    assert (root / "recorder.lock").read_text().strip().isdigit()  # it held the store lock


async def test_collector_starts_shed_when_already_low(tmp_path, monkeypatch, disk):
    root, args, feeds, _guards = _collector(tmp_path, monkeypatch, duration=0.4)
    disk.free_gb = 12.0  # between stop (8) and shed (30): record, but only the essentials
    assert await record.amain(args) == 0
    kinds = [m["kind"] for m in _metas(root)]
    assert kinds == ["session_start", "disk_shed", "session_end"]
    assert "deribit.ws" not in list_streams(root) and "coinbase.ws" in list_streams(root)
    _assert_clean_store(root)


def test_status_line_shows_free_space_and_shed(tmp_path, disk):
    rec = Recorder(tmp_path, start=False)
    mon = record.Monitor(rec)
    assert "free_GB=?" in mon.status_line()
    guard = DiskGuard(tmp_path, rec)
    disk.free_gb = 150.25
    guard.check()
    line = mon.status_line()
    assert "free_GB=150.2" in line and "SHED" not in line
    disk.free_gb = 20.0
    guard.check()
    rec.write("deribit.ws", T0, b"{}")
    line = mon.status_line()
    assert "free_GB=20.0" in line and "LOW-DISK SHED" in line and "not_recorded=1" in line
    assert "SHED (not recorded: low disk)" in line  # the deribit.ws row
    rec.close()


def test_session_meta_records_disk(tmp_path):
    body = orjson.loads(record.session_meta(argparse.Namespace(), {}, {"free_gb": 1.0}))
    assert body["kind"] == "session_start" and body["disk"] == {"free_gb": 1.0} and body["pid"] == os.getpid()
