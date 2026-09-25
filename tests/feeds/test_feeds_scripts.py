"""scripts/record.py, scripts/smoke_feeds.py, scripts/replay_inspect.py (offline)."""

from __future__ import annotations

import argparse
import asyncio
import datetime
import importlib.util
import sys
from pathlib import Path

import orjson
import pytest
import yaml

from dh.core.events import FeedStatus
from dh.store.codec import decode_event
from dh.store.recorder import Recorder
from dh.store.replay import iter_events, iter_raw, list_streams
from tests.feeds.helpers import load_fixture

REPO = Path(__file__).resolve().parents[2]
END = 2**63 - 1


def _load(name: str):
    path = REPO / "scripts" / f"{name}.py"
    spec = importlib.util.spec_from_file_location(f"_script_{name}", path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


smoke = _load("smoke_feeds")
record = _load("record")
inspect_cli = _load("replay_inspect")


def _probe_from_fixture(name: str, upto: int | None = None):
    from dh.feeds.registry import feed_class

    recs = load_fixture(name)[:upto]
    feed = feed_class(recs[0][1].split(".")[0])()
    probe = smoke.Probe(name, feed)
    feed.on_event = probe.events.append
    feed._emit_raw = probe.emit_raw
    for t, _s, _q, raw in recs:
        feed._emit(t, raw)
    return probe


def _args(**kw):
    base = dict(min_updates=1, max_spread_bps=50.0, max_dev=0.01, clock_tolerance_ms=100.0)
    base.update(kw)
    return argparse.Namespace(**base)


def test_smoke_evaluate_pass_and_fail():
    ok = _probe_from_fixture("kraken", upto=9)  # before the bad-checksum update
    checks = {c.name: c for c in smoke.evaluate(ok, 1.0, _args(), median_mid=None)}
    assert checks["connected"].result == "PASS" and checks["snapshot"].result == "PASS"
    assert checks["checksum"].result == "PASS" and checks["continuity"].result == "PASS"
    assert checks["top_of_book"].result == "PASS" and checks["replay"].result == "PASS"
    assert checks["aggressor"].result in ("PASS", "WARN") and "1.00 of 2" in checks["aggressor"].detail
    assert checks["latency"].result == "PASS"
    bad = _probe_from_fixture("coinbase")  # contains a sequence gap
    checks = {c.name: c for c in smoke.evaluate(bad, 1.0, _args(), median_mid=84500.0)}
    assert checks["continuity"].result == "FAIL" and "gaps=1" in checks["continuity"].detail
    far = {c.name: c for c in smoke.evaluate(ok, 1.0, _args(), median_mid=84500.0)}
    assert far["top_of_book"].result == "FAIL"  # 45k book vs 84.5k median: deviation screen


def test_replay_inspect_cli(tmp_path, capsys):
    from tests.store.test_store_replay import build_store

    build_store(tmp_path)
    assert inspect_cli.main(["--root", str(tmp_path), "list"]) == 0
    out = capsys.readouterr().out
    assert "coinbase.ws" in out and "unindexed" not in out
    assert inspect_cli.main(["--root", str(tmp_path), "gaps", "--streams", "kraken.ws"]) == 0
    assert "checksum mismatch" in capsys.readouterr().out
    assert inspect_cli.main(["--root", str(tmp_path), "top", "--at", "2026-09-25T12:00:02Z", "--streams", "coinbase.ws,bitstamp.ws,gemini.ws"]) == 0
    out = capsys.readouterr().out
    assert "BRTI replica=" in out and "coinbase" in out
    assert inspect_cli.main(["--root", str(tmp_path), "count", "--t0", "2026-09-25T12", "--t1", "1790337601"]) == 0
    assert "ExtBookSnapshot" in capsys.readouterr().out
    assert inspect_cli.parse_time("2026-09-25T12:00:00.5Z", 0) == 1790337600_500_000_000
    assert inspect_cli.parse_time("1790337600", 0) == 1790337600 * 10**9
    assert inspect_cli.parse_time("1790337600000", 0) == 1790337600 * 10**9


async def test_supervisor_restarts_and_records(tmp_path):
    rec = Recorder(tmp_path, start=False)
    mon = record.Monitor(rec)
    stop = asyncio.Event()
    calls = []

    async def flaky():
        calls.append(1)
        if len(calls) <= 2:
            raise RuntimeError("boom")
        stop.set()

    await asyncio.wait_for(record.supervise("x", "x.ws", flaky, rec, mon, stop, 0.01, 0.02), 5)
    rec.close()
    st = [decode_event(r.data) for r in iter_raw(tmp_path, ["status"], 0, END)]
    assert len(st) == 2 and all(isinstance(s, FeedStatus) and s.status == "error" and "boom" in s.detail for s in st)
    assert mon.restarts == {"x": 2} and len(calls) == 3


async def test_collector_end_to_end_against_loopback_venue(tmp_path):
    pytest.importorskip("websockets")
    from websockets.asyncio.server import serve

    frames = [raw.decode() for _t, _s, _q, raw in load_fixture("coinbase")[1:9] if not raw.startswith(b'{"_dh"')]
    # trades executed before the connection opened are the venue's history replay and are
    # dropped (dh.feeds.base.is_history), so re-stamp the fixture's trades as executing now
    now = datetime.datetime.now(datetime.timezone.utc).isoformat().replace("+00:00", "Z")

    def _fresh(f: str) -> str:
        m = orjson.loads(f)
        if m.get("channel") != "market_trades":
            return f
        for ev in m.get("events", ()):
            for t in ev.get("trades", ()):
                t["time"] = now
        return orjson.dumps(m).decode()

    frames = [_fresh(f) for f in frames]

    async def handler(ws):
        subs = [await ws.recv() for _ in range(3)]
        assert all(orjson.loads(m)["type"] == "subscribe" for m in subs)
        for f in frames:
            await ws.send(f)
        await ws.wait_closed()

    async with serve(handler, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        cfg = {
            "root": str(tmp_path),
            "recorder": {"flush_interval_s": 0.05},
            "clock": {"sample_interval_s": 60},
            "feeds": {"coinbase": {"enabled": True, "stream": "coinbase.ws", "url": f"ws://127.0.0.1:{port}", "proxy": None,
                                   "resnapshot_interval_s": 0},
                      "kraken": {"enabled": False}},
            "kalshi": {"enabled": False},
        }
        cfg_path = tmp_path / "feeds.yaml"
        cfg_path.write_text(yaml.safe_dump(cfg))
        args = argparse.Namespace(config=str(cfg_path), root=None, only="", no_kalshi=False, duration=1.5, status_interval=0.5)
        assert await record.amain(args) == 0
    assert set(list_streams(tmp_path)) >= {"coinbase.ws", "meta", "clock"}
    ev = list(iter_events(tmp_path, ["coinbase.ws"], 0, END))
    kinds = [type(e).__name__ for e in ev]
    assert kinds[0] == "FeedStatus" and "ExtBookSnapshot" in kinds and "ExtTrade" in kinds
    assert isinstance(ev[-1], FeedStatus) and ev[-1].status == "disconnected"  # graceful shutdown marker
    metas = [orjson.loads(r.data)["kind"] for r in iter_raw(tmp_path, ["meta"], 0, END)]
    assert metas == ["session_start", "session_end"]
    # every segment was closed with an index on shutdown
    from dh.store.replay import read_index, segment_files

    assert all(read_index(p) is not None for _h, _p, p in segment_files(tmp_path, "coinbase.ws"))
