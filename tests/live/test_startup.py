"""Start-up helpers: universe selection, benchmark back-fill + fair-value warm-up, paper
simulator, spec serialization, config / mode rules, hedge venue."""

from __future__ import annotations

import math
from pathlib import Path

import numpy as np
import pytest

from dh.core.actions import CancelHedge, PlaceHedge
from dh.core.events import HedgeOrderUpdate
from dh.core.units import NS_PER_MS, NS_PER_S
from dh.kalshi.fees import FeeEngine
from dh.kalshi.metadata import MarketRegistry
from dh.live.config import (
    LIVE_CONFIRM_FLAG,
    BackfillCfg,
    LiveConfig,
    ModeError,
    PaperCfg,
    load_live_config,
    resolve_mode,
)
from dh.live.startup import (
    PaperFees,
    backfill_fair_value,
    build_paper_sim,
    discover_universe,
    events_with_positions,
    resample,
    select_specs,
    spec_from_dict,
    spec_to_dict,
)
from dh.live.venue_hedge import DisabledHedgeVenue, KalshiPerpHedgeVenue, build_hedge_venue
from dh.models.fvmodel import FairValueModel, load_recommended_config

from ..kalshi import samples as S
from .fakes import T0, FakeRest, kxbtcd_spec

REPO = Path(__file__).resolve().parents[2]


def _iso(ns: int) -> str:
    import time

    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ns // NS_PER_S))


def _market(ticker: str, strike: float, close_ns: int, **kw) -> dict:
    m = S.market(ticker=ticker, event_ticker=ticker.rsplit("-", 1)[0], floor_strike=strike,
                 close_time=_iso(close_ns), expected_expiration_time=_iso(close_ns), open_time=_iso(close_ns - 86400 * NS_PER_S))
    m.update(kw)
    return m


def _registry() -> MarketRegistry:
    h1 = (T0 // (3600 * NS_PER_S) + 1) * 3600 * NS_PER_S
    reg = MarketRegistry(FeeEngine.from_config())
    reg.add_series(dict(S.SERIES_KXBTCD, fee_type="quadratic_with_maker_fees"))
    for h, tag in ((h1, "13"), (h1 + 3600 * NS_PER_S, "14"), (h1 + 5 * 3600 * NS_PER_S, "18")):
        et = f"KXBTCD-26SEP25{tag}"
        reg.add_event(dict(S.EVENT_KXBTCD, event_ticker=et))
        reg.add_market(_market(f"{et}-T84000.00", 84000.0, h))
        reg.add_market(_market(f"{et}-T84250.00", 84250.0, h))
    reg.add_market(_market("KXBTCD-26SEP2513-T90000.00", 90000.0, h1, rules_primary="something else entirely"))
    reg.add_market(_market("KXBTCD-26SEP2513-T70000.00", 70000.0, h1, status="closed"))
    return reg


def test_select_specs_horizon_rules_status_exclusions():
    reg = _registry()
    sel = select_specs(reg, T0, 2 * 3600, series=("KXBTCD",))
    got = sorted(s.ticker for s in sel.specs)
    assert got == ["KXBTCD-26SEP2513-T84000.00", "KXBTCD-26SEP2513-T84250.00",
                   "KXBTCD-26SEP2514-T84000.00", "KXBTCD-26SEP2514-T84250.00"]
    assert sel.skipped["KXBTCD-26SEP2513-T90000.00"].startswith("rules check")
    assert sel.skipped["KXBTCD-26SEP2513-T70000.00"] == "status 'closed'"
    ex = select_specs(reg, T0, 2 * 3600, series=("KXBTCD",), exclude_events={"KXBTCD-26SEP2513"})
    assert all(s.event_ticker == "KXBTCD-26SEP2514" for s in ex.specs)
    assert ex.skipped["KXBTCD-26SEP2513-T84000.00"].startswith("event already holds")
    # known markets are not re-added; a changed spec is reported
    known = {s.ticker: s for s in sel.specs}
    import dataclasses

    t = "KXBTCD-26SEP2514-T84000.00"
    known[t] = dataclasses.replace(known[t], fee_type="quadratic")
    again = select_specs(reg, T0, 6 * 3600, series=("KXBTCD",), known=known)
    assert [s.event_ticker for s in again.specs] == ["KXBTCD-26SEP2518", "KXBTCD-26SEP2518"]
    assert again.changed == {t: "spec changed"}


def test_unresolved_fee_is_kept_but_flagged():
    reg = MarketRegistry(FeeEngine.from_config())
    reg.add_series({k: v for k, v in S.SERIES_KXBTCD.items() if k not in ("fee_type", "fee_multiplier")})
    h1 = (T0 // (3600 * NS_PER_S) + 1) * 3600 * NS_PER_S
    reg.add_market(_market("KXBTCD-26SEP2513-T84000.00", 84000.0, h1))
    sel = select_specs(reg, T0, 7200)
    assert len(sel.specs) == 1 and sel.specs[0].fee_type == ""
    assert "unresolved" in sel.skipped["KXBTCD-26SEP2513-T84000.00"]


async def test_discover_universe_via_rest():
    rest = FakeRest()
    h1 = (T0 // (3600 * NS_PER_S) + 1) * 3600 * NS_PER_S
    rest.series["KXBTCD"] = dict(S.SERIES_KXBTCD, fee_type="quadratic_with_maker_fees")
    rest.events["KXBTCD"] = [dict(S.EVENT_KXBTCD, event_ticker="KXBTCD-26SEP2513",
                                  markets=[_market("KXBTCD-26SEP2513-T84000.00", 84000.0, h1)])]
    sel = await discover_universe(rest, ("KXBTCD",), FeeEngine.from_config(), T0, 7200)
    assert [s.ticker for s in sel.specs] == ["KXBTCD-26SEP2513-T84000.00"]
    assert sel.specs[0].fee_type == "quadratic_with_maker_fees"
    assert rest.of("iter_events")[0][1]["status"] == "open"


def test_events_with_positions():
    assert events_with_positions({"KXBTCD-26SEP2513-T84000.00": 100, "KXBTCD-26SEP2514-T1.00": 0}) == {"KXBTCD-26SEP2513"}


def test_spec_roundtrip():
    s = kxbtcd_spec()
    assert spec_from_dict(spec_to_dict(s)) == s
    import orjson

    assert spec_from_dict(orjson.loads(orjson.dumps(spec_to_dict(s)))) == s


def _cf_history(end_ns: int, days: float, rng_seed: int = 0, step_ms: int = 1000):
    """Fake passthrough: 1 Hz BRTI for the requested window (timespan '<s>s', timestamp end ms)."""
    rng = np.random.default_rng(rng_seed)

    def body(timespan, timestamp):
        span = int(str(timespan).rstrip("s"))
        end_ms = int(timestamp)
        start_ms = end_ms - span * 1000
        ts = np.arange(start_ms, end_ms + 1, step_ms)
        base = 84_000.0 * np.exp(np.cumsum(rng.standard_normal(len(ts)) * 0.35 / math.sqrt(365 * 86400)))
        return {"payload": [{"time": int(t), "value": f"{v:.2f}"} for t, v in zip(ts, base)]}

    return body


async def test_backfill_chunks_resamples_and_warms_model():
    rest = FakeRest()
    rest.cf_history = _cf_history(T0, 2.0, step_ms=10_000)
    fv = FairValueModel.from_config(load_recommended_config())
    assert not fv.ready
    cfg = BackfillCfg(days=2.0, chunk_s=6 * 3600, step_s=60)
    res = await backfill_fair_value(rest, fv, T0, cfg)
    assert res.ready and fv.ready and res.source == "cfbenchmarks_rest"
    assert res.requests == 8 and res.coverage > 0.99
    calls = rest.of("get_cfbenchmarks_history")
    assert calls[0][1] == {"timespan": "21600s", "timestamp": str(T0 // NS_PER_MS)}  # newest chunk first
    assert calls[1][1]["timestamp"] == str((T0 - 6 * 3600 * NS_PER_S) // NS_PER_MS)
    gaps = {b[0] - a[0] for a, b in zip(res.points, res.points[1:])}
    assert gaps == {60 * NS_PER_S} and res.points[-1][0] <= T0


async def test_backfill_unavailable_leaves_model_not_ready():
    rest = FakeRest()  # passthrough returns 404
    fv = FairValueModel.from_config(load_recommended_config())
    res = await backfill_fair_value(rest, fv, T0, BackfillCfg())
    assert not res.ready and res.source == "none" and res.errors and res.points == []


async def test_backfill_too_sparse_is_not_used():
    rest = FakeRest()
    only_recent = _cf_history(T0, 2.0, step_ms=10_000)
    calls = {"n": 0}

    def body(timespan, timestamp):
        calls["n"] += 1
        if calls["n"] > 3:
            return {"payload": []}  # history stops after 3 hours
        return only_recent(timespan, timestamp)

    rest.cf_history = body
    fv = FairValueModel.from_config(load_recommended_config())
    res = await backfill_fair_value(rest, fv, T0, BackfillCfg())
    assert res.source == "none" and not fv.ready and res.coverage < 0.9


def test_resample_carries_last_value_and_skips_outages():
    b = (T0 // (60 * NS_PER_S)) * 60 * NS_PER_S  # the grid is absolute multiples of step_s
    ticks = [(b + 5 * NS_PER_S, 1.0), (b + 65 * NS_PER_S, 2.0), (b + 2000 * NS_PER_S, 3.0)]
    pts = resample(ticks, b + 1, b + 2100 * NS_PER_S, 60, max_stale_s=600)
    assert pts[0] == (b + 60 * NS_PER_S, 1.0) and pts[1] == (b + 120 * NS_PER_S, 2.0)
    assert all(t % (60 * NS_PER_S) == 0 for t, _ in pts)
    assert all(v != 2.0 or t <= b + 665 * NS_PER_S for t, v in pts)  # stale after 600 s: skipped
    assert (b + 720 * NS_PER_S, 2.0) not in pts and pts[-1] == (b + 2100 * NS_PER_S, 3.0)


def test_paper_sim_and_conservative_fees():
    fe = FeeEngine.from_config()
    specs = [kxbtcd_spec(fee_type="quadratic"), kxbtcd_spec(84_250.0, ticker="KXBTCD-26SEP2513-T84250.00",
                                                              fee_type="quadratic_with_maker_fees")]
    sim, fees = build_paper_sim(PaperCfg(), specs, fe)
    assert sim.policy == "conservative" and set(sim.markets) == {s.ticker for s in specs}
    assert sim.orders == {} and sim.id_prefix == "paper"
    maker = fe.schedule_for_spec("quadratic_with_maker_fees", 1.0).trade_fee_micros(5000, 100, False)
    assert fees(5000, 100, False) == maker > 0  # the most expensive schedule
    assert PaperFees(fe)(5000, 100, False) == 0


def test_live_example_config_loads_and_is_paper():
    cfg = load_live_config(REPO / "config" / "live.example.yaml")
    assert cfg.mode == "paper" and cfg.paths.kill_file == "/run/dh/KILL" and cfg.metrics.host == "127.0.0.1"
    assert cfg.backfill.days >= 2 and cfg.paper.policy == "conservative"
    assert cfg.digest() == load_live_config(REPO / "config" / "live.example.yaml").digest()


def test_unknown_live_config_key_rejected(tmp_path):
    p = tmp_path / "live.yaml"
    p.write_text("mode: paper\nvenue:\n  max_batc: 3\n")
    with pytest.raises(KeyError, match="venue.max_batc"):
        load_live_config(p)
    p.write_text("mode: yolo\n")
    with pytest.raises(ValueError):
        load_live_config(p)


def test_resolve_mode_requires_config_and_flag():
    assert resolve_mode(None, "paper", False) == "paper"
    assert resolve_mode("paper", "live", False) == "paper"  # downgrading is always allowed
    assert resolve_mode(None, "live", True) == "live"
    assert resolve_mode("live", "live", True) == "live"
    with pytest.raises(ModeError, match=LIVE_CONFIRM_FLAG):
        resolve_mode("live", "live", False)
    with pytest.raises(ModeError, match="mode: live"):
        resolve_mode("live", "paper", True)
    assert LiveConfig().mode == "paper"


def test_hedge_venue_disabled_rejects_and_perps_skeleton_refuses():
    out: list = []
    h = build_hedge_venue(False, "kalshi_perp", out.append, clock_ns=lambda: 5)
    assert isinstance(h, DisabledHedgeVenue)
    h.submit([PlaceHedge("h-1", "kalshi_perp", "KXBTCPERP", "buy", 0.01), CancelHedge("h-1", "kalshi_perp")], 1)
    assert [type(e) for e in out] == [HedgeOrderUpdate, HedgeOrderUpdate]
    assert out[0].status == "rejected" and "hedging disabled" in out[0].reason and out[0].ts == 5
    with pytest.raises(NotImplementedError):
        build_hedge_venue(True, "kalshi_perp", out.append)
    with pytest.raises(NotImplementedError):
        KalshiPerpHedgeVenue(out.append)
    with pytest.raises(ValueError):
        build_hedge_venue(True, "binance", out.append)


def test_ledger_from_log(tmp_path):
    from dh.live.monitor import JsonLog
    from dh.live.replay import ledger_from_log

    spec = kxbtcd_spec()
    jl = JsonLog(tmp_path / "s.jsonl", "c", "s")
    t = spec.expiration_ts - 1800 * NS_PER_S
    jl.write("log.fv", t, ticker=spec.ticker, F=0.50, delta=0.0001)
    jl.write("log.fill", t + 1, ticker=spec.ticker, coid="c-1", side="bid", px=4500, qty=200, taker=False, fee=10_000, F=0.5)
    jl.write("log.fv", t + 60 * NS_PER_S, ticker=spec.ticker, F=0.55, delta=0.0001)
    jl.write("log.settle", spec.expiration_ts + NS_PER_S, ticker=spec.ticker, px=10_000, position=2.0, pnl=1.09)
    jl.close()
    s = ledger_from_log(tmp_path / "s.jsonl", [spec]).summary()
    assert s["fills"] == 1 and s["contracts"] == 2.0
    assert abs(s["net_usd"] - (2 * (1.0 - 0.45) - 0.01)) < 1e-9  # bought 2 YES at 45c, settled YES, 1c fee
