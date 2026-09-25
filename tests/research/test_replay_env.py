"""dh.research.replay_env: universe rebuild, own-footprint filter, priming, replay determinism."""

from __future__ import annotations

import math

import numpy as np
import pandas as pd
import pytest

from dh.backtest.kat import default_kat_config, warm_fv_model
from dh.backtest.runner import run as runner_run
from dh.core.actions import Log, PlaceOrder
from dh.core.book import KalshiBook
from dh.core.events import (
    FeedStatus,
    KalshiBookDelta,
    KalshiBookSnapshot,
    KalshiFill,
    KalshiOrderUpdate,
    KalshiTrade,
)
from dh.core.units import NS_PER_S
from dh.execution.exchange_sim import KalshiExchangeSim
from dh.execution.latency import LatencyModel
from dh.kalshi.fees import FeeEngine
from dh.models.fvmodel import FairValueModel, load_recommended_config
from dh.research.replay_env import (
    MarketRecord,
    NearestStrikes,
    OwnFootprintFilter,
    ReplayStream,
    Universe,
    _lifecycle_market,
    _merge,
    _try_spec,
    build_universe,
    drive,
    load_price_file,
    parse_warm,
    run_replay,
    warm_fair_value,
)
from dh.sim.synthetic import SynthConfig, SyntheticMarket
from dh.strategy.mm import MarketMaker

T0 = 1_790_000_000 * NS_PER_S


def _iso(ns: int) -> str:
    import time

    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ns // NS_PER_S))


# ============================================================================ universe
def test_universe_rebuilt_from_recording(tiny_rec):
    u = build_universe(tiny_rec.root, tiny_rec.t0, tiny_rec.t1)
    assert u.synthetic and not u.rejected()
    specs = u.specs(["KXBTCD"])
    assert len(specs) == sum(len(v) for v in tiny_rec.tickers.values()) == 10
    settle = u.settlement_values()
    for s in specs:
        assert s.fee_type == "quadratic_with_maker_fees" and s.fee_multiplier == 1.0  # from GET /series/{s}
        assert s.strike_type == "greater" and s.floor_strike is not None
        rec = u.markets[s.ticker]
        open_ns = s.expiration_ts - 240 * NS_PER_S
        assert rec.avail_ns == open_ns + 20 * NS_PER_S  # strike arrives via metadata_updated 20 s after open
        A = tiny_rec.settlement_values[s.event_ticker]
        assert settle[s.ticker] == (1.0 if A > s.floor_strike else 0.0)
        assert rec.expiration_value == pytest.approx(A, abs=0.006)  # REST GET /markets/{t} after settlement


def test_fee_precedence_scheduled_changes_and_ws_override():
    u = Universe(root="", t0=T0, t1=T0 + 3600 * NS_PER_S)
    u.series["KXS"] = [(T0 - 10, {"ticker": "KXS", "fee_type": "quadratic", "fee_multiplier": 1})]
    u.series_fee_changes = [{"series_ticker": "KXS", "fee_type": "quadratic_with_maker_fees", "fee_multiplier": 2,
                             "scheduled_ts": _iso(T0 + 100 * NS_PER_S)}]
    u.markets["KXS-E1-T1"] = MarketRecord("KXS-E1-T1", raw={"ticker": "KXS-E1-T1", "event_ticker": "KXS-E1"})
    assert u.fee_fields("KXS-E1-T1", T0) == ("quadratic", 1.0, "series")
    ft, m, _ = u.fee_fields("KXS-E1-T1", T0 + 200 * NS_PER_S)
    assert (ft, m) == ("quadratic_with_maker_fees", 2.0)
    u.ws_fee_updates.append((T0 + 300 * NS_PER_S, "KXS-E1", "quadratic", "0.5"))
    assert u.fee_fields("KXS-E1-T1", T0 + 400 * NS_PER_S) == ("quadratic", 0.5, "event_override")
    assert len(u.fee_changes_in_window()) == 2


def test_lifecycle_created_without_strike_then_metadata_update():
    created = {"market_ticker": "KXBTC15M-26SEP241215-15", "event_type": "created", "open_ts": T0 // NS_PER_S,
               "close_ts": T0 // NS_PER_S + 900,
               "additional_metadata": {"event_ticker": "KXBTC15M-26SEP241215", "expected_expiration_ts": T0 // NS_PER_S + 900,
                                       "strike_type": "greater", "title": "BTC up?"},
               "price_level_structure": "linear_cent", "price_ranges": [{"start": "0.0000", "end": "1.0000", "step": "0.0100"}]}
    rec = MarketRecord(created["market_ticker"])
    kind, patch = _lifecycle_market(created)
    assert kind == "created"
    _merge(rec.raw, patch)
    assert _try_spec(rec) is None and "floor_strike" in rec.reject
    kind, patch = _lifecycle_market({"market_ticker": created["market_ticker"], "event_type": "metadata_updated",
                                     "strike_type": "greater", "floor_strike": 84123.45})
    _merge(rec.raw, patch)
    _merge(rec.raw, {"floor_strike": None})  # a later record without the strike never erases it
    spec = _try_spec(rec)
    assert spec is not None and spec.floor_strike == 84123.45 and spec.series_ticker == "KXBTC15M"
    assert spec.expiration_ts == T0 + 900 * NS_PER_S and spec.is_valid_px(5600)


# ============================================================================ own footprint
def _apply(book: KalshiBook, evs) -> None:
    for e in evs:
        if isinstance(e, KalshiBookSnapshot):
            book.apply_snapshot(e)
        elif isinstance(e, KalshiBookDelta):
            assert book.apply_delta(e), e


def test_own_footprint_removed_from_book_and_trades():
    tk = "KXBTCD-X-T1"
    fill = KalshiFill(ts=T0 + 5, ts_exch=0, ticker=tk, trade_id="tr1", order_id="o1", client_order_id="me-1",
                      book_side="bid", yes_px=4500, qty=500, is_taker=False, fee_micros=0, post_position=500)
    f = OwnFootprintFilter({"tr1": fill, "tr2": KalshiFill(T0 + 20, 0, tk, "tr2", "o2", "me-2", "bid", 4500, 300, False, 0, 300)})
    down = KalshiBook(tk)
    out = f(KalshiBookSnapshot(T0, 0, tk, 1, 1, yes_bids=((4500, 1000),), no_bids=((5300, 800),)))
    _apply(down, out)
    assert f(KalshiBookDelta(T0 + 1, 0, tk, 1, 2, "yes", 4500, 500, own_client_order_id="me-1")) == []
    _apply(down, f(KalshiBookDelta(T0 + 2, 0, tk, 1, 3, "yes", 4500, 200)))
    assert down.bid_qty(4500) == 1200  # others only
    # taker sells 1500: 1000 from others (tr0) then our 500 (tr1); one level decrease without our id
    t0 = f(KalshiTrade(T0 + 3, 0, tk, "tr0", 4500, 1000, "no"))
    t1 = f(KalshiTrade(T0 + 3, 0, tk, "tr1", 4500, 500, "no"))
    assert [e.qty for e in t0] == [1000] and [e for e in t1 if isinstance(e, KalshiTrade)] == []
    _apply(down, f(KalshiBookDelta(T0 + 4, 0, tk, 1, 4, "yes", 4500, -1500)))
    assert down.bid_qty(4500) == 200
    assert f(fill) == []  # private fill message dropped (already matched by trade_id)
    # decrease BEFORE its print: transient, corrected retroactively when the print arrives
    assert f(KalshiBookDelta(T0 + 10, 0, tk, 1, 5, "yes", 4500, 300, own_client_order_id="me-2")) == []
    _apply(down, f(KalshiBookDelta(T0 + 11, 0, tk, 1, 6, "yes", 4500, -300)))
    _apply(down, f(KalshiTrade(T0 + 12, 0, tk, "tr2", 4500, 300, "no")))
    assert down.bid_qty(4500) == 200
    # snapshots subtract our resting qty; private and order-group messages never pass
    assert f(KalshiBookDelta(T0 + 30, 0, tk, 1, 7, "yes", 4400, 100, own_client_order_id="me-3")) == []
    snap = f(KalshiBookSnapshot(T0 + 31, 0, tk, 1, 8, yes_bids=((4400, 150), (4500, 200)), no_bids=((5300, 800),)))
    assert dict(snap[0].yes_bids) == {4400: 50, 4500: 200}
    assert f(KalshiOrderUpdate(T0, 0, tk, "o", "me-1", "resting", "bid", 4500, 500, 0, 500)) == []
    assert f(FeedStatus(T0, 0, "kalshi.order_group:g", "error")) == []
    assert f.stats["own_print_qty_removed"] == 800 and f.stats["absorbed_qty"] == 500
    assert f.stats["retro_absorbed_qty"] == 300


# ============================================================================ stream priming
def test_stream_primes_state_mid_recording(tiny_rec):
    t0 = tiny_rec.t0 + 120 * NS_PER_S
    s = ReplayStream(tiny_rec.root, t0, t0 + 10 * NS_PER_S)
    evs = list(s)
    first = [e for e in evs if e.ts == t0]
    assert any(isinstance(e, FeedStatus) and e.stream == "kalshi.ws" and e.status == "connected" for e in first)
    snaps = {e.ticker for e in first if isinstance(e, KalshiBookSnapshot)}
    assert snaps == set(tiny_rec.tickers[next(iter(tiny_rec.tickers))])
    assert all(e.ts >= t0 for e in evs) and s.pre_brti and all(t.ts < t0 for t in s.pre_brti)
    assert [e.ts for e in evs] == sorted(e.ts for e in evs)


def test_nearest_strikes_filter(tiny_rec):
    u = build_universe(tiny_rec.root, tiny_rec.t0, tiny_rec.t1)
    specs = u.specs(["KXBTCD"])
    one = NearestStrikes(1)(specs, u)
    three = NearestStrikes(3)(specs, u)
    assert len(one) == 2 and len(three) == 6 and NearestStrikes(0)(specs, u) == specs
    assert {s.ticker for s in one} <= {s.ticker for s in three}


# ============================================================================ warm-up
def test_parse_warm_and_csv_warmup(tmp_path):
    assert parse_warm("recorded+gbm") == ["recorded", "gbm"]
    assert parse_warm("csv:/x/p.csv,recorded") == ["csv:/x/p.csv", "recorded"]
    with pytest.raises(ValueError):
        parse_warm("bogus")
    ts = [(T0 - (3000 - i) * 60 * NS_PER_S) // 1_000_000 for i in range(3000)]
    rng = np.random.default_rng(0)
    px = 84000 * np.exp(np.cumsum(rng.normal(0, 0.0008, len(ts))))
    p = tmp_path / "prices.csv"
    pd.DataFrame({"ts_ms": ts, "price": px}).to_csv(p, index=False)
    assert len(load_price_file(p)) == 3000
    fv = FairValueModel.from_config(load_recommended_config())
    info = warm_fair_value(fv, tmp_path, T0, f"csv:{p}", fv_warm_s=3 * 86400)
    assert info.ready and info.n_ticks == 3000 and not info.synthetic_fallback


# ============================================================================ replay
def test_drive_matches_backtest_runner():
    sm = SyntheticMarket(SynthConfig(duration_s=150, seed=4, n_strikes_each_side=2, informed=False))
    events = sm.generate()
    cfg = default_kat_config()

    def build():
        fv = FairValueModel.from_config(load_recommended_config())
        warm_fv_model(fv, events[0].ts, 84000.0, 0.35)
        fe = FeeEngine.from_config()
        mm = MarketMaker(cfg, sm.specs(), fv_model=fv, fee_engine=fe)
        sched = fe.schedule_for_spec("quadratic_with_maker_fees", 1.0)
        sim = KalshiExchangeSim(LatencyModel.fixed(30, 30, 10), "realistic", sched.trade_fee_micros, seed=2)
        for s in sm.specs():
            sim.register_market(s)
        return mm, sim

    mm, sim = build()
    ref = runner_run(events, mm, sim, timer_period_ns=cfg.timers.quote_period_ms * 1_000_000)
    mm2, sim2 = build()
    got: list = []
    drive(events, mm2, sim2, timer_period_ns=cfg.timers.quote_period_ms * 1_000_000, end_ns=None,
          on_action=lambda ts, a: got.append((ts, a)) if not isinstance(a, Log) else None)
    assert got == ref.actions and len(got) > 0
    assert mm2.stats.fills == mm.stats.fills


def test_replay_end_to_end_is_deterministic_and_causal(tiny_rec, synth_cfg):
    u = build_universe(tiny_rec.root, tiny_rec.t0, tiny_rec.t1)
    placed: list[tuple[int, str]] = []

    def hook(ts, a):
        if isinstance(a, PlaceOrder):
            placed.append((ts, a.ticker))

    r1 = run_replay(tiny_rec.root, tiny_rec.t0, tiny_rec.t1, synth_cfg, "B", universe=u, action_hooks=[hook])
    r2 = run_replay(tiny_rec.root, tiny_rec.t0, tiny_rec.t1, synth_cfg, "B", universe=u)
    s = r1.summary
    assert s["synthetic"] and s["fv_ready_at_t0"] and not s["warnings"] and s["fills"] > 0
    assert s["n_initial"] == 0 and s["n_added_late"] == 10 and s["unsettled_fills"] == 0
    pd.testing.assert_frame_equal(r1.df, r2.df)
    avail = u.availability()
    assert placed and all(ts >= avail[t] for ts, t in placed)  # never quotes a market before its strike is known
    assert set(r1.df["policy"]) == {"B"} and r1.df["coid"].str.startswith("synth-").all()
    assert r1.summary["net_c_per_contract"] == pytest.approx(
        100 * r1.df["net"].sum() / r1.df["contracts"].sum(), rel=1e-9)
    assert math.isfinite(r1.summary["quote_hours"]) and r1.summary["quote_hours"] > 0


def test_synthetic_recording_passes_the_live_sequencer(tiny_rec):
    """Raw frames are validated by dh.kalshi.sequencer exactly like a live capture: contiguous
    per-sid sequence numbers, snapshots before deltas, no parse errors."""
    s = ReplayStream(tiny_rec.root, tiny_rec.t0 - 120 * NS_PER_S, tiny_rec.t1 + 120 * NS_PER_S)
    evs = list(s)
    bad = [e for e in evs if isinstance(e, FeedStatus) and e.status in ("gap", "error")]
    assert not bad, bad[:3]
    assert sum(isinstance(e, KalshiTrade) for e in evs) > 0
    assert s.read_stats.truncated_files == [] and s.read_stats.bad_lines == 0


def _add_own_fills(rec_info, n: int = 3) -> list[str]:
    """Pretend some public trades of the recording were OUR maker fills: write the matching private
    `fill` frames into kalshi.ws (a later part file of the same hours)."""
    import orjson

    from dh.core.units import px_to_dollars, qty_to_fp
    from dh.store.recorder import Recorder

    late = rec_info.t0 + 60 * NS_PER_S  # after every first-event market is available (strike known)
    trades = [e for e in ReplayStream(rec_info.root, rec_info.t0, rec_info.t1, own_filter=False)
              if isinstance(e, KalshiTrade) and e.ts > late][:n]
    with Recorder(rec_info.root, start=False, clock_ns=lambda: rec_info.t0) as r:
        for i, tr in enumerate(trades):
            bid = tr.taker_side == "no"  # taker sold YES into our bid
            msg = {"type": "fill", "sid": 99, "msg": {
                "trade_id": tr.trade_id, "order_id": f"live-o{i}", "client_order_id": f"live-{i}",
                "market_ticker": tr.ticker, "is_taker": False, "yes_price_dollars": px_to_dollars(tr.yes_px),
                "count_fp": qty_to_fp(tr.qty), "fee_cost": "0.001000", "ts_ms": tr.ts_exch // 1_000_000,
                "post_position_fp": qty_to_fp(tr.qty if bid else -tr.qty),
                "outcome_side": "yes" if bid else "no", "book_side": "bid" if bid else "ask"}}
            r.write("kalshi.ws", tr.ts + 1_000_000, orjson.dumps(msg))
    return [t.trade_id for t in trades]


def test_live_fills_are_found_removed_from_prints_and_analyzed(tiny_rec, tmp_path, synth_cfg):
    import shutil

    from dh.research.exp3_toxicity import live_fill_table
    from dh.research.synth_recording import SynthRecordingInfo

    root = tmp_path / "live"
    shutil.copytree(tiny_rec.root, root)
    info = SynthRecordingInfo(str(root), tiny_rec.t0, tiny_rec.t1, tiny_rec.series, tiny_rec.expirations,
                              tiny_rec.tickers, tiny_rec.settlement_values, {}, {})
    ids = _add_own_fills(info)
    u = build_universe(root, info.t0, info.t1)
    assert set(u.own_fills) == set(ids)
    s = ReplayStream(root, info.t0, info.t1, own_fills=u.own_fills)
    public = {e.trade_id for e in s if isinstance(e, KalshiTrade)}
    assert not public & set(ids) and s.filter.stats["private_KalshiFill"] == len(ids)
    df = live_fill_table(root, info.t0, info.t1, synth_cfg, u)
    assert len(df) == len(ids) and df["policy"].eq("live").all()
    assert df["mkpx_1s_c"].notna().all() and df["adv_ext_1s"].notna().all() and df["settle"].notna().all()


def test_restrict_universe_keeps_only_filtered_specs(tiny_rec):
    from dh.research.replay_env import restrict_universe

    u = build_universe(tiny_rec.root, tiny_rec.t0, tiny_rec.t1)
    r = restrict_universe(u, NearestStrikes(2), ["KXBTCD"], reason="--max-strikes 2")
    assert len(r.specs(["KXBTCD"])) == 4 and len(u.specs(["KXBTCD"])) == 10
    assert sum(v == "--max-strikes 2" for v in r.rejected().values()) == 6 and r.notes


def test_open_recording_specs_fees_and_lazy_events(tiny_rec):
    from dh.research.replay_env import open_recording

    rec = open_recording(tiny_rec.root, tiny_rec.t0, tiny_rec.t1, series=["KXBTCD"])
    assert len(rec.specs) == 10
    assert rec.fee_schedules["KXBTCD"].fee_type == "quadratic_with_maker_fees" and rec.fee_schedules["KXBTCD"].supported
    tl = rec.universe.series_fee_timeline("KXBTCD")
    assert tl and tl[0]["source"] == "series_snapshot"
    it = rec.events()
    first = next(it)
    assert first.ts >= tiny_rec.t0  # lazy: nothing read before the first next()
    raw = rec.raw_events()
    assert sum(1 for e in raw if isinstance(e, KalshiTrade)) > 0
