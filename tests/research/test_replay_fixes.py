"""Replay-layer fixes: causal strike selection (audit C4), measured market-data latency (M4),
settlement clusters in ledgers (M5), book priming beyond the warm window (M6), status columns
(M7), causal warm-up and fee lookups (m3, m4), quotable public flow (m5), truncation invariance."""

from __future__ import annotations

import math

import numpy as np
import pandas as pd
import pytest

import dh.research.replay_env as re_
from dh.core.events import IndexTick
from dh.core.market import MarketSpec
from dh.execution.latency import LatencyModel
from dh.research.synth_recording import SynthRecordingConfig, write_synthetic_recording
from dh.sim.synthetic import SynthConfig

NS = 10**9
T0 = 1_800_000_000 * NS


# ------------------------------------------------------------------ C4: NearestStrikes
def _strike_universe(ticks_fn, late_avail_s=2400):
    T = T0 + 3600 * NS
    uni = re_.Universe(root="/nonexistent", t0=T0, t1=T)
    specs = []
    for K, avail in ((60000, T0 - 600 * NS), (60500, T0 - 600 * NS), (61000, T0 - 600 * NS),
                     (62000, T0 + late_avail_s * NS)):
        s = MarketSpec(f"KXBTCD-E-T{K}", "KXBTCD-E", "KXBTCD", "greater", float(K), None, T0 - 3600 * NS, T, T)
        uni.markets[s.ticker] = re_.MarketRecord(s.ticker, avail_ns=avail, spec=s)
        specs.append(s)
    return uni, specs


def _ticks(values_by_time):
    return [IndexTick(ts=t, ts_exch=t, index_id="BRTI", value=v, feed="1hz") for t, v in values_by_time]


def test_nearest_strikes_never_uses_a_price_after_the_reference_time(monkeypatch):
    base = [(t, 60000.0) for t in range(T0 - 7200 * NS, T0 + 1, 60 * NS)]
    later = [(t, 60000.0 + 1000.0 * (t - T0) / (2400 * NS)) for t in range(T0 + 60 * NS, T0 + 3600 * NS, 60 * NS)]
    monkeypatch.setattr(re_, "brti_ticks", lambda *a, **k: _ticks(base + later))
    uni, specs = _strike_universe(None)
    keep1 = [s.ticker for s in re_.NearestStrikes(1, 3900.0)(specs, uni)]
    assert keep1 == ["KXBTCD-E-T60000"]  # BRTI 60000 at the first quotable time t0
    # causality: rewriting every tick after t0 (the late strike's time excepted) changes nothing
    wild = [(t, 90000.0) for t, _ in later]
    monkeypatch.setattr(re_, "brti_ticks", lambda *a, **k: _ticks(base + wild))
    assert [s.ticker for s in re_.NearestStrikes(1, 3900.0)(specs, uni)][:1] == ["KXBTCD-E-T60000"]


def test_late_strike_is_evaluated_at_its_own_availability(monkeypatch):
    ticks = [(t, 60000.0) for t in range(T0 - 7200 * NS, T0 + 2400 * NS, 60 * NS)]
    ticks += [(t, 62100.0) for t in range(T0 + 2400 * NS, T0 + 3600 * NS, 60 * NS)]
    monkeypatch.setattr(re_, "brti_ticks", lambda *a, **k: _ticks(ticks))
    uni, specs = _strike_universe(None)
    keep = [s.ticker for s in re_.NearestStrikes(1, 3900.0)(specs, uni)]
    assert keep == ["KXBTCD-E-T60000", "KXBTCD-E-T62000"]  # 62000 is nearest when it is listed


def test_no_tick_before_the_reference_time_skips_the_event(monkeypatch):
    monkeypatch.setattr(re_, "brti_ticks", lambda *a, **k: _ticks([(T0 + 60 * NS, 60000.0)]))
    uni, specs = _strike_universe(None)
    ns = re_.NearestStrikes(1, 3900.0)
    assert ns(specs, uni) == [] and ns.skipped == ["KXBTCD-E"]


# ------------------------------------------------------------------ m4: fee lookups
def test_fee_lookup_never_uses_a_later_record():
    uni, specs = _strike_universe(None)
    t = specs[0].ticker
    uni.series["KXBTCD"] = [(T0 + 600 * NS, {"ticker": "KXBTCD", "fee_type": "quadratic_with_maker_fees", "fee_multiplier": 1})]
    assert uni.series_at("KXBTCD", T0) is None  # the only snapshot arrives 10 min later
    assert uni.fee_fields(t, T0)[0] == ""
    uni.markets[t].fee_obs.append((T0 + 60 * NS, {"fee_type": "quadratic"}))
    assert uni.fee_fields(t, T0)[0] == "" and uni.fee_fields(t, T0 + 61 * NS)[0] == "quadratic"
    # availability moves to the time the fee is first known
    n = re_.resolve_fee_availability(uni, T0, T0 + 3600 * NS)
    assert n >= 1 and uni.markets[t].avail_ns == T0 + 60 * NS and uni.markets[t].spec.fee_type == "quadratic"


# ------------------------------------------------------------------ m3: warm-up
def test_price_file_bars_are_known_at_their_close(tmp_path):
    p = tmp_path / "bars.csv"
    pd.DataFrame({"ts_ms": [0, 60_000, 120_000], "close": [1.0, 2.0, 3.0]}).to_csv(p, index=False)
    df = re_.load_price_file(p)
    assert list(df.ts_ns) == [60 * NS, 120 * NS, 180 * NS]
    q = tmp_path / "ticks.csv"
    pd.DataFrame({"ts_ms": [0, 60_000], "price": [1.0, 2.0]}).to_csv(q, index=False)
    assert list(re_.load_price_file(q).ts_ns) == [0, 60 * NS]  # point-in-time prices keep their stamp


def test_gbm_fallback_never_anchors_on_a_later_tick(tiny_rec):
    from dh.models.fvmodel import FairValueModel, load_recommended_config

    fv = FairValueModel.from_config(load_recommended_config())
    info = re_.warm_fair_value(fv, tiny_rec.root, tiny_rec.t0, "gbm")  # no price source before t0
    assert info.synthetic_fallback is False and not info.ready


# ------------------------------------------------------------------ M4, M5, M7, m5 on the synthetic recording
def test_replay_uses_measured_md_latency_expiration_clusters_and_status_columns(tiny_rec, synth_cfg):
    md, note = re_.measure_md_latency_ms(tiny_rec.root, tiny_rec.t0)
    assert md == pytest.approx(20.0, abs=1.0) and "measured" in note  # synthetic Kalshi delivery delay: 20 ms
    assert LatencyModel.DEFAULTS["md"].median() == 0.0  # the library default is untouched
    r = re_.run_replay(tiny_rec.root, tiny_rec.t0, tiny_rec.t1, synth_cfg, "B")
    s = r.summary
    assert s["md_latency_ms"] == pytest.approx(md) and "md 20" in s["latency"]
    df = r.df
    assert len(df) and df["event"].str.startswith("exp ").all()
    assert (df["event"] == df["expiration_ns"].map(re_.settlement_cluster)).all() and "event_ticker" in df
    assert df["synthetic"].all() and (df["fv_status"] == "n/a").all() and (df["flow_status"] == "defaults").all()
    assert {"rv_1h", "weekend", "day"} <= set(df.columns)
    assert 0 < s["public_contracts_quotable"] <= s["public_contracts_quoted"]


@pytest.mark.slow
def test_quotable_flow_excludes_markets_beyond_max_tau(tiny_rec, synth_cfg):
    from dataclasses import replace

    near = replace(synth_cfg, quoting=replace(synth_cfg.quoting, max_tau_s=120.0))
    a = re_.run_replay(tiny_rec.root, tiny_rec.t0, tiny_rec.t0 + 240 * NS, synth_cfg, "B").summary
    b = re_.run_replay(tiny_rec.root, tiny_rec.t0, tiny_rec.t0 + 240 * NS, near, "B").summary
    assert b["public_contracts_quotable"] < a["public_contracts_quotable"]


# ------------------------------------------------------------------ M6: priming
@pytest.fixture(scope="module")
def long_rec(tmp_path_factory):
    return write_synthetic_recording(tmp_path_factory.mktemp("long_rec"), SynthRecordingConfig(
        n_events=1, event_spacing_s=1800, seed=11,
        synth=SynthConfig(n_strikes_each_side=1, mm_lag_s=1.5, informed=False, vol_ann=0.6, mm_update_prob=0.3,
                          noise_taker_rate_per_s=0.08)))


@pytest.mark.slow
def test_books_are_primed_from_the_last_snapshot_before_t0(long_rec, synth_cfg):
    """Snapshots only at the recording start: a window starting 16 min later (> 15 min warm-up)
    used to leave every book invalid (0 quotes)."""
    t0 = long_rec.t0 + 16 * 60 * NS
    r = re_.run_replay(long_rec.root, t0, t0 + 180 * NS, synth_cfg, "B")
    assert r.summary["mm_quotes_placed"] > 0 and r.summary["books_never_valid"] == []
    short = re_.run_replay(long_rec.root, t0, t0 + 180 * NS, synth_cfg, "B", prime_search_s=600.0)
    assert short.summary["books_never_valid"] and any("never had a valid order book" in w
                                                      for w in short.summary["warnings"])


# ------------------------------------------------------------------ truncation invariance
@pytest.mark.slow
def test_truncation_leaves_earlier_actions_unchanged(tiny_rec, synth_cfg):
    from dh.core.actions import Log

    t_mid = tiny_rec.t0 + 300 * NS

    def actions(t1):
        acts = []
        uni = re_.build_universe(tiny_rec.root, tiny_rec.t0, t1)
        re_.run_replay(tiny_rec.root, tiny_rec.t0, t1, synth_cfg, "B", universe=uni,
                       action_hooks=[lambda ts, a: acts.append((ts, repr(a))) if not isinstance(a, Log) else None])
        return [x for x in acts if x[0] < t_mid]

    full, cut = actions(tiny_rec.t1), actions(t_mid)
    assert full and full == cut


def test_realized_vol_asof_is_causal():
    t = np.arange(0, 7200) * NS
    v = 60000 * np.exp(np.cumsum(np.random.default_rng(0).normal(0, 1e-4, len(t))))
    q = np.array([3600 * NS, 5400 * NS])
    rv = re_.realized_vol_asof(t, v, q)
    v2 = v.copy()
    v2[4000:] *= 1.5  # a jump after the first query time
    rv2 = re_.realized_vol_asof(t, v2, q)
    assert rv[0] == pytest.approx(rv2[0]) and not math.isclose(rv[1], rv2[1])
