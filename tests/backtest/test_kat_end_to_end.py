"""End-to-end known-answer tests: MarketMaker + exchange simulator + ledger on synthetic data."""
from __future__ import annotations

import math

import pytest

from dh.backtest.kat import default_kat_config, run_synthetic
from dh.core.actions import PlaceOrder
from dh.core.units import NS_PER_S, QTY_SCALE
from dh.sim.synthetic import SynthConfig


@pytest.fixture(scope="module")
def base_run():
    return run_synthetic(SynthConfig(duration_s=600, seed=12, informed=False, mm_lag_s=1.5), default_kat_config())


def test_accounting_identity_and_reconciliation(base_run):
    r = base_run
    assert r.mm.stats.fills > 0
    assert math.isclose(r.summary["net_usd"], r.mm.equity(None), abs_tol=1e-6)
    for t in r.mm.specs:
        assert r.mm.om.position(t) == r.sim.position(t)


def test_limits_respected(base_run):
    r = base_run
    lim = r.mm.cfg.risk.max_pos_per_market
    assert max(abs(r.mm.om.position(t)) / QTY_SCALE for t in r.mm.specs) <= lim
    # every placed order is post-only, on the tick grid and never inside the final near-strike window
    for ts, a in r.run.actions:
        if isinstance(a, PlaceOrder):
            spec = r.mm.specs[a.ticker]
            assert a.post_only and spec.is_valid_px(a.px)
            assert spec.expiration_ts - ts > 0


def test_no_quotes_before_kalshi_ready_or_after_close(base_run):
    r = base_run
    first_connect = None
    for ts, a in r.run.actions:
        if isinstance(a, PlaceOrder):
            first_connect = first_connect or ts
            assert ts < r.mm.specs[a.ticker].close_ts
    assert first_connect is not None


@pytest.mark.slow
def test_informed_flow_worsens_fill_markouts():
    cfg = default_kat_config()
    calm = run_synthetic(SynthConfig(duration_s=900, seed=31, informed=False, mm_lag_s=1.5), cfg)
    toxic = run_synthetic(SynthConfig(duration_s=900, seed=31, informed=True, informed_edge_ticks=0.5,
                                      mm_lag_s=1.5, vol_ann=0.9), cfg)
    assert calm.summary["contracts"] > 0 and toxic.summary["contracts"] > 0
    assert toxic.summary.get("markout_10s_c", 0.0) <= calm.summary.get("markout_10s_c", 0.0) + 0.5


def test_add_markets_roll_over(base_run):
    from dataclasses import replace as dc_replace

    mm = base_run.mm
    spec = next(iter(mm.specs.values()))
    new = dc_replace(spec, ticker=spec.ticker + "-NEXT", expiration_ts=spec.expiration_ts + 3600 * NS_PER_S,
                     close_ts=spec.close_ts + 3600 * NS_PER_S)
    assert mm.add_markets([new]) == [new.ticker]
    assert mm.add_markets([new]) == []
    assert new.ticker in mm.books and new.ticker in mm.fee_sched
