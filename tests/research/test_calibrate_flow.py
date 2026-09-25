"""Audit M3/M4/M6 regressions for flow calibration."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from dh.research.calibrate_flow import calibrate, split_tau


def test_split_tau_exact_durations():
    parts = split_tau(60.0, 0.0)
    assert [round(d, 6) for d, _ in parts] == [30.0, 30.0]
    assert sum(d for d, _ in split_tau(3600.0, 0.0)) == pytest.approx(3600.0)


def _market(i, K=100_000.0, open_ms=0, exp_ms=3_600_000):
    return dict(ticker=f"M{i}", event_ticker="E", strike_type="greater", floor_strike=K, cap_strike=np.nan,
                result="no", open_ts_ms=open_ms, expected_expiration_ts_ms=exp_ms)


def test_constant_flow_rate_recovered_in_every_tau_bucket():
    # one taker order of 1 contract per second selling YES (hits bids) for the whole hour, ATM
    trades = pd.DataFrame([dict(ticker="M0", ts_ms=1000 * s + 500, yes_px=5000, qty=100, taker_outcome_side="no",
                                is_block_trade=False, event_ticker="E") for s in range(3600)])
    markets = pd.DataFrame([_market(0)])
    btc = pd.DataFrame({"ts_ms": [0], "price": [100_000.0]})
    seg = calibrate(trades, markets, btc, prior_s=1e-9)
    for tb in ("<30s", "30-60s", "1-5m", "5-10m", "10-30m", ">30m"):
        assert seg[(tb, "atm", "bid")].rate_contracts_per_s == pytest.approx(1.0, rel=0.05)


def test_thin_segments_shrink_not_default():
    # 100 far-strike markets with 20 orders each over an hour: true rate ~ 20/3600 per market-second
    rows, mk = [], []
    for i in range(100):
        mk.append(_market(i, K=130_000.0))
        rows += [dict(ticker=f"M{i}", ts_ms=100_000 + 170_000 * k, yes_px=200, qty=100, taker_outcome_side="yes",
                      is_block_trade=False) for k in range(20)]
    seg = calibrate(pd.DataFrame(rows), pd.DataFrame(mk), pd.DataFrame({"ts_ms": [0], "price": [100_000.0]}))
    rates = [f.rate_contracts_per_s for k, f in seg.items() if k[1] == "far" and k[2] == "ask"]
    assert rates and max(rates) < 0.05  # nowhere near the 0.5/s global default


def test_block_trades_excluded():
    trades = pd.DataFrame([dict(ticker="M0", ts_ms=1_000_000, yes_px=5000, qty=100_000, taker_outcome_side="no",
                                is_block_trade=True)] +
                          [dict(ticker="M0", ts_ms=2_000_000 + k, yes_px=5000, qty=100, taker_outcome_side="no",
                                is_block_trade=False) for k in range(50)])
    seg = calibrate(trades, pd.DataFrame([_market(0)]), pd.DataFrame({"ts_ms": [0], "price": [100_000.0]}))
    assert seg[("*", "*", "bid")].size_mean == pytest.approx(1.0)
