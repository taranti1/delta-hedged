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
                result="no", open_ts_ms=open_ms, close_ts_ms=exp_ms)


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


def test_near_cap_range_uses_nearest_boundary():
    from dh.research.calibrate_flow import order_sizes
    row = pd.DataFrame([dict(ticker="R", ts_ms=0, expiration_ts_ms=60_000,
                             floor_strike=80_000, cap_strike=100_000, contracts=1, taker_side="no")])
    assert list(order_sizes(row, lambda _: 100_000.0, .4)) == [("30-60s", "atm", "bid")]


def test_gap_excludes_both_orders_and_exposure_but_quiet_time_counts():
    from dh.research.calibrate_flow import flow_stats
    mk = pd.DataFrame([dict(ticker="R", open_ts_ms=0, expiration_ts_ms=60_000,
                            floor_strike=100_000, cap_strike=np.nan)])
    orders = pd.DataFrame([dict(ticker="R", ts_ms=t, expiration_ts_ms=60_000,
                               floor_strike=100_000, cap_strike=np.nan, contracts=1, taker_side="no")
                           for t in (5_000, 25_000, 45_000)])
    coverage = {"R": [(0, 20_000), (40_000, 60_000)]}
    a = flow_stats(orders, mk, lambda _: 100_000.0, grid_s=1, coverage=coverage)
    assert sum(a.exposure.values()) == 80  # 40 seconds * two sides, including quiet seconds
    assert sum(map(len, a.sizes.values())) == 2
    b = flow_stats(orders[orders.ts_ms != 25_000], mk, lambda _: 100_000.0, grid_s=1, coverage=coverage)
    assert a.exposure == b.exposure and a.sizes == b.sizes


def test_flow_proxy_cannot_be_loaded_for_production(tmp_path):
    from dh.research.calibrate_flow import save_segments, load_segments
    from dh.strategy.flow_features import PRODUCTION_FEATURE_VERSION
    p = save_segments({}, tmp_path / "segments.json")
    with pytest.raises(ValueError, match="incompatible flow features"):
        load_segments(p, expected_feature_version=PRODUCTION_FEATURE_VERSION)


def test_reference_staleness():
    from dh.research.calibrate_flow import _price_fn
    f = _price_fn(pd.DataFrame({"ts_ms": [0], "price": [123.0]}), 0, max_age_ms=3000)
    assert f(2999) == 123 and f(3001) is None


def test_delayed_print_health_and_features_use_receive_clock():
    from dh.research.calibrate_flow import order_sizes
    # The first print matched during healthy time but arrived during a recording gap.
    # The second is backlog from the gap and must not count as fresh flow after recovery.
    # Only the third matched and arrived during the same healthy interval.
    rows = pd.DataFrame([dict(ticker="R", ts_ms=1000, ts_recv_ms=2500, expiration_ts_ms=60_000,
                              floor_strike=100_000, cap_strike=np.nan, contracts=9, taker_side="no"),
                         dict(ticker="R", ts_ms=2500, ts_recv_ms=4500, expiration_ts_ms=60_000,
                              floor_strike=100_000, cap_strike=np.nan, contracts=7, taker_side="no"),
                         dict(ticker="R", ts_ms=39_500, ts_recv_ms=40_000, expiration_ts_ms=60_000,
                              floor_strike=100_000, cap_strike=np.nan, contracts=2, taker_side="no")])
    queried = []
    def price(at):
        queried.append(at)
        return 100_000.0 if at >= 4000 else 80_000.0
    result = order_sizes(rows, price, .4, coverage={"R": [(0, 2000), (4000, 60_000)]})
    assert result == {("<30s", "atm", "bid"): [2]}
    assert queried == [40_000]


def test_reconstructed_sweep_crossing_gap_is_excluded():
    from dh.research.calibrate_flow import taker_orders, order_sizes
    prints = pd.DataFrame({"ticker": ["R", "R"], "ts_ms": [1000, 1000], "ts_recv_ms": [1500, 4500],
                           "taker_side": ["no", "no"], "qty": [100, 200], "yes_px": [5000, 4900]})
    orders = taker_orders(prints).assign(expiration_ts_ms=60_000, floor_strike=100_000, cap_strike=np.nan)
    assert orders.iloc[0].ts_recv_ms == 4500 and orders.iloc[0].ts_recv_first_ms == 1500
    assert not order_sizes(orders, lambda _: 100_000.0, .4, coverage={"R": [(0, 2000), (4000, 6000)]})
    # With continuous observation the complete sweep becomes observable at its last receipt.
    queried = []
    def price(at):
        queried.append(at)
        return 100_000.0
    result = order_sizes(orders, price, .4, coverage={"R": [(0, 6000)]})
    assert sum(map(sum, result.values())) == 3 and queried == [4500]


def test_exchange_clock_ahead_of_receipt_still_counts_in_the_numerator():
    # On the recording host the exchange timestamp is often a few ms LATER than receipt; such
    # prints matched inside the healthy interval and must count (their exposure already does).
    from dh.research.calibrate_flow import order_sizes
    rows = pd.DataFrame([dict(ticker="R", ts_ms=40_020, ts_recv_ms=40_000, expiration_ts_ms=60_000,
                              floor_strike=100_000, cap_strike=np.nan, contracts=3, taker_side="no"),
                         dict(ticker="R", ts_ms=4_500, ts_recv_ms=4_010, expiration_ts_ms=60_000,
                              floor_strike=100_000, cap_strike=np.nan, contracts=4, taker_side="no")])
    result = order_sizes(rows, lambda _: 100_000.0, .4, coverage={"R": [(4000, 60_000)]})
    assert sorted(sum(result.values(), [])) == [3, 4]
    # an exchange time far outside the interval (beyond skew) is still backlog, not fresh flow
    late = rows.assign(ts_ms=[40_020, 2_000])
    result = order_sizes(late, lambda _: 100_000.0, .4, coverage={"R": [(4000, 60_000)]})
    assert sum(result.values(), []) == [3]
