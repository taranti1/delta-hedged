"""Ledger regression tests (audit M1 and minor #2)."""
from __future__ import annotations

import math

from dh.backtest.ledger import Ledger
from dh.core.actions import Log
from dh.core.events import HedgeFill, IndexTick, KalshiFill, Settlement

S = 10**9


def _fill(ts, side="bid", px=5000, qty=10000):
    return KalshiFill(ts=ts, ts_exch=0, ticker="M", trade_id=f"t{ts}", order_id="o", client_order_id="c",
                      book_side=side, yes_px=px, qty=qty, is_taker=False, fee_micros=0, post_position=qty)


def _tick(ts, v):
    return IndexTick(ts=ts, ts_exch=0, index_id="BRTI", value=v, feed="1hz")


def test_hedge_pnl_and_fees_allocated_across_clock_hours_and_idempotent():
    T = 12 * 3600 * S
    L = Ledger({"M": "E"}, {"M": T})
    t0 = 11 * 3600 * S - 10 * S
    L.on_log(t0 - S, Log("fv", {"ticker": "M", "F": 0.5, "delta": 0.001}))
    L.on_event(_tick(t0 - S, 84000.0))
    L.on_event(_fill(t0))
    h1 = 11 * 3600 * S + 2 * S
    L.on_event(_tick(h1, 84000.0))
    L.on_event(HedgeFill(ts=h1, ts_exch=0, venue="p", symbol="X", client_order_id="h1", side="sell", qty_btc=0.1,
                         price=84000.0, fee_usd=5.0, is_maker=True))
    L.on_event(_tick(h1 + S, 83700.0))
    L.on_event(HedgeFill(ts=h1 + S, ts_exch=0, venue="p", symbol="X", client_order_id="h2", side="buy", qty_btc=0.1,
                         price=83700.0, fee_usd=5.0, is_maker=True))
    L.on_event(Settlement(ts=T, ts_exch=0, ticker="M", result="no", settlement_px=0))
    df = L.attribute()
    assert math.isclose(df.net.sum(), -30.0)  # -50 Kalshi + 30 hedge - 10 fees
    s1, s2 = L.summary(), L.summary()
    assert math.isclose(s1["net_usd"], -30.0) and s1["net_usd"] == s2["net_usd"]
    assert s1["hedge_pnl"] == 30.0 and s1["hedge_fees"] == 10.0


def test_unallocated_hedge_costs_are_reported_not_dropped():
    L = Ledger({"M": "E"}, {"M": 100 * S})
    L.on_event(_tick(200 * S, 84000.0))
    L.on_event(HedgeFill(ts=200 * S, ts_exch=0, venue="p", symbol="X", client_order_id="h", side="buy", qty_btc=0.1,
                         price=84000.0, fee_usd=3.0, is_maker=False))
    L.attribute()
    assert L.hedge_totals["unallocated_fees"] == 3.0


def test_markouts_are_nan_when_fair_value_logging_stopped():
    L = Ledger({"M": "E"}, {"M": 1000 * S})
    L.on_log(10 * S, Log("fv", {"ticker": "M", "F": 0.5, "delta": 0.0}))
    L.on_event(_fill(11 * S))
    df = L.attribute()
    assert "mo_0.1s_c" in df and not math.isnan(df["mo_0.1s_c"].iloc[0])  # 1.1 s old FV is fresh
    assert "mo_60s_c" not in df or math.isnan(df["mo_60s_c"].iloc[0])  # nothing logged near t+60 s
