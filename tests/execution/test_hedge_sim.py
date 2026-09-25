from __future__ import annotations

import pytest

from dh.core.actions import CancelHedge, PlaceHedge, PlaceOrder
from dh.core.events import ExtBookDelta, ExtBookSnapshot, ExtTrade, HedgeFill, HedgeOrderUpdate, PerpState
from dh.execution import HedgeVenueSim, LatencyModel, run_interleaved
from tests.execution.helpers import MS, S, Scripted

V, SYM = "kalshi_perp", "KXBTCPERP"


def book(ts=0, bids=((99.0, 2.0), (98.0, 3.0)), asks=((100.0, 1.0), (101.0, 2.0))):
    return ExtBookSnapshot(ts, 0, V, SYM, tuple(bids), tuple(asks))


def tr(ts, px, size, agg):
    return ExtTrade(ts, 0, V, SYM, px, size, agg)


def hsim(policy="B", lat=None, **kw):
    return HedgeVenueSim(V, 1.0, 5.0, lat or LatencyModel.zero(), 3, symbol=SYM, fill_policy=policy, **kw)


def run(sim, events, sched):
    strat = Scripted(sched)
    return strat, run_interleaved(events, strat, [sim])


def test_market_order_walks_book_partial_fill_and_slippage():
    sim = hsim()
    strat, out = run(sim, [book()], [(0, PlaceHedge("h1", V, SYM, "buy", 4.0, "market"))])
    fills = [e for e in out if isinstance(e, HedgeFill)]
    assert [(f.price, f.qty_btc, f.is_maker) for f in fills] == [(100.0, 1.0, False), (101.0, 2.0, False)]
    assert fills[0].fee_usd == pytest.approx(100.0 * 1.0 * 5.0 / 1e4)
    up = [e for e in out if isinstance(e, HedgeOrderUpdate)][-1]
    assert (up.status, up.filled_btc, up.remaining_btc, up.reason) == ("canceled", 3.0, 0.0, "insufficient_depth")
    rec = sim.slippage_log[0]
    assert rec.vwap == pytest.approx(302.0 / 3) and rec.decision_mid == 99.5 and rec.levels == 2
    assert rec.slippage_bps_vs_decision == pytest.approx((302.0 / 3 - 99.5) / 99.5 * 1e4)
    assert sim.position_btc == 3.0 and sim.cash_usd == pytest.approx(-302.0)
    assert sim.pnl_usd(mark=100.0) == pytest.approx(300.0 - 302.0 - sim.fees_usd)


def test_latency_market_order_sees_book_at_arrival():
    sim = hsim(lat=LatencyModel.fixed(10, 20, 1))
    evs = [book(), ExtBookDelta(5 * MS, 0, V, SYM, (("a", 100.0, 0.0),))]
    strat, out = run(sim, evs, [(0, PlaceHedge("h1", V, SYM, "buy", 1.0, "market"))])
    fills = [e for e in out if isinstance(e, HedgeFill)]
    assert [(f.price, f.ts) for f in fills] == [(101.0, 11 * MS)]
    assert sim.slippage_log[0].decision_mid == 99.5 and sim.slippage_log[0].arrival_mid == 100.0


def test_post_only_reject_and_resting_queue_fills():
    sim = hsim("B")
    sched = [(0, PlaceHedge("x", V, SYM, "buy", 1.0, "limit", 100.0, post_only=True)),
             (0, PlaceHedge("b", V, SYM, "buy", 1.0, "limit", 99.0, post_only=True))]
    evs = [book(), tr(1 * MS, 99.0, 1.5, "sell"), ExtBookDelta(1 * MS, 0, V, SYM, (("b", 99.0, 0.5),)),
           tr(2 * MS, 99.0, 1.0, "sell"), tr(3 * MS, 98.0, 0.2, "sell")]
    strat, out = run(sim, evs, sched)
    ups = [e for e in out if isinstance(e, HedgeOrderUpdate)]
    assert ("x", "rejected", "post_only_cross") in {(u.client_order_id, u.status, u.reason) for u in ups}
    fills = [e for e in out if isinstance(e, HedgeFill)]
    # queue 2.0 ahead; 1.5 prints (not double counted by the -1.5 size change); 1.0 more -> 0.5 fill;
    # then a print through our price at 98 fills 0.2 by price priority
    assert [(f.qty_btc, f.price, f.is_maker) for f in fills] == [(0.5, 99.0, True), (pytest.approx(0.2), 99.0, True)]
    assert fills[0].fee_usd == pytest.approx(99.0 * 0.5 * 1.0 / 1e4)


def test_conservative_through_fill_needs_empty_queue():
    sim = hsim("C")
    evs = [book(), tr(1 * MS, 98.0, 0.5, "sell")]
    strat, out = run(sim, evs, [(0, PlaceHedge("b", V, SYM, "buy", 1.0, "limit", 99.0, post_only=True))])
    assert [e for e in out if isinstance(e, HedgeFill)] == []


def test_cancel_reduce_only_and_foreign_actions():
    sim = hsim()
    sched = [(0, PlaceHedge("r", V, SYM, "sell", 1.0, "market", reduce_only=True)),
             (0, PlaceHedge("b", V, SYM, "buy", 1.0, "market")),
             (1, PlaceHedge("r2", V, SYM, "sell", 5.0, "market", reduce_only=True)),
             (1, PlaceHedge("l", V, SYM, "buy", 1.0, "limit", 97.0, post_only=True)),
             (2, CancelHedge("l", V)), (2, CancelHedge("zz", V))]
    evs = [book(), ExtBookDelta(1, 0, V, SYM, ()), ExtBookDelta(2, 0, V, SYM, ())]
    strat, out = run(sim, evs, sched)
    st_ = {(u.client_order_id, u.status, u.reason) for u in out if isinstance(u, HedgeOrderUpdate)}
    assert ("r", "rejected", "reduce_only") in st_ and ("l", "canceled", "") in st_
    assert ("zz", "rejected", "not_found") in st_
    assert sim.orders["r2"].qty == 1.0 and sim.position_btc == pytest.approx(0.0)
    assert sim.submit(PlaceOrder("k", "T", "bid", 5000, 100), 0) is False
    assert sim.submit(PlaceHedge("o", "other_venue", SYM, "buy", 1.0), 0) is False


def test_funding_accrual_hook():
    got = []
    sim = hsim(on_funding=lambda ts, usd: got.append((ts, usd)))
    evs = [book(), PerpState(1, 0, V, SYM, mark=100.0, funding_rate=0.0001, next_funding_ts=2 * S),
           PerpState(1 * S, 0, V, SYM, mark=100.0, funding_rate=0.0002, next_funding_ts=2 * S),
           PerpState(3 * S, 0, V, SYM, mark=110.0, funding_rate=0.0001, next_funding_ts=10 * S)]
    strat, out = run(sim, evs, [(0, PlaceHedge("b", V, SYM, "buy", 3.0, "market"))])
    assert sim.position_btc == 3.0
    # at 2 s: long 3 BTC pays 3 * 100 * 0.0002 (latest announced rate for that interval)
    assert got[0] == (2 * S, pytest.approx(-0.06))
    assert len(got) == 2 and got[1][0] == 10 * S  # next interval also accrues when the stream is drained
    assert sim.funding_usd == pytest.approx(-0.06 - 3 * 110.0 * 0.0001)


def test_hedge_determinism():
    def once():
        sim = hsim(lat=LatencyModel(9))
        evs = [book()] + [tr(i * 10 * MS, 99.0, 0.3, "sell") for i in range(1, 30)]
        sched = [(0, PlaceHedge("b", V, SYM, "buy", 2.0, "limit", 99.0, post_only=True)),
                 (0, PlaceHedge("m", V, SYM, "sell", 0.5, "market"))]
        return run(sim, evs, sched)[1]

    assert once() == once()


def test_bbo_only_venue():
    from dh.core.events import ExtBBO

    sim = hsim()
    evs = [ExtBBO(0, 0, V, SYM, 99.0, 1.0, 100.0, 0.5), ExtBBO(5, 0, V, SYM, 99.0, 1.0, 100.5, 2.0)]
    strat, out = run(sim, evs, [(0, PlaceHedge("m", V, SYM, "buy", 1.0, "market")),
                                (5, PlaceHedge("m2", V, SYM, "buy", 1.0, "market"))])
    fills = [(f.client_order_id, f.price, f.qty_btc) for f in out if isinstance(f, HedgeFill)]
    assert fills == [("m", 100.0, 0.5), ("m2", 100.5, 1.0)]
