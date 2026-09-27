"""Sign-off review regressions for the b53216a execution/risk changes."""
from __future__ import annotations

from dataclasses import replace

from dh.backtest.kat import default_kat_config
from dh.core.actions import CancelOrder, PlaceOrder
from dh.core.events import CancelAck, FeedStatus, KalshiBookSnapshot, KalshiFill, OrderAck
from dh.core.units import NS_PER_S
from dh.strategy.config import StrategyConfig
from dh.strategy.mm import MarketMaker
from tests.strategy.mm_driver import T0, TICK, Driver, spec
from tests.strategy.test_profit_review import candidate, fv


def _driver_with_stale_inflight_order() -> Driver:
    """TICK is live and over its event loss limit; settled market 'a' holds a canceled order
    whose 50 filled contracts' fill message has not arrived (in flight)."""
    cfg = default_kat_config()
    cfg = replace(cfg, risk=replace(cfg.risk, max_event_worst_loss=0.5))
    d = Driver([spec(), spec("a", K=85000)], cfg=cfg)
    mm = d.mm
    d.feed(FeedStatus(T0, 0, "kalshi.ws", "connected"))
    d.feed(KalshiBookSnapshot(T0, 0, TICK, 1, 1, ((4500, 10000),), ((4500, 10000),)))
    d.inputs(T0)
    fee = mm.fee_sched[TICK].trade_fee_micros(9900, 400, False)
    d.feed(KalshiFill(T0, 0, TICK, "loss", "oid-loss", "", "bid", 9900, 400, False, fee, 0, False))
    mm.om.request_place(PlaceOrder(client_order_id="x", ticker="a", book_side="bid", px=4000, qty=100), T0)
    mm.om.on_event(OrderAck(T0, 0, "x", "oid-x", "a", 0, 100))
    mm.om.request_cancel(CancelOrder(client_order_id="x", ticker="a", order_id="oid-x"), T0)
    mm.om.on_event(CancelAck(T0, 0, "x", "oid-x", "a", 50))
    assert mm.om.order("x").could_fill_qty == 50
    mm.settled["a"] = 0
    return d


def test_prune_settled_keeps_market_with_inflight_fills_and_cycle_survives():
    d = _driver_with_stale_inflight_order()
    d.mm.prune_settled(10**20)
    assert "a" in d.mm.specs
    d.advance(T0 + 6 * NS_PER_S)
    assert d.mm.stats.reasons.get("event_over_limit", 0) > 0


def test_event_over_limit_loop_skips_orders_of_unknown_markets():
    d = _driver_with_stale_inflight_order()
    d.mm.specs.pop("a")
    d.advance(T0 + 6 * NS_PER_S)  # previously KeyError: 'a'
    assert d.mm.stats.reasons.get("event_over_limit", 0) > 0


def test_only_one_opportunity_cost_eviction_until_cancel_ack():
    a, b, c = spec("a"), spec("b", K=85000), spec("c", K=86000)
    cfg = StrategyConfig()
    cfg = replace(cfg, quoting=replace(cfg.quoting, min_order_age_ms=0, kappa_replace_per_s=0),
                  risk=replace(cfg.risk, max_event_worst_loss=1, max_total_worst_loss=1))
    mm = MarketMaker(cfg, [a, b, c])
    olds = {"old1": "a", "old2": "c"}
    for coid, t in olds.items():
        mm.om.request_place(PlaceOrder(client_order_id=coid, ticker=t, book_side="bid", px=4500, qty=100), T0)
        mm.om.on_event(OrderAck(T0, 0, coid, "x" + coid, t, 0, 100))
        mm.fvc[t] = fv()

    def cycle(now, new):
        mm._retained_candidates = {k: candidate(t, px=4500, size=1, ev_rate=1e-6, score=1e-6)
                                   for k, t in olds.items() if not mm.om.order(k).cancel_requested}
        groups = mm._risk_groups(now, 84000, [w for w in mm.om.all_orders() if w.could_fill_qty])
        ewc = {k: mm._group_loss(g) for k, g in groups.items()}
        acts = mm._admit(now, [(new.score, b, "bid", new, fv())], groups, ewc, sum(ewc.values()), 0, 0)
        return ([x.client_order_id for x in acts if isinstance(x, CancelOrder)],
                [x for x in acts if isinstance(x, PlaceOrder)])

    small = candidate("b", px=4000, size=1, ev_rate=.01, score=.01)
    canceled, placed = cycle(T0 + NS_PER_S, small)
    assert canceled == ["old1"] and not placed
    canceled, placed = cycle(T0 + 2 * NS_PER_S, small)
    assert canceled == [] and not placed
    assert not mm.om.order("old2").cancel_requested
    mm.om.on_event(CancelAck(T0 + 3 * NS_PER_S, 0, "old1", "xold1", "a", 100))
    assert mm.om.order("old1").could_fill_qty == 0
    big = candidate("b", px=4000, size=2, ev_rate=.01, score=.01)
    canceled, placed = cycle(T0 + 4 * NS_PER_S, big)
    assert canceled == ["old2"] and not placed
