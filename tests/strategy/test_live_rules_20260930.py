"""Live-experiment rules of 2026-09-30 (config/m1_live.yaml): the market-disagreement cap, the
abnormal-move kill switch turned off (abnormal_pause_s 0) and digest stability of older configs."""
from __future__ import annotations

from dataclasses import replace

from dh.backtest.kat import default_kat_config
from dh.core.actions import CancelAll, Log, PlaceOrder
from dh.core.events import FeedStatus, KalshiBookDelta, KalshiBookSnapshot, OrderAck
from dh.core.units import NS_PER_S
from dh.strategy.config import RiskCfg, load_config
from dh.strategy.risk import RiskEngine

from .mm_driver import T0, TICK, Driver, spec


def _cfg(cap_c: float):
    base = default_kat_config()
    return replace(base, quoting=replace(base.quoting, max_market_disagreement_c=cap_c))


def _ready(d: Driver, bid: int, no_bid: int) -> None:
    d.feed(FeedStatus(T0, 0, "kalshi.ws", "connected"))
    d.feed(KalshiBookSnapshot(T0, 0, TICK, 1, 1, ((bid, 10000),), ((no_bid, 10000),)))
    d.inputs(T0)


def _places(acts):
    return [a for a in acts if isinstance(a, PlaceOrder)]


def test_market_close_to_fair_value_is_quoted_with_the_cap_on():
    d = Driver([spec()], cfg=_cfg(10.0))  # F ~ 0.5 (spot at the strike); mid 0.50
    _ready(d, bid=4500, no_bid=4500)
    assert _places(d.advance(T0 + 6 * NS_PER_S))
    assert d.mm.stats.reasons.get("market_disagreement", 0) == 0


def test_market_far_from_fair_value_gets_no_new_orders():
    d = Driver([spec()], cfg=_cfg(10.0))  # mid (0.10 + 0.20) / 2 = 0.15 vs F ~ 0.5
    _ready(d, bid=1000, no_bid=8000)
    feed, acts = d.feed, []
    d.feed = lambda ev: acts.extend(d.mm.on_event(ev)) or []  # keep the Log records too
    d.advance(T0 + 6 * NS_PER_S)
    d.feed = feed
    assert not _places(acts)
    assert d.mm.stats.reasons.get("market_disagreement", 0) > 0
    gates = [a.payload for a in acts if isinstance(a, Log) and a.kind == "quote_gate"]
    assert len(gates) == 1 and gates[0]["on"] is True and gates[0]["mid"] == 0.15  # logged once
    assert abs(gates[0]["disagreement_c"] - 100 * abs(d.mm.fvc[TICK].F - 0.15)) < 1e-2


def test_cap_off_quotes_the_same_disagreeing_market():
    d = Driver([spec()], cfg=_cfg(0.0))
    _ready(d, bid=1000, no_bid=8000)
    assert _places(d.advance(T0 + 6 * NS_PER_S))


def test_one_sided_book_is_not_capped():
    d = Driver([spec()], cfg=_cfg(10.0))
    _ready(d, bid=1000, no_bid=8000)
    d.feed(KalshiBookDelta(T0 + 1, 0, TICK, 1, 2, "no", 8000, -10000))  # no asks left
    assert d.mm._market_disagreement(TICK, 0.5) is None


def test_own_resting_orders_are_excluded_from_the_mid_in_live_mode():
    d = Driver([spec()], cfg=_cfg(10.0), book_includes_own=True)
    _ready(d, bid=4500, no_bid=4500)
    # our 5-contract YES bid at 49c joins the live book above everyone else's 45c bid
    d.mm.om.request_place(PlaceOrder("c1", TICK, "bid", 4900, 500), T0)
    d.feed(OrderAck(T0 + 1, 0, "c1", "o1", TICK, 0, 500, "create"))
    d.feed(KalshiBookDelta(T0 + 2, 0, TICK, 1, 2, "yes", 4900, 500, "c1"))
    dis, mid = d.mm._market_disagreement(TICK, 0.5)
    assert mid == 0.5 and dis == 0.0  # others: 45c bid / 55c ask, not our 49c


def test_kill_switch_off_logs_the_move_and_keeps_quoting():
    on = RiskEngine(RiskCfg(abnormal_move_sigma=6.0, abnormal_pause_s=120.0))
    off = RiskEngine(RiskCfg(abnormal_move_sigma=6.0, abnormal_pause_s=0.0))
    a_on = on.on_abnormal_move(T0, 7.0)
    a_off = off.on_abnormal_move(T0, 7.0)
    assert any(isinstance(a, CancelAll) for a in a_on) and on.pause_until_ns > T0
    assert not any(isinstance(a, CancelAll) for a in a_off) and off.pause_until_ns <= T0
    assert [a.payload for a in a_off if isinstance(a, Log)] == [{"event": "abnormal_move", "sigma": 7.0,
                                                                 "action": "none"}]
    assert off.on_abnormal_move(T0, 5.9) == []


def test_new_field_leaves_older_config_digests_unchanged():
    # digests recorded by earlier paper sessions (their replays check them)
    assert load_config("config/m1.yaml").digest() == "ece7ceef2c224a55"
    assert load_config("config/m1_small.yaml").digest() == "860cfc8faa25933c"
    live = load_config("config/m1_live.yaml")
    assert live.quoting.max_market_disagreement_c == 10.0 and live.quoting.min_tau_s == 150.0
    assert live.risk.max_event_worst_loss == 10.0 and live.risk.tail_budget == 10.0
    assert live.risk.abnormal_pause_s == 0.0 and live.digest() != load_config("config/m1_small.yaml").digest()
