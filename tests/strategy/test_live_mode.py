"""MarketMaker behaviour specific to live mode (book_includes_own=True): our own order must not
be counted in the queue ahead of us, whichever of the ack and our own book delta comes first;
and rejected hedges must not stay pending forever."""
from __future__ import annotations

from dh.backtest.kat import default_kat_config, warm_fv_model
from dh.core.actions import PlaceOrder
from dh.core.events import HedgeOrderUpdate, KalshiBookDelta, KalshiBookSnapshot, OrderAck, Timer
from dh.core.units import NS_PER_S
from dh.kalshi.fees import FeeEngine
from dh.models.fvmodel import FairValueModel, load_recommended_config
from dh.strategy.mm import MarketMaker

from .mm_driver import T0, TICK, spec

OTHERS = 1000  # 10 contracts resting at our price before we join
OURS = 500


def _mm() -> MarketMaker:
    fv = FairValueModel.from_config(load_recommended_config())
    warm_fv_model(fv, T0, 84000.0, 0.35)
    mm = MarketMaker(default_kat_config(), [spec()], fv_model=fv, fee_engine=FeeEngine.from_config(),
                     book_includes_own=True)
    mm.on_event(KalshiBookSnapshot(T0, 0, TICK, 1, 1, ((4500, OTHERS),), ((5300, 700),)))
    mm.om.request_place(PlaceOrder("c1", TICK, "bid", 4500, OURS), T0)
    return mm


def _ack(mm: MarketMaker, ts: int) -> None:
    mm.on_event(OrderAck(ts, 0, "c1", "o1", TICK, 0, OURS, "create"))


def _own_delta(mm: MarketMaker, ts: int, seq: int = 2) -> None:
    mm.on_event(KalshiBookDelta(ts, 0, TICK, 1, seq, "yes", 4500, OURS, "c1"))


def test_ack_before_own_delta_waits_for_the_delta():
    mm = _mm()
    _ack(mm, T0 + 1)
    assert mm.queue.queue_ahead("c1") is None  # pending: the book does not show our order yet
    _own_delta(mm, T0 + 2)
    assert mm.queue.queue_ahead("c1") == OTHERS


def test_own_delta_before_ack_joins_immediately():
    mm = _mm()
    _own_delta(mm, T0 + 1)
    _ack(mm, T0 + 2)
    assert mm.queue.queue_ahead("c1") == OTHERS


def test_missing_own_delta_falls_back_to_the_whole_displayed_level():
    mm = _mm()
    _ack(mm, T0 + 1)
    mm.on_event(Timer(T0 + 1 + 2 * NS_PER_S))
    assert mm.queue.queue_ahead("c1") == OTHERS
    assert mm.stats.reasons.get("queue_own_delta_timeout") == 1


def test_rejected_hedge_is_no_longer_pending():
    mm = _mm()
    mm.hedge_pending["h1"] = 0.01
    mm.hedge_pending["h2"] = -0.02
    mm.on_event(HedgeOrderUpdate(T0, 0, "kalshi_perp", "h1", "rejected", 0.0, 0.01, "hedging disabled"))
    mm.on_event(HedgeOrderUpdate(T0, 0, "kalshi_perp", "h2", "accepted", 0.0, 0.02))
    assert "h1" not in mm.hedge_pending and mm.hedge_pending["h2"] == -0.02
