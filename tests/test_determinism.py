"""Determinism and causality of the replay runner (cited by dh/core/strategy.py)."""
from __future__ import annotations

from dh.backtest.runner import run, with_timers
from dh.core.actions import CancelOrder, PlaceOrder
from dh.core.book import KalshiBook
from dh.core.events import KalshiBookDelta, KalshiBookSnapshot, Timer
from dh.core.strategy import IdGen
from dh.core.units import NS_PER_S
from dh.execution.exchange_sim import KalshiExchangeSim
from dh.execution.latency import LatencyModel
from dh.execution.order_manager import OrderManager
from dh.sim.synthetic import SynthConfig, SyntheticMarket


class ToyStrategy:
    """Joins the best bid on one market every 2 s and cancels orders older than 3 s."""

    def __init__(self, ticker: str) -> None:
        self.ticker = ticker
        self.book = KalshiBook(ticker)
        self.om = OrderManager()
        self.ids = IdGen("toy")
        self.last_place = -10**18

    def on_event(self, ev):
        out = []
        if isinstance(ev, KalshiBookSnapshot) and ev.ticker == self.ticker:
            self.book.apply_snapshot(ev)
        elif isinstance(ev, KalshiBookDelta) and ev.ticker == self.ticker:
            self.book.apply_delta(ev)
        self.om.on_event(ev)
        if isinstance(ev, Timer) and self.book.valid:
            for w in self.om.working(self.ticker):
                if ev.ts - w.created_ns > 3 * NS_PER_S and not w.cancel_requested and w.order_id:
                    a = CancelOrder(client_order_id=w.client_order_id, ticker=self.ticker, order_id=w.order_id)
                    if self.om.request_cancel(a, ev.ts):
                        out.append(a)
            bb = self.book.best_bid()
            if bb is not None and ev.ts - self.last_place >= 2 * NS_PER_S:
                a = PlaceOrder(client_order_id=self.ids.next(), ticker=self.ticker, book_side="bid", px=bb, qty=200)
                self.om.request_place(a, ev.ts)
                self.last_place = ev.ts
                out.append(a)
        return out


def _setup(seed=5, duration_s=120):
    sm = SyntheticMarket(SynthConfig(duration_s=duration_s, seed=seed, noise_taker_rate_per_s=0.3))
    evs = sm.generate()
    ticker = sorted(s.ticker for s in sm.specs())[len(sm.specs()) // 2]
    return sm, evs, ticker


def _run(evs, sm, ticker, end_ns=None):
    sim = KalshiExchangeSim(LatencyModel.fixed(submit_ms=30, response_ms=30, ws_ms=20), "realistic",
                            lambda px, qty, taker: 0, seed=1)
    for s in sm.specs():
        sim.register_market(s)
    return run(evs, ToyStrategy(ticker), sim, end_ns=end_ns)


def _key(res):
    return [(t, type(a).__name__, getattr(a, "client_order_id", ""), getattr(a, "px", 0)) for t, a in res.actions]


def test_identical_inputs_identical_actions():
    sm, evs, tk = _setup()
    a, b = _run(evs, sm, tk), _run(evs, sm, tk)
    assert _key(a) == _key(b) and len(_key(a)) > 10 and a.sim_events == b.sim_events > 0


def test_truncation_invariance():
    sm, evs, tk = _setup()
    full = _run(evs, sm, tk)
    cut = evs[0].ts + 61 * NS_PER_S + 150_000_000  # not on the timer grid
    trunc = _run([e for e in evs if e.ts <= cut], sm, tk, end_ns=cut)
    prefix = [k for k in _key(full) if k[0] <= cut]
    assert _key(trunc) == prefix and len(prefix) > 5


def test_timer_after_market_event_at_same_ts():
    class E:
        def __init__(self, ts):
            self.ts = ts

    out = list(with_timers([E(0), E(200), E(200), E(450)], 200, end_ns=900))
    kinds = [(type(e).__name__, e.ts) for e in out]
    assert kinds == [("E", 0), ("Timer", 0), ("E", 200), ("E", 200), ("Timer", 200), ("Timer", 400), ("E", 450),
                     ("Timer", 600), ("Timer", 800)]
