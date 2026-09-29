"""Queue diagnostics (logging only): the optimistic (A) and conservative (C) estimators shadow the
strategy's realistic (B) one; public prints at our levels and our fills carry each policy's
queue state, and the runner logs each policy's estimate against Kalshi's queue positions."""
from __future__ import annotations

from types import SimpleNamespace

from dh.backtest.kat import default_kat_config, warm_fv_model
from dh.core.actions import Log, PlaceOrder
from dh.core.events import KalshiBookDelta, KalshiBookSnapshot, KalshiFill, KalshiTrade, OrderAck
from dh.core.units import NS_PER_MS, NS_PER_S
from dh.execution.queue import CALIBRATION_MAX, QueueEstimator
from dh.kalshi.fees import FeeEngine
from dh.models.fvmodel import FairValueModel, load_recommended_config
from dh.strategy.mm import MarketMaker

from .mm_driver import T0, TICK, spec

OTHERS, OURS, BEHIND, CANCEL = 1000, 500, 300, 400


def _mm(diag: bool = True) -> MarketMaker:
    fv = FairValueModel.from_config(load_recommended_config())
    warm_fv_model(fv, T0, 84000.0, 0.35)
    mm = MarketMaker(default_kat_config(), [spec()], fv_model=fv, fee_engine=FeeEngine.from_config(),
                     book_includes_own=True, queue_diagnostics=diag)
    mm.on_event(KalshiBookSnapshot(T0, 0, TICK, 1, 1, ((4500, OTHERS),), ((5300, 700),)))
    mm.om.request_place(PlaceOrder("c1", TICK, "bid", 4500, OURS), T0)
    mm.on_event(OrderAck(T0 + 1, 0, "c1", "o1", TICK, 0, OURS, "create"))
    mm.on_event(KalshiBookDelta(T0 + 2, 0, TICK, 1, 2, "yes", 4500, OURS, "c1"))  # we join behind OTHERS
    mm.on_event(KalshiBookDelta(T0 + 3, 0, TICK, 1, 3, "yes", 4500, BEHIND))  # someone joins behind us
    mm.on_event(KalshiBookDelta(T0 + 4, 0, TICK, 1, 4, "yes", 4500, -CANCEL))  # unexplained: a cancel
    return mm


def _logs(actions, kind):
    return [a.payload for a in actions if isinstance(a, Log) and a.kind == kind]


def _trade(ts):
    return KalshiTrade(ts, ts - NS_PER_MS, TICK, "tr-1", 4500, 800, "no")  # taker sells YES into YES bids


def test_policies_diverge_on_a_cancel_and_the_print_logs_each_prediction():
    mm = _mm()
    out = mm.on_event(_trade(T0 + NS_PER_S))  # the cancel is classified first (match window passed)
    [rec] = _logs(out, "queue_trade")
    [row] = rec["orders"]
    # level before the cancel (others) = 1300: A all ahead, B pro rata 400*1000//1300, C behind if possible
    assert row["q"] == {"B": 693, "A": 600, "C": 900}
    assert row["joined"] == OTHERS and row["coid"] == "c1" and row["rem"] == OURS
    assert rec["trade_id"] == "tr-1" and rec["qty"] == 800 and rec["book"] == "yes" and rec["px"] == 4500
    assert rec["pred"] == {"B": [["c1", 107, "queue"]], "A": [["c1", 200, "queue"]], "C": []}
    # decisions still use the strategy's own (B) estimator
    assert mm.queue.queue_ahead("c1") == 0


def test_fill_log_carries_the_queue_state_before_the_fill():
    mm = _mm()
    mm.on_event(_trade(T0 + NS_PER_S))
    out = mm.on_event(KalshiFill(T0 + NS_PER_S + 5, T0 + NS_PER_S, TICK, "tr-1", "o1", "c1", "bid", 4500, 107,
                                 False, 0, 107))
    [f] = _logs(out, "fill")
    assert f["q_ahead"] == {"B": 0, "A": 0, "C": 100} and f["q_joined"] == OTHERS
    assert f["rem_before"] == OURS and f["age_ms"] == (NS_PER_S + 5 - 1) // NS_PER_MS
    assert f["level_qty"] == OTHERS + BEHIND - CANCEL  # others displayed (the print's own delta not yet seen)
    assert mm.queue_shadow.queue_ahead("c1") == {"optimistic": 0, "conservative": 100}
    assert mm.queue_shadow.estimators["conservative"].orders["c1"].remaining == OURS - 107


def test_off_by_default_changes_nothing():
    mm = _mm(diag=False)
    assert mm.queue_shadow is None
    out = mm.on_event(_trade(T0 + NS_PER_S))
    assert _logs(out, "queue_trade") == []
    out = mm.on_event(KalshiFill(T0 + NS_PER_S + 5, 0, TICK, "tr-1", "o1", "c1", "bid", 4500, 107, False, 0, 107))
    [f] = _logs(out, "fill")
    assert "q_ahead" not in f


def test_prints_away_from_our_orders_are_not_logged():
    mm = _mm()
    out = mm.on_event(KalshiTrade(T0 + NS_PER_S, 0, TICK, "tr-2", 4600, 50, "no"))  # above our bid: never reaches it
    assert _logs(out, "queue_trade") == []
    out = mm.on_event(KalshiTrade(T0 + NS_PER_S, 0, TICK, "tr-3", 5300, 50, "yes"))  # the other book
    assert _logs(out, "queue_trade") == []


def test_a_diagnostics_failure_disables_the_shadow_and_trading_continues():
    mm = _mm()

    def boom(ev):
        raise RuntimeError("bad")

    mm.queue_shadow.estimators["conservative"].on_trade = boom
    out = mm.on_event(_trade(T0 + NS_PER_S))
    assert mm.queue_shadow is None and mm.stats.reasons.get("queue_diag_error") == 1
    [d] = _logs(out, "queue_diag")
    assert d["event"] == "disabled" and "bad" in d["error"]
    assert mm.queue.queue_ahead("c1") == 0  # the strategy's estimator still processed the print
    out = mm.on_event(KalshiTrade(T0 + 2 * NS_PER_S, 0, TICK, "tr-4", 4500, 1, "no"))
    assert _logs(out, "queue_diag") == []  # logged once


def test_calibration_samples_are_bounded():
    q = QueueEstimator("B", lambda t, b, p: 0)
    q.add_order("k", TICK, "bid", 4500, 100, T0)
    for i in range(CALIBRATION_MAX + 10):
        q.ingest_exchange_queue_position("k", 0, T0 + i)
    assert len(q.calibration) == CALIBRATION_MAX
    assert q.calibration_stats()["n"] == CALIBRATION_MAX


def test_runner_logs_each_policy_against_exchange_queue_positions():
    from dh.live.monitor import Metrics
    from dh.live.runner import LiveRunner

    mm = _mm()
    logs = []
    fake = SimpleNamespace(strategy=mm, cfg=SimpleNamespace(venue=SimpleNamespace(queue_positions_resync=False)),
                           metrics=Metrics(), jlog=lambda kind, ts, **kw: logs.append((kind, kw)), meta=None)
    mm.on_event(KalshiBookDelta(T0 + NS_PER_S, 0, TICK, 1, 5, "yes", 4500, 1))  # expires the pending cancel
    LiveRunner._ingest_queue_positions(fake, T0 + NS_PER_S, [("o1", TICK, 650)])
    [(kind, kw)] = logs
    assert kind == "queue_positions" and kw["rows"] == [("c1", TICK, 650, 693)]
    assert kw["shadow"] == {"A": {"c1": 600}, "C": {"c1": 900}}
    assert fake.metrics.get("dh_queue_shadow_error_contracts_count", policy="conservative") == 1
