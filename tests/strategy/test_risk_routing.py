"""Regression tests for audit finding C1: kill-switch routing must use exact stream names."""
from __future__ import annotations

from dh.core.actions import CancelAll
from dh.core.events import FeedStatus
from dh.core.units import NS_PER_S
from dh.strategy.config import RiskCfg
from dh.strategy.risk import RiskEngine

S = NS_PER_S


def fs(t, stream, status, detail=""):
    return FeedStatus(ts=t, ts_exch=0, stream=stream, status=status, detail=detail)


def ready_engine(t0=100 * S):
    r = RiskEngine(RiskCfg())
    r.on_feed_status(fs(t0, "kalshi.ws", "connected"))
    for dt in range(0, 11):
        r.note_brti(t0 + dt * S)
    return r, t0 + 10 * S


def test_unrelated_kalshi_prefixed_streams_cannot_reenable_quoting():
    r, t = ready_engine()
    assert r.health(t).quoting_allowed
    acts = r.on_feed_status(fs(t, "kalshi.ws", "disconnected"))
    assert any(isinstance(a, CancelAll) for a in acts)
    r.on_feed_status(fs(t + S, "kalshi_perp.ws", "connected"))
    r.on_feed_status(fs(t + S, "kalshi.order_group:g1", "resynced"))
    r.on_feed_status(fs(t + S, "kalshi.book:X", "resynced"))
    r.note_brti(t + 2 * S)
    assert not r.health(t + 2 * S).quoting_allowed
    r.on_feed_status(fs(t + 3 * S, "kalshi.ws", "connected"))
    r.note_brti(t + 8 * S)
    assert not r.health(t + 7 * S).quoting_allowed  # 5 s settle after reconnect
    assert r.health(t + 8 * S).quoting_allowed


def test_hedge_venue_health_tracks_hedge_stream():
    r, t = ready_engine()
    r.on_feed_status(fs(t, "kalshi_perp.ws", "disconnected"))
    assert not r.health(t).hedging_allowed and r.health(t).quoting_allowed
    r.on_feed_status(fs(t + S, "kalshi_perp.ws", "connected"))
    assert r.health(t + S).hedging_allowed


def test_book_gap_is_per_ticker():
    r, t = ready_engine()
    acts = r.on_feed_status(fs(t, "kalshi.book:A", "gap"))
    assert any(isinstance(a, CancelAll) and a.tickers == ("A",) for a in acts)
    assert not r.book_ok("A", t) and r.book_ok("B", t) and r.health(t).quoting_allowed
    r.on_feed_status(fs(t + S, "kalshi.book:A", "resynced"))
    assert not r.book_ok("A", t + 2 * S) and r.book_ok("A", t + 6 * S)


def test_order_group_trigger_pauses_until_reset_and_cooldown():
    r, t = ready_engine()
    r.on_feed_status(fs(t, "kalshi.order_group:g1", "error"))
    for k in range(1, 70):
        r.note_brti(t + k * S)
    assert not r.health(t + 30 * S).quoting_allowed
    assert not r.health(t + 65 * S).quoting_allowed  # cooldown over but group not reset
    r.on_feed_status(fs(t + 66 * S, "kalshi.order_group:g1", "resynced"))
    assert r.health(t + 67 * S).quoting_allowed


def test_brti_outage_requires_30s_of_fresh_ticks():
    r, t = ready_engine()
    r.note_brti(t + 25 * S)  # 15 s gap > 10 s cancel-all threshold -> outage
    assert not r.health(t + 26 * S).quoting_allowed
    for k in range(26, 56):
        r.note_brti(t + k * S)
    assert not r.health(t + 54 * S).quoting_allowed
    assert r.health(t + 56 * S).quoting_allowed


def test_kalshi_error_status_is_informational():
    r, t = ready_engine()
    r.on_feed_status(fs(t, "kalshi.ws", "error", "unparseable frame"))
    assert r.health(t).quoting_allowed


def test_external_venue_disconnect_counts_as_stale():
    r, t = ready_engine()
    r.note_ext("coinbase", t)
    r.note_ext("kraken", t)
    assert r.health(t).quoting_allowed
    r.on_feed_status(fs(t, "kraken.ws", "disconnected"))
    assert not r.health(t).quoting_allowed


def test_non_book_channel_gap_does_not_stop_quoting_forever():
    """Audit M3: a lost trade/benchmark message must not disable quoting permanently."""
    r, t = ready_engine()
    r.on_feed_status(fs(t, "kalshi.ws:cfbenchmarks_value_5hz", "gap", "missed=1"))
    r.on_feed_status(fs(t, "kalshi.ws:trade", "gap", "missed=3"))
    for k in range(1, 5):
        r.note_brti(t + k * S)
    assert r.health(t + 4 * S).quoting_allowed


def test_own_fill_channel_gap_pauses_and_requests_reconcile():
    r, t = ready_engine()
    acts = r.on_feed_status(fs(t, "kalshi.ws:fill", "gap", "missed=1"))
    assert any(isinstance(a, CancelAll) for a in acts)
    for k in range(1, 40):
        r.note_brti(t + k * S)
    assert not r.health(t + 10 * S).quoting_allowed and r.health(t + 31 * S).quoting_allowed
