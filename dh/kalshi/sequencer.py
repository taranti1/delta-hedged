"""Deterministic Kalshi WS frame -> event sequencer (shared by the live client and replay).

Kalshi stamps every message of a subscription with ``sid`` and a ``seq`` that must increase
by exactly 1 per sid (asyncapi ``sequenceNumber``). The counter is shared by every message
of that sid, including ``ok``/``error``/indexlist responses that carry sid+seq, and by
every market in an ``orderbook_delta`` subscription. This module owns that bookkeeping so
that replaying the recorded raw frames reproduces exactly the live event stream:

  * seq == last + 1       -> accepted
  * seq <= last           -> duplicate / out-of-order: suppressed (no events)
  * seq  > last + 1       -> FeedStatus(stream='kalshi.ws', status='gap'); for an
                             orderbook sid every market seen on it is invalidated
                             (FeedStatus('kalshi.book:<ticker>', 'gap')), its deltas are
                             suppressed, and a resync request (sid, tickers) is queued for
                             the live client to send ``update_subscription/get_snapshot``;
                             the next snapshot per market re-validates it
                             (FeedStatus('kalshi.book:<ticker>', 'resynced'), then
                             FeedStatus('kalshi.ws', 'resynced') once the sid is clean)
  * a delta for a market never snapshotted on its sid -> invalid + resync as above
  * fills are de-duplicated by trade_id (bounded memory)

Connection boundaries: sids are per connection. The live client injects synthetic records
``{"type": "dh.feed_status", "status": connected|disconnected|stale|error, "detail": ...}``
into the SAME raw stream (via ``synthetic_status_frame``) so replay sees reconnects too.
'connected'/'disconnected' reset all sid state; 'disconnected' first emits
FeedStatus('kalshi.book:<ticker>', 'disconnected') for every book it was maintaining.
A ``subscribed`` response also resets its sid (sid numbers can be reused).

Pure and deterministic: no clock, no I/O. ``recv_ns`` (int ns) comes from the caller.
"""

from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Any

import orjson

from dh.core.events import (
    Event,
    FeedStatus,
    KalshiBookDelta,
    KalshiBookSnapshot,
    KalshiFill,
)
from dh.kalshi.normalize import WS_STREAM, ws_message_to_events
from dh.kalshi.wire import as_dict

SYNTHETIC_TYPE = "dh.feed_status"
SYNTHETIC_STATUSES = ("connected", "disconnected", "stale", "error")
_CHANNEL_OF_TYPE = {
    "orderbook_snapshot": "orderbook_delta",
    "orderbook_delta": "orderbook_delta",
    "event_fee_update": "market_lifecycle_v2",
    "event_lifecycle": "market_lifecycle_v2",
    "cfbenchmarks_value_indexlist": "cfbenchmarks_value",
    "cfbenchmarks_value_5hz_indexlist": "cfbenchmarks_value_5hz",
    "user_order": "user_orders",
    "market_position": "market_positions",
}
# Responses that share a sid's sequence but do not identify its channel.
_CONTROL_TYPES = frozenset({"ok", "error", "unsubscribed", "subscribed", "list_subscriptions"})


def synthetic_status_frame(status: str, detail: str = "") -> bytes:
    """Raw record the live client writes for connection-state changes (replayable)."""
    if status not in SYNTHETIC_STATUSES:
        raise ValueError(f"bad synthetic status {status!r}")
    return orjson.dumps({"type": SYNTHETIC_TYPE, "status": status, "detail": detail})


def book_stream(ticker: str) -> str:
    """FeedStatus stream name for one Kalshi order book."""
    return f"kalshi.book:{ticker}"


@dataclass
class SidState:
    channel: str = ""
    last_seq: int = 0  # 0 = nothing seen yet
    tickers: set[str] = field(default_factory=set)  # books snapshotted on this sid
    invalid: set[str] = field(default_factory=set)  # books awaiting a fresh snapshot


class KalshiWsState:
    """Mutable sequencing state for one ``kalshi.ws`` stream (live or replay)."""

    def __init__(self, *, use_yes_price: bool = False, fill_dedupe_size: int = 20_000) -> None:
        self.use_yes_price = use_yes_price
        self.sids: dict[int, SidState] = {}
        self.counters: dict[str, int] = {
            "frames": 0,
            "gaps": 0,
            "missed_msgs": 0,
            "dups": 0,
            "suppressed_deltas": 0,
            "dup_fills": 0,
            "parse_errors": 0,
            "resync_requests": 0,
        }
        self._resync: list[tuple[int, tuple[str, ...]]] = []
        self._fill_ids: OrderedDict[str, None] = OrderedDict()
        self._fill_dedupe_size = fill_dedupe_size

    # ------------------------------------------------------------------ queries
    def take_resync_requests(self) -> list[tuple[int, tuple[str, ...]]]:
        """Pending (sid, market_tickers) snapshot requests; clears the queue."""
        out, self._resync = self._resync, []
        return out

    def invalid_books(self) -> dict[int, set[str]]:
        """sid -> tickers whose book is invalid until a snapshot arrives."""
        return {sid: set(st.invalid) for sid, st in self.sids.items() if st.invalid}

    def sid_for_ticker(self, ticker: str) -> int | None:
        """orderbook sid that carries `ticker` (None if unknown)."""
        for sid, st in self.sids.items():
            if ticker in st.tickers:
                return sid
        return None

    def book_tickers(self) -> set[str]:
        out: set[str] = set()
        for st in self.sids.values():
            out |= st.tickers
        return out

    # ------------------------------------------------------------------ mutation
    def reset_connection(self) -> None:
        """Forget all sid state (sids are per connection)."""
        self.sids.clear()
        self._resync.clear()

    def _sid(self, sid: int, typ: str) -> SidState:
        st = self.sids.get(sid)
        if st is None:
            st = self.sids[sid] = SidState()
        if not st.channel and typ not in _CONTROL_TYPES:  # only data messages name the channel
            st.channel = _CHANNEL_OF_TYPE.get(typ, typ)
        return st

    def _seen_fill(self, trade_id: str) -> bool:
        if trade_id in self._fill_ids:
            return True
        self._fill_ids[trade_id] = None
        if len(self._fill_ids) > self._fill_dedupe_size:
            self._fill_ids.popitem(last=False)
        return False


def normalize_ws_frame(raw: bytes | str, recv_ns: int, state: KalshiWsState) -> list[Event]:
    """One raw inbound frame (exactly as recorded) -> events, updating `state`.

    Replay entry point: ``normalize_ws_frame(record.raw, record.recv_ns, state)`` over the
    'kalshi.ws' stream reproduces the live client's events.
    """
    state.counters["frames"] += 1
    try:
        msg = orjson.loads(raw)
    except orjson.JSONDecodeError:
        state.counters["parse_errors"] += 1
        return [FeedStatus(ts=recv_ns, ts_exch=0, stream=WS_STREAM, status="error", detail="unparseable frame")]
    if not isinstance(msg, dict):
        state.counters["parse_errors"] += 1
        return [FeedStatus(ts=recv_ns, ts_exch=0, stream=WS_STREAM, status="error", detail="non-object frame")]
    return normalize_ws_message(msg, recv_ns, state)


def normalize_ws_message(msg: dict[str, Any], recv_ns: int, state: KalshiWsState) -> list[Event]:
    """Parsed message -> events with sequencing (see module docstring)."""
    typ = str(msg.get("type") or "")
    if typ == SYNTHETIC_TYPE:
        return _synthetic(msg, recv_ns, state)
    if typ == "subscribed":
        body = as_dict(msg.get("msg"))
        if body.get("sid") is not None:
            state.sids[int(body["sid"])] = SidState(channel=str(body.get("channel") or ""))
        return []
    if typ == "unsubscribed":
        if msg.get("sid") is not None:
            state.sids.pop(int(msg["sid"]), None)
        return []

    out: list[Event] = []
    sid_raw, seq_raw = msg.get("sid"), msg.get("seq")
    if sid_raw is not None and seq_raw is not None:
        sid, seq = int(sid_raw), int(seq_raw)
        st = state._sid(sid, typ)
        if st.last_seq:
            if seq <= st.last_seq:
                state.counters["dups"] += 1
                return []
            if seq > st.last_seq + 1:
                _gap(state, st, sid, seq, recv_ns, out)
        st.last_seq = seq

    try:
        events = ws_message_to_events(msg, recv_ns, use_yes_price=state.use_yes_price)
    except (ValueError, KeyError, TypeError, ArithmeticError) as exc:
        state.counters["parse_errors"] += 1
        detail = f"malformed {typ}: {type(exc).__name__}: {exc}"
        out.append(FeedStatus(ts=recv_ns, ts_exch=0, stream=WS_STREAM, status="error", detail=detail[:300]))
        # A malformed book message leaves that book unknown: resync it.
        _invalidate_from_msg(msg, typ, recv_ns, state, out)
        return out

    for ev in events:
        if isinstance(ev, KalshiBookSnapshot):
            st = state._sid(ev.sid, "orderbook_snapshot")
            st.tickers.add(ev.ticker)
            out.append(ev)
            if ev.ticker in st.invalid:
                st.invalid.discard(ev.ticker)
                out.append(
                    FeedStatus(ts=recv_ns, ts_exch=0, stream=book_stream(ev.ticker), status="resynced", detail=f"sid={ev.sid} seq={ev.seq}")
                )
                if not st.invalid:
                    out.append(FeedStatus(ts=recv_ns, ts_exch=0, stream=WS_STREAM, status="resynced", detail=f"sid={ev.sid}"))
        elif isinstance(ev, KalshiBookDelta):
            st = state._sid(ev.sid, "orderbook_delta")
            if ev.ticker in st.invalid:
                state.counters["suppressed_deltas"] += 1
                continue
            if ev.ticker not in st.tickers:
                st.tickers.add(ev.ticker)
                st.invalid.add(ev.ticker)
                state._resync.append((ev.sid, (ev.ticker,)))
                state.counters["resync_requests"] += 1
                state.counters["suppressed_deltas"] += 1
                out.append(FeedStatus(ts=recv_ns, ts_exch=0, stream=book_stream(ev.ticker), status="gap", detail=f"sid={ev.sid} delta before snapshot"))
                continue
            out.append(ev)
        elif isinstance(ev, KalshiFill):
            if ev.trade_id and state._seen_fill(ev.trade_id):
                state.counters["dup_fills"] += 1
                continue
            out.append(ev)
        else:
            out.append(ev)
    return out


def _gap(state: KalshiWsState, st: SidState, sid: int, seq: int, recv_ns: int, out: list[Event]) -> None:
    missed = seq - st.last_seq - 1
    state.counters["gaps"] += 1
    state.counters["missed_msgs"] += missed
    # Gaps are reported per CHANNEL ('kalshi.ws:<channel>'), never on the connection stream:
    # a lost trade / benchmark / lifecycle message must not mark the whole connection down
    # (nothing would ever clear it). Book gaps additionally invalidate the affected books
    # below and are resynced with snapshots (audit M3).
    out.append(
        FeedStatus(
            ts=recv_ns,
            ts_exch=0,
            stream=f"{WS_STREAM}:{st.channel or 'unknown'}",
            status="gap",
            detail=f"sid={sid} channel={st.channel} expected={st.last_seq + 1} got={seq} missed={missed}",
        )
    )
    if st.channel != "orderbook_delta" and not st.tickers:
        return
    newly = sorted(st.tickers - st.invalid)
    st.invalid |= st.tickers
    for t in newly:
        out.append(FeedStatus(ts=recv_ns, ts_exch=0, stream=book_stream(t), status="gap", detail=f"sid={sid} seq gap"))
    if newly:
        state._resync.append((sid, tuple(newly)))
        state.counters["resync_requests"] += 1


def _invalidate_from_msg(msg: dict, typ: str, recv_ns: int, state: KalshiWsState, out: list[Event]) -> None:
    if typ not in ("orderbook_snapshot", "orderbook_delta"):
        return
    body = as_dict(msg.get("msg"))
    ticker = body.get("market_ticker")
    if msg.get("sid") is None or not ticker:
        return
    sid = int(msg["sid"])
    st = state._sid(sid, typ)
    st.tickers.add(str(ticker))
    if str(ticker) not in st.invalid:
        st.invalid.add(str(ticker))
        state._resync.append((sid, (str(ticker),)))
        state.counters["resync_requests"] += 1
        out.append(FeedStatus(ts=recv_ns, ts_exch=0, stream=book_stream(str(ticker)), status="gap", detail=f"sid={sid} malformed book message"))


def _synthetic(msg: dict, recv_ns: int, state: KalshiWsState) -> list[Event]:
    status = str(msg.get("status") or "")
    if status not in SYNTHETIC_STATUSES:
        return []
    detail = str(msg.get("detail") or "")
    out: list[Event] = []
    if status == "disconnected":
        for t in sorted(state.book_tickers()):
            out.append(FeedStatus(ts=recv_ns, ts_exch=0, stream=book_stream(t), status="disconnected", detail="ws disconnected"))
    if status in ("connected", "disconnected"):
        state.reset_connection()
    out.append(FeedStatus(ts=recv_ns, ts_exch=0, stream=WS_STREAM, status=status, detail=detail))  # type: ignore[arg-type]
    return out
