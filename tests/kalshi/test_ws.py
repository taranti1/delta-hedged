from __future__ import annotations

import asyncio
import base64
import random
from typing import Any

import orjson
import pytest
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding
from websockets.exceptions import ConnectionClosedError

from dh.core.book import KalshiBook
from dh.core.events import (
    FeedStatus,
    IndexTick,
    KalshiBookDelta,
    KalshiBookSnapshot,
    KalshiTrade,
)
from dh.execution.queue import book_px, resting_book
from dh.kalshi.auth import KalshiSigner
from dh.kalshi.sequencer import KalshiWsState, normalize_ws_frame
from dh.kalshi.ws import KalshiWS, Subscription

from .fake_ws import RELEASE, STALL, FakeKalshi, Pause

URL = "wss://example.test/trade-api/ws/v2"


class Recorder:
    def __init__(self) -> None:
        self.log: list[tuple[str, Any]] = []
        self.raw: list[tuple[str, int, bytes]] = []
        self.events: list[Any] = []

    def on_raw(self, stream: str, recv_ns: int, raw: bytes) -> None:
        self.raw.append((stream, recv_ns, raw))
        self.log.append(("raw", orjson.loads(raw).get("type")))

    def on_event(self, ev: Any) -> None:
        self.events.append(ev)
        self.log.append(("ev", type(ev).__name__))


class Sleeps:
    def __init__(self) -> None:
        self.calls: list[float] = []

    async def __call__(self, dt: float) -> None:
        self.calls.append(dt)
        await asyncio.sleep(0)


def subs() -> list[Subscription]:
    return [Subscription(["orderbook_delta", "trade"], market_tickers=["A", "B"]),
            Subscription(["cfbenchmarks_value_5hz"], index_ids=["BRTI"])]


def make(fake: FakeKalshi, rec: Recorder, **kw: Any) -> tuple[KalshiWS, Sleeps]:
    sleeps = Sleeps()
    ns = iter(range(1_000, 10**9, 7))
    kw.setdefault("stale_after_s", 5.0)
    ws = KalshiWS(URL, kw.pop("signer", None), kw.pop("subscriptions", None) or subs(), on_raw=rec.on_raw,
                  on_event=rec.on_event, connect=fake.connect, clock_ns=lambda: next(ns), sleep=sleeps,
                  rng=random.Random(3), **kw)
    return ws, sleeps


async def run_until(ws: KalshiWS, cond, timeout: float = 3.0) -> None:
    task = asyncio.create_task(ws.run())
    try:
        async with asyncio.timeout(timeout):
            while not cond():
                await asyncio.sleep(0)
    finally:
        await ws.stop()
        await asyncio.wait_for(task, 3.0)


def ws_statuses(events: list[Any]) -> list[str]:
    return [e.status for e in events if isinstance(e, FeedStatus) and e.stream == "kalshi.ws"]


async def test_handshake_subscribe_and_raw_before_events(rsa_key, rsa_pem):
    fake = FakeKalshi([[lambda c: c.delta("A"), lambda c: c.trade("t1"), lambda c: c.index()]])
    rec = Recorder()
    ws, _ = make(fake, rec, signer=KalshiSigner("kid", rsa_pem))
    await run_until(ws, lambda: any(isinstance(e, IndexTick) for e in rec.events))
    h = fake.headers[0]
    msg = f"{h['KALSHI-ACCESS-TIMESTAMP']}GET/trade-api/ws/v2".encode()
    rsa_key.public_key().verify(base64.b64decode(h["KALSHI-ACCESS-SIGNATURE"]), msg,
                                padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=32), hashes.SHA256())
    sent = fake.conns[0].sent
    assert sent[0] == {"id": 1, "cmd": "subscribe", "params": {"channels": ["orderbook_delta", "trade"],
                                                               "market_tickers": ["A", "B"], "use_yes_price": True}}
    # the NO-side price convention in force is declared in-band for replay
    assert orjson.loads(rec.raw[0][2]) == {"type": "dh.feed_status", "status": "connected", "detail": URL, "use_yes_price": True}
    assert sent[1] == {"id": 2, "cmd": "subscribe", "params": {"channels": ["cfbenchmarks_value_5hz"], "index_ids": ["BRTI"]}}
    # every event is preceded by the raw record it came from
    assert rec.log[:2] == [("raw", "dh.feed_status"), ("ev", "FeedStatus")]
    n_raw = 0
    for kind, _what in rec.log:
        if kind == "raw":
            n_raw += 1
        else:
            assert n_raw > 0
    snap_i = rec.log.index(("ev", "KalshiBookSnapshot"))
    assert rec.log[snap_i - 1] == ("raw", "orderbook_snapshot")
    assert all(s == "kalshi.ws" for s, _, _ in rec.raw)
    kinds = [type(e).__name__ for e in rec.events if not isinstance(e, FeedStatus)]
    assert kinds == ["KalshiBookSnapshot", "KalshiBookSnapshot", "KalshiBookDelta", "KalshiTrade", "IndexTick"]


async def test_gap_requests_snapshot_suppresses_deltas_and_resyncs():
    script = [lambda c: c.delta("A"),
              lambda c: c.delta("A", skip=2),   # gap -> get_snapshot for A and B (replies held)
              lambda c: c.delta("B"),           # suppressed: B invalid
              RELEASE,                          # snapshots arrive
              lambda c: c.delta("B", d="2.00")]
    # hold_replies holds ONLY get_snapshot replies (subscribe replies always flow), so the resync
    # snapshots arrive exactly at RELEASE whatever the task scheduling. (This test used to switch
    # holding on from a helper task and silently relied on that task running before the gap.)
    fake = FakeKalshi([script], hold_replies=True)
    rec = Recorder()
    ws, _ = make(fake, rec)
    await run_until(ws, lambda: sum(isinstance(e, KalshiBookDelta) for e in rec.events) >= 2)
    conn = fake.conns[0]
    ob_sid = conn.sid_of("orderbook_delta")
    snaps = [c for c in conn.sent if c["cmd"] == "update_subscription"]
    assert snaps == [{"id": 3, "cmd": "update_subscription",
                      "params": {"sid": ob_sid, "market_tickers": ["A", "B"], "action": "get_snapshot"}}]
    st = [(e.stream, e.status) for e in rec.events if isinstance(e, FeedStatus)]
    assert ("kalshi.ws:orderbook_delta", "gap") in st and ("kalshi.book:A", "gap") in st and ("kalshi.book:B", "gap") in st
    assert st.index(("kalshi.book:B", "resynced")) > st.index(("kalshi.book:B", "gap"))
    assert ("kalshi.ws", "resynced") in st
    deltas = [e for e in rec.events if isinstance(e, KalshiBookDelta)]
    assert [(d.ticker, d.delta) for d in deltas] == [("A", 100), ("B", 200)]
    assert ws.state.counters["suppressed_deltas"] == 2  # the gap-carrying delta A and the stale delta B


async def test_reconnect_backoff_full_resubscribe_and_sid_reuse():
    pause = Pause()
    first = [lambda c: c.delta("A"), pause, ConnectionClosedError(None, None)]
    second = [lambda c: c.delta("C"), lambda c: c.trade("t2")]
    fake = FakeKalshi([first, second])
    rec = Recorder()
    ws, sleeps = make(fake, rec)

    async def add_c_mid_connection():
        # Deterministic: act while connection 1 is up and idle (the test used to poll for
        # ``ws.connected`` and hung forever when the client never yielded during connection 1).
        await pause.reached.wait()
        assert ws.connected and fake.conns[0].sent[-1]["cmd"] == "subscribe"
        await ws.add_markets(["C"])  # reaches the server now: add_markets + C's snapshot
        pause.resume()               # ... then the connection drops

    adder = asyncio.create_task(add_c_mid_connection())
    await run_until(ws, lambda: adder.done() and any(isinstance(e, KalshiTrade) and e.trade_id == "t2" for e in rec.events))
    await adder  # re-raises a failed assertion inside the helper
    assert [c["params"]["action"] for c in fake.conns[0].sent if c["cmd"] == "update_subscription"] == ["add_markets"] * 2
    assert ws_statuses(rec.events)[:3] == ["connected", "disconnected", "connected"]
    assert len(sleeps.calls) == 1 and 0.25 <= sleeps.calls[0] <= 0.5  # jittered 0.5 s * [0.5, 1]
    resub = fake.conns[1].sent[0]
    assert resub["params"]["market_tickers"] == ["A", "B", "C"]  # runtime additions survive reconnect
    assert fake.conns[1].sent[0]["id"] > fake.conns[0].sent[-1]["id"]  # ids keep increasing
    assert ws.state.counters["dups"] == 0  # sid numbers were reused by the new connection
    new_snaps = [e.ticker for e in rec.events if isinstance(e, KalshiBookSnapshot)]
    assert new_snaps.count("C") == 2  # snapshot after add_markets and after resubscribe
    assert len(fake.headers) == 2 and fake.headers[1] == {}  # unsigned client sends no auth headers


async def test_pump_yields_to_other_tasks_between_buffered_frames():
    """Regression: websockets (and this fake) return QUEUED frames without suspending, and since
    Python 3.12 asyncio.wait_for awaits recv() inline rather than in its own Task, so a pump that
    never yields drains a whole backlog while every other task on the loop starves (other
    feeds' receive timestamps, the strategy, add_markets/stop callers, keepalive pings)."""
    n = 40
    fake = FakeKalshi([[lambda c, i=i: c.trade(f"t{i}") for i in range(n)]])
    rec = Recorder()
    ws, _ = make(fake, rec)
    seen: list[int] = []

    async def observer():
        while True:
            seen.append(sum(isinstance(e, KalshiTrade) for e in rec.events))
            await asyncio.sleep(0)

    obs = asyncio.create_task(observer())
    try:
        await run_until(ws, lambda: sum(isinstance(e, KalshiTrade) for e in rec.events) == n)
    finally:
        obs.cancel()
    assert set(range(n)) <= set(seen)  # another task ran after every single frame


async def test_on_message_hook_sees_parsed_exchange_messages():
    fake = FakeKalshi([[lambda c: c.trade("t1")]])
    rec = Recorder()
    seen: list[str] = []
    ws, _ = make(fake, rec, on_message=lambda m: seen.append(m["type"]))
    await run_until(ws, lambda: "trade" in seen)
    assert seen[:1] == ["subscribed"] and "orderbook_snapshot" in seen
    assert "dh.feed_status" not in seen  # synthetic records are not exchange messages


async def test_staleness_watchdog_reconnects():
    fake = FakeKalshi([[STALL], [lambda c: c.trade("t9")]])
    rec = Recorder()
    ws, _ = make(fake, rec, stale_after_s=0.05)
    await run_until(ws, lambda: any(isinstance(e, KalshiTrade) for e in rec.events))
    assert ws_statuses(rec.events)[:4] == ["connected", "stale", "disconnected", "connected"]
    stale = next(e for e in rec.events if isinstance(e, FeedStatus) and e.status == "stale")
    assert "0.05" in stale.detail


async def test_terminal_channel_error_recycles_connection():
    fake = FakeKalshi([[lambda c: c.error(25, sid=1)], [lambda c: c.trade("t1")]])
    rec = Recorder()
    ws, _ = make(fake, rec)
    await run_until(ws, lambda: any(isinstance(e, KalshiTrade) for e in rec.events))
    assert ws_statuses(rec.events)[:4] == ["connected", "error", "disconnected", "connected"]
    disc = [e for e in rec.events if isinstance(e, FeedStatus) and e.status == "disconnected" and e.stream == "kalshi.ws"][0]
    assert "code=25" in disc.detail


async def test_resync_timeout_recycles_connection():
    clock = iter(float(i) for i in range(10**6))
    first = [lambda c: c.delta("A", skip=3)] + [lambda c, i=i: c.trade(f"x{i}") for i in range(6)]
    fake = FakeKalshi([first, [lambda c: c.trade("done")]], drop_snapshots=True)
    rec = Recorder()
    ws, _ = make(fake, rec, stale_after_s=None, resync_timeout_s=3.0, monotonic=lambda: next(clock))
    await run_until(ws, lambda: any(isinstance(e, KalshiTrade) and e.trade_id == "done" for e in rec.events))
    disc = [e for e in rec.events if isinstance(e, FeedStatus) and e.status == "disconnected" and e.stream == "kalshi.ws"][0]
    assert "resync timeout" in disc.detail


async def test_resync_timeout_fires_without_traffic_or_watchdog():
    fake = FakeKalshi([[lambda c: c.delta("A", skip=3), STALL], [lambda c: c.trade("done")]], drop_snapshots=True)
    rec = Recorder()
    ws, _ = make(fake, rec, stale_after_s=None, resync_timeout_s=0.05)
    await run_until(ws, lambda: any(isinstance(e, KalshiTrade) and e.trade_id == "done" for e in rec.events))
    disc = [e for e in rec.events if isinstance(e, FeedStatus) and e.status == "disconnected" and e.stream == "kalshi.ws"][0]
    assert "resync timeout" in disc.detail


async def test_connect_failures_back_off_and_respect_max_reconnects():
    fake = FakeKalshi([OSError("dns"), OSError("dns"), OSError("dns"), OSError("dns")])
    rec = Recorder()
    ws, sleeps = make(fake, rec, max_reconnects=3)
    await asyncio.wait_for(ws.run(), 2.0)
    assert len(fake.urls) == 4 and len(sleeps.calls) == 3
    for i, dt in enumerate(sleeps.calls):
        base = 0.5 * 2**i
        assert base * 0.5 <= dt <= base
    assert ws_statuses(rec.events) == ["error"] * 4
    assert all("connect failed" in e.detail for e in rec.events)


async def test_runtime_subscription_updates():
    fake = FakeKalshi([[]])
    rec = Recorder()
    ws, _ = make(fake, rec)
    task = asyncio.create_task(ws.run())
    while not fake.conns or len(ws._sub_sids) < 2:
        await asyncio.sleep(0)
    await ws.add_markets(["C"])
    await ws.delete_markets(["A"])
    await ws.subscribe_indices(["ETHUSD_RTI"])
    await ws.request_snapshot(["B"])
    await ws.stop()
    await asyncio.wait_for(task, 2)
    conn = fake.conns[0]
    upd = [(c["params"]["action"], c["params"]["sid"], tuple(c["params"].get("market_tickers") or c["params"].get("index_ids")))
           for c in conn.sent if c["cmd"] == "update_subscription"]
    ob, tr, cf = conn.sid_of("orderbook_delta"), conn.sid_of("trade"), conn.sid_of("cfbenchmarks_value_5hz")
    assert upd == [("add_markets", ob, ("C",)), ("add_markets", tr, ("C",)),
                   ("delete_markets", ob, ("A",)), ("delete_markets", tr, ("A",)),
                   ("subscribe_indices", cf, ("ETHUSD_RTI",)), ("get_snapshot", ob, ("B",))]
    assert ws.subscriptions[0].market_tickers == ["B", "C"] and ws.subscriptions[1].index_ids == ["BRTI", "ETHUSD_RTI"]
    with pytest.raises(KeyError):
        await ws.add_markets(["X"], channel="fill")


async def test_replay_of_recorded_raw_reproduces_live_events():
    first = [lambda c: c.delta("A"), lambda c: c.delta("B", skip=1), lambda c: c.trade("t1"),
             ConnectionClosedError(None, None)]
    second = [lambda c: c.delta("A"), lambda c: c.index(), lambda c: c.trade("t2")]
    fake = FakeKalshi([first, second])
    rec = Recorder()
    ws, _ = make(fake, rec)
    await run_until(ws, lambda: any(isinstance(e, KalshiTrade) and e.trade_id == "t2" for e in rec.events))
    state = KalshiWsState()
    replayed = [ev for _, t, raw in rec.raw for ev in normalize_ws_frame(raw, t, state)]
    assert replayed == rec.events
    assert any(isinstance(e, FeedStatus) and e.status == "gap" for e in replayed)


# ---------------------------------------------------------------------------- NO-side price shape
# Books on Kalshi's NO scale; the fake renders NO prices per the subscribe's use_yes_price
# (or per ``server_yes_price``, e.g. True once Kalshi removes the flag). B is subscribed first
# and is NO-only (its snapshot cannot prove a shape); A is two-sided and proves it.
SHAPE_BOOKS = {"A": ((("0.4500", "10.00"),), (("0.2000", "7.00"), ("0.5300", "5.00"))),
               "B": ((), (("0.6000", "4.00"),))}
EXPECTED_BOOKS = {"A": ({4500: 1000, 4600: 100}, {2000: 700, 5300: 300}),
                  "B": ({}, {6000: 400, 6100: 300})}


@pytest.mark.parametrize(("client_flag", "server_flag"), [
    (True, None),    # default: yes-leg wire
    (False, None),   # legacy: no-leg wire (all recordings so far)
    (False, True),   # flag removed by Kalshi but we still send false: yes-leg wire regardless
    (True, False),   # server ignores the flag the other way
])
async def test_books_identical_whatever_the_no_side_wire_shape(client_flag, server_flag):
    script = [lambda c: c.delta("A", side="no", px="0.5300", d="-2.00"),
              # our resting ask (sell YES at 0.39) = a NO bid at 0.61, annotated with our client_order_id
              lambda c: c.delta("B", side="no", px="0.6100", d="3.00", client_order_id="dh-ask-1"),
              lambda c: c.delta("A", side="yes", px="0.4600", d="1.00"),
              lambda c: c.trade("end")]
    fake = FakeKalshi([script], books=SHAPE_BOOKS, server_yes_price=server_flag)
    rec = Recorder()
    ws, _ = make(fake, rec, use_yes_price=client_flag,
                 subscriptions=[Subscription(["orderbook_delta", "trade"], market_tickers=["B", "A"])])
    await run_until(ws, lambda: any(isinstance(e, KalshiTrade) and e.trade_id == "end" for e in rec.events))
    conn = fake.conns[0]
    wire_yes_leg = client_flag if server_flag is None else server_flag
    assert conn.sent[0]["params"]["use_yes_price"] is client_flag
    snap_a = next(orjson.loads(r) for _, _, r in rec.raw if b'"orderbook_snapshot"' in r and b'"A"' in r)
    assert snap_a["msg"]["no_dollars_fp"] == ([["0.4700", "5.00"], ["0.8000", "7.00"]] if wire_yes_leg
                                              else [["0.2000", "7.00"], ["0.5300", "5.00"]])

    books = {t: KalshiBook(t) for t in EXPECTED_BOOKS}
    for e in rec.events:
        if isinstance(e, KalshiBookSnapshot):
            books[e.ticker].apply_snapshot(e)
        elif isinstance(e, KalshiBookDelta):
            assert books[e.ticker].apply_delta(e)
    assert {t: (dict(b.yes_bids), dict(b.no_bids)) for t, b in books.items()} == EXPECTED_BOOKS
    assert all(b.valid for b in books.values()) and books["A"].best_ask() == 4700
    own = [e for e in rec.events if isinstance(e, KalshiBookDelta) and e.own_client_order_id]
    assert [(e.ticker, e.side, e.px, e.delta, e.own_client_order_id) for e in own] == [
        ("B", resting_book("ask"), book_px("ask", 3900), 300, "dh-ask-1")]

    st = [(e.stream, e.status) for e in rec.events if isinstance(e, FeedStatus)]
    if wire_yes_leg == client_flag:
        assert ws.state.counters["pricing_mismatches"] == 0 and ("kalshi.book:B", "gap") not in st
    else:  # A's snapshot proves the other shape: followed, reported, and B (built on the wrong scale) resynced
        assert ws.state.counters["pricing_mismatches"] == 1
        assert ("kalshi.ws:orderbook_delta", "error") in st
        assert st.index(("kalshi.book:B", "gap")) < st.index(("kalshi.book:B", "resynced"))
        assert [c["params"] for c in conn.sent if c["cmd"] == "update_subscription"] == [
            {"sid": conn.sid_of("orderbook_delta"), "market_tickers": ["B"], "action": "get_snapshot"}]
    # the recorded raw stream replays to the same events with a DEFAULT (legacy, no-leg) replay
    # state: the 'connected' record declares the flag, the snapshots prove the rest
    state = KalshiWsState()
    assert [ev for _, t, raw in rec.raw for ev in normalize_ws_frame(raw, t, state)] == rec.events
