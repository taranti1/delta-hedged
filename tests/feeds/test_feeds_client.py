"""FeedClient transport loop against a local (loopback) websocket server: subscription on
connect, verbatim raw capture with markers, gap -> in-place resubscribe, heartbeat replies,
staleness watchdog, reconnect with backoff. No external network."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable

import orjson
import pytest

from dh.core.events import ExtBookDelta, ExtBookSnapshot, FeedStatus
from dh.feeds.base import FeedConfig, NormalizerState, is_marker, safe_normalize
from dh.feeds.coinbase import CoinbaseFeed
from dh.feeds.cryptocom import CryptoComFeed

websockets = pytest.importorskip("websockets")
from websockets.asyncio.server import serve  # noqa: E402

Script = Callable[[object, int, list], Awaitable[None]]


class FakeVenue:
    """Loopback server; ``script(ws, conn_index, received)`` drives each connection."""

    def __init__(self, script: Script) -> None:
        self.script = script
        self.conns = 0
        self.received: list[list[str]] = []

    async def handler(self, ws) -> None:  # type: ignore[no-untyped-def]
        idx = self.conns
        self.conns += 1
        got: list[str] = []
        self.received.append(got)

        async def reader() -> None:
            async for m in ws:
                got.append(m if isinstance(m, str) else m.decode())

        rt = asyncio.create_task(reader())
        try:
            await self.script(ws, idx, got)
        finally:
            rt.cancel()


def _cb(seq: int, typ: str = "update", price: str = "100.00", qty: str = "1") -> str:
    side_updates = [{"side": "bid", "event_time": "x", "price_level": price, "new_quantity": qty}]
    if typ == "snapshot":
        side_updates.append({"side": "offer", "event_time": "x", "price_level": "101.00", "new_quantity": "1"})
    return orjson.dumps({"channel": "l2_data", "client_id": "", "timestamp": "2026-09-25T12:00:00Z", "sequence_num": seq,
                         "events": [{"type": typ, "product_id": "BTC-USD", "updates": side_updates}]}).decode()


async def _wait_for(pred: Callable[[], bool], timeout: float = 5.0) -> None:
    t0 = asyncio.get_running_loop().time()
    while not pred():
        if asyncio.get_running_loop().time() - t0 > timeout:
            raise TimeoutError("condition not reached")
        await asyncio.sleep(0.01)


def _cfg(port: int, **kw) -> FeedConfig:  # type: ignore[no-untyped-def]
    base = dict(url=f"ws://127.0.0.1:{port}", backoff_initial_s=0.05, backoff_max_s=0.1, proxy=None,
                resync_min_interval_s=0.0, resnapshot_interval_s=0)
    base.update(kw)
    return FeedConfig(**base)


async def test_capture_markers_gap_resubscribe_and_reconnect():
    async def script(ws, idx, got):  # type: ignore[no-untyped-def]
        await _wait_for(lambda: len(got) >= 3)  # level2, market_trades, heartbeats subscriptions
        await ws.send(_cb(1, "snapshot"))
        await ws.send(_cb(2))
        if idx == 0:
            await ws.send(_cb(5))  # gap -> client resubscribes level2 in place
            await _wait_for(lambda: len(got) >= 5)
            await ws.send(_cb(6, "snapshot"))
            await ws.send(_cb(7))
            await asyncio.sleep(0.05)
            return  # server closes -> client reconnects
        await ws.wait_closed()

    fake = FakeVenue(script)
    records: list[tuple[str, int, bytes]] = []
    events = []
    statuses = []
    async with serve(fake.handler, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        feed = CoinbaseFeed(_cfg(port), on_event=events.append, status_cb=statuses.append)
        task = asyncio.create_task(feed.run(lambda s, t, r: records.append((s, t, r))))
        await _wait_for(lambda: fake.conns >= 2 and sum(isinstance(e, ExtBookSnapshot) for e in events) >= 3)
        feed.stop()
        await asyncio.wait_for(task, 5)

    assert all(s == "coinbase.ws" for s, _, _ in records)
    ts = [t for _, t, _ in records]
    assert ts == sorted(ts)
    # subscriptions and the gap resync were sent (and recorded as 'sent' markers)
    first = [orjson.loads(m) for m in fake.received[0]]
    assert [m.get("channel") for m in first[:3]] == ["level2", "market_trades", "heartbeats"]
    assert [(m["type"], m["channel"]) for m in first[3:5]] == [("unsubscribe", "level2"), ("subscribe", "level2")]
    sent = [orjson.loads(r) for _, _, r in records if is_marker(r) and orjson.loads(r)["_dh"] == "sent"]
    assert len(sent) >= 5
    # venue frames were captured verbatim
    venue_frames = [r for _, _, r in records if not is_marker(r)]
    assert venue_frames[0] == _cb(1, "snapshot").encode()
    # replaying the capture with a fresh state reproduces the live events exactly
    st = NormalizerState(stream="coinbase.ws", venue="coinbase")
    replayed = []
    for _, t, r in records:
        replayed += safe_normalize(CoinbaseFeed.normalize, r, t, st)
    assert replayed == events
    kinds = [(e.stream, e.status) for e in events if isinstance(e, FeedStatus)]
    assert ("coinbase.book:BTC-USD", "gap") in kinds and ("coinbase.book:BTC-USD", "resynced") in kinds
    assert kinds.count(("coinbase.ws", "connected")) >= 2 and ("coinbase.ws", "disconnected") in kinds
    assert any(isinstance(e, ExtBookDelta) for e in events)
    assert feed.metrics.reconnects >= 1 and feed.metrics.gaps == 1
    assert statuses and all(isinstance(s, FeedStatus) for s in statuses)


async def test_stale_watchdog_and_heartbeat_reply():
    replies: list[str] = []

    async def script(ws, idx, got):  # type: ignore[no-untyped-def]
        await _wait_for(lambda: len(got) >= 2)  # book + trade subscriptions
        await ws.send(orjson.dumps({"id": 777, "method": "public/heartbeat", "code": 0}).decode())
        await _wait_for(lambda: any("respond-heartbeat" in m for m in got))
        replies.extend(m for m in got if "respond-heartbeat" in m)
        await ws.wait_closed()  # go silent: stale, then dead -> client reconnects

    class FastCrypto(CryptoComFeed):
        subscribe_delay_s = 0.0

    fake = FakeVenue(script)
    records: list[tuple[str, int, bytes]] = []
    async with serve(fake.handler, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        feed = FastCrypto(_cfg(port, stale_after_s=0.3, dead_after_s=0.8))
        task = asyncio.create_task(feed.run(lambda s, t, r: records.append((s, t, r))))
        await _wait_for(lambda: fake.conns >= 2, timeout=8)
        feed.stop()
        await asyncio.wait_for(task, 5)
    assert orjson.loads(replies[0]) == {"id": 777, "method": "public/respond-heartbeat"}
    markers = [orjson.loads(r) for _, _, r in records if is_marker(r)]
    statuses = [m["status"] for m in markers if m["_dh"] == "status"]
    assert statuses[:3] == ["connected", "stale", "disconnected"]
    assert "FeedDead" in next(m["detail"] for m in markers if m.get("status") == "disconnected")


async def test_run_refuses_stub():
    from dh.feeds.kalshi_perp import KalshiPerpFeed

    with pytest.raises(NotImplementedError):
        await KalshiPerpFeed().run(lambda s, t, r: None)
