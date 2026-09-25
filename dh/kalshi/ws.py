"""Async Kalshi WebSocket client (asyncapi 2.0.0): auth, subscriptions, integrity, reconnect.

    ws = KalshiWS(url, signer, [Subscription(["orderbook_delta", "trade"], market_tickers=[...]),
                                Subscription(["cfbenchmarks_value_5hz"], index_ids=["BRTI"])],
                  on_raw=recorder.write)
    await ws.run(emit)          # runs until stop(); reconnects forever by default

Behaviour
  * Handshake: signed headers for ``GET /trade-api/ws/v2`` (fresh timestamp per connect).
  * Subscribe: one ``subscribe`` command per Subscription with an incrementing ``id``
    (unique for the client's lifetime); orderbook subscriptions send ``use_yes_price``
    explicitly (default false: NO bids on the NO price scale, the normalizer's convention).
  * Raw capture: ``on_raw('kalshi.ws', recv_ns, raw_bytes)`` for EVERY inbound frame, before
    parsing, plus synthetic ``dh.feed_status`` records for connected / disconnected / stale /
    error (see dh.kalshi.sequencer) so replay reproduces outages.
  * Events: every raw record goes through ``sequencer.normalize_ws_message`` (the same code
    replay uses): per-sid seq continuity, duplicate/out-of-order suppression, gap ->
    FeedStatus('gap') + ``update_subscription/get_snapshot`` for the affected markets, books
    invalid (deltas suppressed) until their snapshot arrives. If a resync does not complete
    within ``resync_timeout_s`` the connection is recycled.
  * Staleness watchdog: no inbound message for ``stale_after_s`` -> FeedStatus('stale'),
    reconnect. (Set None for quiet private-only connections.)
  * Reconnect: exponential backoff with jitter (seeded rng), full resubscribe including
    markets/indices added at runtime. Terminal channel errors (codes 10, 25) also recycle.
  * ``on_message(msg)`` (optional) receives every parsed exchange message after its events,
    e.g. for MarketRegistry.apply_metadata_update on 'metadata_updated' bodies.
  * Heartbeat: Kalshi pings every 10 s; the websockets library answers pings with pongs
    automatically and we also ping (``ping_interval``) so a dead link is detected. Keep the
    ``emit`` callback fast: a full receive queue pauses reading (and thus pongs).

Times: ``recv_ns`` from ``clock_ns()`` (int ns, wall clock); watchdog/backoff use
``monotonic()`` seconds. Both, plus ``sleep`` and ``connect``, are injectable for tests.
"""

from __future__ import annotations

import asyncio
import random
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any, Protocol

import orjson

from dh.core.events import Event
from dh.kalshi.auth import WS_PATH, KalshiSigner
from dh.kalshi.sequencer import (
    KalshiWsState,
    normalize_ws_frame,
    normalize_ws_message,
    synthetic_status_frame,
)
from dh.kalshi.wire import as_dict

PROD_WS_URL = "wss://external-api-ws.kalshi.com/trade-api/ws/v2"
DEMO_WS_URL = "wss://demo-api.kalshi.co/trade-api/ws/v2"
STREAM = "kalshi.ws"
MARKET_CHANNELS = frozenset({"orderbook_delta", "ticker", "trade", "fill", "user_orders", "market_positions"})
INDEX_CHANNELS = frozenset({"cfbenchmarks_value", "cfbenchmarks_value_5hz"})
TERMINAL_ERROR_CODES = frozenset({10, 25})

RawCallback = Callable[[str, int, bytes], None]
EventCallback = Callable[[Event], None]
MessageCallback = Callable[[dict[str, Any]], None]
MAX_PENDING_COMMANDS = 4096


class WSConnection(Protocol):
    async def send(self, message: str) -> None: ...
    async def recv(self) -> str | bytes: ...
    async def close(self) -> None: ...


ConnectFactory = Callable[[str, dict[str, str]], Awaitable[WSConnection]]


@dataclass
class Subscription:
    """One ``subscribe`` command. market_tickers/index_ids may grow at runtime."""

    channels: list[str]
    market_tickers: list[str] = field(default_factory=list)
    index_ids: list[str] = field(default_factory=list)
    extra: dict[str, Any] = field(default_factory=dict)  # e.g. {"send_initial_snapshot": True}

    def params(self, use_yes_price: bool = False) -> dict[str, Any]:
        """``params`` object of the subscribe command."""
        p: dict[str, Any] = {"channels": list(self.channels)}
        if self.market_tickers:
            p["market_tickers"] = list(self.market_tickers)
        if self.index_ids:
            p["index_ids"] = list(self.index_ids)
        if "orderbook_delta" in self.channels:
            p["use_yes_price"] = bool(use_yes_price)
        p.update(self.extra)
        return p


class _WebsocketsConn:
    """Adapter over websockets' asyncio connection returning raw bytes (no UTF-8 decode)."""

    def __init__(self, ws: Any) -> None:
        self._ws = ws

    async def send(self, message: str) -> None:
        await self._ws.send(message)

    async def recv(self) -> bytes:
        return await self._ws.recv(decode=False)

    async def close(self) -> None:
        await self._ws.close()


def websockets_connect_factory(
    *,
    ping_interval: float | None = 10.0,
    ping_timeout: float | None = 10.0,
    open_timeout: float = 10.0,
    max_queue: int = 4096,
    max_size: int = 16 * 1024 * 1024,
    proxy: str | bool | None = True,
) -> ConnectFactory:
    """Default ConnectFactory using ``websockets.asyncio.client.connect``."""

    async def _connect(url: str, headers: dict[str, str]) -> WSConnection:
        from websockets.asyncio.client import connect

        ws = await connect(
            url,
            additional_headers=headers,
            ping_interval=ping_interval,
            ping_timeout=ping_timeout,
            open_timeout=open_timeout,
            max_queue=max_queue,
            max_size=max_size,
            proxy=proxy,  # type: ignore[arg-type]
        )
        return _WebsocketsConn(ws)

    return _connect


class _Recycle(Exception):
    """Internal: close this connection and reconnect (reason in args[0])."""


class KalshiWS:
    """Authenticated, self-healing Kalshi WebSocket feed. See module docstring."""

    def __init__(
        self,
        url: str,
        signer: KalshiSigner | None,
        subscriptions: list[Subscription],
        on_raw: RawCallback | None = None,
        on_event: EventCallback | None = None,
        *,
        connect: ConnectFactory | None = None,
        clock_ns: Callable[[], int] = time.time_ns,
        monotonic: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        rng: random.Random | None = None,
        stale_after_s: float | None = 15.0,
        resync_timeout_s: float = 10.0,
        backoff_initial_s: float = 0.5,
        backoff_max_s: float = 30.0,
        healthy_reset_s: float = 30.0,
        max_reconnects: int | None = None,
        use_yes_price: bool = False,
        ws_path: str = WS_PATH,
        stream: str = STREAM,
        on_message: MessageCallback | None = None,
    ) -> None:
        self.url = url
        self.signer = signer
        self.subscriptions = subscriptions
        self.on_raw = on_raw
        self.on_event = on_event
        self._connect = connect or websockets_connect_factory()
        self._clock_ns = clock_ns
        self._mono = monotonic
        self._sleep = sleep
        self._rng = rng or random.Random(0)
        self.stale_after_s = stale_after_s
        self.resync_timeout_s = resync_timeout_s
        self.backoff_initial_s = backoff_initial_s
        self.backoff_max_s = backoff_max_s
        self.healthy_reset_s = healthy_reset_s
        self.max_reconnects = max_reconnects
        self.use_yes_price = use_yes_price
        self.ws_path = ws_path
        self.stream = stream
        self.on_message = on_message  # parsed exchange messages (e.g. metadata_updated bodies)
        self.state = KalshiWsState(use_yes_price=use_yes_price)
        self._emit: EventCallback = on_event or (lambda ev: None)
        self._conn: WSConnection | None = None
        self._running = False
        self._next_id = 0
        self._pending: dict[int, tuple[str, int | None]] = {}  # cmd id -> (cmd, sub index)
        self._sub_sids: dict[int, dict[str, int]] = {}  # sub index -> channel -> sid
        self._sub_sent: dict[int, tuple[list[str], list[str]]] = {}  # sub index -> (tickers, index_ids) in subscribe
        self._resync_since: dict[tuple[int, str], float] = {}
        self.connects = 0
        self.stats: dict[str, int] = {"frames": 0, "events": 0, "commands": 0}

    # ------------------------------------------------------------------ public API
    @property
    def connected(self) -> bool:
        return self._conn is not None

    async def run(self, emit: EventCallback | None = None) -> None:
        """Connect, subscribe, pump frames; reconnect on failure until stop() is called
        (or ``max_reconnects`` consecutive failed attempts)."""
        if emit is not None:
            self._emit = emit
        self._running = True
        attempt = 0
        while self._running:
            headers = self.signer.ws_headers(path=self.ws_path) if self.signer else {}
            try:
                conn = await self._connect(self.url, headers)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # handshake refused, DNS, TLS, timeout ...
                self._synthetic("error", f"connect failed: {type(exc).__name__}: {exc}"[:300])
                attempt += 1
                if self.max_reconnects is not None and attempt > self.max_reconnects:
                    break
                await self._sleep(self.backoff_delay(attempt))
                continue
            self._conn = conn
            self.connects += 1
            started = self._mono()
            self._pending.clear()
            self._sub_sids.clear()
            self._sub_sent.clear()
            self._resync_since.clear()
            self._synthetic("connected", self.url)
            reason = "closed"
            try:
                for idx in range(len(self.subscriptions)):
                    await self._send_subscribe(idx)
                await self._pump(conn)
            except _Recycle as exc:
                reason = str(exc.args[0]) if exc.args else "recycle"
            except asyncio.CancelledError:
                self._conn = None
                await _safe_close(conn)
                self._synthetic("disconnected", "cancelled")
                raise
            except Exception as exc:  # ConnectionClosed, OSError, protocol errors
                reason = f"{type(exc).__name__}: {exc}"[:300]
            self._conn = None
            await _safe_close(conn)
            self._synthetic("disconnected", reason)
            if not self._running:
                break
            if self._mono() - started >= self.healthy_reset_s:
                attempt = 0
            attempt += 1
            if self.max_reconnects is not None and attempt > self.max_reconnects:
                break
            await self._sleep(self.backoff_delay(attempt))
        self._running = False

    async def stop(self) -> None:
        """Stop the run loop and close the connection."""
        self._running = False
        conn, self._conn = self._conn, None
        if conn is not None:
            await _safe_close(conn)

    def backoff_delay(self, attempt: int) -> float:
        """Jittered exponential backoff (s) for the attempt-th consecutive reconnect (>=1)."""
        base = min(self.backoff_max_s, self.backoff_initial_s * (2 ** max(0, attempt - 1)))
        return base * (0.5 + 0.5 * self._rng.random())

    async def add_markets(self, tickers: list[str], channel: str = "orderbook_delta") -> None:
        """Add markets to the subscription carrying `channel` (applied now and on reconnect)."""
        idx = self._sub_index(channel)
        sub = self.subscriptions[idx]
        new = [t for t in tickers if t not in sub.market_tickers]
        if not new:
            return
        sub.market_tickers.extend(new)
        await self._update_markets(idx, new, "add_markets")

    async def delete_markets(self, tickers: list[str], channel: str = "orderbook_delta") -> None:
        """Remove markets from the subscription carrying `channel`."""
        idx = self._sub_index(channel)
        sub = self.subscriptions[idx]
        gone = [t for t in tickers if t in sub.market_tickers]
        if not gone:
            return
        sub.market_tickers[:] = [t for t in sub.market_tickers if t not in gone]
        await self._update_markets(idx, gone, "delete_markets")

    async def request_snapshot(self, tickers: list[str], sid: int | None = None) -> None:
        """Ask for fresh orderbook snapshots (update_subscription/get_snapshot)."""
        if not self.connected:
            return
        if sid is None:
            by_sid: dict[int, list[str]] = {}
            for t in tickers:
                s = self.state.sid_for_ticker(t)
                if s is None:
                    s = self._channel_sid("orderbook_delta")
                if s is not None:
                    by_sid.setdefault(s, []).append(t)
            for s, ts in by_sid.items():
                await self._send_get_snapshot(s, ts)
        else:
            await self._send_get_snapshot(sid, list(tickers))

    async def subscribe_indices(self, index_ids: list[str], channel: str = "cfbenchmarks_value_5hz") -> None:
        """Add CF Benchmarks index ids (e.g. 'BRTI') to a cfbenchmarks subscription."""
        idx = self._sub_index(channel)
        sub = self.subscriptions[idx]
        new = [i for i in index_ids if i not in sub.index_ids]
        if not new:
            return
        sub.index_ids.extend(new)
        sid = self._sub_sids.get(idx, {}).get(channel)
        if self.connected and sid is not None:
            await self.send_command("update_subscription", {"sid": sid, "action": "subscribe_indices", "index_ids": new})

    async def send_command(self, cmd: str, params: dict[str, Any] | None = None, sub_index: int | None = None) -> int:
        """Send one command with the next id; returns the id. No-op (-1) when disconnected."""
        conn = self._conn
        if conn is None:
            return -1
        self._next_id += 1
        cid = self._next_id
        payload: dict[str, Any] = {"id": cid, "cmd": cmd}
        if params is not None:
            payload["params"] = params
        self._pending[cid] = (cmd, sub_index)
        if len(self._pending) > MAX_PENDING_COMMANDS:  # replies are not always 1:1; keep it bounded
            for old in sorted(self._pending)[: len(self._pending) - MAX_PENDING_COMMANDS]:
                del self._pending[old]
        self.stats["commands"] += 1
        await conn.send(orjson.dumps(payload).decode())
        return cid

    # ------------------------------------------------------------------ internals
    def _sub_index(self, channel: str) -> int:
        for i, sub in enumerate(self.subscriptions):
            if channel in sub.channels:
                return i
        raise KeyError(f"no subscription carries channel {channel!r}")

    def _channel_sid(self, channel: str) -> int | None:
        for chans in self._sub_sids.values():
            if channel in chans:
                return chans[channel]
        return None

    async def _send_subscribe(self, idx: int) -> None:
        sub = self.subscriptions[idx]
        self._sub_sent[idx] = (list(sub.market_tickers), list(sub.index_ids))
        await self.send_command("subscribe", sub.params(self.use_yes_price), sub_index=idx)

    async def _sync_new_sid(self, idx: int, channel: str, sid: int) -> None:
        """Markets/indices changed between our subscribe and its 'subscribed' reply: send the
        difference now so the server-side subscription matches ``self.subscriptions``."""
        sub = self.subscriptions[idx]
        sent_t, sent_i = self._sub_sent.get(idx, ([], []))
        if channel in MARKET_CHANNELS:
            add = [t for t in sub.market_tickers if t not in sent_t]
            rm = [t for t in sent_t if t not in sub.market_tickers]
            if add:
                await self.send_command("update_subscription", {"sid": sid, "market_tickers": add, "action": "add_markets"})
            if rm:
                await self.send_command("update_subscription", {"sid": sid, "market_tickers": rm, "action": "delete_markets"})
        if channel in INDEX_CHANNELS:
            add = [i for i in sub.index_ids if i not in sent_i]
            if add:
                await self.send_command("update_subscription", {"sid": sid, "action": "subscribe_indices", "index_ids": add})

    async def _update_markets(self, idx: int, tickers: list[str], action: str) -> None:
        if not self.connected:
            return
        for channel, sid in sorted(self._sub_sids.get(idx, {}).items()):
            if channel in MARKET_CHANNELS:
                await self.send_command("update_subscription", {"sid": sid, "market_tickers": tickers, "action": action})

    async def _send_get_snapshot(self, sid: int, tickers: list[str]) -> None:
        now = self._mono()
        for t in tickers:
            self._resync_since.setdefault((sid, t), now)
        await self.send_command("update_subscription", {"sid": sid, "market_tickers": list(tickers), "action": "get_snapshot"})

    def _synthetic(self, status: str, detail: str) -> None:
        self._handle_raw(synthetic_status_frame(status, detail))

    def _handle_raw(self, raw: bytes) -> dict[str, Any] | None:
        """Record, parse, sequence and emit one raw record. Returns the parsed message."""
        recv_ns = self._clock_ns()
        if self.on_raw is not None:
            self.on_raw(self.stream, recv_ns, raw)
        try:
            msg = orjson.loads(raw)
        except orjson.JSONDecodeError:
            msg = None
        if isinstance(msg, dict):
            events = normalize_ws_message(msg, recv_ns, self.state)
            self.state.counters["frames"] += 1
        else:
            events = normalize_ws_frame(raw, recv_ns, self.state)
            msg = None
        for ev in events:
            self.stats["events"] += 1
            self._emit(ev)
        return msg

    async def _pump(self, conn: WSConnection) -> None:
        last = self._mono()
        while self._running:
            now = self._mono()
            limits = []
            if self.stale_after_s is not None:
                limits.append(self.stale_after_s - (now - last))
            if self._resync_since:
                limits.append(min(self._resync_since.values()) + self.resync_timeout_s - now)
            timeout = max(0.0, min(limits)) if limits else None
            try:
                if timeout is None:
                    frame = await conn.recv()
                else:
                    frame = await asyncio.wait_for(conn.recv(), timeout)
            except asyncio.TimeoutError:
                self._check_resync_timeout()  # raises _Recycle when a resync is overdue
                if self.stale_after_s is not None and self._mono() - last >= self.stale_after_s:
                    self._synthetic("stale", f"no message for {self.stale_after_s}s")
                    raise _Recycle("stale") from None
                continue
            last = self._mono()
            self.stats["frames"] += 1
            raw = frame.encode("utf-8") if isinstance(frame, str) else bytes(frame)
            msg = self._handle_raw(raw)
            if msg is not None:
                if self.on_message is not None:
                    self.on_message(msg)
                await self._control(msg)
            for sid, tickers in self.state.take_resync_requests():
                await self._send_get_snapshot(sid, list(tickers))
            self._check_resync_timeout()

    async def _control(self, msg: dict[str, Any]) -> None:
        typ = msg.get("type")
        if typ == "subscribed":
            body = as_dict(msg.get("msg"))
            cid = msg.get("id")
            pend = self._pending.get(int(cid)) if cid is not None else None
            if pend is not None and pend[1] is not None and body.get("sid") is not None:
                channel, sid = str(body.get("channel")), int(body["sid"])
                self._sub_sids.setdefault(pend[1], {})[channel] = sid
                await self._sync_new_sid(pend[1], channel, sid)
        elif typ == "error":
            body = as_dict(msg.get("msg"))
            raw_code = body.get("code")
            code = int(raw_code) if isinstance(raw_code, (int, str)) and str(raw_code).lstrip("-").isdigit() else -1
            if code in TERMINAL_ERROR_CODES:
                raise _Recycle(f"terminal channel error code={code}")
        elif typ == "orderbook_snapshot":
            body = as_dict(msg.get("msg"))
            if msg.get("sid") is not None and body.get("market_ticker"):
                self._resync_since.pop((int(msg["sid"]), str(body["market_ticker"])), None)

    def _check_resync_timeout(self) -> None:
        if not self._resync_since:
            return
        invalid = self.state.invalid_books()
        now = self._mono()
        for (sid, t), since in list(self._resync_since.items()):
            if t not in invalid.get(sid, set()):
                self._resync_since.pop((sid, t), None)
            elif now - since >= self.resync_timeout_s:
                raise _Recycle(f"resync timeout sid={sid} ticker={t}")


async def _safe_close(conn: WSConnection) -> None:
    try:
        await asyncio.wait_for(conn.close(), 5.0)
    except Exception:  # noqa: BLE001 - closing a dead socket must not mask the real error
        pass
