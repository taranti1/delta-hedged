"""Offline fakes for dh.live tests: REST client, clock, strategies, synthetic Kalshi data."""

from __future__ import annotations

import asyncio
import copy
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

import orjson

from dh.core.actions import Action
from dh.core.events import Event
from dh.core.market import MarketSpec, PriceRange
from dh.core.units import NS_PER_S
from dh.kalshi.rest import KalshiHTTPError, UnknownOutcome

T0 = 1_790_300_000 * NS_PER_S  # 2026-09-25T...Z, whole second
WRITE_METHODS = frozenset({
    "create_order", "batch_create_orders", "cancel_order", "batch_cancel_orders", "cancel_all_orders", "amend_order",
    "decrease_order", "create_order_group", "reset_order_group", "update_order_group_limit", "delete_order_group",
    "trigger_order_group",
})
ORDER_WRITES = WRITE_METHODS - {"cancel_all_orders"}


class FakeClock:
    """Injectable ns clock (never moves unless advanced)."""

    def __init__(self, t: int = T0) -> None:
        self.t = int(t)

    def __call__(self) -> int:
        return self.t

    def advance(self, ns: int) -> int:
        self.t += int(ns)
        return self.t

    def mono(self) -> float:
        return self.t / NS_PER_S


def http_error(status: int, code: str = "err", message: str = "boom", method: str = "POST", path: str = "/x") -> KalshiHTTPError:
    return KalshiHTTPError(method, path, status, {"error": {"code": code, "message": message}})


def unknown(method: str = "POST", path: str = "/portfolio/events/orders") -> UnknownOutcome:
    return UnknownOutcome(method, path, None, "ResponseLostError: timeout")


def _iso(ns: int) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(ns // NS_PER_S)) + f".{(ns // 1_000_000) % 1000:03d}Z"


def order_row(coid: str, oid: str, ticker: str, *, status: str = "resting", side: str = "bid", px: str = "0.4500",
              filled: str = "0.00", remaining: str = "2.00", initial: str = "2.00", created_ns: int | None = None,
              subaccount: int | None = None) -> dict[str, Any]:
    """openapi Order object (spec field names). ``created_ns`` None: 2026-09-25T12:00:00Z."""
    row = {
        "order_id": oid, "user_id": "u-1", "client_order_id": coid, "ticker": ticker,
        "outcome_side": "yes" if side == "bid" else "no", "book_side": side, "type": "limit", "status": status,
        "yes_price_dollars": px, "no_price_dollars": f"{1 - float(px):.4f}", "fill_count_fp": filled,
        "remaining_count_fp": remaining, "initial_count_fp": initial, "taker_fees_dollars": "0.000000",
        "maker_fees_dollars": "0.000000", "taker_fill_cost_dollars": "0.000000", "maker_fill_cost_dollars": "0.000000",
        "created_time": "2026-09-25T12:00:00Z" if created_ns is None else _iso(created_ns),
        "last_update_time": "2026-09-25T12:00:01Z",
    }
    if subaccount:  # omitempty: the primary account's rows carry no subaccount_number
        row["subaccount_number"] = subaccount
    return row


def fill_row(fid: str, oid: str, ticker: str, *, side: str = "bid", px: str = "0.4500", count: str = "1.00",
             fee: str = "0.000000", taker: bool = False, created_ns: int | None = None, coid: str = "",
             subaccount: int | None = None) -> dict[str, Any]:
    """openapi Fill object (GET /portfolio/fills)."""
    row = {"fill_id": fid, "trade_id": fid, "order_id": oid, "client_order_id": coid, "ticker": ticker,
           "market_ticker": ticker, "outcome_side": "yes" if side == "bid" else "no", "book_side": side,
           "count_fp": count, "yes_price_dollars": px, "no_price_dollars": f"{1 - float(px):.4f}", "is_taker": taker,
           "fee_cost": fee, "exchange_index": 0,
           "created_time": "2026-09-25T12:00:00Z" if created_ns is None else _iso(created_ns)}
    if subaccount:
        row["subaccount_number"] = subaccount
    return row


class FakeRest:
    """Records every call; scripted outcomes per method (FIFO list of dict / UnknownOutcome /
    exception / callable(args, kwargs)); sensible defaults otherwise.

    forbid_order_writes=True makes any order-entry write raise AssertionError (paper mode)."""

    def __init__(self, *, limiter: Any = None, forbid_order_writes: bool = False, forbid_all_writes: bool = False) -> None:
        self.limiter = limiter
        self.calls: list[tuple[str, tuple, dict]] = []
        self.script: dict[str, list[Any]] = {}
        self.forbid_order_writes = forbid_order_writes
        self.forbid_all_writes = forbid_all_writes
        self.orders: dict[str, dict[str, Any]] = {}  # oid -> order row (exchange view)
        self.positions: dict[str, str] = {}  # ticker -> position_fp
        self.queue_positions: dict[str, str] = {}  # oid -> queue_position_fp
        self.groups: list[dict[str, Any]] = []
        self.fills: list[dict[str, Any]] = []  # REST Fill rows
        self.settlements: list[dict[str, Any]] = []  # REST Settlement rows
        self.clock: Callable[[], int] = time.time_ns  # created_time of orders this fake creates
        self.cf_history: Callable[[str | None, str | None], Any] | None = None
        self.series: dict[str, dict] = {}
        self.events: dict[str, list[dict]] = {}
        self.exchange = {"exchange_active": True, "trading_active": True}
        self._n = 0
        self.gate: asyncio.Event | None = None  # when set, writes wait on it (in-flight tests)

    # ------------------------------------------------------------------ plumbing
    def on(self, name: str, *outcomes: Any) -> FakeRest:
        self.script.setdefault(name, []).extend(outcomes)
        return self

    def names(self) -> list[str]:
        return [c[0] for c in self.calls]

    def of(self, name: str) -> list[tuple[tuple, dict]]:
        return [(a, k) for n, a, k in self.calls if n == name]

    async def _call(self, name: str, args: tuple, kwargs: dict, default: Callable[[], Any]) -> Any:
        self.calls.append((name, args, dict(kwargs)))
        if name in WRITE_METHODS:
            if self.forbid_all_writes or (self.forbid_order_writes and name in ORDER_WRITES):
                raise AssertionError(f"write {name} forbidden in this test")
            if self.gate is not None:
                await self.gate.wait()
        q = self.script.get(name)
        if q:
            r = q.pop(0)
            if callable(r) and not isinstance(r, (dict, UnknownOutcome)):
                r = r(args, kwargs)
            if isinstance(r, BaseException):
                raise r
            return copy.deepcopy(r)
        return default()

    def _oid(self) -> str:
        self._n += 1
        return f"oid-{self._n}"

    # ------------------------------------------------------------------ orders
    async def create_order(self, body: dict[str, Any]) -> Any:
        def ok() -> dict[str, Any]:
            oid = self._oid()
            self.orders[oid] = order_row(body["client_order_id"], oid, body["ticker"], side=body["side"], px=body["price"],
                                         remaining=body["count"], initial=body["count"], created_ns=self.clock())
            return {"order_id": oid, "client_order_id": body["client_order_id"], "fill_count": "0.00",
                    "remaining_count": body["count"], "ts_ms": 1790300000123}
        return await self._call("create_order", (body,), {}, ok)

    async def batch_create_orders(self, orders: list[dict[str, Any]]) -> Any:
        def ok() -> dict[str, Any]:
            out = []
            for b in orders:
                oid = self._oid()
                self.orders[oid] = order_row(b["client_order_id"], oid, b["ticker"], side=b["side"], px=b["price"],
                                             remaining=b["count"], initial=b["count"], created_ns=self.clock())
                out.append({"order_id": oid, "client_order_id": b["client_order_id"], "fill_count": "0.00",
                            "remaining_count": b["count"], "ts_ms": 1790300000123})
            return {"orders": out}
        return await self._call("batch_create_orders", (orders,), {}, ok)

    async def cancel_order(self, order_id: str, **kw: Any) -> Any:
        def ok() -> dict[str, Any]:
            o = self.orders.get(order_id)
            rem = o["remaining_count_fp"] if o else "0.00"
            if o:
                o["status"] = "canceled"
                o["remaining_count_fp"] = "0.00"
            return {"order_id": order_id, "client_order_id": (o or {}).get("client_order_id", ""), "reduced_by": rem,
                    "ts_ms": 1790300000456}
        return await self._call("cancel_order", (order_id,), kw, ok)

    async def batch_cancel_orders(self, orders: list[dict[str, Any]]) -> Any:
        def ok() -> dict[str, Any]:
            out = []
            for it in orders:
                o = self.orders.get(it["order_id"])
                rem = o["remaining_count_fp"] if o else "0.00"
                if o:
                    o["status"] = "canceled"
                    o["remaining_count_fp"] = "0.00"
                out.append({"order_id": it["order_id"], "client_order_id": (o or {}).get("client_order_id", ""),
                            "reduced_by": rem, "ts_ms": 1790300000456})
            return {"orders": out}
        return await self._call("batch_cancel_orders", (orders,), {}, ok)

    async def cancel_all_orders(self, **kw: Any) -> Any:
        def ok() -> dict[str, Any]:
            for o in self.orders.values():
                if o["status"] == "resting":
                    o["status"] = "canceled"
                    o["remaining_count_fp"] = "0.00"
            return {}
        return await self._call("cancel_all_orders", (), kw, ok)

    async def amend_order(self, order_id: str, body: dict[str, Any], **kw: Any) -> Any:
        return await self._call("amend_order", (order_id, body), kw, lambda: {"order_id": order_id})

    async def decrease_order(self, order_id: str, **kw: Any) -> Any:
        return await self._call("decrease_order", (order_id,), kw, lambda: {"order_id": order_id})

    # ------------------------------------------------------------------ groups
    async def create_order_group(self, contracts_limit: int, **kw: Any) -> Any:
        def ok() -> dict[str, Any]:
            gid = f"og-{len(self.groups) + 1}"
            self.groups.append({"id": gid, "contracts_limit_fp": f"{contracts_limit / 100:.2f}", "is_auto_cancel_enabled": True})
            return {"order_group_id": gid, "subaccount": 0}
        return await self._call("create_order_group", (contracts_limit,), kw, ok)

    async def get_order_groups(self, **kw: Any) -> Any:
        return await self._call("get_order_groups", (), kw, lambda: {"order_groups": copy.deepcopy(self.groups)})

    async def reset_order_group(self, gid: str, **kw: Any) -> Any:
        return await self._call("reset_order_group", (gid,), kw, lambda: {})

    async def update_order_group_limit(self, gid: str, limit: int, **kw: Any) -> Any:
        return await self._call("update_order_group_limit", (gid, limit), kw, lambda: {})

    async def delete_order_group(self, gid: str, **kw: Any) -> Any:
        return await self._call("delete_order_group", (gid,), kw, lambda: {})

    # ------------------------------------------------------------------ reads
    async def find_order_by_client_id(self, coid: str, *, ticker: str | None = None, min_ts: int | None = None) -> Any:
        def look() -> Any:
            for o in self.orders.values():
                if o["client_order_id"] == coid:
                    return copy.deepcopy(o)
            return None
        return await self._call("find_order_by_client_id", (coid,), {"ticker": ticker, "min_ts": min_ts}, look)

    async def get_order(self, order_id: str) -> Any:
        def look() -> Any:
            o = self.orders.get(order_id)
            if o is None:
                raise http_error(404, "not_found", "order not found", "GET", f"/portfolio/orders/{order_id}")
            return {"order": copy.deepcopy(o)}
        return await self._call("get_order", (order_id,), {}, look)

    async def iter_orders(self, **filters: Any):  # async generator (like KalshiRest.iter_orders)
        rows = await self._call("iter_orders", (), filters, lambda: [
            copy.deepcopy(o) for o in self.orders.values()
            if (filters.get("status") in (None, o["status"])) and (filters.get("ticker") in (None, o["ticker"]))])
        for r in rows:
            yield r

    async def iter_fills(self, **filters: Any):  # async generator (like KalshiRest.iter_fills)
        rows = await self._call("iter_fills", (), filters, lambda: copy.deepcopy(self.fills))
        for r in rows:
            yield r

    async def iter_settlements(self, **filters: Any):  # async generator (like KalshiRest.iter_settlements)
        rows = await self._call("iter_settlements", (), filters, lambda: copy.deepcopy(self.settlements))
        for r in rows:
            yield r

    async def get_queue_positions(self, **kw: Any) -> Any:
        return await self._call("get_queue_positions", (), kw, lambda: {"queue_positions": [
            {"order_id": oid, "market_ticker": self.orders[oid]["ticker"] if oid in self.orders else "", "queue_position_fp": q}
            for oid, q in self.queue_positions.items()]})

    async def get_all_positions(self, **kw: Any) -> Any:
        return await self._call("get_all_positions", (), kw, lambda: {"market_positions": [
            {"ticker": t, "position_fp": p, "market_exposure_dollars": "0", "realized_pnl_dollars": "0",
             "fees_paid_dollars": "0", "total_traded_dollars": "0", "exchange_index": 0, "last_updated_ts": "x"}
            for t, p in self.positions.items()], "event_positions": []})

    async def get_exchange_status(self) -> Any:
        return await self._call("get_exchange_status", (), {}, lambda: dict(self.exchange))

    async def configure_rate_limits(self) -> Any:
        return await self._call("configure_rate_limits", (), {}, lambda: {"limits": {}, "endpoint_costs": {}})

    async def get_cfbenchmarks_history(self, index_id: str = "BRTI", *, timespan: Any = None, timestamp: Any = None,
                                       extra_params: Any = None) -> Any:
        def body() -> Any:
            if self.cf_history is None:
                raise http_error(404, "not_found", "no history", "GET", "/cfbenchmarks/history/values")
            return self.cf_history(timespan, timestamp)
        return await self._call("get_cfbenchmarks_history", (index_id,), {"timespan": timespan, "timestamp": timestamp}, body)

    async def get_series(self, series_ticker: str, **kw: Any) -> Any:
        return await self._call("get_series", (series_ticker,), kw, lambda: {"series": self.series[series_ticker]})

    async def get_series_fee_changes(self, series_ticker: str | None = None, **kw: Any) -> Any:
        return await self._call("get_series_fee_changes", (series_ticker,), kw, lambda: {"series_fee_change_arr": []})

    async def iter_events(self, **filters: Any):
        rows = await self._call("iter_events", (), filters, lambda: copy.deepcopy(self.events.get(filters.get("series_ticker"), [])))
        for r in rows:
            yield r

    async def close(self) -> None:
        self.calls.append(("close", (), {}))


# ============================================================================ strategies
class RecordingStrategy:
    """Minimal Strategy: records events and asserts it is never re-entered; returns scripted
    actions (callable(ev) -> list[Action])."""

    def __init__(self, respond: Callable[[Event], list[Action]] | None = None) -> None:
        self.events: list[Event] = []
        self.respond = respond
        self.active = False
        self.max_depth = 0
        self.tasks: set[Any] = set()

    def on_event(self, ev: Event) -> list[Action]:
        assert not self.active, "strategy re-entered"
        self.active = True
        try:
            t = asyncio.current_task() if _loop_running() else None
            self.tasks.add(t.get_name() if t is not None else None)
            self.events.append(ev)
            return list(self.respond(ev)) if self.respond else []
        finally:
            self.active = False


def _loop_running() -> bool:
    try:
        asyncio.get_running_loop()
        return True
    except RuntimeError:
        return False


# ============================================================================ market fixtures
def kxbtcd_spec(strike: float = 84_000.0, *, hour_ns: int | None = None, ticker: str | None = None,
                fee_type: str = "quadratic_with_maker_fees") -> MarketSpec:
    exp = hour_ns if hour_ns is not None else ((T0 // (3600 * NS_PER_S)) + 1) * 3600 * NS_PER_S
    t = ticker or f"KXBTCD-26SEP2513-T{strike:.2f}"
    return MarketSpec(ticker=t, event_ticker=t.rsplit("-", 1)[0], series_ticker="KXBTCD", strike_type="greater",
                      floor_strike=strike, cap_strike=None, open_ts=exp - 86400 * NS_PER_S, close_ts=exp,
                      expiration_ts=exp, price_ranges=(PriceRange(100, 9900, 100),), fee_type=fee_type,
                      fee_multiplier=1.0, title="test")


NO_SEQ_CHANNELS = frozenset({"fill", "user_orders", "market_positions"})  # asyncapi: no seq on these payloads


@dataclass
class WsFrames:
    """Builders for spec-shaped Kalshi WS frames (sid/seq managed per channel; the own-activity
    channels fill / user_orders / market_positions carry NO seq, as in the asyncapi)."""

    sids: dict[str, int] = field(default_factory=dict)
    seqs: dict[int, int] = field(default_factory=dict)

    def _sid(self, ch: str) -> int:
        if ch not in self.sids:
            self.sids[ch] = len(self.sids) + 1
        return self.sids[ch]

    def _seq(self, sid: int) -> int:
        self.seqs[sid] = self.seqs.get(sid, 0) + 1
        return self.seqs[sid]

    def frame(self, typ: str, ch: str, msg: dict[str, Any]) -> bytes:
        sid = self._sid(ch)
        if ch in NO_SEQ_CHANNELS:
            return orjson.dumps({"type": typ, "sid": sid, "msg": msg})
        return orjson.dumps({"type": typ, "sid": sid, "seq": self._seq(sid), "msg": msg})

    def subscribed(self, ch: str) -> bytes:
        return orjson.dumps({"id": 1, "type": "subscribed", "msg": {"channel": ch, "sid": self._sid(ch)}})

    def snapshot(self, ticker: str, yes: list | None = None, no: list | None = None) -> bytes:
        return self.frame("orderbook_snapshot", "orderbook_delta", {
            "market_ticker": ticker, "market_id": "m", "yes_dollars_fp": yes or [["0.4500", "100.00"]],
            "no_dollars_fp": no or [["0.5300", "100.00"]]})

    def brti(self, value: float, src_ms: int) -> bytes:
        return self.frame("cfbenchmarks_value_5hz", "cfbenchmarks_value_5hz", {
            "index_id": "BRTI", "value_usd": f"{value:.2f}", "source_ts_ms": src_ms, "received_at": src_ms + 5, "data": ""})

    def fill(self, ticker: str, coid: str, oid: str, trade_id: str, *, side: str = "bid", px: str = "0.4500",
             count: str = "1.00", fee: str = "0.004331", post: str | None = None) -> bytes:
        m = {"trade_id": trade_id, "order_id": oid, "client_order_id": coid, "market_ticker": ticker, "is_taker": False,
             "yes_price_dollars": px, "count_fp": count, "fee_cost": fee, "ts_ms": 1790300000789,
             "outcome_side": "yes" if side == "bid" else "no", "book_side": side}
        if post is not None:
            m["post_position_fp"] = post
        return self.frame("fill", "fill", m)

    def order_group(self, gid: str, event_type: str, ts_ms: int = 1790300000999) -> bytes:
        return self.frame("order_group_updates", "order_group_updates",
                          {"event_type": event_type, "order_group_id": gid, "ts_ms": ts_ms})
