"""Async Kalshi REST client (Trade API 3.30.0) with signing, rate limiting, safe retries.

    async with KalshiRest(PROD_REST_URL, signer, KalshiRateLimiter(), on_raw=rec.write) as k:
        await k.configure_rate_limits()
        ob = await k.get_orderbook("KXBTCD-...")

Retry policy
  * Idempotent GETs are retried on HTTP 429 / 5xx and on transport errors with jittered
    exponential backoff (seeded ``rng``; ``Retry-After`` honoured on 429).
  * Writes (POST/PUT/DELETE: orders, cancels, order groups) are NEVER retried. Outcomes:
      2xx                                   -> parsed body (dict; {} for 204)
      4xx except 408/409 (incl. 429)        -> KalshiHTTPError (definitely not applied:
                                               429 is a gateway throttle before processing)
      5xx / 408 / 409 / timeout / connection
      lost after sending / 2xx w/o order_id -> UnknownOutcome (may or may not have been
                                               applied: reconcile via GET /portfolio/orders
                                               by client_order_id before acting)
      connection never established          -> NotSentError (definitely not applied)

Raw capture: every response (and every transport failure) is handed to
``on_raw(stream, recv_ns, raw_bytes)`` where raw_bytes is the JSON record
``{"method","path","params","status","body"}`` (+ "request" for writes, "error" on
transport failures). ``path`` is API-relative (no '/trade-api/v2'), ``body`` is the exact
response JSON (embedded verbatim, not re-serialized) or a string if not JSON. Streams are
'kalshi.rest.<kind>' (orderbook, trades, markets, market, series, events, fees, exchange,
candlesticks, historical, incentives, account, portfolio, orders, cfbenchmarks).
``dh.kalshi.normalize.normalize_rest_record`` turns records back into events.

Prices/counts in request bodies are fixed-point strings; build them with dh.kalshi.orders
from dh.core.actions to guarantee exact units. Timestamps in query params are Unix seconds.
"""

from __future__ import annotations

import asyncio
import random
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

import orjson

from dh.core.units import qty_to_fp
from dh.kalshi.auth import KalshiSigner, full_path
from dh.kalshi.rate_limit import KalshiRateLimiter

PROD_REST_URL = "https://external-api.kalshi.com/trade-api/v2"
PROD_REST_URL_ALT = "https://api.elections.kalshi.com/trade-api/v2"
DEMO_REST_URL = "https://external-api.demo.kalshi.co/trade-api/v2"
DEFAULT_CF_HISTORY_PATH = "/cfbenchmarks/history/values"

RawCallback = Callable[[str, int, bytes], None]
Params = Sequence[tuple[str, str]]


# ============================================================================ errors/results
class KalshiError(Exception):
    """Base class for adapter errors."""


class KalshiHTTPError(KalshiError):
    """Non-success HTTP response (definite outcome). ``code``/``message`` from ErrorResponse."""

    def __init__(self, method: str, path: str, status: int, body: Any) -> None:
        self.method, self.path, self.status, self.body = method, path, status, body
        err = body.get("error") if isinstance(body, dict) and isinstance(body.get("error"), dict) else body
        self.code = str(err.get("code", "")) if isinstance(err, dict) else ""
        self.message = str(err.get("message", "")) if isinstance(err, dict) else str(body)[:200]
        super().__init__(f"{method} {path} -> HTTP {status} {self.code} {self.message}".strip())


class TransportError(KalshiError):
    """Network-level failure."""


class NotSentError(TransportError):
    """The connection was never established: the request certainly did not reach Kalshi."""


class ResponseLostError(TransportError):
    """Timeout / disconnect after the request may have been sent: outcome unknown."""


class KalshiPaginationError(KalshiError):
    """Cursor pagination misbehaved (repeated cursor, page limit exceeded)."""


@dataclass(frozen=True, slots=True)
class UnknownOutcome:
    """A write whose effect is unknown. Reconcile (GET /portfolio/orders, match
    client_order_id) before re-sending or assuming anything."""

    method: str
    path: str
    request: Any
    reason: str
    status: int = 0
    body: Any = None


@dataclass(frozen=True, slots=True)
class HttpResponse:
    status: int
    headers: Mapping[str, str]
    body: bytes


Transport = Callable[[str, str, dict[str, str], Params, bytes | None, float], Awaitable[HttpResponse]]


class AiohttpTransport:
    """Default transport: one shared aiohttp session (created lazily inside the loop)."""

    def __init__(self, *, trust_env: bool = True, limit_per_host: int = 16) -> None:
        self._trust_env = trust_env
        self._limit = limit_per_host
        self._session: Any = None

    async def __call__(
        self, method: str, url: str, headers: dict[str, str], params: Params, data: bytes | None, timeout_s: float
    ) -> HttpResponse:
        import aiohttp

        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                trust_env=self._trust_env, connector=aiohttp.TCPConnector(limit_per_host=self._limit)
            )
        try:
            async with self._session.request(
                method,
                url,
                headers=headers,
                params=list(params),
                data=data,
                timeout=aiohttp.ClientTimeout(total=timeout_s),
            ) as resp:
                body = await resp.read()
                return HttpResponse(resp.status, dict(resp.headers), body)
        except (aiohttp.ClientConnectorError, aiohttp.ConnectionTimeoutError) as exc:
            raise NotSentError(f"{type(exc).__name__}: {exc}") from exc
        except (asyncio.TimeoutError, aiohttp.ClientError) as exc:
            raise ResponseLostError(f"{type(exc).__name__}: {exc}") from exc

    async def close(self) -> None:
        if self._session is not None and not self._session.closed:
            await self._session.close()
        self._session = None


# ============================================================================ helpers
def build_params(**kw: Any) -> list[tuple[str, str]]:
    """Query params: None dropped, bools -> 'true'/'false', lists repeated (explode)."""
    out: list[tuple[str, str]] = []
    for k, v in kw.items():
        if v is None:
            continue
        if isinstance(v, (list, tuple)):
            for x in v:
                out.append((k, _qv(x)))
        else:
            out.append((k, _qv(v)))
    return out


def _qv(v: Any) -> str:
    if isinstance(v, bool):
        return "true" if v else "false"
    return str(v)


def csv(values: Sequence[str] | str | None) -> str | None:
    """Comma-separated list param ('tickers', 'market_tickers', 'event_ticker')."""
    if values is None or isinstance(values, str):
        return values
    return ",".join(values) if values else None


def _params_dict(params: Params) -> dict[str, Any]:
    d: dict[str, Any] = {}
    for k, v in params:
        if k in d:
            d[k] = d[k] if isinstance(d[k], list) else [d[k]]
            d[k].append(v)
        else:
            d[k] = v
    return d


def _parse_json(body: bytes) -> tuple[bool, Any]:
    if not body:
        return True, None
    try:
        return True, orjson.loads(body)
    except orjson.JSONDecodeError:
        return False, None


# ============================================================================ client
@dataclass
class _Config:
    timeout_s: float = 10.0
    write_timeout_s: float = 5.0
    max_get_retries: int = 4
    backoff_base_s: float = 0.25
    backoff_max_s: float = 8.0
    cf_history_path: str = DEFAULT_CF_HISTORY_PATH
    extra_headers: dict[str, str] = field(default_factory=dict)


class KalshiRest:
    """Async REST client. All ``get_*`` return the parsed response object (dict) exactly as
    documented in openapi 3.30.0; ``iter_*`` walk cursor pagination."""

    def __init__(
        self,
        base_url: str = PROD_REST_URL,
        signer: KalshiSigner | None = None,
        limiter: KalshiRateLimiter | None = None,
        *,
        on_raw: RawCallback | None = None,
        transport: Transport | None = None,
        timeout_s: float = 10.0,
        write_timeout_s: float = 5.0,
        max_get_retries: int = 4,
        backoff_base_s: float = 0.25,
        backoff_max_s: float = 8.0,
        rng: random.Random | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        clock_ns: Callable[[], int] = time.time_ns,
        cf_history_path: str = DEFAULT_CF_HISTORY_PATH,
        trust_env: bool = True,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.signer = signer
        self.limiter = limiter
        self.on_raw = on_raw
        self._own_transport = transport is None
        self._transport: Transport = transport or AiohttpTransport(trust_env=trust_env)
        self.cfg = _Config(timeout_s, write_timeout_s, max_get_retries, backoff_base_s, backoff_max_s, cf_history_path)
        self._rng = rng or random.Random(0)
        self._sleep = sleep
        self._clock_ns = clock_ns
        self.stats: dict[str, int] = {"requests": 0, "retries": 0, "http_429": 0, "unknown_outcomes": 0}

    async def __aenter__(self) -> KalshiRest:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()

    async def close(self) -> None:
        if self._own_transport and isinstance(self._transport, AiohttpTransport):
            await self._transport.close()

    # ------------------------------------------------------------------ core
    def backoff_delay(self, attempt: int) -> float:
        """Jittered exponential backoff in seconds for retry number `attempt` (0-based)."""
        base = min(self.cfg.backoff_max_s, self.cfg.backoff_base_s * (2**attempt))
        return base * (0.5 + 0.5 * self._rng.random())

    def _record(
        self,
        stream: str,
        method: str,
        path: str,
        params: Params,
        status: int,
        body: bytes | None,
        request: Any = None,
        error: str | None = None,
    ) -> None:
        if self.on_raw is None:
            return
        rec: dict[str, Any] = {"method": method, "path": path, "params": _params_dict(params), "status": status}
        if body is None or body == b"":
            rec["body"] = None
        else:
            ok, _ = _parse_json(body)
            rec["body"] = orjson.Fragment(body) if ok else body.decode("utf-8", "replace")
        if request is not None:
            rec["request"] = request
        if error is not None:
            rec["error"] = error
        self.on_raw(stream, self._clock_ns(), orjson.dumps(rec))

    async def _send(
        self,
        method: str,
        path: str,
        params: Params,
        json_body: Any,
        stream: str,
        timeout_s: float,
        n_items: int = 1,
    ) -> HttpResponse:
        if self.limiter is not None:
            await self.limiter.acquire(method, path, n_items)
        headers = {"Accept": "application/json", **self.cfg.extra_headers}
        data = None
        if json_body is not None:
            data = orjson.dumps(json_body)
            headers["Content-Type"] = "application/json"
        if self.signer is not None:
            headers.update(self.signer.headers(method, full_path(self.base_url, path)))
        self.stats["requests"] += 1
        try:
            resp = await self._transport(method, self.base_url + path, headers, params, data, timeout_s)
        except TransportError as exc:
            self._record(stream, method, path, params, 0, None, json_body, f"{type(exc).__name__}: {exc}")
            raise
        if resp.status == 429:
            self.stats["http_429"] += 1
            if self.limiter is not None:
                self.limiter.on_429(method)
        self._record(stream, method, path, params, resp.status, resp.body, json_body)
        return resp

    async def get_any(self, path: str, params: Params = (), *, stream: str = "kalshi.rest") -> Any:
        """GET with retries (429/5xx/transport); returns parsed JSON of any type."""
        attempt = 0
        while True:
            try:
                resp = await self._send("GET", path, params, None, stream, self.cfg.timeout_s)
            except TransportError:
                if attempt >= self.cfg.max_get_retries:
                    raise
                await self._sleep(self.backoff_delay(attempt))
                attempt += 1
                self.stats["retries"] += 1
                continue
            ok, parsed = _parse_json(resp.body)
            if resp.status == 429 or resp.status >= 500:
                if attempt >= self.cfg.max_get_retries:
                    raise KalshiHTTPError("GET", path, resp.status, parsed if ok else resp.body[:500])
                delay = self.backoff_delay(attempt)
                ra = _retry_after_s(resp.headers)
                if ra is not None:
                    delay = max(delay, min(ra, 60.0))
                await self._sleep(delay)
                attempt += 1
                self.stats["retries"] += 1
                continue
            if not 200 <= resp.status < 300:
                raise KalshiHTTPError("GET", path, resp.status, parsed if ok else resp.body[:500])
            if not ok:
                raise KalshiHTTPError("GET", path, resp.status, {"error": {"code": "invalid_json", "message": "unparseable body"}})
            return parsed

    async def get(self, path: str, params: Params = (), *, stream: str = "kalshi.rest") -> dict[str, Any]:
        """GET returning a JSON object (dict)."""
        body = await self.get_any(path, params, stream=stream)
        if body is None:
            return {}
        if not isinstance(body, dict):
            raise KalshiHTTPError("GET", path, 200, {"error": {"code": "unexpected_shape", "message": type(body).__name__}})
        return body

    async def write(
        self,
        method: str,
        path: str,
        *,
        json_body: Any = None,
        params: Params = (),
        stream: str = "kalshi.rest.orders",
        n_items: int = 1,
    ) -> dict[str, Any] | UnknownOutcome:
        """Non-idempotent request, never retried. See module docstring for outcomes."""
        try:
            resp = await self._send(method, path, params, json_body, stream, self.cfg.write_timeout_s, n_items)
        except NotSentError:
            raise
        except TransportError as exc:
            self.stats["unknown_outcomes"] += 1
            return UnknownOutcome(method, path, json_body, f"{type(exc).__name__}: {exc}")
        ok, parsed = _parse_json(resp.body)
        if resp.status >= 500 or resp.status in (408, 409):
            self.stats["unknown_outcomes"] += 1
            return UnknownOutcome(method, path, json_body, f"HTTP {resp.status}", resp.status, parsed if ok else None)
        if not 200 <= resp.status < 300:
            raise KalshiHTTPError(method, path, resp.status, parsed if ok else resp.body[:500])
        if not ok:
            self.stats["unknown_outcomes"] += 1
            return UnknownOutcome(method, path, json_body, "unparseable success body", resp.status)
        return parsed if isinstance(parsed, dict) else {}

    async def paginate(
        self,
        path: str,
        params: Mapping[str, Any],
        key: str,
        *,
        stream: str,
        cursor_field: str = "cursor",
        max_pages: int | None = None,
    ) -> AsyncIterator[dict[str, Any]]:
        """Yield items of `key` across cursor pages. Stops on an empty cursor or an empty
        page; raises KalshiPaginationError on a repeated cursor or > max_pages pages."""
        cursor: str | None = None
        seen: set[str] = set()
        pages = 0
        while True:
            q = dict(params)
            if cursor:
                q["cursor"] = cursor
            body = await self.get(path, build_params(**q), stream=stream)
            items = body.get(key) or []
            for it in items:
                yield it
            pages += 1
            nxt = body.get(cursor_field) or ""
            if not nxt or not items:
                return
            if nxt in seen:
                raise KalshiPaginationError(f"{path}: cursor repeated after {pages} pages")
            if max_pages is not None and pages >= max_pages:
                raise KalshiPaginationError(f"{path}: more than {max_pages} pages")
            seen.add(nxt)
            cursor = nxt

    async def collect(self, it: AsyncIterator[dict[str, Any]]) -> list[dict[str, Any]]:
        """Drain an async iterator into a list."""
        return [x async for x in it]

    # ------------------------------------------------------------------ exchange
    async def get_exchange_status(self) -> dict[str, Any]:
        return await self.get("/exchange/status", stream="kalshi.rest.exchange")

    async def get_exchange_schedule(self) -> dict[str, Any]:
        return await self.get("/exchange/schedule", stream="kalshi.rest.exchange")

    async def get_user_data_timestamp(self) -> dict[str, Any]:
        return await self.get("/exchange/user_data_timestamp", stream="kalshi.rest.exchange")

    # ------------------------------------------------------------------ series / events
    async def get_series(self, series_ticker: str, *, include_volume: bool | None = None) -> dict[str, Any]:
        """GET /series/{t} -> {'series': Series} (fee_type, fee_multiplier, ...)."""
        return await self.get(f"/series/{series_ticker}", build_params(include_volume=include_volume), stream="kalshi.rest.series")

    async def get_series_list(
        self,
        *,
        category: str | None = None,
        tags: str | None = None,
        include_product_metadata: bool | None = None,
        include_volume: bool | None = None,
        min_updated_ts: int | None = None,
    ) -> dict[str, Any]:
        return await self.get(
            "/series",
            build_params(
                category=category,
                tags=tags,
                include_product_metadata=include_product_metadata,
                include_volume=include_volume,
                min_updated_ts=min_updated_ts,
            ),
            stream="kalshi.rest.series",
        )

    async def get_series_fee_changes(self, series_ticker: str | None = None, *, show_historical: bool = False) -> dict[str, Any]:
        """GET /series/fee_changes -> {'series_fee_change_arr': [SeriesFeeChange]}."""
        return await self.get(
            "/series/fee_changes",
            build_params(series_ticker=series_ticker, show_historical=show_historical),
            stream="kalshi.rest.fees",
        )

    async def get_event(self, event_ticker: str, *, with_nested_markets: bool = False) -> dict[str, Any]:
        """GET /events/{e} -> {'event': EventData, 'markets': [Market]}."""
        return await self.get(
            f"/events/{event_ticker}", build_params(with_nested_markets=with_nested_markets), stream="kalshi.rest.events"
        )

    async def get_event_metadata(self, event_ticker: str) -> dict[str, Any]:
        return await self.get(f"/events/{event_ticker}/metadata", stream="kalshi.rest.events")

    def _events_params(self, **kw: Any) -> dict[str, Any]:
        if "tickers" in kw:
            kw["tickers"] = csv(kw["tickers"])
        return kw

    async def get_events(
        self,
        *,
        limit: int | None = None,
        cursor: str | None = None,
        with_nested_markets: bool | None = None,
        with_milestones: bool | None = None,
        status: str | None = None,
        series_ticker: str | None = None,
        tickers: Sequence[str] | str | None = None,
        min_close_ts: int | None = None,
        min_updated_ts: int | None = None,
    ) -> dict[str, Any]:
        """One page of GET /events -> {'events': [EventData], 'cursor': str}."""
        p = self._events_params(
            limit=limit,
            cursor=cursor,
            with_nested_markets=with_nested_markets,
            with_milestones=with_milestones,
            status=status,
            series_ticker=series_ticker,
            tickers=tickers,
            min_close_ts=min_close_ts,
            min_updated_ts=min_updated_ts,
        )
        return await self.get("/events", build_params(**p), stream="kalshi.rest.events")

    def iter_events(self, *, limit: int = 200, max_pages: int | None = None, **filters: Any) -> AsyncIterator[dict[str, Any]]:
        """All events matching filters (same keywords as get_events)."""
        p = self._events_params(limit=limit, **filters)
        return self.paginate("/events", p, "events", stream="kalshi.rest.events", max_pages=max_pages)

    async def get_event_fee_changes(
        self, event_ticker: str | None = None, *, limit: int | None = None, cursor: str | None = None
    ) -> dict[str, Any]:
        """GET /events/fee_changes -> {'event_fee_changes': [EventFeeChange], 'cursor'}."""
        return await self.get(
            "/events/fee_changes",
            build_params(event_ticker=event_ticker, limit=limit, cursor=cursor),
            stream="kalshi.rest.fees",
        )

    def iter_event_fee_changes(self, event_ticker: str | None = None, *, limit: int = 1000) -> AsyncIterator[dict[str, Any]]:
        return self.paginate(
            "/events/fee_changes",
            {"event_ticker": event_ticker, "limit": limit},
            "event_fee_changes",
            stream="kalshi.rest.fees",
        )

    # ------------------------------------------------------------------ markets
    def _markets_params(self, **kw: Any) -> dict[str, Any]:
        if "tickers" in kw:
            kw["tickers"] = csv(kw["tickers"])
        return kw

    async def get_markets(
        self,
        *,
        limit: int | None = None,
        cursor: str | None = None,
        event_ticker: str | None = None,
        series_ticker: str | None = None,
        min_created_ts: int | None = None,
        max_created_ts: int | None = None,
        min_updated_ts: int | None = None,
        max_close_ts: int | None = None,
        min_close_ts: int | None = None,
        min_settled_ts: int | None = None,
        max_settled_ts: int | None = None,
        status: str | None = None,
        tickers: Sequence[str] | str | None = None,
        mve_filter: str | None = None,
    ) -> dict[str, Any]:
        """One page of GET /markets -> {'markets': [Market], 'cursor'}. Note the spec's
        filter-compatibility table (timestamp filters vs status)."""
        p = self._markets_params(
            limit=limit,
            cursor=cursor,
            event_ticker=event_ticker,
            series_ticker=series_ticker,
            min_created_ts=min_created_ts,
            max_created_ts=max_created_ts,
            min_updated_ts=min_updated_ts,
            max_close_ts=max_close_ts,
            min_close_ts=min_close_ts,
            min_settled_ts=min_settled_ts,
            max_settled_ts=max_settled_ts,
            status=status,
            tickers=tickers,
            mve_filter=mve_filter,
        )
        return await self.get("/markets", build_params(**p), stream="kalshi.rest.markets")

    def iter_markets(self, *, limit: int = 1000, max_pages: int | None = None, **filters: Any) -> AsyncIterator[dict[str, Any]]:
        p = self._markets_params(limit=limit, **filters)
        return self.paginate("/markets", p, "markets", stream="kalshi.rest.markets", max_pages=max_pages)

    async def get_market(self, ticker: str) -> dict[str, Any]:
        """GET /markets/{t} -> {'market': Market}."""
        return await self.get(f"/markets/{ticker}", stream="kalshi.rest.market")

    async def get_orderbook(self, ticker: str, *, depth: int | None = None) -> dict[str, Any]:
        """GET /markets/{t}/orderbook -> {'orderbook_fp': {'yes_dollars': [[px, qty]], 'no_dollars': ...}}.
        depth 0/None = all levels."""
        return await self.get(f"/markets/{ticker}/orderbook", build_params(depth=depth), stream="kalshi.rest.orderbook")

    async def get_orderbooks(self, tickers: Sequence[str]) -> dict[str, Any]:
        """GET /markets/orderbooks?tickers=A&tickers=B (1..100) -> {'orderbooks': [MarketOrderbookFp]}."""
        if not 1 <= len(tickers) <= 100:
            raise ValueError("get_orderbooks takes 1..100 tickers")
        return await self.get("/markets/orderbooks", build_params(tickers=list(tickers)), stream="kalshi.rest.orderbook")

    async def get_trades(
        self,
        *,
        ticker: str | None = None,
        min_ts: int | None = None,
        max_ts: int | None = None,
        limit: int | None = None,
        cursor: str | None = None,
        is_block_trade: bool | None = None,
        historical: bool = False,
    ) -> dict[str, Any]:
        """One page of GET /markets/trades (or /historical/trades) -> {'trades', 'cursor'}.
        min_ts/max_ts: Unix seconds."""
        path = "/historical/trades" if historical else "/markets/trades"
        return await self.get(
            path,
            build_params(ticker=ticker, min_ts=min_ts, max_ts=max_ts, limit=limit, cursor=cursor, is_block_trade=is_block_trade),
            stream="kalshi.rest.trades",
        )

    async def iter_trades(
        self,
        *,
        ticker: str | None = None,
        min_ts: int | None = None,
        max_ts: int | None = None,
        window_s: int | None = None,
        historical: bool = False,
        limit: int = 1000,
        is_block_trade: bool | None = None,
    ) -> AsyncIterator[dict[str, Any]]:
        """All trades in [min_ts, max_ts] (Unix s), de-duplicated by trade_id.

        With window_s, the range is split into windows overlapping by 1 s (robust to either
        inclusive or exclusive min_ts/max_ts semantics), each walked with the cursor; this
        keeps cursors shallow on very active ranges. Order: server order within windows.
        """
        path = "/historical/trades" if historical else "/markets/trades"
        base = {"ticker": ticker, "limit": limit, "is_block_trade": is_block_trade}
        if window_s is None or min_ts is None or max_ts is None:
            seen: set[str] = set()
            async for t in self.paginate(path, {**base, "min_ts": min_ts, "max_ts": max_ts}, "trades", stream="kalshi.rest.trades"):
                tid = str(t.get("trade_id"))
                if tid not in seen:
                    seen.add(tid)
                    yield t
            return
        if window_s < 2:
            raise ValueError("window_s must be >= 2 seconds")
        # ids yielded in the last two windows (audit m8): with window_s >= 2 and a 1 s overlap a
        # trade can fall in at most three consecutive windows (e.g. t=102 in [100,102],
        # [101,103], [102,104] when window_s=2), so memory stays bounded
        older: set[str] = set()
        prev: set[str] = set()
        a = min_ts
        while True:
            b = min(max_ts, a + window_s)
            cur: set[str] = set()
            async for t in self.paginate(path, {**base, "min_ts": a, "max_ts": b}, "trades", stream="kalshi.rest.trades"):
                tid = str(t.get("trade_id"))
                if tid in prev or tid in cur or tid in older:
                    continue
                cur.add(tid)
                yield t
            if b >= max_ts:
                return
            older, prev = prev, cur
            a = b - 1

    async def get_market_candlesticks(
        self,
        series_ticker: str,
        ticker: str,
        start_ts: int,
        end_ts: int,
        period_interval: int = 1,
        *,
        include_latest_before_start: bool | None = None,
    ) -> dict[str, Any]:
        """GET /series/{s}/markets/{t}/candlesticks (period 1|60|1440 min; ts Unix s)."""
        return await self.get(
            f"/series/{series_ticker}/markets/{ticker}/candlesticks",
            build_params(
                start_ts=start_ts,
                end_ts=end_ts,
                period_interval=period_interval,
                include_latest_before_start=include_latest_before_start,
            ),
            stream="kalshi.rest.candlesticks",
        )

    async def get_batch_candlesticks(
        self, market_tickers: Sequence[str], start_ts: int, end_ts: int, period_interval: int = 1
    ) -> dict[str, Any]:
        """GET /markets/candlesticks (<= 100 tickers, <= 10,000 candles total)."""
        return await self.get(
            "/markets/candlesticks",
            build_params(market_tickers=csv(list(market_tickers)), start_ts=start_ts, end_ts=end_ts, period_interval=period_interval),
            stream="kalshi.rest.candlesticks",
        )

    async def get_event_candlesticks(
        self, series_ticker: str, event_ticker: str, start_ts: int, end_ts: int, period_interval: int = 1
    ) -> dict[str, Any]:
        return await self.get(
            f"/series/{series_ticker}/events/{event_ticker}/candlesticks",
            build_params(start_ts=start_ts, end_ts=end_ts, period_interval=period_interval),
            stream="kalshi.rest.candlesticks",
        )

    # ------------------------------------------------------------------ historical
    async def get_historical_cutoff(self) -> dict[str, Any]:
        """GET /historical/cutoff -> market_settled_ts, trades_created_ts, orders_updated_ts (RFC3339)."""
        return await self.get("/historical/cutoff", stream="kalshi.rest.historical")

    async def get_historical_markets(
        self,
        *,
        limit: int | None = None,
        cursor: str | None = None,
        tickers: Sequence[str] | str | None = None,
        event_ticker: str | None = None,
        series_ticker: str | None = None,
        mve_filter: str | None = None,
    ) -> dict[str, Any]:
        """One page of GET /historical/markets (filters are mutually exclusive)."""
        return await self.get(
            "/historical/markets",
            build_params(
                limit=limit,
                cursor=cursor,
                tickers=csv(tickers),
                event_ticker=event_ticker,
                series_ticker=series_ticker,
                mve_filter=mve_filter,
            ),
            stream="kalshi.rest.markets",
        )

    def iter_historical_markets(self, *, limit: int = 1000, max_pages: int | None = None, **filters: Any) -> AsyncIterator[dict[str, Any]]:
        if "tickers" in filters:
            filters["tickers"] = csv(filters["tickers"])
        return self.paginate(
            "/historical/markets", {"limit": limit, **filters}, "markets", stream="kalshi.rest.markets", max_pages=max_pages
        )

    async def get_historical_market(self, ticker: str) -> dict[str, Any]:
        return await self.get(f"/historical/markets/{ticker}", stream="kalshi.rest.market")

    async def get_historical_trades(self, **kw: Any) -> dict[str, Any]:
        return await self.get_trades(historical=True, **kw)

    def iter_historical_trades(self, **kw: Any) -> AsyncIterator[dict[str, Any]]:
        return self.iter_trades(historical=True, **kw)

    async def get_historical_candlesticks(self, ticker: str, start_ts: int, end_ts: int, period_interval: int = 1) -> dict[str, Any]:
        """GET /historical/markets/{t}/candlesticks (MarketCandlestickHistorical shape)."""
        return await self.get(
            f"/historical/markets/{ticker}/candlesticks",
            build_params(start_ts=start_ts, end_ts=end_ts, period_interval=period_interval),
            stream="kalshi.rest.candlesticks",
        )

    def iter_historical_fills(self, *, limit: int = 1000, **filters: Any) -> AsyncIterator[dict[str, Any]]:
        return self.paginate("/historical/fills", {"limit": limit, **filters}, "fills", stream="kalshi.rest.portfolio")

    def iter_historical_orders(self, *, limit: int = 1000, **filters: Any) -> AsyncIterator[dict[str, Any]]:
        return self.paginate("/historical/orders", {"limit": limit, **filters}, "orders", stream="kalshi.rest.portfolio")

    # ------------------------------------------------------------------ incentives / account
    async def get_incentive_programs(
        self,
        *,
        status: str | None = None,
        type: str | None = None,  # noqa: A002 - spec parameter name
        incentive_description: str | None = None,
        limit: int | None = None,
        cursor: str | None = None,
    ) -> dict[str, Any]:
        """GET /incentive_programs -> {'incentive_programs': [...], 'next_cursor'}."""
        return await self.get(
            "/incentive_programs",
            build_params(status=status, type=type, incentive_description=incentive_description, limit=limit, cursor=cursor),
            stream="kalshi.rest.incentives",
        )

    def iter_incentive_programs(self, *, status: str = "all", type: str = "all", limit: int = 1000) -> AsyncIterator[dict[str, Any]]:  # noqa: A002
        return self.paginate(
            "/incentive_programs",
            {"status": status, "type": type, "limit": limit},
            "incentive_programs",
            stream="kalshi.rest.incentives",
            cursor_field="next_cursor",
        )

    async def get_account_limits(self) -> dict[str, Any]:
        """GET /account/limits -> usage_tier, read/write BucketLimit, grants."""
        return await self.get("/account/limits", stream="kalshi.rest.account")

    async def get_endpoint_costs(self) -> dict[str, Any]:
        """GET /account/endpoint_costs -> default_cost, endpoint_costs[]."""
        return await self.get("/account/endpoint_costs", stream="kalshi.rest.account")

    async def configure_rate_limits(self) -> dict[str, Any]:
        """Load account limits + endpoint costs into the limiter (creates one if absent)."""
        if self.limiter is None:
            self.limiter = KalshiRateLimiter()
        limits = await self.get_account_limits()
        self.limiter.update_from_limits(limits)
        costs: dict[str, Any] = {}
        try:
            costs = await self.get_endpoint_costs()
            self.limiter.update_endpoint_costs(costs)
        except KalshiHTTPError:
            pass  # documented defaults stay in force
        return {"limits": limits, "endpoint_costs": costs}

    # ------------------------------------------------------------------ portfolio (reads)
    async def get_balance(self, *, subaccount: int | None = None, exchange_index: int | None = None) -> dict[str, Any]:
        return await self.get(
            "/portfolio/balance", build_params(subaccount=subaccount, exchange_index=exchange_index), stream="kalshi.rest.portfolio"
        )

    async def get_positions(self, **filters: Any) -> dict[str, Any]:
        """One page of GET /portfolio/positions (cursor, limit, count_filter, ticker, event_ticker,
        subaccount, exchange_index) -> market_positions, event_positions, cursor."""
        return await self.get("/portfolio/positions", build_params(**filters), stream="kalshi.rest.portfolio")

    async def get_all_positions(self, **filters: Any) -> dict[str, list[dict[str, Any]]]:
        """Walk every positions page; returns {'market_positions': [...], 'event_positions': [...]}."""
        out: dict[str, list[dict[str, Any]]] = {"market_positions": [], "event_positions": []}
        cursor: str | None = None
        seen: set[str] = set()
        while True:
            q = dict(filters)
            q.setdefault("limit", 1000)
            if cursor:
                q["cursor"] = cursor
            body = await self.get_positions(**q)
            mp = body.get("market_positions") or []
            ep = body.get("event_positions") or []
            out["market_positions"].extend(mp)
            out["event_positions"].extend(ep)
            nxt = body.get("cursor") or ""
            if not nxt or (not mp and not ep):
                return out
            if nxt in seen:
                raise KalshiPaginationError("/portfolio/positions: cursor repeated")
            seen.add(nxt)
            cursor = nxt

    async def get_fills(self, **filters: Any) -> dict[str, Any]:
        """One page of GET /portfolio/fills (ticker, order_id, min_ts, max_ts, limit, cursor ...)."""
        return await self.get("/portfolio/fills", build_params(**filters), stream="kalshi.rest.portfolio")

    def iter_fills(self, *, limit: int = 1000, **filters: Any) -> AsyncIterator[dict[str, Any]]:
        return self.paginate("/portfolio/fills", {"limit": limit, **filters}, "fills", stream="kalshi.rest.portfolio")

    async def get_orders(self, **filters: Any) -> dict[str, Any]:
        """One page of GET /portfolio/orders (ticker, event_ticker (csv), min_ts, max_ts, status,
        limit, cursor, subaccount, exchange_index)."""
        if "event_ticker" in filters:
            filters["event_ticker"] = csv(filters["event_ticker"])
        return await self.get("/portfolio/orders", build_params(**filters), stream="kalshi.rest.portfolio")

    def iter_orders(self, *, limit: int = 1000, **filters: Any) -> AsyncIterator[dict[str, Any]]:
        if "event_ticker" in filters:
            filters["event_ticker"] = csv(filters["event_ticker"])
        return self.paginate("/portfolio/orders", {"limit": limit, **filters}, "orders", stream="kalshi.rest.portfolio")

    async def get_order(self, order_id: str) -> dict[str, Any]:
        """GET /portfolio/orders/{id} -> {'order': Order} (2 tokens)."""
        return await self.get(f"/portfolio/orders/{order_id}", stream="kalshi.rest.portfolio")

    async def find_order_by_client_id(self, client_order_id: str, *, ticker: str | None = None, min_ts: int | None = None) -> dict[str, Any] | None:
        """Reconcile an UnknownOutcome: scan GET /portfolio/orders for client_order_id."""
        async for o in self.iter_orders(ticker=ticker, min_ts=min_ts):
            if o.get("client_order_id") == client_order_id:
                return o
        return None

    async def get_queue_positions(
        self, *, market_tickers: Sequence[str] | str | None = None, event_ticker: str | None = None, subaccount: int | None = None
    ) -> dict[str, Any]:
        """GET /portfolio/orders/queue_positions -> {'queue_positions': [{order_id, market_ticker, queue_position_fp}]}."""
        return await self.get(
            "/portfolio/orders/queue_positions",
            build_params(market_tickers=csv(market_tickers), event_ticker=event_ticker, subaccount=subaccount),
            stream="kalshi.rest.portfolio",
        )

    async def get_order_queue_position(self, order_id: str) -> dict[str, Any]:
        return await self.get(f"/portfolio/orders/{order_id}/queue_position", stream="kalshi.rest.portfolio")

    async def get_settlements(self, **filters: Any) -> dict[str, Any]:
        return await self.get("/portfolio/settlements", build_params(**filters), stream="kalshi.rest.portfolio")

    def iter_settlements(self, *, limit: int = 1000, **filters: Any) -> AsyncIterator[dict[str, Any]]:
        return self.paginate("/portfolio/settlements", {"limit": limit, **filters}, "settlements", stream="kalshi.rest.portfolio")

    # ------------------------------------------------------------------ orders V2 (writes)
    async def create_order(self, body: dict[str, Any]) -> dict[str, Any] | UnknownOutcome:
        """POST /portfolio/events/orders (CreateOrderV2Request; see dh.kalshi.orders.place_order_body).
        Returns CreateOrderV2Response {order_id, client_order_id, fill_count, remaining_count, ts_ms}."""
        res = await self.write("POST", "/portfolio/events/orders", json_body=body)
        if isinstance(res, dict) and not res.get("order_id"):
            self.stats["unknown_outcomes"] += 1
            return UnknownOutcome("POST", "/portfolio/events/orders", body, "success response without order_id", 0, res)
        return res

    async def batch_create_orders(self, orders: list[dict[str, Any]]) -> dict[str, Any] | UnknownOutcome:
        """POST /portfolio/events/orders/batched -> {'orders': [per-order result or error]}."""
        return await self.write(
            "POST", "/portfolio/events/orders/batched", json_body={"orders": orders}, n_items=max(1, len(orders))
        )

    async def amend_order(self, order_id: str, body: dict[str, Any], *, subaccount: int | None = None) -> dict[str, Any] | UnknownOutcome:
        """POST /portfolio/events/orders/{id}/amend (AmendOrderV2Request; count = filled + new remaining)."""
        return await self.write(
            "POST", f"/portfolio/events/orders/{order_id}/amend", json_body=body, params=build_params(subaccount=subaccount)
        )

    async def decrease_order(
        self,
        order_id: str,
        *,
        reduce_by: int | None = None,
        reduce_to: int | None = None,
        market_ticker: str | None = None,
        exchange_index: int | None = None,
        subaccount: int | None = None,
    ) -> dict[str, Any] | UnknownOutcome:
        """POST /portfolio/events/orders/{id}/decrease. reduce_by / reduce_to in Qty units
        (exactly one)."""
        if (reduce_by is None) == (reduce_to is None):
            raise ValueError("exactly one of reduce_by / reduce_to")
        body: dict[str, Any] = {}
        if reduce_by is not None:
            body["reduce_by"] = qty_to_fp(reduce_by)
        if reduce_to is not None:
            body["reduce_to"] = qty_to_fp(reduce_to)
        if market_ticker:
            body["market_ticker"] = market_ticker
        if exchange_index is not None:
            body["exchange_index"] = exchange_index
        return await self.write(
            "POST", f"/portfolio/events/orders/{order_id}/decrease", json_body=body, params=build_params(subaccount=subaccount)
        )

    async def cancel_order(
        self,
        order_id: str,
        *,
        market_ticker: str | None = None,
        exchange_index: int | None = None,
        subaccount: int | None = None,
    ) -> dict[str, Any] | UnknownOutcome:
        """DELETE /portfolio/events/orders/{id} (pass market_ticker for shard auto-routing)
        -> CancelOrderV2Response {order_id, client_order_id, reduced_by, ts_ms}."""
        return await self.write(
            "DELETE",
            f"/portfolio/events/orders/{order_id}",
            params=build_params(subaccount=subaccount, exchange_index=exchange_index, market_ticker=market_ticker),
        )

    async def batch_cancel_orders(self, orders: list[dict[str, Any]]) -> dict[str, Any] | UnknownOutcome:
        """DELETE /portfolio/events/orders/batched with [{order_id, market_ticker?, ...}]."""
        return await self.write(
            "DELETE", "/portfolio/events/orders/batched", json_body={"orders": orders}, n_items=max(1, len(orders))
        )

    async def cancel_all_orders(self, *, subaccount: int | None = None) -> dict[str, Any] | UnknownOutcome:
        """DELETE /portfolio/events/orders (all resting event-market orders; 204 -> {})."""
        return await self.write("DELETE", "/portfolio/events/orders", params=build_params(subaccount=subaccount))

    # ------------------------------------------------------------------ order groups
    async def create_order_group(
        self, contracts_limit: int, *, subaccount: int | None = None, exchange_index: int | None = None
    ) -> dict[str, Any] | UnknownOutcome:
        """POST /portfolio/order_groups/create; contracts_limit in Qty units over a rolling 15 s."""
        body: dict[str, Any] = {"contracts_limit_fp": qty_to_fp(contracts_limit)}
        if subaccount is not None:
            body["subaccount"] = subaccount
        if exchange_index is not None:
            body["exchange_index"] = exchange_index
        return await self.write("POST", "/portfolio/order_groups/create", json_body=body)

    async def get_order_groups(self, *, subaccount: int | None = None) -> dict[str, Any]:
        return await self.get("/portfolio/order_groups", build_params(subaccount=subaccount), stream="kalshi.rest.orders")

    async def get_order_group(self, order_group_id: str, *, subaccount: int | None = None) -> dict[str, Any]:
        return await self.get(
            f"/portfolio/order_groups/{order_group_id}", build_params(subaccount=subaccount), stream="kalshi.rest.orders"
        )

    async def reset_order_group(
        self, order_group_id: str, *, subaccount: int | None = None, exchange_index: int | None = None
    ) -> dict[str, Any] | UnknownOutcome:
        return await self.write(
            "PUT",
            f"/portfolio/order_groups/{order_group_id}/reset",
            json_body={},
            params=build_params(subaccount=subaccount, exchange_index=exchange_index),
        )

    async def trigger_order_group(
        self, order_group_id: str, *, subaccount: int | None = None, exchange_index: int | None = None
    ) -> dict[str, Any] | UnknownOutcome:
        return await self.write(
            "PUT",
            f"/portfolio/order_groups/{order_group_id}/trigger",
            json_body={},
            params=build_params(subaccount=subaccount, exchange_index=exchange_index),
        )

    async def update_order_group_limit(
        self, order_group_id: str, contracts_limit: int, *, subaccount: int | None = None, exchange_index: int | None = None
    ) -> dict[str, Any] | UnknownOutcome:
        """PUT /portfolio/order_groups/{id}/limit; contracts_limit in Qty units."""
        return await self.write(
            "PUT",
            f"/portfolio/order_groups/{order_group_id}/limit",
            json_body={"contracts_limit_fp": qty_to_fp(contracts_limit)},
            params=build_params(subaccount=subaccount, exchange_index=exchange_index),
        )

    async def delete_order_group(
        self, order_group_id: str, *, subaccount: int | None = None, exchange_index: int | None = None
    ) -> dict[str, Any] | UnknownOutcome:
        return await self.write(
            "DELETE",
            f"/portfolio/order_groups/{order_group_id}",
            params=build_params(subaccount=subaccount, exchange_index=exchange_index),
        )

    # ------------------------------------------------------------------ CF Benchmarks passthrough
    async def get_cfbenchmarks_history(
        self,
        index_id: str = "BRTI",
        *,
        timespan: str | int | None = None,
        timestamp: str | int | None = None,
        extra_params: Mapping[str, Any] | None = None,
    ) -> Any:
        """GET {cf_history_path}?id=...&timespan=...&timestamp=... (authenticated passthrough,
        documented outside openapi). Returns the parsed body AS IS (shape not specified —
        parse with normalize.cf_history_to_ticks); the exact bytes go to on_raw."""
        q: dict[str, Any] = {"id": index_id, "timespan": timespan, "timestamp": timestamp}
        if extra_params:
            q.update(extra_params)
        return await self.get_any(self.cfg.cf_history_path, build_params(**q), stream="kalshi.rest.cfbenchmarks")


def _retry_after_s(headers: Mapping[str, str]) -> float | None:
    for k, v in headers.items():
        if k.lower() == "retry-after":
            try:
                return max(0.0, float(v))
            except ValueError:
                return None
    return None
