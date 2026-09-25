from __future__ import annotations

import asyncio
import base64
import random
from typing import Any

import orjson
import pytest
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding

from dh.kalshi.auth import KalshiSigner
from dh.kalshi.rate_limit import BucketLimit, KalshiRateLimiter
from dh.kalshi.rest import (
    AiohttpTransport,
    HttpResponse,
    KalshiHTTPError,
    KalshiPaginationError,
    KalshiRest,
    NotSentError,
    ResponseLostError,
    UnknownOutcome,
    build_params,
)

from . import samples as S

BASE = "https://example.test/trade-api/v2"


def resp(status: int, body: Any = None, headers: dict | None = None) -> HttpResponse:
    raw = b"" if body is None else (body if isinstance(body, bytes) else orjson.dumps(body))
    return HttpResponse(status, headers or {}, raw)


class FakeTransport:
    def __init__(self, *responses: Any) -> None:
        self.queue = list(responses)
        self.calls: list[dict[str, Any]] = []

    async def __call__(self, method, url, headers, params, data, timeout_s):
        self.calls.append({"method": method, "url": url, "headers": dict(headers), "params": list(params),
                           "data": data, "timeout": timeout_s})
        r = self.queue.pop(0)
        if callable(r) and not isinstance(r, HttpResponse):
            r = r(self.calls[-1])
        if isinstance(r, Exception):
            raise r
        return r


class Sleeps:
    def __init__(self) -> None:
        self.calls: list[float] = []

    async def __call__(self, dt: float) -> None:
        self.calls.append(dt)


def client(transport, **kw) -> tuple[KalshiRest, Sleeps, list]:
    sleeps = Sleeps()
    raw: list = []
    k = KalshiRest(BASE, transport=transport, sleep=sleeps, rng=random.Random(1), clock_ns=lambda: 777,
                   on_raw=lambda s, t, b: raw.append((s, t, b)), **kw)
    return k, sleeps, raw


async def test_signed_request_path_and_params(rsa_key, rsa_pem):
    t = FakeTransport(resp(200, S.ORDERBOOK_BODY))
    k, _, raw = client(t, signer=KalshiSigner("kid", rsa_pem))
    body = await k.get_orderbook("KXBTCD-X", depth=5)
    assert body == S.ORDERBOOK_BODY
    call = t.calls[0]
    assert call["method"] == "GET" and call["url"] == BASE + "/markets/KXBTCD-X/orderbook"
    assert call["params"] == [("depth", "5")]
    h = call["headers"]
    msg = f"{h['KALSHI-ACCESS-TIMESTAMP']}GET/trade-api/v2/markets/KXBTCD-X/orderbook".encode()
    rsa_key.public_key().verify(base64.b64decode(h["KALSHI-ACCESS-SIGNATURE"]), msg,
                                padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=32), hashes.SHA256())
    stream, recv_ns, rec_bytes = raw[0]
    rec = orjson.loads(rec_bytes)
    assert (stream, recv_ns) == ("kalshi.rest.orderbook", 777)
    assert rec == {"method": "GET", "path": "/markets/KXBTCD-X/orderbook", "params": {"depth": "5"},
                   "status": 200, "body": S.ORDERBOOK_BODY}
    assert orjson.dumps(S.ORDERBOOK_BODY) in rec_bytes  # body embedded verbatim


def test_build_params():
    assert build_params(a=None, b=True, c=False, d=[1, "x"], e=3) == [("b", "true"), ("c", "false"), ("d", "1"), ("d", "x"), ("e", "3")]


async def test_list_param_styles():
    t = FakeTransport(resp(200, {"orderbooks": []}), resp(200, {"markets": [], "cursor": ""}), resp(200, {"queue_positions": []}))
    k, _, _ = client(t)
    await k.get_orderbooks(["A", "B"])
    await k.get_markets(tickers=["A", "B"], status="open", limit=5)
    await k.get_queue_positions(market_tickers=["A", "B"])
    assert t.calls[0]["params"] == [("tickers", "A"), ("tickers", "B")]  # form, explode
    assert ("tickers", "A,B") in t.calls[1]["params"] and ("status", "open") in t.calls[1]["params"]
    assert t.calls[2]["params"] == [("market_tickers", "A,B")]
    with pytest.raises(ValueError):
        await k.get_orderbooks([])


async def test_get_retries_429_and_5xx_with_backoff_and_retry_after():
    t = FakeTransport(resp(503, {"code": "unavailable"}), resp(429, {}, {"Retry-After": "2"}), resp(200, {"ok": 1}))
    lim = KalshiRateLimiter()
    k, sleeps, raw = client(t, limiter=lim)
    assert await k.get("/exchange/status") == {"ok": 1}
    assert len(t.calls) == 3 and k.stats["retries"] == 2 and k.stats["http_429"] == 1
    assert 0.125 <= sleeps.calls[0] <= 0.25 and sleeps.calls[1] == 2.0
    assert lim.read.tokens < 100  # drained on 429 then partially spent
    assert [orjson.loads(r[2])["status"] for r in raw] == [503, 429, 200]


async def test_get_does_not_retry_4xx_and_parses_error_body():
    t = FakeTransport(resp(404, {"code": "not_found", "message": "market not found"}))
    k, sleeps, _ = client(t)
    with pytest.raises(KalshiHTTPError) as ei:
        await k.get_market("NOPE")
    assert ei.value.status == 404 and ei.value.code == "not_found" and "market not found" in str(ei.value)
    assert len(t.calls) == 1 and sleeps.calls == []


async def test_get_retry_exhaustion_and_transport_errors():
    t = FakeTransport(*[resp(500)] * 3)
    k, sleeps, _ = client(t, max_get_retries=2)
    with pytest.raises(KalshiHTTPError):
        await k.get("/markets")
    assert len(t.calls) == 3 and len(sleeps.calls) == 2
    t = FakeTransport(ResponseLostError("timeout"), NotSentError("refused"), resp(200, {"x": 1}))
    k, _, raw = client(t)
    assert await k.get("/markets") == {"x": 1}
    recs = [orjson.loads(r[2]) for r in raw]
    assert recs[0]["status"] == 0 and "ResponseLostError" in recs[0]["error"]


async def test_create_order_outcomes_never_retry():
    body = {"ticker": "T", "client_order_id": "c1", "side": "bid", "count": "1.00", "price": "0.4500",
            "time_in_force": "good_till_canceled", "self_trade_prevention_type": "taker_at_cross"}
    ok = {"order_id": "o1", "client_order_id": "c1", "fill_count": "0.00", "remaining_count": "1.00", "ts_ms": 5}
    cases = [
        (resp(201, ok), dict),
        (resp(503, {"code": "x"}), UnknownOutcome),
        (resp(500), UnknownOutcome),
        (resp(408), UnknownOutcome),
        (resp(409, {"code": "duplicate"}), UnknownOutcome),
        (ResponseLostError("read timeout"), UnknownOutcome),
        (resp(201, {"client_order_id": "c1"}), UnknownOutcome),  # 2xx without order_id
        (resp(201, b"<html>"), UnknownOutcome),
    ]
    for r, kind in cases:
        t = FakeTransport(r)
        k, sleeps, raw = client(t)
        res = await k.create_order(body)
        assert isinstance(res, kind), (r, res)
        assert len(t.calls) == 1 and sleeps.calls == []
        assert t.calls[0]["method"] == "POST" and t.calls[0]["url"] == BASE + "/portfolio/events/orders"
        assert orjson.loads(t.calls[0]["data"]) == body
        rec = orjson.loads(raw[0][2])
        assert raw[0][0] == "kalshi.rest.orders" and rec["request"] == body
    for status in (400, 401, 403, 429):
        t = FakeTransport(resp(status, {"code": "bad", "message": "nope"}))
        k, sleeps, _ = client(t)
        with pytest.raises(KalshiHTTPError):
            await k.create_order(body)
        assert len(t.calls) == 1 and sleeps.calls == []
    t = FakeTransport(NotSentError("connection refused"))
    k, _, _ = client(t)
    with pytest.raises(NotSentError):
        await k.create_order(body)


async def test_order_endpoint_shapes_and_write_costs():
    t = FakeTransport(*[resp(200, {"order_id": "o1", "reduced_by": "1.00", "ts_ms": 1})] * 3,
                      resp(201, {"orders": []}), resp(204), resp(201, {"order_group_id": "g1", "subaccount": 0}),
                      resp(200, {}), resp(200, {}))
    lim = KalshiRateLimiter(write=BucketLimit(1000, 1000), clock=lambda: 0.0)  # frozen clock: no refill
    k, _, _ = client(t, limiter=lim)
    await k.cancel_order("o1", market_ticker="T")
    await k.decrease_order("o1", reduce_to=250, market_ticker="T")
    await k.amend_order("o1", {"ticker": "T", "side": "bid", "price": "0.4600", "count": "3.00"})
    await k.batch_create_orders([{"ticker": "T"}] * 5)
    assert await k.cancel_all_orders() == {}
    await k.create_order_group(1500)
    await k.update_order_group_limit("g1", 250)
    await k.reset_order_group("g1")
    c = t.calls
    assert (c[0]["method"], c[0]["url"], c[0]["params"]) == ("DELETE", BASE + "/portfolio/events/orders/o1", [("market_ticker", "T")])
    assert orjson.loads(c[1]["data"]) == {"reduce_to": "2.50", "market_ticker": "T"}
    assert c[2]["url"].endswith("/portfolio/events/orders/o1/amend")
    assert c[3]["url"].endswith("/portfolio/events/orders/batched") and len(orjson.loads(c[3]["data"])["orders"]) == 5
    assert (c[4]["method"], c[4]["url"]) == ("DELETE", BASE + "/portfolio/events/orders")
    assert orjson.loads(c[5]["data"]) == {"contracts_limit_fp": "15.00"}
    assert (c[6]["method"], orjson.loads(c[6]["data"])) == ("PUT", {"contracts_limit_fp": "2.50"})
    # write bucket: cancel 2 + decrease 10 + amend 10 + batch 5x10 + cancel-all 2 + group create/limit/reset 3x10
    assert lim.write.limit.bucket_capacity - lim.write.tokens == 104
    with pytest.raises(ValueError):
        await k.decrease_order("o1", reduce_by=1, reduce_to=2)


async def test_pagination_cursor_rules():
    pages = [resp(200, {"markets": [{"ticker": "A"}], "cursor": "c1"}), resp(200, {"markets": [{"ticker": "B"}], "cursor": "c2"}),
             resp(200, {"markets": [{"ticker": "C"}], "cursor": ""})]
    t = FakeTransport(*pages)
    k, _, _ = client(t)
    got = await k.collect(k.iter_markets(series_ticker="KXBTCD", status="open"))
    assert [m["ticker"] for m in got] == ["A", "B", "C"]
    assert ("cursor", "c1") in t.calls[1]["params"] and ("series_ticker", "KXBTCD") in t.calls[2]["params"]
    t = FakeTransport(resp(200, {"markets": [{"ticker": "A"}], "cursor": "c1"}), resp(200, {"markets": [{"ticker": "B"}], "cursor": "c1"}))
    k, _, _ = client(t)
    with pytest.raises(KalshiPaginationError):
        await k.collect(k.iter_markets())
    t = FakeTransport(resp(200, {"markets": [{"ticker": "A"}], "cursor": "c1"}), resp(200, {"markets": [], "cursor": "c2"}))
    k, _, _ = client(t)
    assert len(await k.collect(k.iter_markets())) == 1  # empty page ends the walk
    t = FakeTransport(*[resp(200, {"markets": [{"ticker": str(i)}], "cursor": f"c{i}"}) for i in range(5)])
    k, _, _ = client(t)
    with pytest.raises(KalshiPaginationError):
        await k.collect(k.iter_markets(max_pages=3))
    t = FakeTransport(resp(200, {"incentive_programs": [{"id": "1"}], "next_cursor": "n1"}), resp(200, {"incentive_programs": [{"id": "2"}]}))
    k, _, _ = client(t)
    assert [p["id"] for p in await k.collect(k.iter_incentive_programs())] == ["1", "2"]


@pytest.mark.parametrize("inclusive", [True, False])
async def test_windowed_trade_pagination_complete_and_deduped(inclusive):
    trades = [{"trade_id": f"t{s}", "ts": s} for s in range(100, 200)] + [{"trade_id": "t150b", "ts": 150}]

    def server(call):
        p = dict(call["params"])
        lo, hi = int(p["min_ts"]), int(p["max_ts"])
        sel = [x for x in trades if (lo <= x["ts"] <= hi if inclusive else lo < x["ts"] < hi)]
        start = int(p.get("cursor", 0))
        page = sel[start:start + 7]
        nxt = str(start + 7) if start + 7 < len(sel) else ""
        return resp(200, {"trades": page, "cursor": nxt})

    t = FakeTransport(*[server] * 200)
    k, _, _ = client(t)
    lo, hi = (100, 199) if inclusive else (99, 200)
    got = await k.collect(k.iter_trades(ticker="T", min_ts=lo, max_ts=hi, window_s=10))
    ids = [g["trade_id"] for g in got]
    assert sorted(ids) == sorted(x["trade_id"] for x in trades) and len(ids) == len(set(ids))


async def test_configure_rate_limits_and_cf_passthrough():
    t = FakeTransport(
        resp(200, {"usage_tier": "basic", "read": {"refill_rate": 200, "bucket_capacity": 200},
                   "write": {"refill_rate": 100, "bucket_capacity": 100}, "grants": []}),
        resp(200, {"default_cost": 10, "endpoint_costs": [{"method": "GET", "path": "/markets/{ticker}/orderbook", "cost": 5}]}),
        resp(200, [[1710000000000, "68000.1"]]),
    )
    k, _, raw = client(t, cf_history_path="/cfbenchmarks/history/values")
    out = await k.configure_rate_limits()
    assert k.limiter is not None and k.limiter.usage_tier == "basic" and out["limits"]["usage_tier"] == "basic"
    assert k.limiter.cost_for("GET", "/markets/X/orderbook") == 5
    body = await k.get_cfbenchmarks_history("BRTI", timespan="5m", timestamp=1710000000000)
    assert body == [[1710000000000, "68000.1"]]
    assert t.calls[2]["url"] == BASE + "/cfbenchmarks/history/values"
    assert t.calls[2]["params"] == [("id", "BRTI"), ("timespan", "5m"), ("timestamp", "1710000000000")]
    assert raw[-1][0] == "kalshi.rest.cfbenchmarks"


async def test_find_order_by_client_id_and_positions():
    t = FakeTransport(resp(200, {"orders": [dict(S.ORDER_ROW, client_order_id="x")], "cursor": "c"}),
                      resp(200, {"orders": [S.ORDER_ROW], "cursor": ""}),
                      resp(200, {"market_positions": [{"ticker": "A"}], "event_positions": [], "cursor": "p1"}),
                      resp(200, {"market_positions": [{"ticker": "B"}], "event_positions": [{"event_ticker": "E"}], "cursor": ""}))
    k, _, _ = client(t)
    o = await k.find_order_by_client_id("dhA-1", ticker=S.MARKET_KXBTCD["ticker"])
    assert o is not None and o["order_id"] == "o-1"
    pos = await k.get_all_positions(count_filter="position")
    assert [p["ticker"] for p in pos["market_positions"]] == ["A", "B"] and len(pos["event_positions"]) == 1


# ------------------------------------------------------------------------------ real aiohttp transport (loopback only)
async def test_aiohttp_transport_loopback():
    from aiohttp import web

    seen: dict[str, Any] = {}

    async def h(req: web.Request) -> web.Response:
        seen["query"] = list(req.query.items())
        seen["auth"] = req.headers.get("KALSHI-ACCESS-KEY")
        seen["body"] = await req.read()
        return web.json_response({"order_id": "o1"}, status=201)

    async def slow(req: web.Request) -> web.Response:
        await asyncio.sleep(1.0)
        return web.json_response({})

    app = web.Application()
    app.router.add_post("/trade-api/v2/portfolio/events/orders", h)
    app.router.add_get("/trade-api/v2/slow", slow)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]  # type: ignore[union-attr]
    tr = AiohttpTransport(trust_env=False)
    try:
        r = await tr("POST", f"http://127.0.0.1:{port}/trade-api/v2/portfolio/events/orders",
                     {"KALSHI-ACCESS-KEY": "k", "Content-Type": "application/json"}, [("subaccount", "0")], b'{"a":1}', 5.0)
        assert r.status == 201 and orjson.loads(r.body) == {"order_id": "o1"}
        assert seen == {"query": [("subaccount", "0")], "auth": "k", "body": b'{"a":1}'}
        with pytest.raises(ResponseLostError):
            await tr("GET", f"http://127.0.0.1:{port}/trade-api/v2/slow", {}, [], None, 0.2)
    finally:
        await tr.close()
        await runner.cleanup()
    tr = AiohttpTransport(trust_env=False)
    try:
        with pytest.raises(NotSentError):  # nothing listens there any more
            await tr("GET", f"http://127.0.0.1:{port}/x", {}, [], None, 2.0)
    finally:
        await tr.close()


async def test_windowed_trades_deduped_across_three_overlapping_windows():
    """Audit m8: with window_s=2 a trade on a boundary lies in three consecutive windows."""
    trades = [{"trade_id": f"t{h}", "ts": h / 2} for h in range(200, 213)]  # every 0.5 s in [100, 106]

    def server(call):
        p = dict(call["params"])
        lo, hi = int(p["min_ts"]), int(p["max_ts"])
        return resp(200, {"trades": [x for x in trades if lo <= x["ts"] <= hi], "cursor": ""})

    k, _, _ = client(FakeTransport(*[server] * 20))
    got = await k.collect(k.iter_trades(ticker="T", min_ts=100, max_ts=106, window_s=2))
    ids = [g["trade_id"] for g in got]
    assert sorted(ids) == sorted(x["trade_id"] for x in trades) and len(ids) == len(set(ids))
