"""Regenerate the venue fixture files (python tests/feeds/fixtures/_generate.py).

Each fixture is JSON lines in the recorder's record format {"t","s","q","d"} (uncompressed):
``d`` is the raw frame text exactly as the venue would send it (shapes from each venue's
documented format; see the ASSUMED WIRE FORMAT block of each dh/feeds/<venue>.py). Times are
around 2026-09-25T12:00:00Z; exchange timestamps precede receive times by a few ms.

Kraken checksums are computed here with an independent, string-based reference
implementation of Kraken's documented algorithm (not with dh.feeds.kraken), anchored by the
documentation's own snapshot example (checksum 3310070434).
"""

from __future__ import annotations

import json
import zlib
from pathlib import Path

HERE = Path(__file__).parent
T0_S = 1790337600  # 2026-09-25T12:00:00Z
T0_MS = T0_S * 1000
T0_NS = T0_S * 10**9


def iso(off_ms: int, frac_digits: int = 6, extra_ns: int = 0) -> str:
    """RFC3339 UTC text for T0 + off_ms (+ extra_ns)."""
    import time

    s, rem = divmod(T0_MS + off_ms, 1000)
    tm = time.gmtime(s)
    ns = rem * 1_000_000 + extra_ns
    frac = f"{ns:09d}"[:frac_digits]
    return f"{tm.tm_year:04d}-{tm.tm_mon:02d}-{tm.tm_mday:02d}T{tm.tm_hour:02d}:{tm.tm_min:02d}:{tm.tm_sec:02d}.{frac}Z"


def marker(status: str, conn: int = 1, **kw: object) -> str:
    d = {"_dh": "status", "conn": conn, "status": status}
    d.update(kw)
    return json.dumps(d, separators=(",", ":"))


def write(name: str, stream: str, frames: list[tuple[int, str]]) -> None:
    """frames: (recv offset ms from T0, raw text)."""
    with open(HERE / f"{name}.jsonl", "w") as f:
        for q, (off_ms, raw) in enumerate(frames):
            f.write(json.dumps({"t": T0_NS + off_ms * 1_000_000 + q, "s": stream, "q": q, "d": raw}, separators=(",", ":")) + "\n")


def j(o: object) -> str:
    return json.dumps(o, separators=(",", ":"))


# ----------------------------------------------------------------------------- coinbase
def coinbase() -> None:
    def env(ch: str, seq: int, ms: int, events: list) -> str:
        return j({"channel": ch, "client_id": "", "timestamp": iso(ms, 9, 123), "sequence_num": seq, "events": events})

    snap = [
        {"side": "bid", "event_time": "1970-01-01T00:00:00Z", "price_level": "84500.01", "new_quantity": "0.51000000"},
        {"side": "bid", "event_time": "1970-01-01T00:00:00Z", "price_level": "84499.50", "new_quantity": "1.20000000"},
        {"side": "bid", "event_time": "1970-01-01T00:00:00Z", "price_level": "84498.00", "new_quantity": "2.00000000"},
        {"side": "offer", "event_time": "1970-01-01T00:00:00Z", "price_level": "84500.02", "new_quantity": "0.30000000"},
        {"side": "offer", "event_time": "1970-01-01T00:00:00Z", "price_level": "84501.00", "new_quantity": "0.75000000"},
        {"side": "offer", "event_time": "1970-01-01T00:00:00Z", "price_level": "84503.10", "new_quantity": "1.10000000"},
    ]
    frames = [(0, marker("connected", url="wss://advanced-trade-ws.coinbase.com"))]
    frames.append((5, j({"_dh": "sent", "conn": 1, "msg": j({"type": "subscribe", "product_ids": ["BTC-USD"], "channel": "level2"})})))
    frames.append((40, env("subscriptions", 0, 38, [{"subscriptions": {"level2": ["BTC-USD"]}}])))
    frames.append((60, env("l2_data", 1, 55, [{"type": "snapshot", "product_id": "BTC-USD", "updates": snap}])))
    frames.append((80, env("heartbeats", 2, 78, [{"current_time": "2026-09-25 12:00:00.078 +0000 UTC m=+1.1", "heartbeat_counter": 1}])))
    frames.append((95, env("l2_data", 3, 92, [{"type": "update", "product_id": "BTC-USD", "updates": [
        {"side": "bid", "event_time": iso(91), "price_level": "84500.01", "new_quantity": "0.61000000"},
        {"side": "offer", "event_time": iso(91), "price_level": "84500.02", "new_quantity": "0"}]}])))
    frames.append((110, env("market_trades", 4, 105, [{"type": "snapshot", "trades": [
        {"trade_id": "812345670", "product_id": "BTC-USD", "price": "84499.90", "size": "0.01", "side": "SELL", "time": iso(-5000)},
        {"trade_id": "812345671", "product_id": "BTC-USD", "price": "84500.02", "size": "0.02", "side": "BUY", "time": iso(-3000)}]}])))
    frames.append((130, env("market_trades", 5, 127, [{"type": "update", "trades": [
        {"trade_id": "812345672", "product_id": "BTC-USD", "price": "84501.00", "size": "0.05", "side": "BUY", "time": iso(125)}]}])))
    frames.append((150, env("l2_data", 6, 148, [{"type": "update", "product_id": "BTC-USD", "updates": [
        {"side": "offer", "event_time": iso(147), "price_level": "84500.50", "new_quantity": "0.40000000"}]}])))
    frames.append((170, env("market_trades", 7, 166, [{"type": "update", "trades": [
        {"trade_id": "812345672", "product_id": "BTC-USD", "price": "84501.00", "size": "0.05", "side": "BUY", "time": iso(125)},
        {"trade_id": "812345673", "product_id": "BTC-USD", "price": "84500.01", "size": "0.10", "side": "SELL", "time": iso(165)}]}])))
    # gap: 7 -> 9
    frames.append((190, env("l2_data", 9, 188, [{"type": "update", "product_id": "BTC-USD", "updates": [
        {"side": "bid", "event_time": iso(187), "price_level": "84499.50", "new_quantity": "0"}]}])))
    frames.append((200, env("l2_data", 10, 198, [{"type": "update", "product_id": "BTC-USD", "updates": [
        {"side": "bid", "event_time": iso(197), "price_level": "84498.00", "new_quantity": "3.0"}]}])))
    # resubscribe -> fresh snapshot
    frames.append((260, env("l2_data", 11, 257, [{"type": "snapshot", "product_id": "BTC-USD", "updates": snap[:2] + snap[3:5]}])))
    frames.append((280, env("l2_data", 12, 277, [{"type": "update", "product_id": "BTC-USD", "updates": [
        {"side": "bid", "event_time": iso(276), "price_level": "84499.00", "new_quantity": "0.25"}]}])))
    frames.append((300, marker("disconnected", detail="ConnectionClosedError: 1006")))
    write("coinbase", "coinbase.ws", frames)


# ----------------------------------------------------------------------------- kraken
def _kr_fmt(text: str) -> str:
    return text.replace(".", "").lstrip("0")


def kraken_ref_checksum(bids: list[tuple[str, str]], asks: list[tuple[str, str]]) -> int:
    """Reference: bids/asks as decimal TEXT, any order; top 10 asks asc + top 10 bids desc."""
    a = sorted(asks, key=lambda x: float(x[0]))[:10]
    b = sorted(bids, key=lambda x: -float(x[0]))[:10]
    s = "".join(_kr_fmt(p) + _kr_fmt(q) for p, q in a) + "".join(_kr_fmt(p) + _kr_fmt(q) for p, q in b)
    return zlib.crc32(s.encode()) & 0xFFFFFFFF


KR_BIDS = [("45283.5", "0.10000000"), ("45283.4", "1.54582015"), ("45282.1", "0.10000000"), ("45281.0", "0.10000000"),
           ("45280.3", "1.54592586"), ("45279.0", "0.07990000"), ("45277.6", "0.03310103"), ("45277.5", "0.30000000"),
           ("45277.3", "1.54602737"), ("45276.6", "0.15445238")]
KR_ASKS = [("45285.2", "0.00100000"), ("45286.4", "1.54571953"), ("45286.6", "1.54571109"), ("45289.6", "1.54560911"),
           ("45290.2", "0.15890660"), ("45291.8", "1.54553491"), ("45294.7", "0.04454749"), ("45296.1", "0.35380000"),
           ("45297.5", "0.09945542"), ("45299.5", "0.18772827")]


def _kr_levels(lv: list[tuple[str, str]]) -> str:
    # numbers as JSON numbers with full precision text (Kraken v2 style)
    return "[" + ",".join('{"price":%s,"qty":%s}' % (p, q) for p, q in lv) + "]"


def _kr_book(typ: str, bids: list[tuple[str, str]], asks: list[tuple[str, str]], checksum: int, ts: str | None) -> str:
    ts_part = f',"timestamp":"{ts}"' if ts else ""
    return ('{"channel":"book","type":"%s","data":[{"symbol":"BTC/USD","bids":%s,"asks":%s,"checksum":%d%s}]}'
            % (typ, _kr_levels(bids), _kr_levels(asks), checksum, ts_part))


def _apply(book: dict[str, str], ups: list[tuple[str, str]]) -> None:
    for p, q in ups:
        if float(q) == 0:
            book.pop(p, None)
        else:
            book[p] = q


def _trunc(bids: dict[str, str], asks: dict[str, str], depth: int) -> None:
    for p in sorted(bids, key=float)[: max(0, len(bids) - depth)]:
        del bids[p]
    for p in sorted(asks, key=lambda x: -float(x))[: max(0, len(asks) - depth)]:
        del asks[p]


def kraken() -> None:
    assert kraken_ref_checksum(KR_BIDS, KR_ASKS) == 3310070434  # documented example
    frames = [(0, marker("connected", url="wss://ws.kraken.com/v2"))]
    frames.append((20, j({"channel": "status", "type": "update", "data": [{"version": "2.0.10", "system": "online", "api_version": "v2", "connection_id": 1234567890}]})))
    frames.append((50, j({"method": "subscribe", "result": {"channel": "book", "depth": 10, "snapshot": True, "symbol": "BTC/USD"}, "success": True, "time_in": iso(45), "time_out": iso(46), "req_id": 1})))
    frames.append((60, _kr_book("snapshot", KR_BIDS, KR_ASKS, 3310070434, None)))
    bids, asks = dict(KR_BIDS), dict(KR_ASKS)
    # update 1: delete best bid, add a deep bid
    u1b = [("45283.5", "0.00000000"), ("45275.0", "0.50000000")]
    _apply(bids, u1b)
    _trunc(bids, asks, 10)
    frames.append((80, _kr_book("update", u1b, [], kraken_ref_checksum(list(bids.items()), list(asks.items())), iso(78))))
    # update 2: new best ask pushes the worst ask (45299.5) out of the depth-10 window
    u2a = [("45284.0", "0.25000000")]
    _apply(asks, u2a)
    _trunc(bids, asks, 10)
    frames.append((100, _kr_book("update", [], u2a, kraken_ref_checksum(list(bids.items()), list(asks.items())), iso(98))))
    frames.append((110, '{"channel":"heartbeat"}'))
    frames.append((120, '{"channel":"trade","type":"update","data":[{"symbol":"BTC/USD","side":"buy","price":45284.0,"qty":0.00150000,"ord_type":"market","trade_id":74125001,"timestamp":"%s"}]}' % iso(118)))
    frames.append((125, '{"channel":"trade","type":"update","data":[{"symbol":"BTC/USD","side":"sell","price":45283.4,"qty":0.02000000,"ord_type":"limit","trade_id":74125002,"timestamp":"%s"}]}' % iso(123)))
    # update 3 with a WRONG checksum -> gap, suppressed until the next snapshot
    u3b = [("45283.4", "1.00000000")]
    frames.append((140, _kr_book("update", u3b, [], 12345, iso(138))))
    frames.append((150, _kr_book("update", [("45282.1", "0.20000000")], [], 99, iso(148))))
    # resubscribe -> new snapshot (documented example again)
    frames.append((200, _kr_book("snapshot", KR_BIDS, KR_ASKS, 3310070434, None)))
    write("kraken", "kraken.ws", frames)


# ----------------------------------------------------------------------------- bitstamp
def bitstamp() -> None:
    def diff(us: int, bids: list, asks: list) -> str:
        return j({"data": {"timestamp": str(us // 1_000_000), "microtimestamp": str(us), "bids": bids, "asks": asks},
                  "channel": "diff_order_book_btcusd", "event": "data"})

    base_us = T0_S * 1_000_000
    frames = [(0, marker("connected", url="wss://ws.bitstamp.net"))]
    frames.append((30, j({"event": "bts:subscription_succeeded", "channel": "diff_order_book_btcusd", "data": {}})))
    frames.append((31, j({"event": "bts:subscription_succeeded", "channel": "live_trades_btcusd", "data": {}})))
    frames.append((100, diff(base_us + 90_000, [["84490", "0.20000000"]], [])))  # before snapshot time: dropped
    frames.append((300, diff(base_us + 290_000, [["84495", "0.00000000"]], [["84510", "0.70000000"]])))  # after: folded
    body = j({"timestamp": str(T0_S), "microtimestamp": str(base_us + 150_000),
              "bids": [["84495", "1.00000000"], ["84494", "0.50000000"], ["84490", "0.10000000"]],
              "asks": [["84505", "0.40000000"], ["84510", "0.60000000"], ["84520", "2.00000000"]]})
    frames.append((1600, j({"_dh": "rest", "conn": 1, "kind": "order_book", "url": "https://www.bitstamp.net/api/v2/order_book/btcusd/",
                            "status": 200, "req_ns": T0_NS + 1_500_000_000, "body": body, "symbol": "btcusd"})))
    frames.append((1610, diff(base_us + 250_000, [["84494", "9.0"]], [])))  # late but older than book: ignored
    frames.append((1700, diff(base_us + 1_690_000, [["84496", "0.30000000"]], [["84505", "0.00000000"]])))
    frames.append((1750, j({"data": {"id": 351234567, "timestamp": str(T0_S + 1), "amount": 0.0125, "amount_str": "0.01250000",
                                     "price": 84510, "price_str": "84510", "type": 0, "microtimestamp": str(base_us + 1_745_000),
                                     "buy_order_id": 1900000000000001, "sell_order_id": 1900000000000002},
                            "channel": "live_trades_btcusd", "event": "trade"})))
    frames.append((1760, j({"data": {"id": 351234568, "timestamp": str(T0_S + 1), "amount": 0.5, "amount_str": "0.50000000",
                                     "price": 84496, "price_str": "84496", "type": 1, "microtimestamp": str(base_us + 1_755_000),
                                     "buy_order_id": 1900000000000003, "sell_order_id": 1900000000000004},
                            "channel": "live_trades_btcusd", "event": "trade"})))
    frames.append((1800, j({"event": "bts:request_reconnect", "channel": "", "data": ""})))
    write("bitstamp", "bitstamp.ws", frames)


# ----------------------------------------------------------------------------- gemini
def gemini() -> None:
    frames = [(0, marker("connected", url="wss://api.gemini.com/v2/marketdata"))]
    frames.append((70, j({"type": "l2_updates", "symbol": "BTCUSD",
                          "changes": [["buy", "84498.12", "0.25"], ["buy", "84497.00", "1.5"], ["sell", "84500.50", "0.4"], ["sell", "84502.00", "0.9"]],
                          "trades": [{"type": "trade", "symbol": "BTCUSD", "event_id": 207000001, "timestamp": T0_MS - 4000, "price": "84499.00", "quantity": "0.01", "side": "buy"}],
                          "auction_events": []})))
    frames.append((90, j({"type": "l2_updates", "symbol": "BTCUSD", "changes": [["buy", "84498.12", "0"], ["sell", "84500.00", "0.1"]]})))
    frames.append((110, j({"type": "trade", "symbol": "BTCUSD", "event_id": 207000002, "timestamp": T0_MS + 105, "price": "84500.00", "quantity": "0.05", "side": "buy"})))
    frames.append((115, j({"type": "trade", "symbol": "BTCUSD", "event_id": 207000002, "timestamp": T0_MS + 105, "price": "84500.00", "quantity": "0.05", "side": "buy"})))
    frames.append((120, j({"type": "heartbeat", "timestamp": T0_MS + 118})))
    write("gemini", "gemini.ws", frames)


# ----------------------------------------------------------------------------- crypto.com
def cryptocom() -> None:
    def book(ch: str, data: list) -> str:
        return j({"id": -1, "method": "subscribe", "code": 0, "result": {"instrument_name": "BTC_USD", "subscription": "book.BTC_USD.50", "channel": ch, "depth": 50, "data": data}})

    frames = [(0, marker("connected", url="wss://stream.crypto.com/exchange/v1/market"))]
    frames.append((1050, j({"id": 1, "method": "subscribe", "code": 0})))
    frames.append((1060, book("book", [{"asks": [["84501.10", "0.2100", "2"], ["84502.00", "0.5000", "1"]],
                                         "bids": [["84499.90", "0.3000", "3"], ["84499.00", "1.0000", "1"]],
                                         "t": T0_MS + 1055, "tt": T0_MS + 1054, "u": 900000100}])))
    frames.append((1070, book("book.update", [{"update": {"asks": [["84501.10", "0", "0"]], "bids": [["84500.00", "0.1500", "1"]]},
                                                "t": T0_MS + 1066, "tt": T0_MS + 1065, "u": 900000108, "pu": 900000100}])))
    frames.append((1080, j({"id": 1790337601080, "method": "public/heartbeat", "code": 0})))
    frames.append((1090, j({"id": -1, "method": "subscribe", "code": 0, "result": {"instrument_name": "BTC_USD", "subscription": "trade.BTC_USD", "channel": "trade",
                                                                                  "data": [{"d": "4611686018427387905", "t": T0_MS + 1085, "tn": str((T0_MS + 1085) * 1_000_000 + 123), "q": "0.0020", "p": "84500.00", "s": "BUY", "i": "BTC_USD", "m": "4611686018427387904"}]}})))
    frames.append((1100, book("book.update", [{"update": {"asks": [], "bids": [["84499.00", "0.8000", "1"]]},
                                                "t": T0_MS + 1096, "tt": T0_MS + 1095, "u": 900000120, "pu": 900000110}])))  # gap
    frames.append((1110, book("book.update", [{"update": {"asks": [["84502.00", "0.1", "1"]], "bids": []},
                                                "t": T0_MS + 1106, "tt": T0_MS + 1105, "u": 900000130, "pu": 900000120}])))  # suppressed
    write("cryptocom", "cryptocom.ws", frames)


# ----------------------------------------------------------------------------- deribit
def deribit() -> None:
    def sub(ch: str, data: object) -> str:
        return j({"jsonrpc": "2.0", "method": "subscription", "params": {"channel": ch, "data": data}})

    frames = [(0, marker("connected", url="wss://www.deribit.com/ws/api/v2"))]
    frames.append((40, j({"jsonrpc": "2.0", "id": 101, "result": "ok", "usIn": 1, "usOut": 2, "usDiff": 1, "testnet": False})))
    frames.append((45, j({"jsonrpc": "2.0", "id": 102, "result": ["book.BTC-PERPETUAL.100ms", "ticker.BTC-PERPETUAL.100ms", "trades.BTC-PERPETUAL.100ms",
                                                                    "deribit_price_index.btc_usd", "deribit_volatility_index.btc_usd"], "usIn": 1, "usOut": 2, "usDiff": 1, "testnet": False})))
    frames.append((60, sub("book.BTC-PERPETUAL.100ms", {"type": "snapshot", "timestamp": T0_MS + 55, "instrument_name": "BTC-PERPETUAL", "change_id": 71000000100,
                                                         "bids": [["new", 84510.0, 169000], ["new", 84509.5, 84500]],
                                                         "asks": [["new", 84510.5, 42255], ["new", 84511.0, 845110]]})))
    frames.append((75, sub("book.BTC-PERPETUAL.100ms", {"type": "change", "timestamp": T0_MS + 70, "instrument_name": "BTC-PERPETUAL", "prev_change_id": 71000000100, "change_id": 71000000107,
                                                         "bids": [["change", 84510.0, 84510], ["delete", 84509.5, 0.0]], "asks": [["new", 84510.25, 8451]]})))
    frames.append((80, sub("ticker.BTC-PERPETUAL.100ms", {"timestamp": T0_MS + 78, "stats": {"volume_usd": 1.2e9}, "state": "open", "settlement_price": 84400.0,
                                                           "open_interest": 1_014_000_000, "min_price": 83200.0, "max_price": 85700.0, "mark_price": 84500.0,
                                                           "last_price": 84510.0, "instrument_name": "BTC-PERPETUAL", "index_price": 84480.0, "funding_8h": 0.0001,
                                                           "current_funding": 0.00002, "estimated_delivery_price": 84480.0, "best_bid_price": 84510.0, "best_bid_amount": 84510,
                                                           "best_ask_price": 84510.25, "best_ask_amount": 8451, "interest_value": 0.0})))
    frames.append((90, sub("trades.BTC-PERPETUAL.100ms", [
        {"trade_seq": 150000001, "trade_id": "310000001", "timestamp": T0_MS + 86, "tick_direction": 0, "price": 84510.25, "mark_price": 84500.0, "index_price": 84480.0,
         "instrument_name": "BTC-PERPETUAL", "direction": "buy", "amount": 16902.0},
        {"trade_seq": 150000002, "trade_id": "310000002", "timestamp": T0_MS + 87, "tick_direction": 2, "price": 84510.0, "mark_price": 84500.0, "index_price": 84480.0,
         "instrument_name": "BTC-PERPETUAL", "direction": "sell", "amount": 84510.0, "liquidation": "T"}])))
    frames.append((95, sub("deribit_price_index.btc_usd", {"timestamp": T0_MS + 93, "price": 84480.12, "index_name": "btc_usd"})))
    frames.append((96, sub("deribit_volatility_index.btc_usd", {"timestamp": T0_MS + 94, "volatility": 42.5, "index_name": "btc_usd"})))
    frames.append((100, j({"jsonrpc": "2.0", "method": "heartbeat", "params": {"type": "test_request"}})))
    frames.append((110, sub("book.BTC-PERPETUAL.100ms", {"type": "change", "timestamp": T0_MS + 105, "instrument_name": "BTC-PERPETUAL", "prev_change_id": 71000000110, "change_id": 71000000115,
                                                          "bids": [["new", 84509.0, 1000]], "asks": []})))  # gap
    frames.append((120, sub("ticker.BTC-26SEP26-85000-C.100ms", {"underlying_price": 84495.0, "underlying_index": "SYN.BTC-26SEP26", "timestamp": T0_MS + 117,
                                                                 "stats": {}, "state": "open", "settlement_price": 0.0123, "open_interest": 152.3, "min_price": 0.0001, "max_price": 0.05,
                                                                 "mark_price": 0.0089, "mark_iv": 38.5, "last_price": 0.009, "interest_rate": 0.0,
                                                                 "instrument_name": "BTC-26SEP26-85000-C", "index_price": 84480.0,
                                                                 "greeks": {"delta": 0.42, "gamma": 0.0003, "vega": 12.1, "theta": -150.0, "rho": 0.5},
                                                                 "estimated_delivery_price": 84480.0, "bid_iv": 37.9, "best_bid_price": 0.0085, "best_bid_amount": 12.5,
                                                                 "best_ask_price": 0.0093, "best_ask_amount": 8.0, "ask_iv": 39.2})))
    frames.append((130, j({"jsonrpc": "2.0", "id": 150, "error": {"message": "Invalid params", "code": -32602}, "usIn": 1, "usOut": 2, "usDiff": 1, "testnet": False})))
    write("deribit", "deribit.ws", frames)


# ----------------------------------------------------------------------------- binance futures
def binance() -> None:
    def w(stream: str, data: dict) -> str:
        return j({"stream": stream, "data": data})

    frames = [(0, marker("connected", url="wss://fstream.binance.com/stream?streams=..."))]
    frames.append((20, w("btcusdt@bookTicker", {"e": "bookTicker", "u": 8812345678901, "s": "BTCUSDT", "b": "84520.10", "B": "3.512", "a": "84520.20", "A": "1.004", "T": T0_MS + 17, "E": T0_MS + 18})))
    frames.append((25, w("btcusdt@bookTicker", {"e": "bookTicker", "u": 8812345678800, "s": "BTCUSDT", "b": "84520.00", "B": "3.0", "a": "84520.20", "A": "1.0", "T": T0_MS + 16, "E": T0_MS + 17})))  # older u
    frames.append((30, w("btcusdt@aggTrade", {"e": "aggTrade", "E": T0_MS + 28, "a": 2412345001, "s": "BTCUSDT", "p": "84520.20", "q": "0.150", "f": 5812345001, "l": 5812345003, "T": T0_MS + 27, "m": False})))
    frames.append((35, w("btcusdt@aggTrade", {"e": "aggTrade", "E": T0_MS + 33, "a": 2412345002, "s": "BTCUSDT", "p": "84520.10", "q": "0.020", "f": 5812345004, "l": 5812345004, "T": T0_MS + 32, "m": True})))
    frames.append((40, w("btcusdt@markPrice@1s", {"e": "markPriceUpdate", "E": T0_MS + 39, "s": "BTCUSDT", "p": "84515.30000000", "P": "84512.1", "i": "84490.52000000", "r": "0.00010000", "T": T0_MS + 4 * 3600 * 1000})))
    frames.append((45, w("btcusdt@forceOrder", {"e": "forceOrder", "E": T0_MS + 44, "o": {"s": "BTCUSDT", "S": "SELL", "o": "LIMIT", "f": "IOC", "q": "0.500", "p": "84400.00", "ap": "84480.50", "X": "FILLED", "l": "0.500", "z": "0.500", "T": T0_MS + 43}})))
    write("binance_futures", "binance_futures.ws", frames)


# ----------------------------------------------------------------------------- bybit
def bybit() -> None:
    frames = [(0, marker("connected", url="wss://stream.bybit.com/v5/public/linear"))]
    frames.append((20, j({"success": True, "ret_msg": "", "conn_id": "d4e5f6", "op": "subscribe"})))
    frames.append((30, j({"topic": "orderbook.50.BTCUSDT", "type": "snapshot", "ts": T0_MS + 27, "data": {"s": "BTCUSDT", "b": [["84525.10", "1.204"], ["84525.00", "0.500"]], "a": [["84525.20", "2.114"], ["84525.50", "0.010"]], "u": 5100001, "seq": 88000001}, "cts": T0_MS + 26})))
    frames.append((40, j({"topic": "orderbook.50.BTCUSDT", "type": "delta", "ts": T0_MS + 37, "data": {"s": "BTCUSDT", "b": [["84525.10", "0"]], "a": [["84525.30", "0.300"]], "u": 5100002, "seq": 88000005}, "cts": T0_MS + 36})))
    frames.append((45, j({"topic": "orderbook.50.BTCUSDT", "type": "delta", "ts": T0_MS + 38, "data": {"s": "BTCUSDT", "b": [["84520.00", "9.9"]], "a": [], "u": 5100002, "seq": 88000005}, "cts": T0_MS + 36})))  # dup u
    frames.append((50, j({"topic": "publicTrade.BTCUSDT", "type": "snapshot", "ts": T0_MS + 48, "data": [{"T": T0_MS + 47, "s": "BTCUSDT", "S": "Buy", "v": "0.012", "p": "84525.20", "L": "PlusTick", "i": "a1b2c3d4-0000-5000-8000-000000000001", "BT": False}]})))
    frames.append((55, j({"topic": "tickers.BTCUSDT", "type": "snapshot", "data": {"symbol": "BTCUSDT", "tickDirection": "PlusTick", "lastPrice": "84525.20", "markPrice": "84522.10", "indexPrice": "84497.80",
                                                                              "openInterest": "51234.567", "openInterestValue": "4330000000", "fundingRate": "0.0001", "nextFundingTime": str(T0_MS + 4 * 3600 * 1000),
                                                                              "bid1Price": "84525.00", "bid1Size": "0.5", "ask1Price": "84525.20", "ask1Size": "2.1", "fundingIntervalHour": "8"}, "cs": 88000006, "ts": T0_MS + 53})))
    frames.append((60, j({"topic": "tickers.BTCUSDT", "type": "delta", "data": {"symbol": "BTCUSDT", "markPrice": "84523.00"}, "cs": 88000007, "ts": T0_MS + 58})))
    frames.append((65, j({"topic": "tickers.BTCUSDT", "type": "delta", "data": {"symbol": "BTCUSDT", "lastPrice": "84525.30"}, "cs": 88000008, "ts": T0_MS + 63})))
    frames.append((70, j({"topic": "allLiquidation.BTCUSDT", "type": "snapshot", "ts": T0_MS + 68, "data": [{"T": T0_MS + 66, "s": "BTCUSDT", "S": "Buy", "v": "0.250", "p": "84400.00"}]})))
    frames.append((75, j({"success": True, "ret_msg": "pong", "conn_id": "d4e5f6", "op": "ping"})))
    write("bybit", "bybit.ws", frames)


# ----------------------------------------------------------------------------- okx
def okx() -> None:
    frames = [(0, marker("connected", url="wss://ws.okx.com:8443/ws/v5/public"))]
    frames.append((20, j({"event": "subscribe", "arg": {"channel": "books5", "instId": "BTC-USDT-SWAP"}, "connId": "a4d3ae55"})))
    frames.append((30, j({"arg": {"channel": "books5", "instId": "BTC-USDT-SWAP"}, "data": [{"asks": [["84530.1", "120", "0", "5"], ["84530.2", "15", "0", "1"]],
                                                                                            "bids": [["84530", "250", "0", "7"], ["84529.9", "40", "0", "2"]],
                                                                                            "instId": "BTC-USDT-SWAP", "ts": str(T0_MS + 27), "seqId": 12345678901}]})))
    frames.append((40, j({"arg": {"channel": "trades", "instId": "BTC-USDT-SWAP"}, "data": [{"instId": "BTC-USDT-SWAP", "tradeId": "1234567890", "px": "84530.1", "sz": "12", "side": "buy", "ts": str(T0_MS + 37), "count": "2"}]})))
    frames.append((45, j({"arg": {"channel": "funding-rate", "instId": "BTC-USDT-SWAP"}, "data": [{"fundingRate": "0.0000875", "fundingTime": str(T0_MS + 4 * 3600 * 1000), "instId": "BTC-USDT-SWAP", "instType": "SWAP",
                                                                                                   "method": "current_period", "nextFundingRate": "", "nextFundingTime": str(T0_MS + 12 * 3600 * 1000), "ts": str(T0_MS + 44)}]})))
    frames.append((50, j({"arg": {"channel": "index-tickers", "instId": "BTC-USDT"}, "data": [{"instId": "BTC-USDT", "idxPx": "84501.2", "open24h": "84000", "high24h": "85000", "low24h": "83000", "sodUtc0": "84100", "sodUtc8": "84200", "ts": str(T0_MS + 48)}]})))
    frames.append((55, j({"arg": {"channel": "mark-price", "instId": "BTC-USDT-SWAP"}, "data": [{"instType": "SWAP", "instId": "BTC-USDT-SWAP", "markPx": "84522.4", "ts": str(T0_MS + 53)}]})))
    frames.append((60, j({"arg": {"channel": "open-interest", "instId": "BTC-USDT-SWAP"}, "data": [{"instId": "BTC-USDT-SWAP", "instType": "SWAP", "oi": "2500000", "oiCcy": "25000", "oiUsd": "2113000000", "ts": str(T0_MS + 58)}]})))
    frames.append((65, j({"arg": {"channel": "liquidation-orders", "instType": "SWAP"}, "data": [
        {"details": [{"bkLoss": "0", "bkPx": "0.3120", "ccy": "", "posSide": "short", "side": "buy", "sz": "13", "ts": str(T0_MS + 60)}], "instFamily": "IOST-USDT", "instId": "IOST-USDT-SWAP", "instType": "SWAP", "uly": "IOST-USDT"},
        {"details": [{"bkLoss": "0", "bkPx": "84390.5", "ccy": "", "posSide": "long", "side": "sell", "sz": "30", "ts": str(T0_MS + 61)}], "instFamily": "BTC-USDT", "instId": "BTC-USDT-SWAP", "instType": "SWAP", "uly": "BTC-USDT"}]})))
    frames.append((70, "pong"))
    frames.append((75, j({"event": "error", "code": "60012", "msg": "Invalid request"})))
    write("okx", "okx.ws", frames)


# ----------------------------------------------------------------------------- hyperliquid
def hyperliquid() -> None:
    frames = [(0, marker("connected", url="wss://api.hyperliquid.xyz/ws"))]
    frames.append((20, j({"channel": "subscriptionResponse", "data": {"method": "subscribe", "subscription": {"type": "l2Book", "coin": "BTC"}}})))
    frames.append((30, j({"channel": "l2Book", "data": {"coin": "BTC", "time": T0_MS + 26, "levels": [
        [{"px": "84540", "sz": "3.1204", "n": 7}, {"px": "84539", "sz": "5.5", "n": 11}],
        [{"px": "84541", "sz": "1.8", "n": 4}, {"px": "84542", "sz": "7.25", "n": 9}]]}})))
    frames.append((40, j({"channel": "trades", "data": [
        {"coin": "BTC", "side": "A", "px": "84500", "sz": "0.5", "hash": "0xaaa", "time": T0_MS - 60_000, "tid": 900000000000001, "users": ["0x1", "0x2"]},
        {"coin": "BTC", "side": "B", "px": "84541", "sz": "0.02", "hash": "0xbbb", "time": T0_MS + 35, "tid": 900000000000002, "users": ["0x3", "0x4"]}]})))
    frames.append((45, j({"channel": "trades", "data": [
        {"coin": "BTC", "side": "B", "px": "84541", "sz": "0.02", "hash": "0xbbb", "time": T0_MS + 35, "tid": 900000000000002, "users": ["0x3", "0x4"]},
        {"coin": "BTC", "side": "A", "px": "84540", "sz": "0.10", "hash": "0xccc", "time": T0_MS + 42, "tid": 900000000000003, "users": ["0x5", "0x6"]}]})))
    frames.append((50, j({"channel": "activeAssetCtx", "data": {"coin": "BTC", "ctx": {"dayNtlVlm": "1500000000.0", "prevDayPx": "84000.0", "markPx": "84538.0", "midPx": "84540.5",
                                                                                     "funding": "0.0000125", "openInterest": "25123.45", "oraclePx": "84510.0", "premium": "0.0003",
                                                                                     "impactPxs": ["84540.0", "84541.0"], "dayBaseVlm": "17800.0"}}})))
    frames.append((55, j({"channel": "pong"})))
    write("hyperliquid", "hyperliquid.ws", frames)


if __name__ == "__main__":
    coinbase()
    kraken()
    bitstamp()
    gemini()
    cryptocom()
    deribit()
    binance()
    bybit()
    okx()
    hyperliquid()
    print("fixtures written to", HERE)
