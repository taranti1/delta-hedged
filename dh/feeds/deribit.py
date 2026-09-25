"""Deribit public WebSocket (JSON-RPC 2.0): BTC-PERPETUAL book/ticker/trades, the BTC price
index, the DVOL volatility index, and BTC option tickers for the nearest expiries.

ASSUMED WIRE FORMAT (docs.deribit.com API v2 "Subscriptions" and "public/get_instruments";
verify live with ``python scripts/smoke_feeds.py --venues deribit,deribit_options``):

  endpoint   wss://www.deribit.com/ws/api/v2
  requests   {"jsonrpc":"2.0","id":N,"method":"public/subscribe","params":{"channels":[...]}}
             {"jsonrpc":"2.0","id":N,"method":"public/set_heartbeat","params":{"interval":30}}
             -> server sends {"method":"heartbeat","params":{"type":"test_request"}} which must
             be answered with public/test, else the connection is closed.
             {"jsonrpc":"2.0","id":N,"method":"public/get_instruments",
              "params":{"currency":"BTC","kind":"option","expired":false}}
             -> result [{"instrument_name":"BTC-27SEP26-60000-C","expiration_timestamp":<ms>,
                 "strike":60000.0,"option_type":"call","is_active":true,...}]
             {"jsonrpc":"2.0","id":N,"method":"public/get_index_price","params":{"index_name":"btc_usd"}}
  notif.     {"jsonrpc":"2.0","method":"subscription","params":{"channel":"...","data":{...}}}
  book.{instrument}.100ms   data {"type":"snapshot"|"change","timestamp":<ms>,"change_id":N,
             "prev_change_id":N-k (changes only),"bids":[["new"|"change"|"delete",price,amount]],
             "asks":[...]}; amount is the ABSOLUTE new size (0 on delete). prev_change_id must
             equal the previous change_id (gap -> resubscribe). Full depth.
  ticker.{instrument}.100ms data {"timestamp","best_bid_price","best_bid_amount","best_ask_price",
             "best_ask_amount","mark_price","index_price","funding_8h","current_funding",
             "open_interest", options also "mark_iv","bid_iv","ask_iv" (PERCENT),
             "underlying_price","greeks"}
  trades.{instrument}.100ms data [{"trade_id","trade_seq","timestamp","price","amount",
             "direction":"buy"|"sell" (TAKER direction),"liquidation":"M"|"T"|"MT" (optional)}]
  deribit_price_index.btc_usd       data {"timestamp","price","index_name"}
  deribit_volatility_index.btc_usd  data {"timestamp","volatility","index_name"}  (percent)

UNITS (MUST VERIFY LIVE): BTC-PERPETUAL/futures amounts and open_interest are in USD
(converted here to BTC = USD / price); option amounts are in BTC; option prices are in BTC.
Option expiries are 08:00 UTC on the date in the instrument name.

Event mapping: book -> ExtBookSnapshot/ExtBookDelta, ticker -> PerpState + ExtBBO (perp) or
OptionQuote (IVs as fractions), trades -> ExtTrade (+ Liquidation), price index ->
IndexTick(index_id='deribit:btc_usd', feed='deribit'), DVOL -> IndexTick(index_id=
'deribit:dvol_btc_usd', value as a FRACTION, feed='deribit'). Consumers of IndexTick must filter
on index_id ('BRTI' is the settlement benchmark).
"""

from __future__ import annotations

import asyncio
import calendar
from functools import lru_cache
from typing import Any, ClassVar

import orjson

from dh.core.events import Event, ExtBBO, ExtBookDelta, ExtTrade, FeedStatus, IndexTick, Liquidation, OptionQuote, PerpState
from dh.core.units import NS_PER_S
from dh.feeds.base import (
    FeedClient,
    NormalizerState,
    book_gap,
    book_snapshot_ok,
    book_valid,
    dumps,
    handle_marker,
    is_history,
    is_marker,
    ms_to_ns,
    snapshot_event,
    trade_seen,
)

VENUE = "deribit"
URL = "wss://www.deribit.com/ws/api/v2"
EXPIRY_HOUR_UTC = 8
FUNDING_INTERVAL_S = 8 * 3600  # funding_8h is quoted per 8 hours (funding accrues continuously)
_MONTHS = {m: i for i, m in enumerate(("JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"), 1)}
INDEX_ID_PREFIX = "deribit:"


@lru_cache(maxsize=65536)
def parse_option_name(name: str) -> tuple[int, float, str] | None:
    """'BTC-27SEP26-60000-C' -> (expiry_ns at 08:00 UTC, strike, 'C'|'P'); None if not an option."""
    parts = name.split("-")
    if len(parts) != 4 or parts[3] not in ("C", "P"):
        return None
    ds = parts[1]
    try:
        day = int(ds[:-5])
        mon = _MONTHS[ds[-5:-2]]
        year = 2000 + int(ds[-2:])
        strike = float(parts[2].replace("d", "."))
    except (KeyError, ValueError):
        return None
    exp = calendar.timegm((year, mon, day, EXPIRY_HOUR_UTC, 0, 0, 0, 0, 0)) * NS_PER_S
    return exp, strike, parts[3]


def is_inverse_usd(instrument: str) -> bool:
    """Perpetual/future sized in USD (BTC-PERPETUAL, BTC-27SEP26); not options, not *_USDC."""
    return parse_option_name(instrument) is None and "_" not in instrument.split("-")[0]


def _size_btc(instrument: str, amount: float, price: float) -> float:
    if is_inverse_usd(instrument):
        return amount / price if price > 0 else 0.0
    return amount


def select_options(
    instruments: list[dict[str, Any]], index_px: float, now_ms: int, n_expiries: int, moneyness: float, max_n: int
) -> list[str]:
    """Pure option selection: nearest ``n_expiries`` live expiries, strikes with
    |K/S - 1| <= moneyness, closest-to-the-money first, at most ``max_n`` names (sorted)."""
    live = [
        i
        for i in instruments
        if i.get("kind", "option") == "option"
        and i.get("is_active", True)
        and int(i.get("expiration_timestamp", 0)) > now_ms
        and "instrument_name" in i
    ]
    expiries = sorted({int(i["expiration_timestamp"]) for i in live})[: max(0, n_expiries)]
    chosen = [
        i
        for i in live
        if int(i["expiration_timestamp"]) in expiries
        and index_px > 0
        and abs(float(i.get("strike", 0.0)) / index_px - 1.0) <= moneyness
    ]
    chosen.sort(key=lambda i: (abs(float(i.get("strike", 0.0)) - index_px), int(i["expiration_timestamp"]), i["instrument_name"]))
    return sorted(i["instrument_name"] for i in chosen[:max_n])


class DeribitFeed(FeedClient):
    """One Deribit connection. Configure two instances: ``deribit.ws`` (perp + indices) and
    ``deribit.options`` (channels: [options]) so option volume never delays the perp book."""

    venue: ClassVar[str] = VENUE
    default_stream: ClassVar[str] = "deribit.ws"
    default_url: ClassVar[str] = URL
    default_symbols: ClassVar[tuple[str, ...]] = ("BTC-PERPETUAL",)
    default_channels: ClassVar[tuple[str, ...]] = ("book", "ticker", "trades", "index", "dvol")
    stale_after_s: ClassVar[float] = 10.0
    dead_after_s: ClassVar[float] = 60.0
    ws_ping_interval_s: ClassVar[float | None] = None  # JSON-RPC heartbeats instead
    max_frame_bytes: ClassVar[int] = 64 * 2**20

    def __init__(self, *a: Any, **kw: Any) -> None:
        super().__init__(*a, **kw)
        self._id = 100
        self._idx_req: int | None = None
        self._instr_req: int | None = None
        self._index_px = 0.0
        self._instruments: list[dict[str, Any]] | None = None
        self._option_chans: list[str] = []

    # ------------------------------------------------------------------ config
    @property
    def interval(self) -> str:
        return str(self.options.get("interval", "100ms"))

    @property
    def index_name(self) -> str:
        return str(self.options.get("index_name", "btc_usd"))

    def _rpc(self, method: str, params: dict[str, Any]) -> tuple[int, str]:
        self._id += 1
        return self._id, dumps({"jsonrpc": "2.0", "id": self._id, "method": method, "params": params})

    def _book_channels(self) -> list[str]:
        return [f"book.{s}.{self.interval}" for s in self.symbols] if "book" in self.channels else []

    def _subscribe_batches(self, method: str, chans: list[str], batch: int = 100) -> list[str]:
        return [self._rpc(method, {"channels": chans[i : i + batch]})[1] for i in range(0, len(chans), batch)]

    # ------------------------------------------------------------------ hooks
    def subscribe_messages(self) -> list[str]:
        self._option_chans = []
        self._instruments = None
        out = [self._rpc("public/set_heartbeat", {"interval": int(self.options.get("heartbeat_s", 30))})[1]]
        chans = list(self._book_channels())
        for s in self.symbols:
            if "ticker" in self.channels:
                chans.append(f"ticker.{s}.{self.interval}")
            if "trades" in self.channels:
                chans.append(f"trades.{s}.{self.interval}")
        if "index" in self.channels:
            chans.append(f"deribit_price_index.{self.index_name}")
        if "dvol" in self.channels:
            chans.append(f"deribit_volatility_index.{self.index_name}")
        out += self._subscribe_batches("public/subscribe", chans)
        if "options" in self.channels:
            out += self._discovery_requests()
        return out

    def _discovery_requests(self) -> list[str]:
        self._idx_req, m1 = self._rpc("public/get_index_price", {"index_name": self.index_name})
        self._instr_req, m2 = self._rpc(
            "public/get_instruments", {"currency": str(self.options.get("currency", "BTC")), "kind": "option", "expired": False}
        )
        return [m1, m2]

    def resubscribe_messages(self) -> list[str] | None:
        chans = self._book_channels()
        if not chans:
            return []
        return self._subscribe_batches("public/unsubscribe", chans) + self._subscribe_batches("public/subscribe", chans)

    def extra_tasks(self) -> list:
        if "options" in self.channels:
            return [self._rediscover_loop()]
        return []

    async def _rediscover_loop(self) -> None:
        period = float(self.options.get("rediscover_s", 900))
        while True:
            await asyncio.sleep(period)
            for m in self._discovery_requests():
                await self.send(m)

    def control_replies(self, raw: bytes) -> list[str]:
        out: list[str] = []
        if b"test_request" in raw:
            out.append(self._rpc("public/test", {})[1])
        if (self._idx_req is not None or self._instr_req is not None) and b'"result"' in raw:
            msg = orjson.loads(raw)
            rid = msg.get("id")
            if rid is not None and rid == self._idx_req:
                self._index_px = float((msg.get("result") or {}).get("index_price", 0.0))
                self._idx_req = None
            elif rid is not None and rid == self._instr_req:
                self._instruments = list(msg.get("result") or [])
                self._instr_req = None
            if self._instruments is not None and self._index_px > 0 and self._idx_req is None and self._instr_req is None:
                out += self._apply_option_selection()
        return out

    def _apply_option_selection(self) -> list[str]:
        assert self._instruments is not None
        names = select_options(
            self._instruments,
            self._index_px,
            self.clock_ns() // 1_000_000,
            int(self.options.get("option_expiries", 2)),
            float(self.options.get("option_moneyness", 0.10)),
            int(self.options.get("option_max", 300)),
        )
        ival = str(self.options.get("option_interval", self.interval))
        new = [f"ticker.{n}.{ival}" for n in names]
        old = set(self._option_chans)
        add = [c for c in new if c not in old]
        drop = [c for c in self._option_chans if c not in set(new)]
        self._option_chans = new
        self._instruments = None
        return self._subscribe_batches("public/unsubscribe", drop) + self._subscribe_batches("public/subscribe", add)

    # ------------------------------------------------------------------ normalization
    @staticmethod
    def normalize(raw: bytes, recv_ns: int, state: NormalizerState) -> list[Event]:
        if is_marker(raw):
            return handle_marker(raw, recv_ns, state)
        msg = orjson.loads(raw)
        method = msg.get("method")
        if method == "subscription":
            p = msg.get("params") or {}
            ch = str(p.get("channel", ""))
            data = p.get("data")
            kind = ch.split(".", 1)[0]
            if kind == "book":
                return _book(ch.split(".")[1], data, recv_ns, state)
            if kind == "ticker":
                return _ticker(data, recv_ns, state)
            if kind == "trades":
                return _trades(data, recv_ns, state)
            if kind == "deribit_price_index":
                return [
                    IndexTick(recv_ns, ms_to_ns(data.get("timestamp", 0)), INDEX_ID_PREFIX + str(data.get("index_name", "")), float(data["price"]), "deribit")
                ]
            if kind == "deribit_volatility_index":
                return [
                    IndexTick(
                        recv_ns,
                        ms_to_ns(data.get("timestamp", 0)),
                        INDEX_ID_PREFIX + "dvol_" + str(data.get("index_name", "")),
                        float(data["volatility"]) / 100.0,
                        "deribit",
                    )
                ]
            return []
        if "error" in msg and msg.get("error"):
            err = msg["error"]
            return [FeedStatus(recv_ns, 0, state.stream, "error", f"{err.get('code')}: {err.get('message')}"[:300])]
        return []


def _book(inst: str, d: dict[str, Any], recv_ns: int, state: NormalizerState) -> list[Event]:
    out: list[Event] = []
    tx = ms_to_ns(d.get("timestamp", 0))
    cid = int(d.get("change_id", 0))
    key = f"cid:{inst}"
    if d.get("type") == "snapshot":
        bids = [(float(px), _size_btc(inst, float(a), float(px))) for _, px, a in d.get("bids", ())]
        asks = [(float(px), _size_btc(inst, float(a), float(px))) for _, px, a in d.get("asks", ())]
        state.seq[key] = cid
        out.append(snapshot_event(recv_ns, tx, VENUE, inst, bids, asks, seq=cid, depth_limited=False))
        out += book_snapshot_ok(state, inst, recv_ns)
        return out
    if not book_valid(state, inst):
        state.bump("deltas_suppressed")
        return out
    prev = int(d.get("prev_change_id", 0))
    last = state.seq.get(key)
    if last is not None and prev != last:
        if cid <= last:
            state.bump("stale_change_dropped")
            return out
        return book_gap(state, inst, recv_ns, f"prev_change_id {prev} != last change_id {last}")
    state.seq[key] = cid
    changes: list[tuple[str, float, float]] = []
    for side, rows in (("b", d.get("bids", ())), ("a", d.get("asks", ()))):
        for action, px, a in rows:
            p = float(px)
            changes.append((side, p, 0.0 if action == "delete" else _size_btc(inst, float(a), p)))
    if changes:
        out.append(ExtBookDelta(recv_ns, tx, VENUE, inst, tuple(changes), seq=cid, prev_seq=prev))
    return out


def _f(d: dict[str, Any], k: str) -> float:
    v = d.get(k)
    return float(v) if v is not None else 0.0


def _ticker(d: dict[str, Any], recv_ns: int, state: NormalizerState) -> list[Event]:
    inst = str(d.get("instrument_name", ""))
    tx = ms_to_ns(d.get("timestamp", 0))
    opt = parse_option_name(inst)
    if opt is not None:
        exp, strike, cp = opt
        return [
            OptionQuote(
                ts=recv_ns,
                ts_exch=tx,
                venue=VENUE,
                instrument=inst,
                expiry_ts=exp,
                strike=strike,
                cp=cp,  # type: ignore[arg-type]
                bid=_f(d, "best_bid_price"),
                ask=_f(d, "best_ask_price"),
                mark_iv=_f(d, "mark_iv") / 100.0,
                bid_iv=_f(d, "bid_iv") / 100.0,
                ask_iv=_f(d, "ask_iv") / 100.0,
                underlying=_f(d, "underlying_price") or _f(d, "index_price"),
            )
        ]
    mark = _f(d, "mark_price")
    oi = _f(d, "open_interest")
    out: list[Event] = [
        PerpState(
            ts=recv_ns,
            ts_exch=tx,
            venue=VENUE,
            symbol=inst,
            mark=mark,
            index=_f(d, "index_price"),
            funding_rate=_f(d, "funding_8h"),
            funding_interval_s=FUNDING_INTERVAL_S if "funding_8h" in d else 0,
            next_funding_ts=0,
            open_interest=(oi / mark if is_inverse_usd(inst) and mark > 0 else oi),
        )
    ]
    bb, ba = _f(d, "best_bid_price"), _f(d, "best_ask_price")
    if bb > 0 and ba > 0:
        out.append(
            ExtBBO(recv_ns, tx, VENUE, inst, bb, _size_btc(inst, _f(d, "best_bid_amount"), bb), ba, _size_btc(inst, _f(d, "best_ask_amount"), ba))
        )
    return out


def _trades(rows: list[dict[str, Any]], recv_ns: int, state: NormalizerState) -> list[Event]:
    out: list[Event] = []
    for t in rows or ():
        inst = str(t.get("instrument_name", ""))
        tid = str(t.get("trade_id", ""))
        tx = ms_to_ns(t.get("timestamp", 0))
        if trade_seen(state, inst, tx, tid) or is_history(state, tx):
            continue
        px = float(t["price"])
        size = _size_btc(inst, float(t["amount"]), px)
        direction = str(t.get("direction", ""))
        aggr = direction if direction in ("buy", "sell") else ""
        out.append(ExtTrade(recv_ns, tx, VENUE, inst, px, size, aggr, tid))  # type: ignore[arg-type]
        liq = t.get("liquidation")
        if liq and aggr:
            opposite = "sell" if aggr == "buy" else "buy"
            if "T" in liq:  # taker order was a liquidation: its side = trade direction
                out.append(Liquidation(recv_ns, tx, VENUE, inst, aggr, px, size))  # type: ignore[arg-type]
            if "M" in liq:  # resting maker order was a liquidation
                out.append(Liquidation(recv_ns, tx, VENUE, inst, opposite, px, size))  # type: ignore[arg-type]
    return out
