"""OKX v5 public WebSocket for BTC-USDT-SWAP: books5 (or bbo-tbt), trades, funding-rate,
mark-price, open-interest, index-tickers, liquidation-orders.

ASSUMED WIRE FORMAT (okx.com/docs-v5 "WebSocket Public Channel"; verify live with
``python scripts/smoke_feeds.py --venues okx``):

  endpoint   wss://ws.okx.com:8443/ws/v5/public
  subscribe  {"op":"subscribe","args":[{"channel":"books5","instId":"BTC-USDT-SWAP"},
              {"channel":"trades","instId":...},{"channel":"funding-rate","instId":...},
              {"channel":"mark-price","instId":...},{"channel":"open-interest","instId":...},
              {"channel":"index-tickers","instId":"BTC-USDT"},
              {"channel":"liquidation-orders","instType":"SWAP"}]}
             ack {"event":"subscribe","arg":{...},"connId":"..."}; errors {"event":"error","code":..,"msg":..}
  keepalive  send the text "ping" when idle (<30 s) -> server replies the text "pong" (not JSON)
  books5     {"arg":{"channel":"books5","instId":"BTC-USDT-SWAP"},"data":[{"asks":[["8446","95",
              "0","3"],...],"bids":[...],"instId":"BTC-USDT-SWAP","ts":"1597026383085",
              "seqId":123}]}   every message is a full top-5 SNAPSHOT;
              level = [price, size in CONTRACTS, "0" (deprecated), order count]
  bbo-tbt    same shape, top 1, tick-by-tick -> ExtBBO
  trades     {"arg":{"channel":"trades",...},"data":[{"instId","tradeId","px","sz"(contracts),
              "side":"buy"|"sell" (TAKER side),"ts"}]}
  funding-rate {"data":[{"instId","fundingRate","fundingTime"(ms),"nextFundingTime"(ms),...}]}
  mark-price   {"data":[{"instId","markPx","ts"}]}
  open-interest {"data":[{"instId","oi"(contracts),"oiCcy"(BTC),"ts"}]}
  index-tickers {"data":[{"instId":"BTC-USDT","idxPx","ts"}]}
  liquidation-orders {"data":[{"instId":"BTC-USDT-SWAP","details":[{"side":"buy"|"sell" (order
              side of the liquidation; buy = a short was liquidated),"posSide","bkPx","sz"
              (contracts),"ts"}]}]}  (all SWAP instruments; filtered to known BTC contracts)

CONTRACT VALUE (MUST VERIFY LIVE, GET /api/v5/public/instruments ctVal): BTC-USDT-SWAP = 0.01 BTC
per contract; sizes are converted to BTC. The funding interval is nextFundingTime - fundingTime.
"""

from __future__ import annotations

from typing import Any, ClassVar

import orjson

from dh.core.events import Event, ExtBBO, ExtTrade, FeedStatus, Liquidation, PerpState
from dh.feeds.base import FeedClient, NormalizerState, dumps, handle_marker, is_history, is_marker, ms_to_ns, snapshot_event, trade_seen

VENUE = "okx"
URL = "wss://ws.okx.com:8443/ws/v5/public"
CT_VAL_BTC: dict[str, float] = {"BTC-USDT-SWAP": 0.01, "BTC-USDC-SWAP": 0.0001}
INDEX_FOR: dict[str, str] = {"BTC-USDT-SWAP": "BTC-USDT", "BTC-USDC-SWAP": "BTC-USDC"}


def _ctval(inst: str) -> float | None:
    return CT_VAL_BTC.get(inst)


def _underlying_index(inst: str) -> str:
    return INDEX_FOR.get(inst, "-".join(inst.split("-")[:2]))


class OkxFeed(FeedClient):
    venue: ClassVar[str] = VENUE
    default_stream: ClassVar[str] = "okx.ws"
    default_url: ClassVar[str] = URL
    default_symbols: ClassVar[tuple[str, ...]] = ("BTC-USDT-SWAP",)
    default_channels: ClassVar[tuple[str, ...]] = (
        "books5",
        "trades",
        "funding-rate",
        "mark-price",
        "open-interest",
        "index-tickers",
        "liquidation-orders",
    )
    stale_after_s: ClassVar[float] = 5.0
    dead_after_s: ClassVar[float] = 30.0
    keepalive_interval_s: ClassVar[float | None] = 20.0
    resnapshot_interval_s: ClassVar[float] = 0.0  # books5/bbo-tbt are full snapshots

    def subscribe_messages(self) -> list[str]:
        args: list[dict[str, str]] = []
        for ch in self.channels:
            if ch == "liquidation-orders":
                args.append({"channel": ch, "instType": "SWAP"})
            elif ch == "index-tickers":
                for s in sorted({_underlying_index(s) for s in self.symbols}):
                    args.append({"channel": ch, "instId": s})
            else:
                args += [{"channel": ch, "instId": s} for s in self.symbols]
        return [dumps({"op": "subscribe", "args": args})]

    def keepalive_message(self) -> str | None:
        return "ping"

    @staticmethod
    def normalize(raw: bytes, recv_ns: int, state: NormalizerState) -> list[Event]:
        if is_marker(raw):
            return handle_marker(raw, recv_ns, state)
        if raw in (b"pong", b"ping"):
            return []
        msg = orjson.loads(raw)
        if "event" in msg:
            if msg.get("event") == "error":
                return [FeedStatus(recv_ns, 0, state.stream, "error", f"{msg.get('code')}: {msg.get('msg')}"[:300])]
            return []
        arg = msg.get("arg") or {}
        ch = arg.get("channel")
        out: list[Event] = []
        for d in msg.get("data") or ():
            inst = str(d.get("instId", arg.get("instId", "")))
            if ch in ("books5", "bbo-tbt", "books", "books50-l2-tbt", "books-l2-tbt"):
                cv = _ctval(inst)
                if cv is None:
                    continue
                bids = [(float(r[0]), float(r[1]) * cv) for r in d.get("bids", ())]
                asks = [(float(r[0]), float(r[1]) * cv) for r in d.get("asks", ())]
                tx = ms_to_ns(d.get("ts", 0))
                seq = int(d.get("seqId", 0) or 0)
                if ch == "bbo-tbt":
                    if bids and asks:
                        out.append(ExtBBO(recv_ns, tx, VENUE, inst, bids[0][0], bids[0][1], asks[0][0], asks[0][1], seq=seq))
                else:
                    out.append(snapshot_event(recv_ns, tx, VENUE, inst, bids, asks, seq=seq, depth_limited=True))
            elif ch == "trades":
                cv = _ctval(inst)
                if cv is None:
                    continue
                tid = str(d.get("tradeId", ""))
                tx = ms_to_ns(d.get("ts", 0))
                if trade_seen(state, inst, tx, tid) or is_history(state, tx):
                    continue
                side = str(d.get("side", ""))
                out.append(ExtTrade(recv_ns, tx, VENUE, inst, float(d["px"]), float(d["sz"]) * cv, side if side in ("buy", "sell") else "", tid))  # type: ignore[arg-type]
            elif ch in ("funding-rate", "mark-price", "open-interest"):
                f = state.fields.setdefault(inst, {})
                if ch == "funding-rate":
                    for k in ("fundingRate", "fundingTime", "nextFundingTime"):
                        if d.get(k) not in (None, ""):
                            f[k] = d[k]
                elif ch == "mark-price":
                    if d.get("markPx") not in (None, ""):
                        f["markPx"] = d["markPx"]
                elif d.get("oiCcy") not in (None, ""):
                    f["oiCcy"] = d["oiCcy"]
                elif d.get("oi") not in (None, "") and _ctval(inst):
                    f["oiCcy"] = str(float(d["oi"]) * (_ctval(inst) or 0))
                if f.get("markPx"):  # PerpState only once the mark is known (no zero marks)
                    out.append(_perp(inst, state, recv_ns, ms_to_ns(d.get("ts", 0) or 0)))
            elif ch == "index-tickers":
                state.fields.setdefault("index:" + inst, {})["idxPx"] = d.get("idxPx")
            elif ch == "liquidation-orders":
                cv = _ctval(inst)
                if cv is None:
                    continue
                for det in d.get("details", ()):
                    side = str(det.get("side", ""))
                    out.append(Liquidation(recv_ns, ms_to_ns(det.get("ts", 0)), VENUE, inst, side if side in ("buy", "sell") else "", float(det.get("bkPx") or 0), float(det.get("sz") or 0) * cv))  # type: ignore[arg-type]
        return out


def _perp(inst: str, state: NormalizerState, ts: int, tx: int) -> PerpState:
    f: dict[str, Any] = state.fields.get(inst, {})
    idx = state.fields.get("index:" + _underlying_index(inst), {}).get("idxPx")
    ft, nft = f.get("fundingTime"), f.get("nextFundingTime")
    interval = (int(nft) - int(ft)) // 1000 if ft and nft and int(nft) > int(ft) else 0
    return PerpState(
        ts=ts,
        ts_exch=tx,
        venue=VENUE,
        symbol=inst,
        mark=float(f.get("markPx") or 0),
        index=float(idx or 0),
        funding_rate=float(f.get("fundingRate") or 0),
        funding_interval_s=interval,
        next_funding_ts=ms_to_ns(ft) if ft else 0,
        open_interest=float(f.get("oiCcy") or 0),
    )
