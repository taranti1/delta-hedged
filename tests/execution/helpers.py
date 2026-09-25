"""Shared builders for dh.execution tests (spec-shaped normalized events, scripted strategies)."""

from __future__ import annotations

import dataclasses

from dh.core.actions import CancelOrder, PlaceOrder
from dh.core.events import (
    CancelAck,
    KalshiBookDelta,
    KalshiBookSnapshot,
    KalshiFill,
    KalshiOrderUpdate,
    KalshiTrade,
    OrderAck,
    OrderReject,
)
from dh.core.units import PX_ONE
from dh.execution import KalshiExchangeSim, LatencyModel, OrderManager

T = "KXBTCD-26SEP2517-T85000"
MS = 1_000_000
S = 1_000_000_000


def no_fee(px: int, qty: int, is_taker: bool) -> int:
    return 0


def simple_fee(px: int, qty: int, is_taker: bool) -> int:
    """Kalshi-shaped quadratic fee (taker 0.07, maker 0.0175) on px*(1-px), ceil to micros."""
    rate_num = 700 if is_taker else 175  # per 10_000
    # fee$ = rate * contracts * P * (1-P) ; contracts = qty/100 ; P = px/1e4
    num = rate_num * qty * px * (10_000 - px)  # scaled by 1e4 * 1e2 * 1e4 * 1e4
    den = 10_000 * 100 * 10_000 * 10_000
    return -(-num * 1_000_000 // den)


def snap(ts: int, yes=((4500, 1000),), no=((5300, 700),), ticker: str = T) -> KalshiBookSnapshot:
    return KalshiBookSnapshot(ts, 0, ticker, 1, 1, tuple(yes), tuple(no))


def delta(ts: int, side: str, px: int, d: int, own: str = "", ticker: str = T) -> KalshiBookDelta:
    return KalshiBookDelta(ts, 0, ticker, 1, 0, side, px, d, own)


_TID = [0]


def trade(ts: int, yes_px: int, qty: int, taker_side: str, ticker: str = T, trade_id: str = "") -> KalshiTrade:
    _TID[0] += 1
    return KalshiTrade(ts, 0, ticker, trade_id or f"pub{_TID[0]}", yes_px, qty, taker_side)


def fill(ts: int, coid: str, oid: str, side: str, px: int, qty: int, trade_id: str, *, post: int | None = None,
         fee: int = 0, taker: bool = False, ts_exch: int = 0, ticker: str = T) -> KalshiFill:
    return KalshiFill(ts, ts_exch, ticker, trade_id, oid, coid, side, px, qty, taker, fee,
                      0 if post is None else post, post is not None)


def ack(ts: int, coid: str, oid: str, fill_qty: int = 0, remaining: int = 0, request: str = "create",
        ts_exch: int = 0, ticker: str = T) -> OrderAck:
    return OrderAck(ts, ts_exch, coid, oid, ticker, fill_qty, remaining, request)


def reject(ts: int, coid: str, reason: str, request: str = "create", ticker: str = T) -> OrderReject:
    return OrderReject(ts, 0, coid, ticker, reason, 400, request)


def cancel_ack(ts: int, coid: str, oid: str, qty: int, ticker: str = T) -> CancelAck:
    return CancelAck(ts, 0, coid, oid, ticker, qty)


def update(ts: int, coid: str, oid: str, status: str, side: str, px: int, initial: int, filled: int, remaining: int,
           ts_exch: int = 0, ticker: str = T) -> KalshiOrderUpdate:
    return KalshiOrderUpdate(ts, ts_exch, ticker, oid, coid, status, side, px, initial, filled, remaining)


class Scripted:
    """Strategy stand-in: fires scheduled actions at the first event at/after their time and
    feeds every event to an OrderManager (so tests can check live-identical state)."""

    def __init__(self, schedule: list[tuple[int, object]], om: OrderManager | None = None) -> None:
        self.schedule = sorted(schedule, key=lambda x: x[0])
        self.i = 0
        self.om = om or OrderManager()
        self.events: list = []
        self.order_events: list = []

    def __call__(self, ev) -> list:
        self.events.append(ev)
        self.order_events.extend(self.om.on_event(ev))
        out = []
        while self.i < len(self.schedule) and self.schedule[self.i][0] <= ev.ts:
            a = self.schedule[self.i][1]
            self.i += 1
            if isinstance(a, PlaceOrder):
                self.om.request_place(a, ev.ts)
            elif isinstance(a, CancelOrder):
                self.om.request_cancel(a, ev.ts)
            out.append(a)
        return out


def make_sim(policy: str = "B", *, lat: LatencyModel | None = None, fee=no_fee, seed: int = 7, mult: float = 1.0,
             **kw) -> KalshiExchangeSim:
    return KalshiExchangeSim(lat or LatencyModel.zero(), policy, fee, seed, latency_multiplier=mult, **kw)


# ---------------------------------------------------------------------- random market streams
YES_LEVELS = (4200, 4300, 4400, 4500)  # YES bids (YES scale)
NO_LEVELS = (5100, 5200, 5300, 5400)  # NO bids == YES asks at 49, 48, 47, 46 (never crossed)


def gen_stream(seed: int, n: int, *, ticker: str = T, t0: int = 0) -> list:
    """A self-consistent recorded Kalshi stream: snapshot, joins, cancels, trades with their
    book deltas in either order (sometimes separated in time), multi-level sweeps."""
    import random

    rng = random.Random(seed)
    book = {"yes": {p: rng.randint(0, 1500) for p in YES_LEVELS}, "no": {p: rng.randint(0, 1500) for p in NO_LEVELS}}
    ts = t0
    out: list = [KalshiBookSnapshot(ts, 0, ticker, 1, 1, tuple((p, q) for p, q in book["yes"].items() if q),
                                    tuple((p, q) for p, q in book["no"].items() if q))]
    gaps = (0, 1 * MS, 5 * MS, 50 * MS, 300 * MS, 1 * S)
    for _ in range(n):
        ts += rng.choice(gaps)
        op = rng.choice(("add", "add", "cancel", "hit", "hit", "sweep"))
        side = rng.choice(("yes", "no"))
        lv = book[side]
        if op == "add":
            px = rng.choice(tuple(lv))
            q = rng.randint(1, 800)
            lv[px] += q
            out.append(KalshiBookDelta(ts, 0, ticker, 1, 0, side, px, q))
            continue
        live = sorted((p for p, q in lv.items() if q > 0), reverse=True)
        if not live:
            continue
        if op == "cancel":
            px = rng.choice(live)
            q = rng.randint(1, lv[px])
            lv[px] -= q
            out.append(KalshiBookDelta(ts, 0, ticker, 1, 0, side, px, -q))
            continue
        levels = live[:1] if op == "hit" else live[: rng.randint(2, 3)]
        taker = "no" if side == "yes" else "yes"  # selling YES hits YES bids; buying YES hits NO bids
        msgs = []
        for i, px in enumerate(levels):
            q = lv[px] if (op == "sweep" and i < len(levels) - 1) else rng.randint(1, lv[px])
            lv[px] -= q
            yes_px = px if side == "yes" else PX_ONE - px
            _TID[0] += 1
            tr = KalshiTrade(ts, 0, ticker, f"pub{_TID[0]}", yes_px, q, taker)
            de = KalshiBookDelta(ts, 0, ticker, 1, 0, side, px, -q)
            msgs.extend([tr, de] if rng.random() < 0.5 else [de, tr])
        lag = rng.choice((0, 0, 2 * MS, 20 * MS))
        for m in msgs:  # the second channel may lag a little
            out.append(m if isinstance(m, KalshiTrade) else dataclasses.replace(m, ts=m.ts + lag) if lag else m)
    out.sort(key=lambda e: e.ts)  # stable: same-ts order preserved
    return out


def gen_schedule(seed: int, n_orders: int, horizon_ns: int, *, ticker: str = T) -> list[tuple[int, object]]:
    """Fixed (non-reactive) post-only order schedule: bids <= 46c, asks >= 47c (never self-cross),
    some canceled later."""
    import random

    rng = random.Random(seed * 7919 + 1)
    sched: list[tuple[int, object]] = []
    for k in range(n_orders):
        t = rng.randint(0, max(1, horizon_ns // 2))
        side = rng.choice(("bid", "ask"))
        px = rng.choice((4400, 4500, 4600)) if side == "bid" else rng.choice((4700, 4800, 4900))
        coid = f"o{k}"
        sched.append((t, PlaceOrder(coid, ticker, side, px, rng.randint(1, 20) * 100)))
        if rng.random() < 0.4:
            sched.append((t + rng.randint(1, max(2, horizon_ns // 2)), CancelOrder(coid, ticker)))
    sched.sort(key=lambda x: x[0])
    return sched
