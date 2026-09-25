"""MarketMaker: the deterministic Strategy (identical in live trading and replay).

    actions = mm.on_event(ev)

Per event it updates state (books, benchmark, vol, orders, positions, risk). On every Timer
(and immediately after a benchmark move larger than `requote_move_sigma` one-second sds) it
runs the quote cycle of docs/MODELS.md / docs/LIFECYCLE.md:

  health -> nowcast -> per event: window state -> fair-value band + greeks per market
  -> scenario grid from positions -> capacity from hard limits -> per market-side
  keep / replace / place / pull by EV rate -> hedge decision -> equity check.

Nothing here reads the wall clock, the network or the filesystem.
"""

from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass, field
from typing import Iterable

import numpy as np

from dh.core.actions import (
    Action,
    CancelAll,
    CancelOrder,
    CreateOrderGroup,
    Halt,
    Log,
    PlaceHedge,
    PlaceOrder,
    ResetOrderGroup,
)
from dh.core.book import ExtBook, KalshiBook
from dh.core.events import (
    CancelAck,
    ExtBBO,
    ExtBookDelta,
    ExtBookSnapshot,
    FeedStatus,
    HedgeFill,
    HedgeOrderUpdate,
    IndexTick,
    KalshiBookDelta,
    KalshiBookSnapshot,
    KalshiFeeUpdate,
    KalshiFill,
    KalshiMarketLifecycle,
    KalshiOrderGroupUpdate,
    KalshiOrderUpdate,
    KalshiPositionSnapshot,
    KalshiTrade,
    OrderAck,
    OrderReject,
    Settlement,
    Timer,
)
from dh.core.market import MarketSpec
from dh.core.strategy import IdGen
from dh.core.units import NS_PER_MS, NS_PER_S, PX_SCALE, QTY_SCALE
from dh.execution.order_manager import OrderManager
from dh.execution.queue import QueueEstimator
from dh.models.fairvalue import digital
from dh.models.fvmodel import FairValueModel, load_recommended_config
from dh.models.tails import GAUSS, make_tail
from dh.settlement.window import SettlementTracker
from dh.strategy.config import StrategyConfig
from dh.strategy.fill_model import AdverseSelectionModel, FillIntensityModel, SegmentFlow
from dh.strategy.hedging import decide_hedge, hedge_cost_frac
from dh.strategy.quoting import ExistingOrder, MarketQuoteContext, decide_side
from dh.strategy.risk import RiskEngine
from dh.strategy.scenario import EventGrid, book_pnl, payoff_vector, worst_case_loss_with_orders

SEC_YR = 365.0 * 24 * 3600


@dataclass
class MarketFV:
    ts: int
    F: float
    F_lo: float
    F_hi: float
    delta: float
    gamma: float
    z: float
    sd_R: float
    tail_nu: float
    z_cap: float = math.nan  # 'between' markets: z of the cap strike

    @property
    def z_near(self) -> float:
        """Distance (in sd) to the nearest strike boundary (audit M7: 'between' has two)."""
        if math.isnan(self.z_cap):
            return abs(self.z)
        return min(abs(self.z), abs(self.z_cap))


@dataclass
class MMStats:
    cycles: int = 0
    quotes_placed: int = 0
    cancels: int = 0
    fills: int = 0
    halted_cycles: int = 0
    skipped_no_fee: int = 0
    reasons: dict[str, int] = field(default_factory=dict)

    def bump(self, key: str) -> None:
        self.reasons[key] = self.reasons.get(key, 0) + 1


class MarketMaker:
    ORDER_GROUP_ID = "dh-main"

    def __init__(
        self,
        cfg: StrategyConfig,
        specs: Iterable[MarketSpec],
        *,
        fv_model: FairValueModel | None = None,
        fee_engine=None,
        flow_segments: dict[tuple[str, str, str], SegmentFlow] | None = None,
        as_coefs: dict | None = None,
        book_includes_own: bool = False,
        use_order_group: bool = True,
        log_fv_every_ns: int = NS_PER_S,
        requote_move_sigma: float = 2.0,
    ) -> None:
        self.cfg = cfg
        self.specs: dict[str, MarketSpec] = {s.ticker: s for s in specs}
        self.books: dict[str, KalshiBook] = {t: KalshiBook(t) for t in self.specs}
        self.ext: dict[str, ExtBook] = {}
        self.tracker = SettlementTracker()
        self.fv = fv_model or FairValueModel.from_config(load_recommended_config())
        if fee_engine is None:
            from dh.kalshi.fees import FeeEngine

            fee_engine = FeeEngine.from_config()
        self.fee_engine = fee_engine
        self.fee_sched: dict[str, object] = {}
        self._fee_cache: dict[tuple[str, int, int, str], float] = {}  # cleared on fee changes
        for t, s in self.specs.items():
            self._resolve_fee(t, s)
        self.om = OrderManager()
        self.queue = QueueEstimator("realistic", level_qty=self._level_qty, book_includes_own=book_includes_own)
        # live book (contains our orders): an order joins the queue when our own positive book
        # delta shows it; if that delta beat the ack it is remembered here (coid -> ts)
        self.book_includes_own = book_includes_own
        self._own_delta_seen: dict[str, int] = {}
        self._queue_pending_since: dict[str, int] = {}
        self.flow = FillIntensityModel(cfg.fill, dict(flow_segments or {}))
        self.adverse = AdverseSelectionModel(cfg.adverse, dict(as_coefs or {}))
        self.risk = RiskEngine(cfg.risk)
        self.ids = IdGen(cfg.run_prefix)
        self.alt_tail = make_tail(cfg.fair_value.band_tail_alt) if cfg.fair_value.band_tail_alt else GAUSS
        self.hedge_pos = 0.0
        self.hedge_cash = 0.0
        self.hedge_fees = 0.0
        self.hedge_pending: dict[str, float] = {}
        self.fvc: dict[str, MarketFV] = {}
        self.fv_hist: dict[str, deque[tuple[int, float]]] = {t: deque() for t in self.specs}
        self.last_fv_log: dict[str, tuple[int, float]] = {}
        self.log_fv_every_ns = log_fv_every_ns
        self.last_place: dict[tuple[str, str], int] = {}
        self.settled: dict[str, int] = {}  # ticker -> settlement px
        self.settled_cash = 0.0
        self.event_pnl: dict[str, float] = {}
        self.last_cycle_ns = 0
        self.last_cycle_spot = math.nan
        self.last_brti_ns = 0
        self.requote_move_sigma = requote_move_sigma
        self.use_order_group = use_order_group
        self.group_created = False
        self.group_triggered_at = 0
        self.halted_all = False
        self.paused: set[str] = set()
        self.base_fee: dict[str, tuple[str, float]] = {t: (sp.fee_type, sp.fee_multiplier) for t, sp in self.specs.items()}
        self.fee_tolerance_micros = 10_000  # one cent of balance rounding per fill
        self.stats = MMStats()
        self.brti_hist: deque[tuple[int, float]] = deque()

    # ================================================================== universe
    def add_markets(self, specs: Iterable[MarketSpec]) -> list[str]:
        """Add newly listed markets (hourly roll-over) without restarting. Deterministic:
        call it from the event stream (the live runner emits it on discovery; replay does the
        same at the recorded discovery time). Returns the tickers actually added."""
        added = []
        for s in specs:
            if s.ticker in self.specs:
                continue
            self.specs[s.ticker] = s
            self.books[s.ticker] = KalshiBook(s.ticker)
            self.fv_hist[s.ticker] = deque()
            self.base_fee[s.ticker] = (s.fee_type, s.fee_multiplier)
            self._resolve_fee(s.ticker, s)
            added.append(s.ticker)
        return added

    def prune_settled(self, before_ns: int) -> int:
        """Drop state for markets settled before `before_ns` with no position/working orders."""
        drop = [t for t, s in self.specs.items()
                if t in self.settled and s.expiration_ts < before_ns and not self.om.working(t)]
        for t in drop:
            self.specs.pop(t, None)
            self.books.pop(t, None)
            self.fv_hist.pop(t, None)
            self.fvc.pop(t, None)
            self.last_fv_log.pop(t, None)
        return len(drop)

    # ================================================================== helpers
    def _resolve_fee(self, ticker: str, spec: MarketSpec) -> None:
        if not spec.fee_type:
            return  # unresolved -> market is not tradable (never assume a fee schedule)
        try:
            sched = self.fee_engine.schedule_for_spec(spec.fee_type, spec.fee_multiplier)
        except Exception:  # unsupported fee type (e.g. flat): refuse to trade
            return
        if getattr(sched, "supported", True):
            self.fee_sched[ticker] = sched
            self._fee_cache.clear()

    def _order_fee(self, ticker: str, px: int, size: float, side: str) -> float:
        """Exact net fee $ per contract of a maker order of ``size`` contracts filled in one
        fill: trade fee plus Kalshi's balance rounding (zero for a fee-free maker fill at a
        whole-cent price and whole contracts; up to 1c per order otherwise; audit M8)."""
        qty = int(round(size * QTY_SCALE))
        if qty <= 0:
            return float("inf")
        key = (ticker, px, qty, side)
        v = self._fee_cache.get(key)
        if v is None:
            fb = self.fee_sched[ticker].single_fill_fees(px, qty, False, side)
            v = self._fee_cache[key] = fb.net_micros / 1e6 / (qty / QTY_SCALE)
        return v

    def _level_qty(self, ticker: str, book: str, px: int) -> int:
        b = self.books.get(ticker)
        if b is None:
            return 0
        return b.yes_bids.get(px, 0) if book == "yes" else b.no_bids.get(px, 0)

    def _spot(self) -> float | None:
        return self.tracker.latest_value()

    def _sigma_1s(self, now: int, spot: float) -> float:
        # $ per sqrt(second) for a 1-minute-ahead window; floor from config
        s = self.fv.sigma_abs(now, now + 60 * NS_PER_S, spot) if self.fv.ready else 0.0
        floor = spot * self.cfg.fair_value.vol_floor_ann / math.sqrt(SEC_YR)
        return max(s, floor)

    def _contracts(self, ticker: str) -> float:
        return self.om.position(ticker) / QTY_SCALE

    def _cost_basis(self, ticker: str) -> float:
        q = self.om.position(ticker)
        if q == 0:
            return 0.0
        return -self.om.cash_micros(ticker) / 1e6 / (q / QTY_SCALE)

    # ================================================================== event entry point
    def on_event(self, ev) -> list[Action]:
        out: list[Action] = []
        if isinstance(ev, KalshiBookSnapshot):
            b = self.books.get(ev.ticker)
            if b is not None:
                b.apply_snapshot(ev)
                self.queue.on_snapshot(ev)
        elif isinstance(ev, KalshiBookDelta):
            b = self.books.get(ev.ticker)
            if b is not None:
                if not b.apply_delta(ev):
                    out += self._cancel_market(ev.ts, ev.ticker, "book_invalid")
                else:
                    own = ev.own_client_order_id
                    if own and self.book_includes_own and ev.delta > 0:
                        if own in self.queue.orders:
                            self._queue_pending_since.pop(own, None)  # activated by this delta
                        else:
                            self._own_delta_seen[own] = ev.ts  # our delta beat the ack
                    self.queue.on_book_delta(ev)
        elif isinstance(ev, KalshiTrade):
            if ev.ticker in self.books:
                self.queue.on_trade(ev)
        elif isinstance(ev, IndexTick):
            out += self._on_index(ev)
        elif isinstance(ev, (ExtBBO, ExtBookSnapshot, ExtBookDelta)):
            b = self.ext.setdefault(ev.venue, ExtBook(ev.venue, ev.symbol))
            if isinstance(ev, ExtBBO):
                b.apply_bbo(ev)
            elif isinstance(ev, ExtBookSnapshot):
                b.apply_snapshot(ev)
            else:
                b.apply(ev)
            self.risk.note_ext(ev.venue, ev.ts)
        elif isinstance(ev, (OrderAck, OrderReject, CancelAck, KalshiFill, KalshiOrderUpdate,
                             KalshiOrderGroupUpdate, KalshiPositionSnapshot)):
            if isinstance(ev, KalshiFill):
                out += self._reconcile_fee(ev)
            out += self._on_order_event(ev)
        elif isinstance(ev, KalshiFeeUpdate):
            out += self._on_fee_update(ev)
        elif isinstance(ev, FeedStatus):
            for a in self.risk.on_feed_status(ev):
                out += self._apply_risk_action(ev.ts, a)
        elif isinstance(ev, HedgeFill):
            sgn = 1.0 if ev.side == "buy" else -1.0
            self.hedge_pos += sgn * ev.qty_btc
            self.hedge_cash -= sgn * ev.qty_btc * ev.price
            self.hedge_fees += ev.fee_usd
            left = self.hedge_pending.get(ev.client_order_id)
            if left is not None:
                left -= sgn * ev.qty_btc
                if abs(left) < 1e-12:
                    self.hedge_pending.pop(ev.client_order_id, None)
                else:
                    self.hedge_pending[ev.client_order_id] = left
        elif isinstance(ev, HedgeOrderUpdate):
            if ev.status in ("rejected", "canceled"):
                # the unfilled rest will never fill (fills already delivered were applied by
                # HedgeFill; a filled order clears itself when its HedgeFills sum to the target)
                self.hedge_pending.pop(ev.client_order_id, None)
                if ev.status == "rejected":
                    out.append(Log("hedge_rejected", {"coid": ev.client_order_id, "venue": ev.venue,
                                                      "reason": ev.reason}))
        elif isinstance(ev, (Settlement, KalshiMarketLifecycle)):
            out += self._on_settlement(ev)
        elif isinstance(ev, Timer):
            out += self._on_timer(ev.ts)
        return out

    # ================================================================== handlers
    def _on_index(self, ev: IndexTick) -> list[Action]:
        if ev.index_id != "BRTI":
            return []
        self.tracker.on_index(ev)
        src = ev.ts_exch or ev.ts
        if ev.feed in ("1hz", "5hz", "rest"):
            self.fv.update(src, ev.value)
        self.risk.note_brti(ev.ts, ev.ts_exch)
        self.last_brti_ns = ev.ts
        if ev.ts_exch:
            self.last_brti_src_ns = max(getattr(self, "last_brti_src_ns", 0), ev.ts_exch)
        out: list[Action] = []
        # abnormal-move kill switch on 60 s returns
        self.brti_hist.append((ev.ts, ev.value))
        while self.brti_hist and ev.ts - self.brti_hist[0][0] > 60 * NS_PER_S:
            self.brti_hist.popleft()
        if len(self.brti_hist) > 1 and self.fv.ready:
            t0, v0 = self.brti_hist[0]
            dt = max((ev.ts - t0) / NS_PER_S, 1.0)
            sd = self._sigma_1s(ev.ts, ev.value) * math.sqrt(dt)
            if sd > 0:
                for a in self.risk.on_abnormal_move(ev.ts, (ev.value - v0) / sd):
                    out += self._apply_risk_action(ev.ts, a)
        # reactive requote on a large move since the last cycle
        if self.fv.ready and not math.isnan(self.last_cycle_spot):
            sd1 = self._sigma_1s(ev.ts, ev.value)
            if sd1 > 0 and abs(ev.value - self.last_cycle_spot) > self.requote_move_sigma * sd1:
                out += self._cycle(ev.ts)
        return out

    def _on_order_event(self, ev) -> list[Action]:
        out: list[Action] = []
        for oe in self.om.on_event(ev):
            k = oe.kind
            if k == "accepted":
                w = self.om.order(oe.client_order_id)
                if w is not None and w.remaining_qty > 0 and oe.client_order_id not in self.queue.orders:
                    # live: until our own book delta shows the order, the displayed level may not
                    # contain it, so joining now would count our own size ahead of us
                    pending = self.book_includes_own and self._own_delta_seen.pop(oe.client_order_id, None) is None
                    self.queue.add_order(oe.client_order_id, w.ticker, w.book_side, w.px, w.remaining_qty, oe.ts,
                                         pending=pending)
                    if pending:
                        self._queue_pending_since[oe.client_order_id] = oe.ts
            elif k in ("fill", "orphan_fill"):
                self.stats.fills += 1
                if oe.client_order_id in self.queue.orders:
                    self.queue.on_own_fill(oe.client_order_id, oe.qty)
                fv = self.fvc.get(oe.ticker)
                out.append(Log("fill", {"ticker": oe.ticker, "coid": oe.client_order_id, "side": oe.book_side,
                                        "px": oe.px, "qty": oe.qty, "taker": oe.is_taker, "fee": oe.fee_micros,
                                        "F": None if fv is None else round(fv.F, 6)}))
            elif k in ("filled", "canceled", "rejected"):
                self._queue_pending_since.pop(oe.client_order_id, None)
                self._own_delta_seen.pop(oe.client_order_id, None)
                if oe.client_order_id in self.queue.orders:
                    self.queue.remove_order(oe.client_order_id)
            elif k == "cancel_ready":
                w = self.om.order(oe.client_order_id)
                if w is not None and w.order_id:
                    a = CancelOrder(client_order_id=w.client_order_id, ticker=w.ticker, order_id=w.order_id,
                                    reason="deferred")
                    if self.om.request_cancel(a, oe.ts):
                        out.append(a)
            elif k == "position_mismatch":
                for a in self.risk.on_reconciliation_mismatch(oe.ts, oe.detail or oe.ticker):
                    out += self._apply_risk_action(oe.ts, a)
            elif k == "group_triggered":
                self.group_triggered_at = oe.ts
                self.risk.on_feed_status(FeedStatus(ts=oe.ts, ts_exch=0, stream=f"kalshi.order_group:{oe.client_order_id or self.ORDER_GROUP_ID}", status="error"))
                out.append(Log("risk", {"event": "order_group_triggered"}))
            elif k == "group_reset":
                self.risk.on_feed_status(FeedStatus(ts=oe.ts, ts_exch=0, stream=f"kalshi.order_group:{oe.client_order_id or self.ORDER_GROUP_ID}", status="resynced"))
        return out

    def _on_settlement(self, ev) -> list[Action]:
        if isinstance(ev, KalshiMarketLifecycle):
            if ev.event_type not in ("determined", "settled") or not ev.result:
                if ev.event_type == "deactivated" or ev.is_deactivated:
                    self.paused.add(ev.ticker)  # audit m3: no quoting until re-activated
                    return self._cancel_market(ev.ts, ev.ticker, "deactivated")
                if ev.event_type == "activated" or ev.is_deactivated is False:
                    self.paused.discard(ev.ticker)
                return []
            px = PX_SCALE if ev.result == "yes" else 0
        else:
            px = ev.settlement_px
        t = ev.ticker
        if t not in self.specs or t in self.settled:
            return []
        self.settled[t] = px
        q = self._contracts(t)
        pnl = self.om.settled_pnl_micros(t, px) / 1e6
        self.settled_cash += q * px / PX_SCALE
        e = self.specs[t].event_ticker
        self.event_pnl[e] = self.event_pnl.get(e, 0.0) + pnl
        out: list[Action] = [Log("settle", {"ticker": t, "px": px, "position": q, "pnl": round(pnl, 6)})]
        out += self._cancel_market(ev.ts, t, "settled")
        if all(s.ticker in self.settled for s in self.specs.values() if s.event_ticker == e):
            for a in self.risk.on_settlement_pnl(ev.ts, self.event_pnl[e]):
                out += self._apply_risk_action(ev.ts, a)
        return out

    def _on_fee_update(self, ev: KalshiFeeUpdate) -> list[Action]:
        """Event-level fee override (audit M6): override > series base; None clears it.
        Markets whose resulting fee type is unsupported become untradable immediately."""
        out: list[Action] = []
        for t, spec in list(self.specs.items()):
            if spec.event_ticker != ev.event_ticker:
                continue
            base_type, base_mult = self.base_fee.get(t, (spec.fee_type, spec.fee_multiplier))
            ftype = ev.fee_type_override if ev.fee_type_override is not None else base_type
            mult = float(ev.fee_multiplier_override) if ev.fee_multiplier_override not in (None, "") else base_mult
            self.fee_sched.pop(t, None)
            self._fee_cache.clear()
            if ftype:
                try:
                    sched = self.fee_engine.schedule_for_spec(ftype, mult)
                    if getattr(sched, "supported", True):
                        self.fee_sched[t] = sched
                except Exception:  # unsupported (e.g. flat): leave untradable
                    pass
            if t not in self.fee_sched:
                out += self._cancel_market(ev.ts, t, "fee_unsupported")
            out.append(Log("fees", {"ticker": t, "fee_type": ftype, "multiplier": mult,
                                    "tradable": t in self.fee_sched}))
        return out

    def _reconcile_fee(self, ev: KalshiFill) -> list[Action]:
        """Compare the exchange-reported fee of our fill with the fee model (audit M6).
        The reported fee may include up to one balance unit of rounding (or a rebate)."""
        sched = self.fee_sched.get(ev.ticker)
        if sched is None:
            return []
        expected = sched.trade_fee_micros(ev.yes_px, ev.qty, ev.is_taker)
        diff = ev.fee_micros - expected
        if abs(diff) <= self.fee_tolerance_micros:
            return []
        out: list[Action] = [Log("fees", {"event": "fee_mismatch", "ticker": ev.ticker, "reported": ev.fee_micros,
                                          "expected": expected, "px": ev.yes_px, "qty": ev.qty,
                                          "taker": ev.is_taker})]
        for a in self.risk.on_fee_mismatch(ev.ts, f"{ev.ticker}:{ev.fee_micros}vs{expected}"):
            out += self._apply_risk_action(ev.ts, a)
        return out

    def _apply_risk_action(self, ts: int, a: Action) -> list[Action]:
        """Route a risk-engine action through the order manager so pending cancels are tracked."""
        if isinstance(a, CancelAll):
            out: list[Action] = []
            for w in self.om.working():
                if a.tickers and w.ticker not in a.tickers:
                    continue
                out += self._cancel(ts, w, a.reason)
            self.om.request_cancel_all(a, ts)
            out.append(a)
            return out
        if isinstance(a, Halt):
            if a.scope == "all":
                self.halted_all = True
            return [a]
        return [a]

    def _cancel(self, ts: int, w, reason: str) -> list[Action]:
        if w.cancel_requested or w.remaining_qty <= 0:
            return []
        a = CancelOrder(client_order_id=w.client_order_id, ticker=w.ticker, order_id=w.order_id, reason=reason)
        if self.om.request_cancel(a, ts):
            self.stats.cancels += 1
            self.stats.bump("cancel:" + reason)
            return [a]
        return []

    def _cancel_market(self, ts: int, ticker: str, reason: str) -> list[Action]:
        out: list[Action] = []
        for w in self.om.working(ticker):
            out += self._cancel(ts, w, reason)
        return out

    # ================================================================== timer / cycle
    OWN_DELTA_TIMEOUT_NS = 2 * NS_PER_S

    def _expire_queue_pending(self, now: int) -> None:
        """Live fallback: an order whose own book delta was never seen joins behind the WHOLE
        displayed level after OWN_DELTA_TIMEOUT_NS (the level may or may not show our order;
        overstating the queue ahead is the safe error). Stale early-delta records are dropped."""
        for coid, t in list(self._queue_pending_since.items()):
            if now - t >= self.OWN_DELTA_TIMEOUT_NS:
                del self._queue_pending_since[coid]
                o = self.queue.orders.get(coid)
                if o is not None and o.pending:
                    self.queue.activate(coid, queue_ahead=int(self._level_qty(*o.level)))
                    self.stats.bump("queue_own_delta_timeout")
        for coid, t in list(self._own_delta_seen.items()):
            if now - t >= 30 * NS_PER_S:
                del self._own_delta_seen[coid]

    def _on_timer(self, now: int) -> list[Action]:
        out: list[Action] = []
        self.om.on_event(Timer(ts=now))  # ack/change timeouts -> unknown-outcome handling
        if self._queue_pending_since or self._own_delta_seen:
            self._expire_queue_pending(now)
        if self.use_order_group and not self.group_created:
            self.group_created = True
            lim = int(self.cfg.risk.order_group_limit_contracts * QTY_SCALE)
            out.append(CreateOrderGroup(order_group_id=self.ORDER_GROUP_ID, contracts_limit=lim, reason="startup"))
        if (self.group_triggered_at and now - self.group_triggered_at
                >= int(self.cfg.risk.order_group_cooldown_s * NS_PER_S)):
            self.group_triggered_at = 0
            out.append(ResetOrderGroup(order_group_id=self.ORDER_GROUP_ID, reason="cooldown elapsed"))
        if now - self.last_cycle_ns >= self.cfg.timers.quote_period_ms * NS_PER_MS:
            out += self._cycle(now)
        return out

    def _nowcast(self, now: int) -> tuple[float | None, float]:
        S = self._spot()
        if S is None:
            return None, 0.0
        age = max((now - self.last_brti_ns) / NS_PER_S, 0.0)
        src = getattr(self, "last_brti_src_ns", 0)
        if src:
            age = max(age, (now - src) / NS_PER_S)  # audit m2: source age counts too
        age += 0.25  # relay latency allowance
        return S, self._sigma_1s(now, S) * math.sqrt(age)

    def _band(self, spec: MarketSpec, ws, S: float, now: int, ns_sd: float) -> MarketFV:
        horizon = max(0.0, (spec.expiration_ts - now) / NS_PER_S)
        tail, c = self.fv.tails.at(horizon)
        sig = self.fv.sigma_abs(now, spec.expiration_ts, S) * c
        sig = max(sig, S * self.cfg.fair_value.vol_floor_ann / math.sqrt(SEC_YR))
        center = digital(spec, ws, S, sig, tail, nowcast_sd=ns_sd)
        lo = hi = center.p_yes
        fvc = self.cfg.fair_value
        for m in (fvc.band_sigma_lo_mult, fvc.band_sigma_hi_mult):
            for tl in (tail, self.alt_tail):
                for nsd in (ns_sd, 2.0 * ns_sd):
                    p = digital(spec, ws, S, sig * m, tl, nowcast_sd=nsd).p_yes
                    lo, hi = min(lo, p), max(hi, p)
        nu = getattr(tail, "nu", math.inf)
        return MarketFV(now, center.p_yes, lo, hi, center.delta, center.gamma, center.z, center.sd_remaining, nu,
                        center.z_cap)

    def _cycle(self, now: int) -> list[Action]:
        cfg = self.cfg
        q = cfg.quoting
        out: list[Action] = []
        self.last_cycle_ns = now
        self.stats.cycles += 1
        health = self.risk.health(now)
        if self.halted_all or not health.quoting_allowed or not self.fv.ready:
            self.stats.halted_cycles += 1
            for r in (health.reasons or (["fv_not_ready"] if not self.fv.ready else ["halted"])):
                self.stats.bump(r.split("_")[0] if r.startswith("brti_stale") else r)
            for w in self.om.working():
                out += self._cancel(now, w, "unhealthy")
            return out
        S, ns_sd = self._nowcast(now)
        if S is None:
            return out
        self.last_cycle_spot = S
        # ---------------------------------------------------- per event
        events: dict[int, list[MarketSpec]] = {}
        for t, spec in self.specs.items():
            if t in self.settled or now >= spec.close_ts or spec.series_ticker not in q.enabled_series:
                continue
            tau = (spec.expiration_ts - now) / NS_PER_S
            if tau <= 0 or tau > q.max_tau_s:
                continue
            events.setdefault(spec.expiration_ts, []).append(spec)
        D = self.hedge_pos
        ctxs: list[tuple[MarketSpec, MarketFV, EventGrid, dict, np.ndarray]] = []
        for T, specs in sorted(events.items()):
            ws = self.tracker.window_state(specs[0].settlement, T, now)
            fvs = {s.ticker: self._band(s, ws, S, now, ns_sd) for s in specs}
            for s in specs:
                f = fvs[s.ticker]
                self.fvc[s.ticker] = f
                h = self.fv_hist[s.ticker]
                h.append((now, f.F))
                while h and now - h[0][0] > int(cfg.adverse.lookback_s * NS_PER_S) + NS_PER_S:
                    h.popleft()
                last = self.last_fv_log.get(s.ticker)
                if last is None or now - last[0] >= self.log_fv_every_ns or abs(f.F - last[1]) > 0.002:
                    self.last_fv_log[s.ticker] = (now, f.F)
                    out.append(Log("fv", {"ticker": s.ticker, "F": round(f.F, 6), "F_lo": round(f.F_lo, 6),
                                          "F_hi": round(f.F_hi, 6), "delta": f.delta, "z": round(f.z, 4),
                                          "S": S, "sd_R": f.sd_R}))
                D += self._contracts(s.ticker) * f.delta
            first = next(iter(fvs.values()))
            tail_obj, _ = self.fv.tails.at(max(0.0, (T - now) / NS_PER_S))
            bps = sorted({b for s in specs for b in (s.floor_strike, s.cap_strike) if b is not None})
            grid = EventGrid.build(n_obs=ws.n_obs, sum_fixed=ws.sum_fixed, m_remaining=ws.m_remaining,
                                   mu_R=S, sd_R=first.sd_R, tail=tail_obj, spot=S, breakpoints_A=bps)
            payoffs = {s.ticker: payoff_vector(s, grid.A) for s in specs}
            positions = {s.ticker: self._contracts(s.ticker) for s in specs}
            basis = {s.ticker: self._cost_basis(s.ticker) for s in specs}
            base = book_pnl(grid, payoffs, positions, basis)
            for s in specs:
                ctxs.append((s, fvs[s.ticker], grid, {"payoffs": payoffs, "positions": positions, "basis": basis,
                                                      "ws": ws, "specs": specs}, base))
        # ---------------------------------------------------- per market-side decisions
        c_h = hedge_cost_frac(cfg.hedge, urgent=False)
        total_wc = 0.0
        event_wc: dict[str, float] = {}
        working_all = [w for w in self.om.working() if w.remaining_qty > 0]
        for s, f, grid, ev, base in ctxs:
            e = s.event_ticker
            if e in event_wc:
                continue
            ws = ev["ws"]
            mine = {x.ticker for x in ev["specs"]}
            wk = [(w.ticker, w.book_side, w.px / PX_SCALE, w.remaining_qty / QTY_SCALE)
                  for w in working_all if w.ticker in mine]
            event_wc[e] = worst_case_loss_with_orders(
                {x.ticker: x for x in ev["specs"]}, ev["positions"], ev["basis"], wk, S,
                stress_frac=cfg.risk.stress_move_frac, n_obs=ws.n_obs, sum_fixed=ws.sum_fixed,
                m_remaining=ws.m_remaining)
            total_wc += event_wc[e]
        # events already over their loss limit (e.g. after fills): pull their working orders
        for e, wc in event_wc.items():
            if wc > cfg.risk.max_event_worst_loss:
                self.stats.bump("event_over_limit")
                for w in working_all:
                    if self.specs[w.ticker].event_ticker == e:
                        out += self._cancel(now, w, "event_over_limit")
        proposals: list[tuple[float, MarketSpec, str, object, MarketFV]] = []
        for s, f, grid, ev, base in ctxs:
            acts, props = self._quote_market(now, s, f, grid, ev, base, S, D, c_h, health)
            out += acts
            proposals += props
        d_up, d_dn = D, D
        for w in working_all:
            fw = self.fvc.get(w.ticker)
            if fw is None:
                continue
            c = (1 if w.book_side == "bid" else -1) * (w.remaining_qty + w.inflight_fill_qty) / QTY_SCALE * fw.delta
            if c > 0:
                d_up += c
            else:
                d_dn += c
        out += self._admit(now, proposals, event_wc, total_wc, d_up, d_dn)
        # ---------------------------------------------------- hedge
        if cfg.hedge.enabled and health.hedging_allowed:
            out += self._hedge(now, S, D)
        # ---------------------------------------------------- equity
        eq = self.equity(S)
        for a in self.risk.on_equity(now, eq):
            out += self._apply_risk_action(now, a)
        return out

    def _quote_market(self, now, s: MarketSpec, f: MarketFV, grid, ev, base, S, D, c_h, health):
        """Per market: cancels are returned as actions; new orders as proposals for ranking."""
        cfg = self.cfg
        q = cfg.quoting
        t = s.ticker
        out: list[Action] = []
        props: list[tuple[float, MarketSpec, str, object, MarketFV]] = []
        tau = (s.expiration_ts - now) / NS_PER_S
        book = self.books[t]
        reason = ""
        if t in self.paused:
            reason = "paused"
        elif t not in self.fee_sched:
            reason = "fee_unresolved"
        elif not book.valid or not self.risk.book_ok(t, now):
            reason = "book_invalid"
        elif tau < q.min_tau_s and f.z_near < q.z_min_final:
            reason = "final_window_near_strike"
        elif tau < 600 and not health.near_expiry_allowed:
            reason = "brti_not_fresh_near_expiry"
        if reason:
            self.stats.bump(reason)
            return self._cancel_market(now, t, reason), props
        sched = self.fee_sched[t]
        pos = self._contracts(t)
        existing: dict[str, list[ExistingOrder]] = {"bid": [], "ask": []}
        for w in self.om.working(t):
            if w.cancel_requested or w.remaining_qty <= 0 or w.state.name not in ("RESTING", "PENDING_NEW"):
                continue
            qa = self.queue.queue_ahead(w.client_order_id)
            existing[w.book_side].append(ExistingOrder(w.client_order_id, w.px, w.remaining_qty,
                                                       (qa if qa is not None else 0) / QTY_SCALE,
                                                       age_ns=now - w.created_ns))
        # capacity = limit minus everything that could still fill on that side EXCEPT the kept
        # orders passed to decide_side (pending cancels and in-flight fills count: audit M1)
        ex_rem = {sd: sum(o.remaining_qty for o in existing[sd]) for sd in ("bid", "ask")}
        other_bid = (self.om.worst_case_exposure(t, "bid") - self.om.position(t) - ex_rem["bid"]) / QTY_SCALE
        other_ask = (self.om.position(t) - self.om.worst_case_exposure(t, "ask") - ex_rem["ask"]) / QTY_SCALE
        cap = {
            "bid": self.risk.market_capacity(side="bid", position=pos, working_bid=max(0.0, other_bid),
                                             working_ask=0.0, tau_s=tau),
            "ask": self.risk.market_capacity(side="ask", position=pos, working_bid=0.0,
                                             working_ask=max(0.0, other_ask), tau_s=tau),
        }
        dF = 0.0
        h = self.fv_hist[t]
        if len(h) > 1:
            dF = h[-1][1] - h[0][1]
        ctx = MarketQuoteContext(
            spec=s, book=book, F=f.F, F_lo=f.F_lo, F_hi=f.F_hi, delta_btc=f.delta, tau_s=tau, z=f.z_near,
            maker_fee=lambda px, sc=sched: sc.expected_fee_per_contract(px, False), dF_recent=dF, grid=grid,
            base_pnl=base, payoff_k=ev["payoffs"][t], lam=self.cfg.lam, lambda_tail=cfg.risk.lambda_tail,
            tail_budget=cfg.risk.tail_budget, D_btc=D, hedge_cost_frac=c_h, spot=S,
            rho_hedged=cfg.hedge.rho_hedged_fraction if cfg.hedge.enabled else 0.0,
            clip_contracts=q.clip_contracts, capacity_contracts=cap, existing=existing,
            max_ticks_from_touch=q.max_ticks_from_touch, price_floor_px=q.price_floor_px,
            price_cap_px=q.price_cap_px, rounding_per_order=q.expected_rounding_per_order,
            order_fee=lambda px, size, side, tk=t: self._order_fee(tk, px, size, side),
        )
        for side in ("bid", "ask"):
            d = decide_side(ctx, side, self.flow, self.adverse, q.v_min_dollars, q.kappa_replace_per_s,
                            replace_rel=q.replace_rel, min_age_ns=q.min_order_age_ms * NS_PER_MS)
            for coid in d.cancel:
                w = self.om.order(coid)
                if w is not None:
                    out += self._cancel(now, w, "ev")
            if d.place is not None:
                if now - self.last_place.get((t, side), -10**18) < q.requote_min_interval_ms * NS_PER_MS:
                    continue
                props.append((d.place.score, s, side, d.place, f))
        return out, props

    def _admit(self, now: int, proposals, event_wc: dict[str, float], total_wc: float, d_up: float,
               d_dn: float) -> list[Action]:
        """Greedy cross-market admission by score (EV rate per $ of collateral) under the
        event/total worst-case loss limits and the portfolio delta limit. Each admitted order
        adds its maximum loss (bid: px * n, ask: (1 - px) * n) to the headroom accounting.
        The delta limit is checked against one-sided worst cases that already include every
        working order (d_up: all delta-increasing orders fill; d_dn: all decreasing ones),
        so orders blocked in one cycle cannot slip in on the next (audit M2)."""
        cfg = self.cfg
        q = cfg.quoting
        out: list[Action] = []
        ewc = dict(event_wc)
        twc = total_wc
        for score, s, side, cand, f in sorted(proposals, key=lambda p: (-p[0], p[1].ticker, p[2])):
            px = cand.px / PX_SCALE
            n = cand.size
            add = n * (px if side == "bid" else 1.0 - px)
            e = s.event_ticker
            if not self.risk.loss_limits_ok(event_worst_loss=ewc.get(e, 0.0) + add, total_worst_loss=twc + add):
                self.stats.bump("blocked:loss_limit")
                continue
            c = (1 if side == "bid" else -1) * n * f.delta
            base = d_up if c >= 0 else d_dn
            if not self.risk.delta_ok(abs(base + c), abs(base)):
                self.stats.bump("blocked:delta")
                continue
            ewc[e] = ewc.get(e, 0.0) + add
            twc += add
            if c >= 0:
                d_up += c
            else:
                d_dn += c
            self.last_place[(s.ticker, side)] = now
            a = PlaceOrder(
                client_order_id=self.ids.next(), ticker=s.ticker, book_side=side, px=cand.px,
                qty=int(round(n * QTY_SCALE)), post_only=q.post_only,
                expiration_ts=int(now + q.order_expiry_s * NS_PER_S) if q.order_expiry_s > 0 else 0,
                order_group_id=self.ORDER_GROUP_ID if self.use_order_group else "",
                reason=f"score={score:.3g}",
            )
            self.om.request_place(a, now)
            self.stats.quotes_placed += 1
            out.append(a)
            out.append(Log("quote", cand.as_log()))
        return out

    def _hedge(self, now: int, S: float, D: float) -> list[Action]:
        cfg = self.cfg
        taus = [(sp.expiration_ts - now) / NS_PER_S for t, sp in self.specs.items()
                if t not in self.settled and self._contracts(t) != 0]
        h_eff = min(taus) if taus else cfg.hedge.h_eff_cap_s
        pending = sum(self.hedge_pending.values())
        dec = decide_hedge(cfg.hedge, D_btc=D, spot=S, sigma_abs_per_sqrt_s=self._sigma_1s(now, S), h_eff_s=h_eff,
                           lam=cfg.lam, pending_btc=pending)
        if dec.target_btc == 0.0:
            return []
        notional_after = abs(self.hedge_pos + pending + dec.target_btc) * S
        if notional_after > cfg.risk.max_hedge_notional and abs(self.hedge_pos + pending + dec.target_btc) > abs(self.hedge_pos + pending):
            return [Log("hedge", {"blocked": "max_hedge_notional"})]
        coid = self.ids.next()
        side = "buy" if dec.target_btc > 0 else "sell"
        self.hedge_pending[coid] = dec.target_btc
        return [PlaceHedge(client_order_id=coid, venue=cfg.hedge.venue, symbol=cfg.hedge.symbol, side=side,
                           qty_btc=abs(dec.target_btc), order_type="market" if dec.urgent else "limit",
                           post_only=not dec.urgent, reason=f"D={D:.4f} band={dec.band_btc:.4f}"),
                Log("hedge", {"D": D, "band": dec.band_btc, "trade": dec.target_btc, "urgent": dec.urgent})]

    # ================================================================== reporting
    def equity(self, S: float | None = None) -> float:
        """Cash P&L since start + mark-to-fair of open Kalshi positions + hedge mark."""
        eq = self.om.cash_micros() / 1e6 - self.om.fees_micros() / 1e6 + self.settled_cash
        for t in self.specs:
            if t in self.settled:
                continue
            qn = self._contracts(t)
            if qn:
                f = self.fvc.get(t)
                eq += qn * (f.F if f is not None else 0.5)
        if S is not None:
            eq += self.hedge_cash + self.hedge_pos * S - self.hedge_fees
        return eq
