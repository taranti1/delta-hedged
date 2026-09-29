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
import zlib
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
    RiskStateSeed,
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
from dh.execution.order_manager import ORPHAN_ATTACHED, OrderManager
from dh.execution.queue import POLICY_LETTER, QueueCalibrator, QueueEstimator, trade_maker_book
from dh.models.fairvalue import digital
from dh.models.fvmodel import FairValueModel, load_recommended_config
from dh.models.tails import GAUSS, make_tail
from dh.settlement.closemark import CloseMark, WindowOutcome, close_mark, evaluate_window
from dh.settlement.window import SettlementTracker
from dh.strategy.config import FairValueCfg, StrategyConfig
from dh.strategy.fill_model import AdverseSelectionModel, FillIntensityModel, SegmentFlow
from dh.strategy.flow_features import PRODUCTION_FEATURE_VERSION
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
    FEE_STATE_MAX = 50_000  # fee reconciliation memory (fills / orders) kept per session

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
        id_prefix: str | None = None,
        log_fv_every_ns: int = NS_PER_S,
        requote_move_sigma: float = 2.0,
        queue_diagnostics: bool = False,
    ) -> None:
        self.cfg = cfg
        legacy = FairValueCfg()
        unsupported = [k for k in ("tail", "student_nu", "mixture_cv", "vol_half_life_s", "use_seasonality",
                                     "nowcast", "nowcast_beta")
                       if getattr(cfg.fair_value, k) != getattr(legacy, k)]
        if unsupported:
            raise ValueError("Unsupported fair_value overrides: " + ", ".join(unsupported)
                             + "; use an explicit fitted fv_model or research nowcast implementation")
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
        # queue diagnostics (logging only, never read by a decision): the optimistic (A) and
        # conservative (C) estimators shadow the strategy's realistic (B) one on the same stream,
        # so live queue positions and fills show which cancel policy matches the exchange. The
        # runner records the flag in session_start; replay rebuilds the same logs from it.
        self.queue_diagnostics = queue_diagnostics
        self.queue_shadow: QueueCalibrator | None = (
            QueueCalibrator(self._level_qty, book_includes_own=book_includes_own,
                            policies=("optimistic", "conservative")) if queue_diagnostics else None)
        self._diag_logs: list[Log] = []
        # live book (contains our orders): an order joins the queue when our own positive book
        # delta shows it; if that delta beat the ack it is remembered here (coid -> ts)
        self.book_includes_own = book_includes_own
        self._own_delta_seen: dict[str, int] = {}
        self._queue_pending_since: dict[str, int] = {}
        self.flow = FillIntensityModel(cfg.fill, dict(flow_segments or {}))
        self.adverse = AdverseSelectionModel(cfg.adverse, dict(as_coefs or {}))
        self.risk = RiskEngine(cfg.risk)
        # client_order_ids must not repeat across sessions (audit live M6): the live runner passes
        # f"{run_prefix}-{session token}" (recorded, so replay uses the same prefix)
        self.ids = IdGen(id_prefix or cfg.run_prefix)
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
        # positions held between a market's close and its result (dh.settlement.closemark):
        # valued at the payout our own BRTI prints of the window imply (else worst case / last
        # trade), never at the pre-close fair value; refreshed every cycle, logged on change
        self.close_marks: dict[str, CloseMark] = {}
        self._close_outcomes: dict[str, WindowOutcome] = {}  # final window evaluations
        self.last_trade_px: dict[str, int] = {}  # ticker -> last public trade (YES px)
        self.settled_cash = 0.0
        self.event_pnl: dict[str, float] = {}
        self.last_cycle_ns = 0
        self.last_cycle_spot = math.nan
        self.last_brti_ns = 0
        self.requote_move_sigma = requote_move_sigma
        self.use_order_group = use_order_group
        self.group_created = False
        self.group_triggered_at = 0
        self.group_reset_sent_at = 0  # re-sent every cooldown until the reset is confirmed
        self._last_prune_ns = 0
        self.halted_all = False
        self.paused: set[str] = set()
        # fee without event overrides (restored when an override is cleared; audit live m7)
        self.base_fee: dict[str, tuple[str, float]] = {t: sp.base_fee for t, sp in self.specs.items()}
        self.fee_tolerance_micros = 1  # wire/model micro-dollar precision (plus each fill's rounding part)
        self._fee_accumulators: dict = {}  # insertion ordered, bounded by FEE_STATE_MAX
        self._fee_seen: dict[str, None] = {}  # insertion-ordered set, bounded by FEE_STATE_MAX
        self._fee_residual: dict = {}  # per-order cumulative |reported - expected| (micros)
        self._retained_candidates: dict = {}
        self._pending_evictions: set[str] = set()  # opportunity_cost cancels awaiting their ack
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
            self.base_fee[s.ticker] = s.base_fee
            self._resolve_fee(s.ticker, s)
            added.append(s.ticker)
        return added

    def prune_settled(self, before_ns: int) -> int:
        """Drop state for markets settled before `before_ns` with no position/working orders."""
        reserved = {w.ticker for w in self.om.all_orders() if w.could_fill_qty > 0}
        drop = [t for t, s in self.specs.items()
                if t in self.settled and s.expiration_ts < before_ns and not self.om.working(t)
                and t not in reserved]
        for t in drop:
            self.specs.pop(t, None)
            self.close_marks.pop(t, None)
            self._close_outcomes.pop(t, None)
            self.last_trade_px.pop(t, None)
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
        return min(max(s, floor), spot * self.cfg.fair_value.vol_cap_ann / math.sqrt(SEC_YR))

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
                if self.queue_shadow is not None:
                    self._shadow(lambda sh: sh.on_snapshot(ev))
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
                    if self.queue_shadow is not None:
                        self._shadow(lambda sh: sh.on_book_delta(ev))
        elif isinstance(ev, KalshiTrade):
            if ev.ticker in self.books:
                if self.queue_diagnostics:
                    out += self._queue_trade_diag(ev)
                else:
                    self.queue.on_trade(ev)
                self.last_trade_px[ev.ticker] = int(ev.yes_px)
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
        elif isinstance(ev, RiskStateSeed):
            for a in self.risk.on_seed(ev):
                out += self._apply_risk_action(ev.ts, a)
        elif isinstance(ev, (Settlement, KalshiMarketLifecycle)):
            out += self._on_settlement(ev)
        elif isinstance(ev, Timer):
            out += self._on_timer(ev.ts)
        if self._diag_logs:
            out += self._diag_logs
            self._diag_logs = []
        return out

    # ================================================================== queue diagnostics
    def _shadow(self, fn) -> None:
        """Apply fn to the shadow estimators; a diagnostics failure disables them (logged once)
        instead of stopping the strategy."""
        sh = self.queue_shadow
        if sh is None:
            return
        try:
            fn(sh)
        except Exception as exc:  # noqa: BLE001 - diagnostics must never stop trading
            self.queue_shadow = None
            self.stats.bump("queue_diag_error")
            self._diag_logs.append(Log("queue_diag", {"event": "disabled", "error": f"{type(exc).__name__}: {exc}"[:300]}))

    def _queue_state(self, coid: str) -> dict[str, int | None]:
        """Queue ahead of one of our orders under each policy (B = the strategy's estimator)."""
        q: dict[str, int | None] = {"B": self.queue.queue_ahead(coid)}
        sh = self.queue_shadow
        if sh is not None:
            for pol, v in sh.queue_ahead(coid).items():
                q[POLICY_LETTER[pol]] = v
        return q

    def _queue_trade_diag(self, ev: KalshiTrade) -> list[Action]:
        """Public print with queue logging: our orders the print could reach (queue ahead under
        A/B/C before it) and each policy's predicted fills. Joined offline to our real fills by
        trade_id; the taker's order size is the sum of prints sharing (ticker, ts_exch, side)."""
        book, p = trade_maker_book(ev)
        # classify expired depletions first (on_trade does this itself as its first step), so
        # the logged queue state is the one this print actually meets
        self.queue.advance(ev.ts)
        self._shadow(lambda s: s.advance(ev.ts))
        rows = []
        if not ev.is_block and ev.qty > 0:
            for o in self.queue.orders_at_or_better(ev.ticker, book, p):
                rows.append({"coid": o.key, "px": o.px, "rem": o.remaining, "q": self._queue_state(o.key),
                             "joined": o.joined_queue, "age_ms": (ev.ts - o.arrival_ts) // NS_PER_MS})
        level = int(self._level_qty(ev.ticker, book, p)) if rows else 0
        pred = {"B": self.queue.on_trade(ev)}
        sh = self.queue_shadow
        if sh is not None:
            res: dict = {}
            self._shadow(lambda s: res.update(s.on_trade(ev)))
            for pol, fills in res.items():
                pred[POLICY_LETTER[pol]] = fills
        if not rows:
            return []
        return [Log("queue_trade", {"ticker": ev.ticker, "trade_id": ev.trade_id, "book": book, "px": p,
                                    "yes_px": ev.yes_px, "qty": ev.qty, "taker_side": ev.taker_side,
                                    "ts_exch": ev.ts_exch, "level_qty": level, "orders": rows,
                                    "pred": {k: [list(f) for f in v] for k, v in pred.items()}})]

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
        return self._handle_order_events(self.om.on_event(ev), fill_source=ev if isinstance(ev, KalshiFill) else None)

    def _handle_order_events(self, oes, fill_source=None) -> list[Action]:
        out: list[Action] = []
        for oe in oes:
            k = oe.kind
            if k == "accepted":
                w = self.om.order(oe.client_order_id)
                if w is not None and w.remaining_qty > 0 and oe.client_order_id not in self.queue.orders:
                    # live: until our own book delta shows the order, the displayed level may not
                    # contain it, so joining now would count our own size ahead of us
                    pending = self.book_includes_own and self._own_delta_seen.pop(oe.client_order_id, None) is None
                    self.queue.add_order(oe.client_order_id, w.ticker, w.book_side, w.px, w.remaining_qty, oe.ts,
                                         pending=pending)
                    if self.queue_shadow is not None:
                        self._shadow(lambda sh: sh.add_order(oe.client_order_id, w.ticker, w.book_side, w.px,
                                                             w.remaining_qty, oe.ts, pending=pending))
                    if pending:
                        self._queue_pending_since[oe.client_order_id] = oe.ts
            elif k in ("fill", "orphan_fill"):
                qdiag = self._fill_queue_diag(oe) if self.queue_diagnostics else None
                if oe.client_order_id in self.queue.orders:
                    self.queue.on_own_fill(oe.client_order_id, oe.qty)
                if self.queue_shadow is not None:
                    self._shadow(lambda sh: sh.on_own_fill(oe.client_order_id, oe.qty))
                if oe.detail == ORPHAN_ATTACHED:
                    continue  # logged and counted once, as the orphan_fill (review L1)
                self.stats.fills += 1
                fv = self.fvc.get(oe.ticker)
                rec = {"ticker": oe.ticker, "coid": oe.client_order_id, "side": oe.book_side,
                       "px": oe.px, "qty": oe.qty, "taker": oe.is_taker, "fee": oe.fee_micros,
                       "F": None if fv is None else round(fv.F, 6), "trade_id": oe.trade_id,
                       "ts_recv": oe.ts, "ts_exch": getattr(fill_source, "ts_exch", 0),
                       "fv_ts": None if fv is None else fv.ts}
                if qdiag is not None:
                    rec.update(qdiag)
                out.append(Log("fill", rec))
            elif k in ("filled", "canceled", "rejected"):
                self._queue_pending_since.pop(oe.client_order_id, None)
                self._own_delta_seen.pop(oe.client_order_id, None)
                if oe.client_order_id in self.queue.orders:
                    self.queue.remove_order(oe.client_order_id)
                if self.queue_shadow is not None:
                    self._shadow(lambda sh: sh.remove_order(oe.client_order_id))
            elif k == "cancel_ready":
                w = self.om.order(oe.client_order_id)
                if w is not None and w.order_id:
                    a = CancelOrder(client_order_id=w.client_order_id, ticker=w.ticker, order_id=w.order_id,
                                    reason="deferred")
                    if self.om.request_cancel(a, oe.ts):
                        out.append(a)
            elif k == "position_mismatch":  # confirmed snapshot mismatch (REST / forwarded WS)
                for a in self.risk.on_reconciliation_mismatch(oe.ts, oe.detail or oe.ticker):
                    out += self._apply_risk_action(oe.ts, a)
            elif k == "fill_position_mismatch":
                # a fill's post_position disagrees: stop quoting and reconcile; the runner's REST
                # check halts if the mismatch is confirmed (audit live m3)
                self.risk.pause_until_ns = max(self.risk.pause_until_ns,
                                               oe.ts + int(self.cfg.risk.own_gap_pause_s * NS_PER_S))
                out += self._apply_risk_action(oe.ts, CancelAll(reason="fill_position_mismatch"))
                out.append(Log("risk", {"event": "reconcile_requested", "channel": "fill", "ticker": oe.ticker,
                                        "detail": oe.detail}))
            elif k == "reconcile_needed":
                w = self.om.order(oe.client_order_id)
                if oe.detail == "cancel_timeout" and w is not None and self.om.retry_cancel(oe.client_order_id, oe.ts):
                    # no definite answer to our cancel: send it again (idempotent), every
                    # change timeout until the order resolves (audit live C2)
                    self.stats.bump("cancel_retry")
                    out.append(CancelOrder(client_order_id=w.client_order_id, ticker=w.ticker, order_id=w.order_id,
                                           reason="cancel_retry"))
                elif oe.detail == "resting_after_reject" and w is not None:
                    out += self._cancel(oe.ts, w, "revived")  # declared missing, found resting: pull it
                out.append(Log("order", {"event": "reconcile_needed", "coid": oe.client_order_id,
                                         "ticker": oe.ticker, "detail": oe.detail}))
            elif k == "group_triggered":
                self.group_triggered_at = oe.ts
                self.risk.on_feed_status(FeedStatus(ts=oe.ts, ts_exch=0, stream=f"kalshi.order_group:{oe.client_order_id or self.ORDER_GROUP_ID}", status="error"))
                out.append(Log("risk", {"event": "order_group_triggered"}))
            elif k == "group_reset":
                self.group_reset_sent_at = 0
                self.risk.on_feed_status(FeedStatus(ts=oe.ts, ts_exch=0, stream=f"kalshi.order_group:{oe.client_order_id or self.ORDER_GROUP_ID}", status="resynced"))
        return out

    def _fill_queue_diag(self, oe) -> dict:
        """Queue state of the filled order just BEFORE this fill (queue diagnostics)."""
        coid = oe.client_order_id
        o = self.queue.orders.get(coid)
        if o is None:
            return {"q_ahead": None, "q_joined": None, "level_qty": None, "rem_before": None, "age_ms": None}
        return {"q_ahead": self._queue_state(coid), "q_joined": o.joined_queue,
                "level_qty": self.queue.level_others(coid), "rem_before": o.remaining,
                "age_ms": (oe.ts - o.arrival_ts) // NS_PER_MS}

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
        self.close_marks.pop(t, None)
        self._close_outcomes.pop(t, None)
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
            base_type, base_mult = self.base_fee.get(t, spec.base_fee)
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
        Reconcile known trade/net conventions with per-order rounding and rebate carry."""
        sched = self.fee_sched.get(ev.ticker)
        if sched is None:
            return []
        aliases = [x for x in (ev.trade_id, ev.fill_id) if x]
        if any(x in self._fee_seen for x in aliases):
            return []
        self._fee_seen.update(dict.fromkeys(aliases))
        key = (ev.ticker, ev.order_id or ev.client_order_id, ev.book_side)
        accumulator = self._fee_accumulators.get(key)
        if accumulator is None:
            accumulator = self._fee_accumulators[key] = sched.order_accumulator(ev.book_side)
        for d in (self._fee_seen, self._fee_accumulators, self._fee_residual):
            while len(d) > self.FEE_STATE_MAX:
                d.pop(next(iter(d)))
        accumulator.schedule = sched  # fee overrides preserve this order's rebate carry
        breakdown = accumulator.apply_fill(ev.yes_px, ev.qty, ev.is_taker, ev.ts_exch or ev.ts)
        # Recorded WS/REST fee_cost may report trade fees alone or net fees. Accept
        # those two known conventions, not arbitrary discrepancies within one cent.
        expected = min((breakdown.trade_micros, breakdown.net_micros),
                       key=lambda x: abs(ev.fee_micros - x))
        diff = ev.fee_micros - expected
        residual = self._fee_residual[key] = self._fee_residual.get(key, 0) + abs(diff)
        precision = sched.rates.balance_precision_micros
        # Per fill: the rounding/rebate part is convention-dependent (same bound as the live
        # runner's reconcile_fill_fee); per order: small differences may not accumulate to a
        # balance unit. Never a whole-session sum, which would halt on benign drift.
        tolerance = max(self.fee_tolerance_micros, abs(breakdown.rounding_micros - breakdown.rebate_micros))
        if abs(diff) <= tolerance and residual < precision:
            return []
        out: list[Action] = [Log("fees", {"event": "fee_mismatch", "ticker": ev.ticker, "reported": ev.fee_micros,
                                          "expected": expected, "px": ev.yes_px, "qty": ev.qty,
                                          "trade_fee": breakdown.trade_micros, "net_fee": breakdown.net_micros,
                                          "order_residual": residual, "tolerance": tolerance,
                                          "balance_precision": precision, "taker": ev.is_taker})]
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
                    qa = int(self._level_qty(*o.level))
                    self.queue.activate(coid, queue_ahead=qa)
                    if self.queue_shadow is not None:
                        self._shadow(lambda sh: sh.activate(coid, queue_ahead=qa))
                    self.stats.bump("queue_own_delta_timeout")
        for coid, t in list(self._own_delta_seen.items()):
            if now - t >= 30 * NS_PER_S:
                del self._own_delta_seen[coid]

    PRUNE_EVERY_NS = 60 * NS_PER_S
    PRUNE_AGE_NS = 600 * NS_PER_S

    def _on_timer(self, now: int) -> list[Action]:
        out: list[Action] = []
        # ack/change timeouts -> unknown-outcome handling (re-cancel, revive; audit live C2)
        out += self._handle_order_events(self.om.on_event(Timer(ts=now)))
        if self._queue_pending_since or self._own_delta_seen:
            self._expire_queue_pending(now)
        if now - self._last_prune_ns >= self.PRUNE_EVERY_NS:
            # bounded order book-keeping over a long session (audit live M5); deterministic in
            # event time, so replay prunes identically
            self._last_prune_ns = now
            self.om.prune(now - self.PRUNE_AGE_NS)
        if self.use_order_group and not self.group_created:
            self.group_created = True
            lim = int(self.cfg.risk.order_group_limit_contracts * QTY_SCALE)
            out.append(CreateOrderGroup(order_group_id=self.ORDER_GROUP_ID, contracts_limit=lim, reason="startup"))
        cool = int(self.cfg.risk.order_group_cooldown_s * NS_PER_S)
        if self.group_triggered_at and now - self.group_triggered_at >= cool:
            self.group_triggered_at = 0
            self.group_reset_sent_at = now
            out.append(ResetOrderGroup(order_group_id=self.ORDER_GROUP_ID, reason="cooldown elapsed"))
        elif self.group_reset_sent_at and self.risk.groups_triggered and now - self.group_reset_sent_at >= cool:
            self.group_reset_sent_at = now  # reset not confirmed yet: send it again (audit live m11)
            out.append(ResetOrderGroup(order_group_id=self.ORDER_GROUP_ID, reason="reset not confirmed"))
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
        if age > self.cfg.fair_value.max_benchmark_age_s:
            return None, 0.0
        age += 0.25  # relay latency allowance
        return S, self._sigma_1s(now, S) * math.sqrt(age)

    def _band(self, spec: MarketSpec, ws, S: float, now: int, ns_sd: float) -> MarketFV:
        horizon = max(0.0, (spec.expiration_ts - now) / NS_PER_S)
        tail, c = self.fv.tails.at(horizon)
        sig = self.fv.sigma_abs(now, spec.expiration_ts, S) * c
        sig = min(max(sig, S * self.cfg.fair_value.vol_floor_ann / math.sqrt(SEC_YR)),
                  S * self.cfg.fair_value.vol_cap_ann / math.sqrt(SEC_YR))
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
        if self.stats.cycles == 1:
            out.append(Log("effective_model", {"fv_model_hash": self.fv.effective_hash,
                "fv_artifact_hash": self.fv.artifact_hash, "fv_fitted_to_utc": self.fv.fitted_to_utc,
                "effective_config": self.fv.effective_config(), "strategy_digest": cfg.digest(),
                "strategy_implementation": "profit_review_v2", "feature_version": PRODUCTION_FEATURE_VERSION,
                "nowcast_implementation": type(self).__name__, "nowcast": cfg.fair_value.nowcast,
                "legacy_tail_field": cfg.fair_value.tail}))
        health = self.risk.health(now)
        # loss limits first and every cycle, healthy or not: a cycle that halts sends no new
        # orders (audit live m5), and losses are checked while quoting is paused too; positions
        # in closed markets awaiting their result are re-marked first
        out += self._update_close_marks(now)
        for a in self.risk.on_equity(now, self.equity(self._spot())):
            out += self._apply_risk_action(now, a)
        if self.halted_all or not health.quoting_allowed or not self.fv.ready:
            self.stats.halted_cycles += 1
            for r in (health.reasons or (["fv_not_ready"] if not self.fv.ready else ["halted"])):
                self.stats.bump(r.split("_")[0] if r.startswith("brti_stale") else r)
            for w in self.om.working():
                out += self._cancel(now, w, "unhealthy")
            return out
        S, ns_sd = self._nowcast(now)
        if S is None:
            for w in self.om.working():
                out += self._cancel(now, w, "benchmark_age")
            return out
        self.last_cycle_spot = S
        # ---------------------------------------------------- per event
        events: dict[str, list[MarketSpec]] = {}
        for t, spec in self.specs.items():
            if t in self.settled or now >= spec.close_ts or spec.series_ticker not in q.enabled_series:
                continue
            tau = (spec.expiration_ts - now) / NS_PER_S
            if tau <= 0 or tau > q.max_tau_s:
                continue
            events.setdefault(self._settlement_key(spec), []).append(spec)
        D = self.hedge_pos
        ctxs: list[tuple[MarketSpec, MarketFV, EventGrid, dict, np.ndarray]] = []
        for _, specs in sorted(events.items()):
            T = specs[0].expiration_ts
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
            bps = sorted({b for s in specs for b in s.breakpoints()})  # strikes shifted by the cents rounding
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
        # Risk covers every unresolved exposure, including closed/disabled/out-of-horizon
        # markets and terminal orders whose fill messages have not arrived yet.
        working_all = [w for w in self.om.all_orders()
                       if w.could_fill_qty > 0]
        risk_groups = self._risk_groups(now, S, working_all)
        event_wc = {e: self._group_loss(g) for e, g in risk_groups.items()}
        total_wc = sum(event_wc.values())
        for e, wc in event_wc.items():
            if wc > cfg.risk.max_event_worst_loss:
                self.stats.bump("event_over_limit")
                for w in working_all:
                    spec = self.specs.get(w.ticker)
                    if spec is not None and self._settlement_key(spec) == e:
                        out += self._cancel(now, w, "event_over_limit")
        self._retained_candidates = {}
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
            c = (1 if w.book_side == "bid" else -1) * w.could_fill_qty / QTY_SCALE * fw.delta
            if c > 0:
                d_up += c
            else:
                d_dn += c
        out += self._admit(now, proposals, risk_groups, event_wc, total_wc, d_up, d_dn)
        # ---------------------------------------------------- hedge
        if cfg.hedge.enabled and health.hedging_allowed:
            out += self._hedge(now, S, D)
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
        elif ev["ws"].n_missing > 0 and f.F_hi > q.window_gap_max_yes_p:
            # a benchmark print of this settlement window is missing: "no data / incomplete data"
            # resolves No (contract terms), so every non-negligible YES is at risk
            reason = "brti_gap_in_window"
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
            max_ticks_from_touch=q.max_ticks_from_touch, touch_only=q.touch_only, price_floor_px=q.price_floor_px,
            price_cap_px=q.price_cap_px, rounding_per_order=q.expected_rounding_per_order,
            order_fee=lambda px, size, side, tk=t: self._order_fee(tk, px, size, side),
        )
        for side in ("bid", "ask"):
            d = decide_side(ctx, side, self.flow, self.adverse, q.v_min_dollars, q.kappa_replace_per_s,
                            replace_rel=q.replace_rel, min_age_ns=q.min_order_age_ms * NS_PER_MS)
            for candidate in d.existing_eval:
                if candidate.existing_id in d.keep:
                    self._retained_candidates[candidate.existing_id] = candidate
            for coid in d.cancel:
                w = self.om.order(coid)
                if w is not None:
                    out += self._cancel(now, w, "ev")
            if d.place is not None:
                if now - self.last_place.get((t, side), -10**18) < q.requote_min_interval_ms * NS_PER_MS:
                    continue
                props.append((d.place.score, s, side, d.place, f))
        return out, props

    @staticmethod
    def _settlement_key(spec: MarketSpec) -> str:
        st = spec.settlement
        return f"{spec.expiration_ts}:{st.index_id}:{st.n_obs}:{st.step_ns}:{st.include_close_tick}:{st.round_decimals}"

    def _risk_groups(self, now, spot, working):
        groups = {}
        exposed = {w.ticker for w in working}
        for t, spec in self.specs.items():
            if t in self.settled:
                continue
            tau = (spec.expiration_ts - now) / NS_PER_S
            eligible = (spec.series_ticker in self.cfg.quoting.enabled_series and now < spec.close_ts
                        and 0 < tau <= self.cfg.quoting.max_tau_s)
            if not eligible and t not in exposed and not self._contracts(t):
                continue
            key = self._settlement_key(spec)
            if key not in groups:
                groups[key] = {"specs": {}, "positions": {}, "basis": {}, "working": [], "admitted": [],
                               "spot": spot, "ws": self._risk_window(spec, now)}
            g = groups[key]
            g["specs"][t] = spec
            g["positions"][t] = self._contracts(t)
            g["basis"][t] = self._cost_basis(t)
        for w in working:
            spec = self.specs.get(w.ticker)
            if spec is not None and w.ticker not in self.settled:
                groups[self._settlement_key(spec)]["working"].append(w)
        return groups

    def _risk_window(self, spec, now):
        """Settlement window for risk; None when it cannot be reconstructed (every observation
        skipped, e.g. an expired unsettled market whose prints are gone or a benchmark outage
        over the whole window). Risk then treats the outcome as unknown instead of crashing."""
        try:
            return self.tracker.window_state(spec.settlement, spec.expiration_ts, now)
        except ValueError:
            self.stats.bump("risk_window_undefined")
            return None

    def _group_loss(self, group, extra=(), exclude=frozenset()):
        ws = group["ws"]
        working = [(w.ticker, w.book_side, w.worst_case_px / PX_SCALE,
                    w.could_fill_qty / QTY_SCALE)
                   for w in group["working"] if w.client_order_id not in exclude]
        if ws is None:  # outcome unknown: any settlement average, i.e. the full price range
            n_obs, sum_fixed, m_remaining, stress = 1, 0.0, 1, max(1.0, self.cfg.risk.stress_move_frac)
        else:
            n_obs, sum_fixed, m_remaining = ws.n_obs, ws.sum_fixed, ws.m_remaining
            stress = self.cfg.risk.stress_move_frac
        return worst_case_loss_with_orders(
            group["specs"], group["positions"], group["basis"], working + group["admitted"] + list(extra),
            group["spot"], stress_frac=stress, n_obs=n_obs, sum_fixed=sum_fixed, m_remaining=m_remaining)

    def _reserved_collateral(self) -> float:
        """Conservative funding bound, independent of scenario/risk netting.

        Positions reserve the full $1 payout per contract (cost basis can include realized
        cash, so is unsafe as an available-balance estimate). Every pending order and
        undelivered fill reserves its standalone YES/NO premium until acknowledged.
        No account balance or cross-order collateral netting is assumed here.
        """
        positions = sum(abs(self._contracts(t)) for t in self.specs if t not in self.settled)
        orders = sum((w.worst_case_px / PX_SCALE if w.book_side == "bid" else 1 - w.worst_case_px / PX_SCALE)
                     * w.could_fill_qty / QTY_SCALE
                     for w in self.om.all_orders() if w.ticker not in self.settled)
        return positions + orders

    def _admit(self, now: int, proposals, groups, event_wc: dict[str, float], total_wc: float,
               d_up: float, d_dn: float) -> list[Action]:
        """Rank opportunities and recompute exact scenario loss over every fill subset.

        Cancellation never releases capacity until acknowledged. A blocked better opportunity
        can cancel a weaker retained quote; admission is reconsidered with fresh inputs on the
        next cycle. Queue priority, age and absolute/relative improvement enter that decision.
        """
        cfg, q = self.cfg, self.cfg.quoting
        out: list[Action] = []
        ewc, twc = dict(event_wc), total_wc
        collateral = self._reserved_collateral()
        logged_constraints = set()
        for score, s, side, cand, f in sorted(proposals, key=lambda p: (-p[0], p[1].ticker, p[2])):
            n = cand.size
            e = self._settlement_key(s)
            order = (s.ticker, side, cand.px / PX_SCALE, n)
            new_loss = self._group_loss(groups[e], extra=[order])
            new_total = twc - ewc[e] + new_loss
            c = (1 if side == "bid" else -1) * n * f.delta
            base = d_up if c >= 0 else d_dn
            added_collateral = n * (cand.px / PX_SCALE if side == "bid" else 1 - cand.px / PX_SCALE)
            reason = ""
            if collateral + added_collateral > cfg.risk.risk_capital + 1e-9:
                reason = "collateral"
            elif not self.risk.loss_limits_ok(event_worst_loss=new_loss, total_worst_loss=new_total):
                reason = "loss_limit"
            elif not self.risk.delta_ok(abs(base + c), abs(base)):
                reason = "delta"
            if reason:
                self.stats.bump("blocked:" + reason)
                # Best rejected opportunity per binding constraint, without recording every loser.
                if reason not in logged_constraints:
                    logged_constraints.add(reason)
                    out.append(Log("opportunity_rejected", {**cand.as_log(), "constraint": reason,
                                "event_worst_loss": new_loss, "total_worst_loss": new_total,
                                "required_collateral": collateral + added_collateral,
                                "capital_budget": cfg.risk.risk_capital}))
                # One eviction at a time: a cancel keeps its reservation until acknowledged, so a
                # candidate still blocked next cycle must not evict a second quote meanwhile.
                self._pending_evictions = {k for k in self._pending_evictions
                                           if (pw := self.om.order(k)) is not None and pw.could_fill_qty > 0}
                if self._pending_evictions:
                    continue
                for coid, old in sorted(self._retained_candidates.items(), key=lambda item: item[1].score):
                    w = self.om.order(coid)
                    if (w is None or w.cancel_requested or w.state.name != "RESTING"
                            or w.inflight_fill_qty > 0 or now - w.created_ns < q.min_order_age_ms * NS_PER_MS
                            or score <= old.score or cand.ev_rate <= old.ev_rate * (1 + q.replace_rel) + q.kappa_replace_per_s):
                        continue
                    old_spec = self.specs.get(w.ticker)
                    if old_spec is None:
                        continue
                    old_e = self._settlement_key(old_spec)
                    if old_e not in groups:
                        continue
                    excluded = frozenset([coid])
                    reduced_old = self._group_loss(groups[old_e], exclude=excluded,
                                                   extra=[order] if old_e == e else [])
                    trial_event = reduced_old if old_e == e else new_loss
                    trial_total = twc - ewc[old_e] + reduced_old
                    if old_e != e:
                        trial_total += new_loss - ewc[e]
                    fw = self.fvc.get(w.ticker)
                    old_delta = 0.0 if fw is None else (1 if w.book_side == "bid" else -1) * old.size * fw.delta
                    trial_base = base - old_delta if c * old_delta > 0 else base
                    old_collateral = old.size * (w.px / PX_SCALE if w.book_side == "bid" else 1 - w.px / PX_SCALE)
                    if (collateral - old_collateral + added_collateral > cfg.risk.risk_capital + 1e-9
                            or not self.risk.loss_limits_ok(event_worst_loss=trial_event, total_worst_loss=trial_total)
                            or not self.risk.delta_ok(abs(trial_base + c), abs(trial_base))):
                        continue
                    canceled = self._cancel(now, w, "opportunity_cost")
                    out += canceled
                    if canceled:
                        self._pending_evictions.add(coid)
                        out.append(Log("allocation", {"event": "cancel_for_better_opportunity", "coid": coid,
                                    "old_ev_rate": old.ev_rate, "candidate": cand.as_log(),
                                    "constraint": reason, "await_cancel_ack": True}))
                    break
                continue
            groups[e]["admitted"].append(order)
            collateral += added_collateral
            ewc[e], twc = new_loss, new_total
            if c >= 0:
                d_up += c
            else:
                d_dn += c
            self.last_place[(s.ticker, side)] = now
            coid = self.ids.next()
            a = PlaceOrder(
                client_order_id=coid, ticker=s.ticker, book_side=side, px=cand.px,
                qty=int(round(n * QTY_SCALE)), post_only=q.post_only,
                expiration_ts=self._expiry_ns(now, coid),
                order_group_id=self.ORDER_GROUP_ID if self.use_order_group else "",
                reason=f"score={score:.3g}",
            )
            self.om.request_place(a, now)
            self.stats.quotes_placed += 1
            out.append(a)
            out.append(Log("quote", {**cand.as_log(), "coid": coid, "ts_decision": now,
                                     "F": f.F, "F_lo": f.F_lo, "F_hi": f.F_hi, "fv_ts": f.ts,
                                     "delta": f.delta, "z_near": f.z_near, "sd_R": f.sd_R,
                                     "tau_s": (s.expiration_ts - now) / NS_PER_S,
                                     "feature_version": PRODUCTION_FEATURE_VERSION,
                                     "strategy_digest": cfg.digest(), "fv_model_hash": self.fv.effective_hash,
                                     "fv_artifact_hash": self.fv.artifact_hash,
                                     "fv_fitted_to_utc": self.fv.fitted_to_utc}))
        return out

    def _expiry_ns(self, now: int, coid: str) -> int:
        """Exchange-side expiry (dead-man backstop). Spread over [T, 1.25 T] by a stable hash of
        the client_order_id, so quotes placed together do not all expire and need re-placing
        in the same instant; deterministic, so replay reproduces it. 0 = good till canceled."""
        exp_s = self.cfg.quoting.order_expiry_s
        if exp_s <= 0:
            return 0
        frac = (zlib.crc32(coid.encode()) & 0xFFFF) / 0x10000
        return int(now + exp_s * (1.0 + 0.25 * frac) * NS_PER_S)

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

    def _update_close_marks(self, now: int) -> list[Action]:
        """Re-mark every position in a market past its ``close_ts`` without a result yet
        (dh.settlement.closemark): the exact payout from our BRTI prints of the window, the
        worst case when a print is missing or the value is within $0.01 of a strike, the last
        trade (then the worst case) when the window cannot be evaluated. A window evaluation is
        kept once final (the tracker prunes old prints). Logs ``close_mark`` on every change."""
        out: list[Action] = []
        for t, spec in self.specs.items():
            if t in self.settled or now < spec.close_ts:
                continue
            q = self._contracts(t)
            if not q:
                self.close_marks.pop(t, None)
                continue
            oc = self._close_outcomes.get(t)
            if oc is None:
                oc = evaluate_window(spec, self.tracker)
                if oc.final:
                    self._close_outcomes[t] = oc
            cm = close_mark(q, oc, last_trade_px=self.last_trade_px.get(t))
            prev = self.close_marks.get(t)
            if prev is None or (prev.px, prev.source) != (cm.px, cm.source):
                self.close_marks[t] = cm
                out.append(Log("close_mark", {"ticker": t, "position": q, **cm.as_log(),
                                              "prev_px": None if prev is None else prev.px}))
        return out

    # ================================================================== reporting
    def equity(self, S: float | None = None) -> float:
        """Cash P&L since start + mark-to-fair of open Kalshi positions + hedge mark. A
        position in a closed market awaiting its result is valued at its close mark
        (``close_marks``), never at the pre-close fair value."""
        eq = self.om.cash_micros() / 1e6 - self.om.fees_micros() / 1e6 + self.settled_cash
        for t in self.specs:
            if t in self.settled:
                continue
            qn = self._contracts(t)
            if qn:
                cm = self.close_marks.get(t)
                if cm is not None:
                    eq += qn * cm.px / PX_SCALE
                    continue
                f = self.fvc.get(t)
                eq += qn * (f.F if f is not None else 0.5)
        if S is not None:
            eq += self.hedge_cash + self.hedge_pos * S - self.hedge_fees
        return eq
