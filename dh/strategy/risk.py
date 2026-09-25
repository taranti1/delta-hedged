"""Deterministic hard limits and kill switches (docs/MODELS.md section 6).

All inputs arrive through events or explicit calls carrying `now_ns`; there is no clock.
The engine never *reduces* protection automatically except via explicit timed pauses; a
Halt(scope='all') requires a manual Resume (a live operator action, or the replay script).
"""

from __future__ import annotations

from dataclasses import dataclass, field

from dh.core.actions import Action, CancelAll, Halt, Log
from dh.core.events import FeedStatus
from dh.core.units import NS_PER_S
from dh.strategy.config import RiskCfg


@dataclass
class Health:
    quoting_allowed: bool
    near_expiry_allowed: bool  # quoting on markets with tau < 10 min allowed (BRTI fresh)
    hedging_allowed: bool
    reasons: list[str] = field(default_factory=list)


class RiskEngine:
    """Feed-status routing is by EXACT stream name (never by prefix):

      cfg.kalshi_stream ('kalshi.ws')      connection: disconnected/stale/gap -> not ready + CancelAll;
      'kalshi.ws:<channel>'                per-channel sequence gaps: own-activity channels (fill,
                                           user_orders, market_positions, order_group_updates) ->
                                           CancelAll + pause own_gap_pause_s + reconcile; others
                                           (trade, cfbenchmarks, lifecycle) are informational
                                           connected/resynced -> ready after book_resume_after_s;
                                           'error' (bad frame, command error) is logged only
      'kalshi.book:<ticker>'               per-market book validity (gap/disconnected -> invalid;
                                           resynced -> valid after book_resume_after_s)
      'kalshi.order_group:<id>'            'error' = exchange auto-canceled the group (fill burst):
                                           pause quoting order_group_cooldown_s; 'resynced' = reset
      cfg.hedge_stream ('kalshi_perp.ws')  hedge venue connection
      '<venue>.ws'                         external venue: disconnected/stale -> venue counted stale
    """

    def __init__(self, cfg: RiskCfg) -> None:
        self.cfg = cfg
        self.halted_all = False
        self.halted_quoting = False
        self.halt_reason = ""
        self.pause_until_ns = 0
        self.last_brti_ns = 0
        self.brti_resume_ns = 0
        self.last_ext_ns: dict[str, int] = {}
        self.kalshi_ok = False
        self.kalshi_resume_ns = 0
        self.invalid_books: set[str] = set()
        self.book_resume_ns: dict[str, int] = {}
        self.group_pause_until_ns = 0
        self.groups_triggered: set[str] = set()
        self.hedge_venue_ok = True
        self.day = -1
        self.day_start_equity = 0.0
        # carried over from earlier sessions of the same UTC day (RiskStateSeed; audit live C1)
        self.seed_day = -1
        self.seed_day_pnl = 0.0
        self.lag_ok = True  # runner consumer/data lag (audit live M1)
        self.clock_ok = True  # runner receive-clock offset within limits (audit live m9)
        self.reconciling = False  # own-activity reconciliation after a reconnect (audit live M3)
        self.fee_mismatch = False
        self.recon_mismatch = False
        self.log: list[tuple[int, str]] = []  # (ts, message) informational, bounded by caller

    # ------------------------------------------------------------------ feed health
    def note_brti(self, ts_ns: int, src_ns: int = 0) -> None:
        """Called on every settlement-benchmark tick (receive time ts_ns, CF source time
        src_ns when known). An inter-tick gap longer than the cancel-all threshold is an
        outage: quoting resumes only after brti_resume_after_s of fresh ticks. Staleness is
        judged on the SOURCE time too (audit m2): a tick delivered now but printed 20 s ago is
        stale."""
        c = self.cfg
        if self.last_brti_ns and ts_ns - self.last_brti_ns > c.stale_brti_cancel_all_s * NS_PER_S:
            self.brti_resume_ns = ts_ns + int(c.brti_resume_after_s * NS_PER_S)
        self.last_brti_ns = max(self.last_brti_ns, ts_ns)
        if src_ns:
            self.last_brti_src_ns = max(getattr(self, "last_brti_src_ns", 0), src_ns)

    def brti_age_s(self, now_ns: int) -> float:
        """Benchmark age: max of receive age and source age (source age only when known)."""
        if not self.last_brti_ns:
            return float("inf")
        age = (now_ns - self.last_brti_ns) / NS_PER_S
        src = getattr(self, "last_brti_src_ns", 0)
        if src:
            age = max(age, (now_ns - src) / NS_PER_S)
        return age

    def note_ext(self, venue: str, ts_ns: int) -> None:
        self.last_ext_ns[venue] = max(self.last_ext_ns.get(venue, 0), ts_ns)

    def on_seed(self, ev) -> list[Action]:
        """RiskStateSeed: the UTC day's P&L before this session, a carried-over halt or pause."""
        day_ns = 86_400 * NS_PER_S
        day = ev.day_start_ns // day_ns
        out: list[Action] = [Log("risk", {"event": "risk_seed", "day_start_ns": ev.day_start_ns,
                                          "day_pnl_usd": ev.day_pnl_usd, "halted": ev.halted,
                                          "halt_reason": ev.halt_reason, "pause_until_ns": ev.pause_until_ns})]
        if day != ev.ts // day_ns:
            out.append(Log("risk", {"event": "risk_seed_ignored", "reason": "seed is for another UTC day"}))
            return out
        self.seed_day, self.seed_day_pnl = day, float(ev.day_pnl_usd)
        self.pause_until_ns = max(self.pause_until_ns, int(ev.pause_until_ns))
        scope = "quoting" if getattr(ev, "halt_scope", "all") == "quoting" else "all"
        if ev.halted and not self.halted_all and not (scope == "quoting" and self.halted_quoting):
            out += self._halt(ev.ts, f"carried_over:{ev.halt_reason or 'halt'}", scope=scope)
        elif not self.halted_all and self.seed_day_pnl <= -self.cfg.daily_loss_halt:
            out += self._halt(ev.ts, "daily_loss", scope="all")
        return out

    def on_feed_status(self, ev: FeedStatus) -> list[Action]:
        c = self.cfg
        out: list[Action] = []
        st = ev.stream
        if st == c.lag_stream:
            if ev.status in ("stale", "gap", "disconnected"):
                if self.lag_ok:
                    out.append(CancelAll(reason="runner_lag"))
                self.lag_ok = False
            elif ev.status in ("resumed", "resynced", "connected"):
                self.lag_ok = True
            return out
        if st == c.clock_stream:
            if ev.status in ("stale", "gap", "disconnected", "error"):
                if self.clock_ok:
                    out.append(CancelAll(reason="clock_offset"))
                self.clock_ok = False
            elif ev.status in ("resumed", "resynced", "connected"):
                self.clock_ok = True
            return out
        if st == c.reconcile_stream:
            if ev.status in ("stale", "gap", "disconnected"):
                if not self.reconciling:
                    out.append(CancelAll(reason="reconciling"))
                self.reconciling = True
            elif ev.status in ("resynced", "resumed", "connected"):
                self.reconciling = False
            return out
        if st == c.kalshi_stream:
            if ev.status in ("disconnected", "gap", "stale"):
                if self.kalshi_ok or ev.status == "gap":
                    out.append(CancelAll(reason=f"kalshi_{ev.status}"))
                self.kalshi_ok = False
            elif ev.status in ("connected", "resynced", "resumed"):
                self.kalshi_ok = True
                self.kalshi_resume_ns = ev.ts + int(c.book_resume_after_s * NS_PER_S)
            else:  # 'error': malformed frame / command error -> informational
                self.log.append((ev.ts, f"kalshi error: {ev.detail}"))
        elif st.startswith(c.kalshi_stream + ":"):
            channel = st.split(":", 1)[1]
            if ev.status == "gap" and channel in ("fill", "user_orders", "market_positions", "order_group_updates"):
                # own-activity messages lost: our position/order view may be wrong until reconciled
                self.pause_until_ns = max(self.pause_until_ns, ev.ts + int(c.own_gap_pause_s * NS_PER_S))
                out.append(CancelAll(reason=f"own_channel_gap:{channel}"))
                out.append(Log("risk", {"event": "reconcile_requested", "channel": channel, "detail": ev.detail}))
            else:
                self.log.append((ev.ts, f"channel {channel} {ev.status}: {ev.detail}"))
        elif st.startswith("kalshi.book:"):
            ticker = st.split(":", 1)[1]
            if ev.status in ("gap", "disconnected", "stale", "error"):
                if ticker not in self.invalid_books:
                    out.append(CancelAll(reason=f"book_{ev.status}", tickers=(ticker,)))
                self.invalid_books.add(ticker)
            elif ev.status in ("resynced", "connected", "resumed"):
                self.invalid_books.discard(ticker)
                self.book_resume_ns[ticker] = ev.ts + int(c.book_resume_after_s * NS_PER_S)
        elif st.startswith("kalshi.order_group:"):
            gid = st.split(":", 1)[1]
            if ev.status == "error":  # triggered: exchange auto-canceled the group's orders
                self.groups_triggered.add(gid)
                self.group_pause_until_ns = max(self.group_pause_until_ns,
                                                ev.ts + int(c.order_group_cooldown_s * NS_PER_S))
                out.append(Log("risk", {"event": "order_group_triggered", "group": gid}))
            elif ev.status in ("resynced", "connected"):
                self.groups_triggered.discard(gid)
        elif st == c.hedge_stream:
            if ev.status in ("disconnected", "stale", "error", "gap"):
                self.hedge_venue_ok = False
            elif ev.status in ("connected", "resynced", "resumed"):
                self.hedge_venue_ok = True
        elif st.endswith(".ws"):
            venue = st[: -len(".ws")]
            if ev.status in ("disconnected", "stale") and venue in self.last_ext_ns:
                self.last_ext_ns[venue] = 0  # counts as stale until data flows again
        return out

    def book_ok(self, ticker: str, now_ns: int) -> bool:
        return ticker not in self.invalid_books and now_ns >= self.book_resume_ns.get(ticker, 0)

    def health(self, now_ns: int) -> Health:
        c = self.cfg
        reasons: list[str] = []
        brti_age = self.brti_age_s(now_ns)
        fresh_ext = sum(1 for t in self.last_ext_ns.values() if t and (now_ns - t) / NS_PER_S <= c.stale_ext_s)
        quoting = True
        if self.halted_all or self.halted_quoting:
            quoting = False
            reasons.append(f"halted:{self.halt_reason}")
        if now_ns < self.pause_until_ns:
            quoting = False
            reasons.append("paused")
        if not self.lag_ok:
            quoting = False
            reasons.append("runner_lag")
        if not self.clock_ok:
            quoting = False
            reasons.append("clock_offset")
        if self.reconciling:
            quoting = False
            reasons.append("reconciling")
        if now_ns < self.group_pause_until_ns or self.groups_triggered:
            quoting = False
            reasons.append("order_group_triggered")
        if not self.kalshi_ok or now_ns < self.kalshi_resume_ns:
            quoting = False
            reasons.append("kalshi_not_ready")
        if brti_age > c.stale_brti_cancel_all_s:
            quoting = False
            reasons.append(f"brti_stale_{brti_age:.1f}s")
        if now_ns < self.brti_resume_ns:
            quoting = False
            reasons.append("brti_recovering")
        if self.last_ext_ns and fresh_ext < min(2, len(self.last_ext_ns)):
            quoting = False
            reasons.append("external_stale")
        if self.fee_mismatch:
            quoting = False
            reasons.append("fee_mismatch")
        if self.recon_mismatch:
            quoting = False
            reasons.append("position_reconciliation")
        near_ok = quoting and brti_age <= c.stale_brti_cancel_near_s
        hedging = (not self.halted_all) and self.hedge_venue_ok
        return Health(quoting, near_ok, hedging, reasons)

    # ------------------------------------------------------------------ limits
    def market_capacity(
        self,
        *,
        side: str,
        position: float,
        working_bid: float,
        working_ask: float,
        tau_s: float,
    ) -> float:
        """Max additional contracts on `side` keeping |worst-case position| <= limit."""
        lim = self.cfg.max_pos_per_market
        if tau_s < self.cfg.near_expiry_s:
            lim *= self.cfg.near_expiry_limit_mult
        if side == "bid":
            return max(0.0, lim - (position + working_bid))
        return max(0.0, lim + (position - working_ask))

    def loss_limits_ok(self, *, event_worst_loss: float, total_worst_loss: float) -> bool:
        return (
            event_worst_loss <= self.cfg.max_event_worst_loss
            and total_worst_loss <= self.cfg.max_total_worst_loss
        )

    def delta_ok(self, new_abs_delta_btc: float, current_abs_delta_btc: float) -> bool:
        """Allow if within the limit, or if the order reduces |delta|."""
        return new_abs_delta_btc <= self.cfg.max_abs_delta_btc or new_abs_delta_btc < current_abs_delta_btc

    # ------------------------------------------------------------------ P&L / events
    def on_equity(self, now_ns: int, equity: float) -> list[Action]:
        """equity = realized cash P&L + mark-to-fair of open positions (since start)."""
        day = now_ns // (86_400 * NS_PER_S)
        if day != self.day:
            self.day = day
            self.day_start_equity = equity
        carried = self.seed_day_pnl if day == self.seed_day else 0.0  # earlier sessions today
        if not self.halted_all and equity - self.day_start_equity + carried <= -self.cfg.daily_loss_halt:
            return self._halt(now_ns, "daily_loss", scope="all")
        return []

    def day_pnl(self, now_ns: int, equity: float) -> float:
        """The UTC day's P&L so far, including sessions before this one (for the runner)."""
        day = now_ns // (86_400 * NS_PER_S)
        start = self.day_start_equity if day == self.day else equity
        return equity - start + (self.seed_day_pnl if day == self.seed_day else 0.0)

    def on_settlement_pnl(self, now_ns: int, pnl: float) -> list[Action]:
        if pnl <= -self.cfg.settlement_loss_halt:
            self.pause_until_ns = max(self.pause_until_ns, now_ns + 3600 * NS_PER_S)
            return [CancelAll(reason="settlement_loss"), Log("risk", {"event": "settlement_loss_pause", "pnl": pnl})]
        return []

    def on_abnormal_move(self, now_ns: int, move_sigma: float) -> list[Action]:
        if abs(move_sigma) >= self.cfg.abnormal_move_sigma:
            self.pause_until_ns = max(self.pause_until_ns, now_ns + int(self.cfg.abnormal_pause_s * NS_PER_S))
            return [CancelAll(reason="abnormal_move"), Log("risk", {"event": "abnormal_move", "sigma": move_sigma})]
        return []

    def on_fee_mismatch(self, now_ns: int, detail: str) -> list[Action]:
        self.fee_mismatch = True
        return self._halt(now_ns, f"fee_mismatch:{detail}", scope="quoting")

    def on_reconciliation_mismatch(self, now_ns: int, detail: str) -> list[Action]:
        self.recon_mismatch = True
        return self._halt(now_ns, f"reconciliation:{detail}", scope="all")

    def manual_resume(self) -> None:
        self.halted_all = self.halted_quoting = False
        self.fee_mismatch = self.recon_mismatch = False
        self.halt_reason = ""

    def _halt(self, now_ns: int, reason: str, scope: str) -> list[Action]:
        if scope == "all":
            self.halted_all = True
        self.halted_quoting = True
        self.halt_reason = reason
        return [CancelAll(reason=reason), Halt(reason=reason, scope=scope)]  # type: ignore[arg-type]
