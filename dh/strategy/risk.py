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
    def __init__(self, cfg: RiskCfg) -> None:
        self.cfg = cfg
        self.halted_all = False
        self.halted_quoting = False
        self.halt_reason = ""
        self.pause_until_ns = 0
        self.last_brti_ns = 0
        self.last_ext_ns: dict[str, int] = {}
        self.kalshi_ok = False
        self.kalshi_resume_ns = 0
        self.hedge_venue_ok = True
        self.day = -1
        self.day_start_equity = 0.0
        self.fee_mismatch = False
        self.recon_mismatch = False

    # ------------------------------------------------------------------ feed health
    def note_brti(self, ts_ns: int) -> None:
        self.last_brti_ns = max(self.last_brti_ns, ts_ns)

    def note_ext(self, venue: str, ts_ns: int) -> None:
        self.last_ext_ns[venue] = max(self.last_ext_ns.get(venue, 0), ts_ns)

    def on_feed_status(self, ev: FeedStatus) -> list[Action]:
        out: list[Action] = []
        if ev.stream.startswith("kalshi"):
            if ev.status in ("disconnected", "gap", "error", "stale"):
                if self.kalshi_ok or ev.status == "gap":
                    out.append(CancelAll(reason=f"kalshi_{ev.status}"))
                self.kalshi_ok = False
            elif ev.status in ("connected", "resynced"):
                self.kalshi_ok = True
                self.kalshi_resume_ns = ev.ts + 5 * NS_PER_S  # books must be valid for 5 s
        elif ev.stream.startswith("hedge"):
            self.hedge_venue_ok = ev.status in ("connected", "resynced")
        return out

    def health(self, now_ns: int) -> Health:
        c = self.cfg
        reasons: list[str] = []
        brti_age = (now_ns - self.last_brti_ns) / NS_PER_S if self.last_brti_ns else float("inf")
        fresh_ext = sum(1 for t in self.last_ext_ns.values() if (now_ns - t) / NS_PER_S <= c.stale_ext_s)
        quoting = True
        if self.halted_all or self.halted_quoting:
            quoting = False
            reasons.append(f"halted:{self.halt_reason}")
        if now_ns < self.pause_until_ns:
            quoting = False
            reasons.append("paused")
        if not self.kalshi_ok or now_ns < self.kalshi_resume_ns:
            quoting = False
            reasons.append("kalshi_not_ready")
        if brti_age > c.stale_brti_cancel_all_s:
            quoting = False
            reasons.append(f"brti_stale_{brti_age:.1f}s")
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
        if not self.halted_all and equity - self.day_start_equity <= -self.cfg.daily_loss_halt:
            return self._halt(now_ns, "daily_loss", scope="all")
        return []

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
