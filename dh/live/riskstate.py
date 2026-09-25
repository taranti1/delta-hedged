"""Risk state carried across restarts of the same UTC day (audit live C1).

A restart must never hand the strategy a fresh daily-loss budget, forget a Halt or cut a
pause short. Two sources, combined conservatively at start-up:

  * the PERSISTED state of the previous session(s) (``RiskStateStore``): day P&L as the
    strategy measured it (RiskEngine.day_pnl: cash + settlements + open positions marked to
    fair value, including earlier seeds), the halt flag and reason, pause_until. Written
    atomically every ``loop.risk_state_interval_s``, on every Halt and at shutdown;
  * live only: the day P&L RE-DERIVED from Kalshi (``derive_day_pnl``): GET /portfolio/fills
    and GET /portfolio/settlements since UTC midnight and the current positions, INCLUDING
    events excluded from this session. Without fair values at start-up it is a strict LOWER
    BOUND of the true day P&L:

        cash of today's fills (YES terms: buy -px*q, sell +px*q) - their fees
      + today's settlements in YES terms ((yes_count - no_count) * YES payout)
      + worst-case value of the positions held now (long YES -> 0, short YES -> -$1 each)
      - best-case value of the positions held at midnight (long YES -> $1 each, short -> 0)

    Exact when the account was flat at midnight and is flat now; otherwise pessimistic by at
    most the notional of the open positions (M1 limits keep that small).

The seed (dh.core.events.RiskStateSeed, pushed once before any Timer and recorded for
replay) takes the LOWER of the two day P&Ls, and the carried halt / pause. A halt whose
reason is not the daily loss (position reconciliation, fee mismatch) is carried into later
days too: only an operator clears it (``--reset-daily-halt``, which also forgives the day's
loss so far; logged).
"""

from __future__ import annotations

import json
import os
from collections.abc import Iterable
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from dh.core.units import NS_PER_S, PX_SCALE, QTY_SCALE, qty_from_fp
from dh.kalshi.wire import opt_iso_to_ns

DAY_NS = 86_400 * NS_PER_S
CARRIED = "carried_over:"


def day_start(ts_ns: int) -> int:
    return ts_ns - ts_ns % DAY_NS


def base_reason(reason: str) -> str:
    """Strip 'carried_over:' prefixes (a carried halt keeps its original reason)."""
    r = reason or ""
    while r.startswith(CARRIED):
        r = r[len(CARRIED):]
    return r


def sticky(reason: str) -> bool:
    """A halt that survives the UTC day boundary (everything but the daily-loss halt)."""
    return bool(reason) and not base_reason(reason).startswith("daily_loss")


# ============================================================================ persistence
@dataclass
class RiskState:
    day_start_ns: int = 0
    day_pnl_usd: float = 0.0
    halted: bool = False
    halt_reason: str = ""
    pause_until_ns: int = 0
    session: str = ""
    mode: str = ""
    updated_ns: int = 0


class RiskStateStore:
    """One JSON file, replaced atomically (tmp + fsync + rename)."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.last: RiskState | None = None

    def load(self) -> RiskState | None:
        try:
            d = json.loads(self.path.read_text())
        except FileNotFoundError:
            return None
        except (OSError, ValueError) as exc:
            raise RiskStateError(f"risk state file {self.path} is unreadable ({exc}): fix or remove it "
                                 "deliberately (it carries halts across restarts)") from exc
        if not isinstance(d, dict):
            raise RiskStateError(f"risk state file {self.path}: expected an object")
        known = {k: d[k] for k in RiskState.__dataclass_fields__ if k in d}
        return RiskState(**known)

    def save(self, st: RiskState, *, fsync: bool = False) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_name(self.path.name + f".tmp{os.getpid()}")
        data = json.dumps(asdict(st), sort_keys=True)
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(data)
            if fsync:
                f.flush()
                os.fsync(f.fileno())
        os.replace(tmp, self.path)
        self.last = st


class RiskStateError(RuntimeError):
    pass


# ============================================================================ REST re-derivation
@dataclass
class DayPnl:
    day_start_ns: int
    pnl_usd: float = 0.0
    cash_usd: float = 0.0
    fees_usd: float = 0.0
    settlements_usd: float = 0.0
    worst_open_usd: float = 0.0
    best_midnight_usd: float = 0.0
    fills: int = 0
    settlements: int = 0
    positions_now: dict[str, int] = field(default_factory=dict)
    positions_midnight: dict[str, int] = field(default_factory=dict)

    def summary(self) -> dict[str, Any]:
        d = asdict(self)
        for k in ("pnl_usd", "cash_usd", "fees_usd", "settlements_usd", "worst_open_usd", "best_midnight_usd"):
            d[k] = round(d[k], 6)
        return d


def _settle_px(row: dict[str, Any]) -> int | None:
    """YES payout of a settlement in px units (10_000 = $1), None if unknown."""
    v = row.get("value")
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        return int(round(float(v) * PX_SCALE / 100))
    res = str(row.get("market_result") or "").lower()
    if res == "yes":
        return PX_SCALE
    if res == "no":
        return 0
    return None


def day_pnl_from_rows(day_start_ns: int, fills: Iterable[dict[str, Any]], settlements: Iterable[dict[str, Any]],
                      positions: dict[str, int], *, subaccount: int = 0) -> DayPnl:
    """The lower bound described in the module docstring (pure; tests feed rows directly).
    ``positions`` is {ticker: signed YES qty (0.01 contracts)}."""
    from dh.kalshi.normalize import rest_fill_to_event, subaccount_of

    out = DayPnl(day_start_ns)
    flow: dict[str, int] = {}
    settled: dict[str, int] = {}
    cash_micros = fee_micros = 0
    for row in fills:
        if subaccount_of(row) != subaccount:
            continue
        try:
            f = rest_fill_to_event(row, 0)
        except (KeyError, ValueError, TypeError):
            continue
        if f.ts_exch and f.ts_exch < day_start_ns:
            continue
        sgn = 1 if f.book_side == "bid" else -1
        flow[f.ticker] = flow.get(f.ticker, 0) + sgn * f.qty
        cash_micros -= sgn * f.yes_px * f.qty  # px (1e-4 $) * qty (1e-2) = 1e-6 $
        fee_micros += f.fee_micros
        out.fills += 1
    settle_micros = 0
    for row in settlements:
        if subaccount_of(row) != subaccount:
            continue
        t = opt_iso_to_ns(row.get("settled_time"))
        if t and t < day_start_ns:
            continue
        px = _settle_px(row)
        try:
            q = qty_from_fp(str(row.get("yes_count_fp") or "0")) - qty_from_fp(str(row.get("no_count_fp") or "0"))
        except ValueError:
            continue
        tk = str(row.get("ticker") or "")
        settled[tk] = settled.get(tk, 0) + q
        if px is None:  # unknown payout: assume the worst for our side
            px = 0 if q > 0 else PX_SCALE
        settle_micros += px * q
        out.settlements += 1
    out.cash_usd = cash_micros / 1e6
    out.fees_usd = fee_micros / 1e6
    out.settlements_usd = settle_micros / 1e6
    worst = 0
    best = 0
    for tk in sorted(set(positions) | set(flow) | set(settled)):
        q_now = int(positions.get(tk, 0))
        q0 = q_now - flow.get(tk, 0) + settled.get(tk, 0)
        if q_now:
            out.positions_now[tk] = q_now
        if q0:
            out.positions_midnight[tk] = q0
        if q_now < 0:
            worst += q_now * PX_SCALE  # short YES pays $1 per contract if YES wins
        if q0 > 0:
            best += q0 * PX_SCALE  # long YES at midnight could have been worth $1 each
    out.worst_open_usd = worst / 1e6
    out.best_midnight_usd = best / 1e6
    out.pnl_usd = out.cash_usd - out.fees_usd + out.settlements_usd + out.worst_open_usd - out.best_midnight_usd
    return out


async def derive_day_pnl(rest: Any, day_start_ns: int, positions: dict[str, int], *, subaccount: int = 0) -> DayPnl:
    """Fetch today's fills and settlements (every page, explicit subaccount) -> DayPnl."""
    min_ts = day_start_ns // NS_PER_S
    fills = [f async for f in rest.iter_fills(min_ts=min_ts, subaccount=subaccount)]
    settles = [s async for s in rest.iter_settlements(min_ts=min_ts, subaccount=subaccount)]
    return day_pnl_from_rows(day_start_ns, fills, settles, positions, subaccount=subaccount)


# ============================================================================ the seed
@dataclass
class SeedDecision:
    day_start_ns: int
    day_pnl_usd: float
    halted: bool
    halt_reason: str
    pause_until_ns: int
    notes: list[str] = field(default_factory=list)
    overridden: dict[str, Any] = field(default_factory=dict)  # what --reset-daily-halt forgave


def decide_seed(now_ns: int, prev: RiskState | None, rest_pnl: DayPnl | None, *, reset: bool = False) -> SeedDecision:
    """Combine the persisted state and the REST lower bound (see module docstring)."""
    ds = day_start(now_ns)
    same_day = prev is not None and prev.day_start_ns == ds
    cands: list[float] = []
    notes: list[str] = []
    if same_day and prev is not None:
        cands.append(float(prev.day_pnl_usd))
        notes.append(f"persisted day P&L {prev.day_pnl_usd:+.2f} (session {prev.session or '?'})")
    if rest_pnl is not None:
        cands.append(float(rest_pnl.pnl_usd))
        notes.append(f"REST day P&L lower bound {rest_pnl.pnl_usd:+.2f}")
    pnl = min(cands) if cands else 0.0
    halted = False
    reason = ""
    pause = 0
    if prev is not None and prev.halted and (same_day or sticky(prev.halt_reason)):
        halted, reason = True, base_reason(prev.halt_reason) or "halt"
        notes.append(f"halt carried over: {reason}")
    if prev is not None and prev.pause_until_ns > now_ns:
        pause = int(prev.pause_until_ns)
        notes.append(f"pause carried over until {pause}")
    dec = SeedDecision(ds, pnl, halted, reason, pause, notes)
    if reset:
        dec.overridden = {"day_pnl_usd": pnl, "halted": halted, "halt_reason": reason, "pause_until_ns": pause}
        dec.day_pnl_usd, dec.halted, dec.halt_reason, dec.pause_until_ns = 0.0, False, "", 0
        dec.notes.append("OPERATOR RESET (--reset-daily-halt): halt, pause and the day's loss so far forgiven")
    return dec


__all__ = ["DAY_NS", "DayPnl", "RiskState", "RiskStateError", "RiskStateStore", "SeedDecision", "base_reason",
           "day_pnl_from_rows", "day_start", "decide_seed", "derive_day_pnl", "sticky"]
