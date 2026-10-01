#!/usr/bin/env python3
"""Live view of the delta-hedged runner: a pinned terminal screen, READ-ONLY.

    python scripts/live_view.py                 # the live runner (data/live_logs, :9108)
    python scripts/live_view.py --paper         # the paper runner (data/paper_logs, :9109)
    python scripts/live_view.py --log data/live_logs/<session>.jsonl --once   # one frame, no loop

It only reads: the newest session log (tailed by byte offset, a torn last line carried over),
the runner's heartbeat file, and its /health and /metrics endpoints. It never places, cancels
or decides anything; if it breaks, trading is unaffected (Ctrl-C it freely). Stop trading from
the runner's own terminal (Ctrl-C) or the kill file (docs/RUNBOOK.md section 6).

Layout, after System 2's watch screen (trading-strategy DECISIONS D-083 / D-052):
  header        mode, session, uptime, heartbeat age, Kalshi WS, BRTI age, clock offset
  money         day P&L vs the daily-loss halt (bar), realized / open, shard funds vs required
  banner        red when new orders are blocked or the runner is halted / stale / stopped,
                every reason in words; unknown reason codes are shown raw and count as bad
  positions     open positions with cost, model value and mark
  last 15 min   quotes, cancels by reason, fills, gates: repeats folded with counts
  events        one line per money- or state-changing event (fills, settlements, halts, gates)
Alerts (--notify, on by default on macOS): one notification + sound per NEW problem (halt,
gate closed, heartbeat stale, runner stopped), re-armed when it clears; fills only with
--notify-fills. NO_COLOR is respected; output that is not a terminal gets plain log lines.
"""

from __future__ import annotations

import argparse
import collections
import glob
import json
import os
import re
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
NS = 1_000_000_000
ET = timezone(timedelta(hours=-4))  # EDT (the exchange's clock); fine for display

# ------------------------------------------------------------------ words for machine codes
GATE_WORDS = {
    "lag": "market data is lagging",
    "reconciling": "re-checking orders with Kalshi after a reconnect",
    "reconcile": "re-checking orders with Kalshi",
    "cancel_all_hold": "holding after a cancel-all",
    "clock": "computer clock is off",
    "exchange_pause": "Kalshi trading is paused",
    "balance": "shard funds below the required amount",
    "watchdog": "watchdog is not protecting the runner",
    "disk": "disk space is low",
    "shutdown": "runner is shutting down",
    "fee_mismatch": "a fill's fee did not match the model",
    "kill": "kill switch",
    "unknown_order": "an order Kalshi reported is not recognized",
}
CANCEL_WORDS = {
    "ev": "re-priced", "unhealthy": "data not healthy", "benchmark_age": "BRTI stale",
    "final_window_near_strike": "final window", "event_over_limit": "event loss limit",
    "book_invalid": "order book invalid", "brti_gap_in_window": "BRTI gap in window",
    "brti_not_fresh_near_expiry": "BRTI stale near expiry", "paused": "market paused",
    "deferred": "deferred", "cancel_retry": "retry", "revived": "stray order",
    "opportunity_cost": "freed for a better quote", "market_disagreement": "Kalshi price disagrees",
}


# ------------------------------------------------------------------ presentation helpers
class Paint:
    def __init__(self, on: bool) -> None:
        self.on = on

    def _w(self, code: str, s: str) -> str:
        return f"\x1b[{code}m{s}\x1b[0m" if self.on else s

    def green(self, s): return self._w("32", s)
    def amber(self, s): return self._w("33", s)
    def red(self, s): return self._w("31", s)
    def cyan(self, s): return self._w("36", s)
    def dim(self, s): return self._w("2", s)
    def bold(self, s): return self._w("1", s)
    def head(self, s): return self._w("1;7", s)
    def alarm(self, s): return self._w("1;41;97", s)


_ANSI = re.compile(r"\x1b\[[0-9;]*m")


def visible_len(s: str) -> int:
    return len(_ANSI.sub("", s))


def clip(s: str, width: int) -> str:
    """Clip to ``width`` visible columns, keeping colour codes intact."""
    if visible_len(s) <= width:
        return s
    out, n, i = [], 0, 0
    while i < len(s) and n < width - 1:
        m = _ANSI.match(s, i)
        if m:
            out.append(m.group(0))
            i = m.end()
            continue
        out.append(s[i])
        n += 1
        i += 1
    return "".join(out) + ("…\x1b[0m" if "\x1b[" in s else "…")


def money(x: float | None, plus: bool = True) -> str:
    if x is None:
        return "—"
    if abs(x) < 0.005:
        return "$0.00"
    return ("+" if x > 0 and plus else "−" if x < 0 else "") + f"${abs(x):,.2f}"


def bar(frac: float, width: int = 20) -> str:
    frac = max(0.0, min(1.0, frac))
    full = int(round(frac * width))
    return "▕" + "█" * full + " " * (width - full) + "▏"


def hhmmss(t_ns: int) -> str:
    return datetime.fromtimestamp(t_ns / NS, ET).strftime("%H:%M:%S")


def ago(seconds: float) -> str:
    s = int(max(0, seconds))
    return f"{s}s" if s < 90 else f"{s // 60}m" if s < 5400 else f"{s // 3600}h{(s % 3600) // 60:02d}"


def market_name(ticker: str) -> str:
    """KXBTCD-26SEP3023-T83299.99 -> '11pm ≥83,300' (expiry hour ET, YES above the strike)."""
    try:
        _, code, strike = ticker.split("-", 2)
        hour = int(code[7:9])
        h12 = f"{(hour % 12) or 12}{'am' if hour < 12 else 'pm'}"
        k = float(strike[1:])
        return f"{h12} ≥{round(k + 0.01):,}"
    except (ValueError, IndexError):
        return ticker


def cents(px: float) -> str:
    return f"{px * 100:.0f}¢"


# ------------------------------------------------------------------ log tail
class Tail:
    """Incremental reader of a growing JSON-lines file (byte offset, torn line carried)."""

    MAX_SLICE = 4 * 1024 * 1024

    def __init__(self, path: Path, from_start: bool = True) -> None:
        self.path = path
        self.pos = 0 if from_start else path.stat().st_size
        self.carry = b""

    def read(self, keep=None) -> list[dict]:
        """New complete records; ``keep(line_bytes)`` may skip lines before JSON parsing."""
        out: list[dict] = []
        try:
            with open(self.path, "rb") as f:
                f.seek(self.pos)
                chunk = f.read(self.MAX_SLICE)
        except OSError:
            return out
        if not chunk:
            return out
        self.pos += len(chunk)
        data = self.carry + chunk
        lines = data.split(b"\n")
        self.carry = lines.pop()  # a torn last line waits for the rest
        for ln in lines:
            if not ln or b'"k":"log.opportunity_rejected"' in ln or b'"k":"log.queue_trade"' in ln:
                continue
            if keep is not None and not keep(ln):
                continue
            try:
                out.append(json.loads(ln))
            except ValueError:
                continue
        return out


def newest_log(log_dir: Path) -> Path | None:
    files = sorted(glob.glob(str(log_dir / "*.jsonl")), key=os.path.getmtime)
    return Path(files[-1]) if files else None


# ------------------------------------------------------------------ state from the log
@dataclass
class Position:
    qty: float = 0.0  # signed YES contracts
    cash: float = 0.0  # $ paid (negative) / received (positive)


@dataclass
class State:
    session: str = ""
    mode: str = ""
    started_ns: int = 0
    ended: bool = False
    last_t: int = 0
    positions: dict[str, Position] = field(default_factory=dict)
    fv: dict[str, float] = field(default_factory=dict)
    settled_pnl: float = 0.0
    fills: int = 0
    contracts: float = 0.0
    events: collections.deque = field(default_factory=lambda: collections.deque(maxlen=400))
    window: collections.deque = field(default_factory=collections.deque)  # (t_ns, category, label)
    gates_open: dict[str, str] = field(default_factory=dict)  # reason -> detail (from the log)
    disagree_on: set = field(default_factory=set)
    _last_fill: list | None = None
    _fold: dict = field(default_factory=dict)

    def _event(self, t: int, tone: str, text: str, fold_key: str | None = None) -> None:
        """Scrollback line; a repeat of the same fold_key within 15 min updates a counter."""
        if fold_key is not None:
            prev = self._fold.get(fold_key)
            if prev is not None and t - prev[0] < 900 * NS and prev[1] in self.events:
                entry = prev[1]
                entry[3] += 1
                entry[0] = t
                return
        entry = [t, tone, text, 1]
        self.events.append(entry)
        if fold_key is not None:
            self._fold[fold_key] = (t, entry)

    def _count(self, t: int, cat: str, label: str) -> None:
        self.window.append((t, cat, label))

    def apply(self, r: dict) -> None:
        k, t = r.get("k", ""), int(r.get("t", 0) or 0)
        self.last_t = max(self.last_t, t)
        if k == "session_start":
            self.session, self.mode, self.started_ns = r.get("session", ""), r.get("mode", ""), t
            self._event(t, "cyan", f"session {self.session} started ({self.mode})")
        elif k == "log.fv":
            if r.get("ticker") in self.positions and r.get("F") is not None:
                self.fv[r["ticker"]] = float(r["F"])
        elif k == "log.fill":
            tk, q, px = r["ticker"], float(r["qty"]) / 100, float(r["px"]) / 10_000
            sgn = 1 if r.get("side") == "bid" else -1
            p = self.positions.setdefault(tk, Position())
            p.qty += sgn * q
            p.cash -= sgn * q * px
            p.cash -= float(r.get("fee") or 0) / 1e6
            if r.get("F") is not None:
                self.fv[tk] = float(r["F"])
            self.fills += 1
            self.contracts += q
            verb = "bought YES" if sgn > 0 else "sold YES"
            edge = None if r.get("F") is None else sgn * (float(r["F"]) - px) * 100
            e = "" if edge is None else f" · model {float(r['F']) * 100:.1f}% ({edge:+.1f}¢ edge)"
            lf = self._last_fill
            if lf and lf[0] == r.get("coid") and lf[1] == px and t - lf[2] < 2 * NS and self.events and self.events[-1] is lf[4]:
                lf[3] += q  # a partial fill of the same order: one line, summed
                lf[4][2] = f"FILL  {verb} {lf[3]:g} × {market_name(tk)} @ {cents(px)}{e}"
            else:
                self._event(t, "green", f"FILL  {verb} {q:g} × {market_name(tk)} @ {cents(px)}{e}")
                self._last_fill = [r.get("coid"), px, t, q, self.events[-1]]
            self._count(t, "fill", verb)
        elif k == "log.settle":
            tk = r.get("ticker", "")
            pnl = float(r.get("pnl") or 0)  # the market's whole P&L, round trips included
            held = float(r.get("position") or 0)
            if held or abs(pnl) > 1e-9:
                self.settled_pnl += pnl
                won = "YES" if int(r.get("px", 0)) >= 10_000 else "NO"
                what = f"long {held:g}" if held > 0 else f"short {-held:g}" if held < 0 else "closed before expiry"
                self._event(t, "green" if pnl >= 0 else "red",
                            f"SETTLED {market_name(tk)} → {won} · {what} · {money(pnl)}")
            self.positions.pop(tk, None)
            self.fv.pop(tk, None)
        elif k == "action":
            typ = r.get("type")
            if typ == "PlaceOrder":
                self._count(t, "quote", "placed")
            elif typ == "CancelOrder":
                self._count(t, "cancel", CANCEL_WORDS.get(r.get("reason", ""), r.get("reason", "?")))
            elif typ == "CancelAll":
                self._event(t, "amber", f"CANCEL ALL · {r.get('reason', '')}", fold_key="cancelall:" + r.get("reason", ""))
            elif typ == "Halt":
                self._event(t, "red", f"HALT ({r.get('scope', '')}) · {r.get('reason', '')}")
        elif k == "gate":
            reason = r.get("reason", "?")
            words = GATE_WORDS.get(reason, reason)
            if r.get("action") == "close":
                self.gates_open[reason] = str(r.get("why") or "")[:80]
                self._event(t, "red", f"BLOCKED · {words}", fold_key="gate:" + reason)
                self._count(t, "gate", words)
            else:
                self.gates_open.pop(reason, None)
                self._event(t, "green", f"unblocked · {words}", fold_key="gateopen:" + reason)
        elif k == "halt":
            self._event(t, "red", f"HALT ({r.get('scope', '')}) · {r.get('reason', '')}")
        elif k in ("kill", "kill_switch"):
            self._event(t, "red", f"KILL · {r.get('reason') or r.get('why') or ''}")
        elif k == "log.risk":
            ev = r.get("event", "")
            if ev == "abnormal_move":
                self._event(t, "amber", "big BTC move (6σ) · kept quoting" if r.get("action") == "none"
                            else "big BTC move (6σ) · quotes pulled, paused", fold_key="abnormal")
                self._count(t, "risk", "big BTC move")
            elif ev not in ("risk_seed",):
                self._event(t, "amber", f"risk · {ev}", fold_key="risk:" + ev)
        elif k == "log.quote_gate":
            tk = r.get("ticker", "")
            if r.get("on"):
                self.disagree_on.add(tk)
                self._count(t, "gate", "Kalshi price disagrees >10¢")
            else:
                self.disagree_on.discard(tk)
        elif k in ("shutdown", "session_end"):
            self.ended = True
            self._event(t, "cyan", "runner shutting down" if k == "shutdown" else "session ended")

    def window_counts(self, now_ns: int) -> dict[str, collections.Counter]:
        while self.window and now_ns - self.window[0][0] > 900 * NS:
            self.window.popleft()
        out: dict[str, collections.Counter] = collections.defaultdict(collections.Counter)
        for _, cat, label in self.window:
            out[cat][label] += 1
        return out


# ------------------------------------------------------------------ runner endpoints
def get_json(url: str) -> dict | None:
    """The runner's /health (HTTP 503 = not ok, still with its JSON body); None if unreachable."""
    try:
        with urllib.request.urlopen(url, timeout=0.7) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        try:
            return json.loads(e.read())
        except ValueError:
            return None
    except Exception:  # noqa: BLE001 - runner not running / not listening
        return None


_METRIC = re.compile(r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{[^}]*\})?\s+(\S+)")


def get_metrics(url: str) -> dict[str, float]:
    try:
        with urllib.request.urlopen(url, timeout=0.7) as r:
            text = r.read().decode()
    except Exception:  # noqa: BLE001
        return {}
    out: dict[str, float] = {}
    for line in text.splitlines():
        m = _METRIC.match(line)
        if m and not line.startswith("#"):
            try:
                out[m.group(1) + (m.group(2) or "")] = float(m.group(3))
            except ValueError:
                pass
    return out


def read_heartbeat(path: Path) -> dict | None:
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError):
        return None


# ------------------------------------------------------------------ problems (banner + alerts)
def problems(state: State, health: dict | None, hb: dict | None, now_ns: int, stale_s: float) -> list[str]:
    out: list[str] = []
    if hb is None and health is None:
        return ["runner not running (no heartbeat, no /health)"]
    if hb is not None:
        age = (now_ns - int(hb.get("t", 0))) / NS
        if hb.get("state") in ("stopped", "stopping"):
            out.append(f"runner {hb.get('state')}")
        elif age > stale_s:
            out.append(f"heartbeat stale ({age:.0f}s old): runner frozen or dead")
    if health is not None:
        for g in health.get("gate") or []:
            if g == "shutdown" and any(p.startswith("runner st") for p in out):
                continue
            out.append(f"new orders blocked: {GATE_WORDS.get(g, g)}")
        for h in health.get("halts") or []:
            out.append(f"HALTED: {h if isinstance(h, str) else json.dumps(h)}")
        if health.get("kill_latched"):
            out.append(f"KILL: {health['kill_latched']}")
        if health.get("fv_ready") is False:
            out.append("fair-value model not ready: no quoting (warm-up missing)")
        if health.get("stuck_cancels"):
            out.append(f"{len(health['stuck_cancels'])} cancel(s) not confirmed by Kalshi")
        if health.get("watchdog") not in (None, "ok"):
            out.append(f"watchdog: {health.get('watchdog')}")
        if health.get("disk") not in (None, "ok"):
            out.append(f"disk: {health.get('disk')}")
    return out


class Notifier:
    """macOS notification + sound, once per new problem; re-armed when it clears."""

    def __init__(self, enabled: bool) -> None:
        self.enabled = enabled and sys.platform == "darwin" and shutil.which("osascript") is not None
        self.active: set[str] = set()

    @staticmethod
    def key(problem: str) -> str:
        return re.sub(r"\(.*?\)|\d+", "", problem).strip()  # "stale (5s old)" == "stale (6s old)"

    def update(self, current: list[str]) -> None:
        keys = {self.key(p) for p in current}
        for p in current:
            if self.key(p) not in self.active:
                self.send("delta-hedged: problem", p, sound="Basso")
        self.active = keys

    def send(self, title: str, text: str, sound: str = "Glass") -> None:
        if not self.enabled:
            return
        # text passed as arguments, never spliced into the script (D-047)
        script = ('on run argv\n display notification (item 2 of argv) with title (item 1 of argv) '
                  'sound name (item 3 of argv)\nend run')
        try:
            subprocess.Popen(["osascript", "-e", script, title, text[:200], sound],
                             stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except OSError:
            self.enabled = False


# ------------------------------------------------------------------ frame
@dataclass
class Limits:
    daily_loss_halt: float = 15.0
    max_total_worst_loss: float = 15.0
    max_event_worst_loss: float = 10.0


def render(state: State, health: dict | None, met: dict[str, float], hb: dict | None, now_ns: int,
           width: int, height: int, pt: Paint, lim: Limits, stale_s: float) -> list[str]:
    L: list[str] = []
    mode = (health or {}).get("mode") or (hb or {}).get("mode") or state.mode or "?"
    up = ago((now_ns - state.started_ns) / NS) if state.started_ns else "—"
    hb_age = (now_ns - int(hb["t"])) / NS if hb and hb.get("t") else None
    dot = pt.green("●") if hb_age is not None and hb_age <= stale_s and (hb or {}).get("state") == "running" \
        else pt.red("●")
    ws = met.get("dh_kalshi_ws_ok")
    brti = met.get("dh_brti_age_seconds")
    clk = met.get("dh_clock_offset_seconds")
    L.append(pt.head(f" DELTA-HEDGED {mode.upper()} ") + f" {dot} {state.session or '—'}  "
             + pt.dim(f"{datetime.fromtimestamp(now_ns / NS, ET):%a %H:%M:%S} ET · up {up}"))
    parts = [f"heartbeat {'—' if hb_age is None else f'{hb_age:.1f}s'}",
             "Kalshi " + ("—" if ws is None else pt.green("connected") if ws >= 1 else pt.red("DOWN")),
             "BRTI " + ("—" if brti is None else (pt.green if brti < 3 else pt.red)(f"{brti:.1f}s old")),
             "clock " + ("—" if clk is None else (pt.green if abs(clk) < 0.1 else pt.amber)(f"{clk * 1000:+.0f} ms")),
             f"orders working {met.get('dh_working_orders', 0):.0f}"]
    L.append(" " + pt.dim(" · ").join(parts))

    # money
    day = met.get("dh_day_pnl_dollars")
    real, mark = met.get("dh_day_realized_dollars"), met.get("dh_day_mark_dollars")
    loss_used = max(0.0, -(day or 0.0))
    frac = loss_used / lim.daily_loss_halt if lim.daily_loss_halt else 0
    tone = pt.green if frac < 0.5 else pt.amber if frac < 0.8 else pt.red
    L.append("")
    L.append(f" {pt.bold('TODAY')}  P&L {pt.bold(money(day))}  "
             + pt.dim(f"(realized {money(real)}, open {money(mark)})"))
    L.append(f"        loss used {tone(bar(frac, 16))} {money(loss_used, plus=False)} of ${lim.daily_loss_halt:.0f} halt")
    funds = next((v for k, v in met.items() if k.startswith("dh_shard_funds_dollars")), None)
    need = met.get("dh_balance_required_dollars")
    fees = met.get("dh_fees_paid_dollars")
    L.append(f" {pt.bold('ACCOUNT')} shard funds {money(funds, plus=False)}"
             + (f" (needs ≥ ${need:.0f})" if need else ""))
    L.append(f" {pt.bold('SESSION')} {state.fills} fills, {state.contracts:g} contracts, settled {money(state.settled_pnl)}"
             + (f", fees {money(fees, plus=False)}" if fees else ""))

    # banner
    probs = problems(state, health, hb, now_ns, stale_s)
    L.append("")
    if probs:
        L.append(pt.alarm(f" ■ {len(probs)} PROBLEM{'S' if len(probs) > 1 else ''} "))
        for p in probs[:6]:
            L.append("   " + pt.red(p))
    else:
        L.append(" " + pt.green("✓ trading normally: no blocks, no halts"))

    # positions
    L.append("")
    L.append(pt.head(" POSITIONS ") + pt.dim("  (YES contracts; mark = value at the model's probability)"))
    rows = [(tk, p) for tk, p in state.positions.items() if abs(p.qty) > 1e-9]
    if not rows:
        L.append(pt.dim("   none"))
    for tk, p in sorted(rows):
        F = state.fv.get(tk)
        avg = -p.cash / p.qty if p.qty else 0.0
        markv = None if F is None else p.qty * F + p.cash
        worst = min(p.qty * 1.0 + p.cash, p.cash)  # settles NO (0) or YES (1)
        side = pt.green(f"long {p.qty:g}") if p.qty > 0 else pt.amber(f"short {-p.qty:g}")
        side_txt = f"long {p.qty:g}" if p.qty > 0 else f"short {-p.qty:g}"
        L.append(f"   {market_name(tk):<14} {side}{' ' * max(1, 9 - len(side_txt))}avg {cents(avg):>4}  model "
                 + ("  —  " if F is None else f"{F * 100:5.1f}%") + f"  mark {money(markv):>7}  worst {money(worst):>7}")

    # last 15 min
    w = state.window_counts(now_ns)
    L.append("")
    L.append(pt.head(" LAST 15 MIN "))
    q = sum(w["quote"].values())
    c = w["cancel"]
    f = w["fill"]
    L.append(f"   quotes placed {q}   cancels {sum(c.values())}"
             + (pt.dim(" (" + ", ".join(f"{k} {n}" for k, n in c.most_common(4)) + ")") if c else "")
             + f"   fills {sum(f.values())}" + (pt.dim(" (" + ", ".join(f"{k} {n}" for k, n in f.items()) + ")") if f else ""))
    g = w["gate"] + w["risk"]
    if g:
        L.append("   " + pt.amber("held back: ") + ", ".join(f"{k} ×{n}" for k, n in g.most_common(4)))
    if state.disagree_on:
        L.append("   " + pt.amber(f"no new orders (Kalshi price disagrees >10¢): ")
                 + ", ".join(market_name(t) for t in sorted(state.disagree_on)[:5]))

    # events (scrollback)
    L.append("")
    L.append(pt.head(" EVENTS ") + pt.dim("  fills, settlements, blocks, halts"))
    room = max(3, height - len(L) - 1)
    tones = {"green": pt.green, "red": pt.red, "amber": pt.amber, "cyan": pt.cyan}
    for t, tone, text, n in list(state.events)[-room:]:
        rep = pt.dim(f" ×{n}") if n > 1 else ""
        L.append(f" {pt.dim(hhmmss(t))} {tones.get(tone, str)(text)}{rep}")
    return [clip(x, width) for x in L]


# ------------------------------------------------------------------ main loop
def load_limits(config: str) -> Limits:
    try:
        sys.path.insert(0, str(REPO))
        from dh.strategy.config import load_config

        r = load_config(config).risk
        return Limits(r.daily_loss_halt, r.max_total_worst_loss, r.max_event_worst_loss)
    except Exception:  # noqa: BLE001 - the view still works with defaults
        return Limits()


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--paper", action="store_true", help="watch the paper runner (data/paper_logs, port 9109)")
    ap.add_argument("--log", default="", help="session log (default: newest in the log dir)")
    ap.add_argument("--port", type=int, default=0)
    ap.add_argument("--config", default="config/m1_live.yaml", help="strategy config (loss limits shown)")
    ap.add_argument("--refresh", type=float, default=0.5)
    ap.add_argument("--stale-s", type=float, default=3.0)
    ap.add_argument("--once", action="store_true", help="print one frame and exit")
    ap.add_argument("--no-notify", action="store_true")
    ap.add_argument("--notify-fills", action="store_true")
    a = ap.parse_args(argv)
    log_dir = REPO / ("data/paper_logs" if a.paper else "data/live_logs")
    port = a.port or (9109 if a.paper else 9108)
    hb_path = REPO / "data/run" / ("heartbeat.paper.json" if a.paper else "heartbeat.json")
    lim = load_limits(str(REPO / a.config))
    tty = sys.stdout.isatty() and not a.once
    pt = Paint(on=(sys.stdout.isatty() and "NO_COLOR" not in os.environ))
    notifier = Notifier(enabled=not a.no_notify and not a.once)

    state, tail, cur = State(), None, None
    printed_events = 0
    last_frame: list[str] = []
    last_h = 0
    try:
        if tty:
            sys.stdout.write("\x1b[?25l")  # hide cursor
        while True:
            path = Path(a.log) if a.log else newest_log(log_dir)
            if path is not None and path != cur:  # a new session: start over on its log
                cur, state, tail, printed_events = path, State(), Tail(path), 0
            fills_before = state.fills
            if tail is not None:
                def keep(ln: bytes) -> bool:  # model values only for markets we hold
                    if b'"k":"log.fv"' not in ln:
                        return True
                    return any(tk.encode() in ln for tk in state.positions)

                for _ in range(10_000 if a.once else 16):  # catch up in bounded slices
                    recs = tail.read(keep)
                    for r in recs:
                        state.apply(r)
                    if tail.pos >= os.path.getsize(tail.path):
                        break
            now = time.time_ns()
            health = get_json(f"http://127.0.0.1:{port}/health")
            met = get_metrics(f"http://127.0.0.1:{port}/metrics")
            hb = read_heartbeat(hb_path)
            notifier.update(problems(state, health, hb, now, a.stale_s))
            if a.notify_fills and state.fills > fills_before and state.events:
                notifier.send("delta-hedged: fill", state.events[-1][2])
            size = shutil.get_terminal_size((120, 40))
            if tty or a.once:
                view_now = state.last_t if (a.once and state.ended and state.last_t) else now  # a finished log: its own end
                frame = render(state, health, met, hb, view_now, size.columns, size.lines, pt, lim, a.stale_s)
                if a.once:
                    print("\n".join(frame))
                    return 0
                if frame != last_frame:
                    out = (f"\x1b[{last_h}F" if last_h else "") + "\x1b[J" + "\n".join(frame) + "\n"
                    sys.stdout.write(out)
                    sys.stdout.flush()
                    last_frame, last_h = frame, len(frame)
            else:  # not a terminal: plain log lines for new events only
                evs = list(state.events)
                for t, _, text, n in evs[printed_events:]:
                    print(f"{hhmmss(t)} {text}" + (f" ×{n}" if n > 1 else ""), flush=True)
                printed_events = len(evs)
            time.sleep(a.refresh)
    except KeyboardInterrupt:
        return 0
    finally:
        if tty:
            sys.stdout.write("\x1b[?25h\n")


if __name__ == "__main__":
    sys.exit(main())
