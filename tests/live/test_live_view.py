"""scripts/live_view.py: the read-only live screen's bookkeeping (no runner, no network)."""
from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location("live_view", ROOT / "scripts" / "live_view.py")
lv = importlib.util.module_from_spec(_spec)
sys.modules["live_view"] = lv  # dataclasses resolve their module
_spec.loader.exec_module(lv)  # type: ignore[union-attr]

NS = 10**9
T0 = 1_790_820_000 * NS
TK = "KXBTCD-26SEP3023-T83299.99"


def fill(t, side, px, qty, coid="c1", F=0.5):
    return {"k": "log.fill", "t": t, "ticker": TK, "coid": coid, "side": side, "px": px, "qty": qty, "fee": 0, "F": F}


def test_partial_fills_merge_and_round_trip_pnl_counts():
    s = lv.State()
    s.apply({"k": "session_start", "t": T0, "session": "live-x", "mode": "live"})
    s.apply(fill(T0 + 1, "bid", 4800, 100))
    s.apply(fill(T0 + 2, "bid", 4800, 400))  # same order, same price, within 2 s: one line
    assert [e[2] for e in s.events][-1].startswith("FILL  bought YES 5 × 11pm ≥83,300 @ 48¢")
    assert s.positions[TK].qty == 5 and abs(s.positions[TK].cash + 2.40) < 1e-9
    s.apply(fill(T0 + 10 * NS, "ask", 3000, 500, coid="c2"))  # sold back at a loss: flat
    # settle carries the market's whole P&L even with no position left (round trip)
    s.apply({"k": "log.settle", "t": T0 + 20 * NS, "ticker": TK, "px": 0, "position": 0.0, "pnl": -0.9})
    assert abs(s.settled_pnl + 0.9) < 1e-9 and TK not in s.positions
    assert "closed before expiry · −$0.90" in s.events[-1][2]
    assert s.fills == 3 and s.contracts == 10


def test_gate_folding_window_and_disagreement():
    s = lv.State()
    for i in range(3):
        s.apply({"k": "gate", "t": T0 + i * NS, "action": "close", "reason": "lag", "why": "x"})
    assert [e[3] for e in s.events] == [3]  # folded into one line, counted
    assert s.events[0][2] == "BLOCKED · market data is lagging"
    s.apply({"k": "log.quote_gate", "t": T0 + 5 * NS, "ticker": TK, "on": True})
    assert TK in s.disagree_on
    w = s.window_counts(T0 + 10 * NS)
    assert w["gate"]["market data is lagging"] == 3 and w["gate"]["Kalshi price disagrees >10¢"] == 1
    assert s.window_counts(T0 + 1000 * NS) == {}  # older than 15 min: dropped


def test_problems_and_alerts_fire_once_per_problem():
    hb = {"t": T0, "state": "running"}
    health = {"gate": ["balance"], "halts": [], "fv_ready": True, "watchdog": "ok", "disk": "ok"}
    p = lv.problems(lv.State(), health, hb, T0 + NS, stale_s=3.0)
    assert p == ["new orders blocked: shard funds below the required amount"]
    assert lv.problems(lv.State(), {"gate": ["new_code"]}, hb, T0, 3.0) == ["new orders blocked: new_code"]  # raw
    assert "stale" in lv.problems(lv.State(), None, hb, T0 + 10 * NS, 3.0)[0]
    assert lv.problems(lv.State(), None, None, T0, 3.0) == ["runner not running (no heartbeat, no /health)"]
    n = lv.Notifier(enabled=False)
    sent = []
    n.enabled, n.send = True, lambda title, text, sound="": sent.append(text)
    n.update(["heartbeat stale (5s old): runner frozen or dead"])
    n.update(["heartbeat stale (6s old): runner frozen or dead"])  # same problem: no repeat
    n.update([])
    n.update(["heartbeat stale (9s old): runner frozen or dead"])  # cleared, then back: alert again
    assert len(sent) == 2


def test_tail_carries_a_torn_line(tmp_path):
    f = tmp_path / "s.jsonl"
    rec = json.dumps({"k": "log.settle", "t": T0, "ticker": TK, "px": 10000, "position": 1.0, "pnl": 0.5})
    f.write_text(rec[:20])
    t = lv.Tail(f)
    assert t.read() == []
    with open(f, "a") as fh:
        fh.write(rec[20:] + "\n")
    assert t.read()[0]["pnl"] == 0.5


def test_render_fits_the_terminal_and_names_markets():
    s = lv.State()
    s.apply({"k": "session_start", "t": T0, "session": "live-x", "mode": "live"})
    s.apply(fill(T0 + NS, "ask", 1500, 500, F=0.04))
    lines = lv.render(s, {"mode": "live", "gate": [], "halts": [], "fv_ready": True}, {"dh_day_pnl_dollars": -3.0},
                      {"t": T0 + NS, "state": "running"}, T0 + 2 * NS, 80, 40, lv.Paint(False), lv.Limits(), 3.0)
    assert all(lv.visible_len(x) <= 80 for x in lines)
    text = "\n".join(lines)
    assert "short 5" in text and "11pm ≥83,300" in text and "trading normally" in text and "$3.00 of $15 halt" in text
