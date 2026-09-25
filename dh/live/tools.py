"""Operator tools for the live/paper runner (docs/RUNBOOK.md). Read-only: none places orders.

    python -m dh.live.tools backfill  [--live-config config/live.yaml]
        test the CF Benchmarks passthrough call used for the fair-value warm-up
    python -m dh.live.tools orders    [--live-config ...]
        list resting orders on the account (after a kill: must be empty)
    python -m dh.live.tools ledger    --log data/live_logs/<session>.jsonl [--data data/live]
        P&L attribution of a session from its JSON log (net c/contract CI, markouts)
    python -m dh.live.tools replay    --log <session log> [--data data/live] [--config config/m1.yaml]
        determinism check: replay the recorded session, compare every decision
    python -m dh.live.tools reconcile --log <session log> [--live-config ...]
        daily reconciliation: exchange fills (GET /portfolio/fills) vs the session's log.fill
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Any

from dh.core.units import NS_PER_S

REPO_ROOT = Path(__file__).resolve().parents[2]


def _resolve(p: str) -> Path:
    q = Path(p).expanduser()
    return q if q.is_absolute() else REPO_ROOT / q


def _live_cfg(path: str) -> Any:
    from dh.live.config import load_live_config

    p = _resolve(path)
    if not p.is_file():
        p = REPO_ROOT / "config" / "live.example.yaml"
    return load_live_config(p)


def make_rest(lcfg: Any) -> Any:
    from dh.kalshi.config import load_config
    from dh.kalshi.rest import KalshiRest

    kc = load_config(_resolve(lcfg.kalshi_config) if lcfg.kalshi_config else None, env=lcfg.kalshi_env or None)
    return KalshiRest(kc.rest_url, kc.signer(), kc.limiter(), **kc.rest_kwargs())


# ============================================================================ commands
async def cmd_backfill(lcfg: Any, rest: Any, out: Any = print) -> int:
    from dh.live.startup import fetch_benchmark_history, resample

    now = time.time_ns()
    cfg = lcfg.backfill
    ticks, n, errors = await fetch_benchmark_history(rest, now, cfg)
    pts = resample(ticks, now - int(cfg.days * 86400 * NS_PER_S), now, cfg.step_s)
    requested = max(1, int(cfg.days * 86400) // int(cfg.step_s))
    cov = len(pts) / requested
    out(json.dumps({"requests": n, "ticks": len(ticks), "points": len(pts), "requested": requested,
                    "coverage": round(cov, 4), "first": ticks[0].ts_exch if ticks else None,
                    "last": ticks[-1].ts_exch if ticks else None, "errors": errors[:5],
                    "ok": cov >= cfg.min_coverage}, indent=1))
    return 0 if cov >= cfg.min_coverage else 1


async def cmd_orders(lcfg: Any, rest: Any, out: Any = print) -> int:
    kw: dict[str, Any] = {"status": "resting"}
    if lcfg.venue.subaccount is not None:
        kw["subaccount"] = lcfg.venue.subaccount
    rows = [o async for o in rest.iter_orders(**kw)]
    for o in rows:
        out(f"{o.get('ticker')} {o.get('book_side')} {o.get('yes_price_dollars')} rem={o.get('remaining_count_fp')} "
            f"coid={o.get('client_order_id')} oid={o.get('order_id')}")
    out(f"{len(rows)} resting orders")
    return 0 if not rows else 1


def cmd_ledger(log: str, data: str, out: Any = print) -> int:
    from dh.live.replay import ledger_from_log, load_session, session_specs

    info = load_session(_resolve(data), _session_of(log))
    led = ledger_from_log(_resolve(log), session_specs(info))
    # Ledger.summary() needs at least one fill (dh.backtest.ledger raises on an empty frame)
    s = led.summary() if led.fills else {"fills": 0, "note": "no fills in this session"}
    out(json.dumps(s, indent=1, default=str))
    return 0


def cmd_replay(log: str, data: str, config: str, out: Any = print, extra_streams: tuple[str, ...] = ()) -> int:
    from dh.live.replay import load_session, logged_decisions, replay_session, replayed_decisions
    from dh.strategy.config import load_config

    info = load_session(_resolve(data), _session_of(log))
    res = replay_session(_resolve(data), load_config(_resolve(config)), session=info.session, extra_streams=extra_streams)
    la, ll = logged_decisions(_resolve(log), info.last_ts)
    ra, rl = replayed_decisions(res, info.last_ts)
    first = next((i for i, (x, y) in enumerate(zip(la, ra, strict=False)) if x != y), None)
    if first is None and len(la) != len(ra):
        first = min(len(la), len(ra))
    ok = la == ra and ll == rl
    out(json.dumps({"session": info.session, "actions_live": len(la), "actions_replay": len(ra), "logs_live": len(ll),
                    "logs_replay": len(rl), "identical": ok, "first_action_diff": first,
                    "live_at_diff": la[first] if first is not None and first < len(la) else None,
                    "replay_at_diff": ra[first] if first is not None and first < len(ra) else None}, indent=1, default=str))
    return 0 if ok else 1


async def cmd_reconcile(log: str, lcfg: Any, rest: Any, out: Any = print) -> int:
    """Exchange fills vs the session log (count, contracts, fees per ticker)."""
    ours: dict[str, list[int]] = defaultdict(lambda: [0, 0, 0])
    t0 = None
    for line in _resolve(log).read_text().splitlines():
        r = json.loads(line)
        if t0 is None and r.get("k") == "session_start":
            t0 = int(r["t"])
        if r.get("k") == "log.fill":
            o = ours[r["ticker"]]
            o[0] += 1
            o[1] += int(r["qty"])
            o[2] += int(r.get("fee", 0))
    if t0 is None:
        out("no session_start in the log")
        return 2
    from dh.core.units import micros_from_dollars, qty_from_fp

    kw: dict[str, Any] = {"min_ts": t0 // NS_PER_S - 60}
    if lcfg.venue.subaccount is not None:
        kw["subaccount"] = lcfg.venue.subaccount
    theirs: dict[str, list[int]] = defaultdict(lambda: [0, 0, 0])
    async for f in rest.iter_fills(**kw):
        t = str(f.get("ticker") or f.get("market_ticker"))
        e = theirs[t]
        e[0] += 1
        e[1] += qty_from_fp(str(f["count_fp"]))
        e[2] += micros_from_dollars(str(f.get("fee_cost") or "0"))
    bad = 0
    for t in sorted(set(ours) | set(theirs)):
        a, b = ours.get(t, [0, 0, 0]), theirs.get(t, [0, 0, 0])
        flag = "" if a[1] == b[1] and a[2] == b[2] else "  <-- MISMATCH"
        bad += bool(flag)
        out(f"{t}: log fills={a[0]} qty={a[1] / 100:g} fees=${a[2] / 1e6:.6f} | exchange fills={b[0]} "
            f"qty={b[1] / 100:g} fees=${b[2] / 1e6:.6f}{flag}")
    out(f"{bad} tickers mismatched")
    return 0 if bad == 0 else 1


def _session_of(log: str) -> str | None:
    stem = Path(log).name
    return stem[: -len(".jsonl")] if stem.endswith(".jsonl") else None


# ============================================================================ main
def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m dh.live.tools", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("command", choices=("backfill", "orders", "ledger", "replay", "reconcile"))
    ap.add_argument("--live-config", default="config/live.yaml")
    ap.add_argument("--config", default="config/m1.yaml", help="strategy config (replay)")
    ap.add_argument("--log", default="", help="session JSON log (data/live_logs/<session>.jsonl)")
    ap.add_argument("--data", default="", help="session store (default: live config paths.data_root)")
    ap.add_argument("--streams", default="", help="replay: extra recorded event streams (comma list)")
    a = ap.parse_args(argv)
    lcfg = _live_cfg(a.live_config)
    data = a.data or lcfg.paths.data_root
    if a.command in ("ledger", "replay", "reconcile") and not a.log:
        ap.error("--log is required")
    if a.command == "ledger":
        return cmd_ledger(a.log, data)
    if a.command == "replay":
        return cmd_replay(a.log, data, a.config, extra_streams=tuple(x for x in a.streams.split(",") if x))

    async def run() -> int:
        rest = make_rest(lcfg)
        try:
            if a.command == "backfill":
                return await cmd_backfill(lcfg, rest)
            if a.command == "orders":
                return await cmd_orders(lcfg, rest)
            return await cmd_reconcile(a.log, lcfg, rest)
        finally:
            await rest.close()

    return asyncio.run(run())


if __name__ == "__main__":
    sys.exit(main())
