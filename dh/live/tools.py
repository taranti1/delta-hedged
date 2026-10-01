"""Operator tools for the live/paper runner (docs/RUNBOOK.md). Read-only: none places orders.

    python -m dh.live.tools backfill  [--live-config config/live.yaml]
        test the CF Benchmarks passthrough call used for the fair-value warm-up
    python -m dh.live.tools warmfile  --kalshi-config config/kalshi.history.yaml [--out data/live/brti_warm.json]
        save the fair-value warm-up history for a runner whose restricted key cannot read the
        CF passthrough (backfill.warm_file); the read-only key never enters the live runner
    python -m dh.live.tools orders    [--live-config ...]
        list resting orders of venue.subaccount (after a kill: must be empty)
    python -m dh.live.tools balance   [--live-config ...] [--config config/m1_live.yaml]
        balance of venue.subaccount on every venue.exchange_indexes shard vs the start-up requirement
        of that strategy config
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


def make_rest(lcfg: Any, kalshi_config: str = "") -> Any:
    from dh.kalshi.config import load_config
    from dh.kalshi.rest import KalshiRest

    path = kalshi_config or lcfg.kalshi_config
    kc = load_config(_resolve(path) if path else None, env=lcfg.kalshi_env or None)
    return KalshiRest(kc.rest_url, kc.signer(), kc.limiter(), read_only=True, **kc.rest_kwargs())  # tools never write


# ============================================================================ commands
async def cmd_warmfile(lcfg: Any, rest: Any, out_path: str, out: Any = print) -> int:
    """Download the benchmark history the live warm-up needs and save it for
    ``backfill.warm_file`` (read-only; for a runner whose restricted key gets 403 on the CF
    passthrough). Run it with a key that may read /cfbenchmarks (--kalshi-config)."""
    from dh.live.startup import fetch_benchmark_history, resample, write_warm_file

    cfg = lcfg.backfill
    path = out_path or cfg.warm_file
    if not path:
        out("no output: pass --out or set backfill.warm_file in the live config")
        return 2
    now = time.time_ns()
    start = now - int(cfg.days * 86400 * NS_PER_S)
    ticks, n_req, errors = await fetch_benchmark_history(rest, now, cfg)
    points = resample(ticks, start, now, cfg.step_s)
    requested = max(1, (now - start) // (int(cfg.step_s) * NS_PER_S))
    coverage = len(points) / requested
    for e in errors:
        out(f"  {e}")
    if not points or coverage < cfg.min_coverage:
        out(f"NOT written: coverage {coverage:.1%} < {cfg.min_coverage:.0%} ({len(points)} points, {n_req} requests)")
        return 1
    write_warm_file(_resolve(path), points, now, cfg)
    age = (now - points[-1][0]) / NS_PER_S
    out(f"wrote {path}: {len(points)} points, coverage {coverage:.1%}, newest {age:.0f} s old; "
        f"the runner accepts it for {cfg.warm_file_max_age_s:.0f} s after that point")
    return 0


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


def _no_subaccount(lcfg: Any, out: Any) -> bool:
    """A config that does not NAME its subaccount is refused: a read of subaccount 0 by default
    would show the other system's account (e.g. '0 resting orders' after a kill of subaccount 1)."""
    if getattr(lcfg.venue, "subaccount", None) is None:
        out("refused: venue.subaccount is not set in the live config (set it explicitly, e.g. 1)")
        return True
    return False


async def cmd_orders(lcfg: Any, rest: Any, out: Any = print) -> int:
    if _no_subaccount(lcfg, out):
        return 2
    kw: dict[str, Any] = {"status": "resting"}
    kw["subaccount"] = lcfg.venue.sub  # explicit: omitted means ALL subaccounts
    rows = [o async for o in rest.iter_orders(**kw)]
    for o in rows:
        out(f"{o.get('ticker')} {o.get('book_side')} {o.get('yes_price_dollars')} rem={o.get('remaining_count_fp')} "
            f"shard={o.get('exchange_index')} coid={o.get('client_order_id')} oid={o.get('order_id')}")
    out(f"{len(rows)} resting orders (subaccount {lcfg.venue.sub})")
    return 0 if not rows else 1


async def cmd_balance(lcfg: Any, rest: Any, out: Any = print, scfg: Any = None) -> int:
    """GET /portfolio/balance?subaccount=<n>&exchange_index=<shard> for every configured shard vs
    the live start-up requirement (worst-case total loss of config/m1.yaml + margin)."""
    from dh.live.startup import required_balance_usd
    from dh.live.venue_kalshi import balance_dollars

    if scfg is None:
        from dh.strategy.config import load_config

        scfg = load_config(REPO_ROOT / "config" / "m1.yaml")
    from dh.live.venue_kalshi import KalshiVenue

    if _no_subaccount(lcfg, out):
        return 2
    need = required_balance_usd(scfg.risk, lcfg.venue.min_balance_margin_dollars)
    try:  # the runner's own computation: available + positions at cost + resting collateral
        venue = KalshiVenue(rest, sink=lambda ev: None, cfg=lcfg.venue)
    except ValueError as exc:
        out(f"refused: {exc}")
        return 2
    bad = 0
    for sh, f in sorted((await venue.fetch_shard_funds()).items()):
        usd = f["funds"]
        ok = usd is not None and usd >= need
        bad += not ok
        avail = balance_dollars(f["body"])
        out(f"subaccount {lcfg.venue.sub} shard {sh}: available {'?' if avail is None else f'${avail:.4f}'}, "
            f"positions ${f['positions']:.4f}, resting ${f['resting']:.4f} -> funds "
            f"{'?' if usd is None else f'${usd:.4f}'} (required ${need:.2f}) {'OK' if ok else '<-- TOO LOW'}")
    return 0 if bad == 0 else 1


def cmd_ledger(log: str, data: str, out: Any = print, logs_dir: str = "", outcomes_root: str = "") -> int:
    from dh.live.replay import ledger_from_logs, load_session, session_specs

    paths = [_resolve(log)] if not logs_dir else sorted(
        p for p in _resolve(logs_dir).iterdir()
        if p.name.startswith("paper-") and p.name.endswith((".jsonl", ".jsonl.gz", ".jsonl.zst")))
    if not paths:
        raise ValueError("no paper session logs found")
    identities = [_session_of(str(p)) for p in paths]
    if len(identities) != len(set(identities)):
        raise ValueError("multiple compressed/uncompressed copies of the same session")
    specs = {}
    for path in paths:
        info = load_session(_resolve(data), _session_of(str(path)))
        if logs_dir and info.mode != "paper":
            raise ValueError("lifetime --logs-dir audit accepts independent paper sessions only")
        specs.update({s.ticker: s for s in session_specs(info)})
    led = ledger_from_logs(paths, list(specs.values()))
    if outcomes_root:
        from dh.live.replay import add_downloaded_outcomes
        add_downloaded_outcomes(led, _resolve(outcomes_root))
    s = led.summary()
    s["sessions"] = len(paths)
    s["duration_basis"] = "sum of complete observed session spans"
    if logs_dir:
        s["accounting_basis"] = "independent paper portfolios; all fills joined to known later outcomes"
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
    from dh.live.replay import iter_log_records, log_fill_is_new

    ours: dict[str, list[int]] = defaultdict(lambda: [0, 0, 0])
    t0 = None
    seen: set[str] = set()
    for r in iter_log_records(_resolve(log)):
        if t0 is None and r.get("k") == "session_start":
            t0 = int(r["t"])
        if r.get("k") == "log.fill" and log_fill_is_new(r, seen):
            o = ours[r["ticker"]]
            o[0] += 1
            o[1] += int(r["qty"])
            o[2] += int(r.get("fee", 0))
    if t0 is None:
        out("no session_start in the log")
        return 2
    if _no_subaccount(lcfg, out):
        return 2
    from dh.core.units import micros_from_dollars, qty_from_fp

    kw: dict[str, Any] = {"min_ts": t0 // NS_PER_S - 60}
    kw["subaccount"] = lcfg.venue.sub  # explicit: omitted means ALL subaccounts
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
    for suffix in (".gz", ".zst"):
        if stem.endswith(suffix):
            stem = stem[:-len(suffix)]
    return stem[: -len(".jsonl")] if stem.endswith(".jsonl") else None


# ============================================================================ main
def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m dh.live.tools", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("command", choices=("backfill", "warmfile", "orders", "balance", "ledger", "replay", "reconcile"))
    ap.add_argument("--live-config", default="config/live.yaml")
    ap.add_argument("--config", default="config/m1.yaml", help="strategy config (replay; balance requirement)")
    ap.add_argument("--log", default="", help="session JSON log (data/live_logs/<session>.jsonl)")
    ap.add_argument("--outcomes-root", default="", help="ledger: join known settlements from downloaded Kalshi history")
    ap.add_argument("--logs-dir", default="", help="ledger: audit every paper session in this directory")
    ap.add_argument("--data", default="", help="session store (default: live config paths.data_root)")
    ap.add_argument("--streams", default="", help="replay: extra recorded event streams (comma list)")
    ap.add_argument("--kalshi-config", default="", help="warmfile: Kalshi config whose key may read /cfbenchmarks")
    ap.add_argument("--out", default="", help="warmfile: output (default backfill.warm_file)")
    a = ap.parse_args(argv)
    lcfg = _live_cfg(a.live_config)
    data = a.data or lcfg.paths.data_root
    if a.command in ("ledger", "replay", "reconcile") and not a.log and not (a.command == "ledger" and a.logs_dir):
        ap.error("--log is required")
    if a.command == "ledger":
        return cmd_ledger(a.log, data, logs_dir=a.logs_dir, outcomes_root=a.outcomes_root)
    if a.command == "replay":
        return cmd_replay(a.log, data, a.config, extra_streams=tuple(x for x in a.streams.split(",") if x))

    async def run() -> int:
        rest = make_rest(lcfg, a.kalshi_config if a.command == "warmfile" else "")
        try:
            if a.command == "warmfile":
                return await cmd_warmfile(lcfg, rest, a.out)
            if a.command == "backfill":
                return await cmd_backfill(lcfg, rest)
            if a.command == "orders":
                return await cmd_orders(lcfg, rest)
            if a.command == "balance":
                from dh.strategy.config import load_config

                return await cmd_balance(lcfg, rest, scfg=load_config(a.config))
            return await cmd_reconcile(a.log, lcfg, rest)
        finally:
            await rest.close()

    return asyncio.run(run())


if __name__ == "__main__":
    sys.exit(main())
