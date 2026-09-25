#!/usr/bin/env python
"""Dead-man watchdog: cancel ALL resting Kalshi orders when the live runner's heartbeat stops.

Run it as its own process/service next to scripts/run_live.py (never inside it):

    python scripts/watchdog.py --live-config config/live.yaml

It reads the heartbeat file (live config paths.heartbeat_file; paper runners write their own
file). Once it has seen a fresh heartbeat of a LIVE runner it locks onto that runner (pid +
session): heartbeats written by any other process are ignored. The watched runner's heartbeat
older than watchdog.stale_s (default 2 s), a vanished file, or a shutdown hung longer than the
runner's shutdown_timeout_s + watchdog.stopping_grace_s triggers, with the watchdog's own
signed session: first ``PUT /portfolio/order_groups/{id}/trigger?subaccount=<n>&exchange_index=
<shard>`` for every order group the runner's heartbeat names (scoped, no trailing tail), then
``DELETE /portfolio/events/orders?subaccount=<n>`` (explicit: omitted = ALL subaccounts),
retried until it succeeds and repeated every watchdog.repeat_s while it stays stale; each
attempt is announced in <heartbeat>.cancel_all
(a live runner that finds a marker about itself, written after it started, halts; one about
another runner holds new orders for a minute). A clean runner shutdown (state 'stopped',
written only after a confirmed cancel-all) disarms it; a new live runner re-arms it. It never
places orders, and every write names venue.subaccount (the REST client refuses anything else);
with ``venue.shared_account: true`` it refuses to run for subaccount null/0. Credentials:
KALSHI_WATCHDOG_KEY_ID / KALSHI_WATCHDOG_PRIVATE_KEY_PATH (a separate key, restricted to the
runner's subaccount, is recommended), else the runner's KALSHI_KEY_ID / KALSHI_PRIVATE_KEY_PATH.

    --once              one check (+ cancel if stale) and exit (cron / manual use)
    --cancel-now        trigger the order groups named in the heartbeat file (whatever its state)
                        and cancel all immediately, then exit (manual kill procedure)
    --arm-on-start      on the first poll, act on an EXISTING LIVE heartbeat (live mode, state
                        running/stopping): lock onto it if fresh, cancel all at once if stale.
                        A missing file or any other heartbeat leaves it waiting (DISARMED)
                        for a fresh live heartbeat.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import signal
import sys
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from dh.live.config import LiveConfig, load_live_config  # noqa: E402
from dh.live.monitor import read_heartbeat  # noqa: E402
from dh.live.watchdog import Watchdog, rest_cancel_all, rest_trigger_groups  # noqa: E402

log = logging.getLogger("watchdog")


def _resolve(p: str) -> Path:
    q = Path(p).expanduser()
    return q if q.is_absolute() else REPO / q


def build_rest(lcfg: LiveConfig) -> Any:
    """Signed KalshiRest for the watchdog (its own key if configured)."""
    from dh.kalshi.auth import KalshiSigner
    from dh.kalshi.config import load_config
    from dh.kalshi.rest import KalshiRest

    # load_config also loads the kalshi config's auth.env_file (if any) into the environment
    # (never overriding set variables, never ALLOW_*), so the watchdog key lookup below sees it
    kc = load_config(_resolve(lcfg.kalshi_config) if lcfg.kalshi_config else None, env=lcfg.kalshi_env or None)
    wd = lcfg.watchdog
    key_id = os.environ.get(wd.key_id_env, "")
    key_path = os.environ.get(wd.private_key_path_env, "")
    signer = None
    if key_id and key_path and Path(key_path).expanduser().is_file():
        signer = KalshiSigner(key_id, Path(key_path).expanduser())
    else:
        signer = kc.signer()
    if signer is None:
        raise SystemExit(f"watchdog: no Kalshi credentials (set {wd.key_id_env}/{wd.private_key_path_env} or "
                         f"{kc.credentials_hint()})")
    kw = kc.rest_kwargs()
    kw.pop("cf_history_path", None)
    # every write must name the runner's subaccount (and a shard): refused before signing otherwise
    return KalshiRest(kc.rest_url, signer, kc.limiter(), write_subaccount=lcfg.venue.sub, **kw)


async def amain(args: argparse.Namespace, rest: Any = None) -> int:
    lcfg = load_live_config(_resolve(args.live_config) if args.live_config else None)
    if lcfg.venue.shared_account and lcfg.venue.sub == 0:
        log.error("venue.shared_account is true: refusing to act for subaccount null/0 (the other system's); "
                  "set venue.subaccount to the runner's dedicated subaccount")
        return 2
    hb = _resolve(args.heartbeat or lcfg.paths.heartbeat_file)
    own = rest is None
    rest = rest or build_rest(lcfg)
    cancel = rest_cancel_all(rest, lcfg.venue.sub)  # explicit subaccount (0 = primary)
    trigger = rest_trigger_groups(rest, lcfg.venue.sub)
    try:
        if args.cancel_now:
            beat = read_heartbeat(hb) or {}
            groups = [g for g in beat.get("order_groups") or [] if isinstance(g, dict)]
            if groups:
                n = await trigger(groups)
                log.warning("manual kill: %d of %d order group(s) triggered", n, len(groups))
            ok = await cancel()
            log.warning("manual cancel-all (subaccount %d): %s", lcfg.venue.sub, "OK" if ok else "FAILED")
            return 0 if ok else 1
        wcfg = lcfg.watchdog
        if getattr(args, "max_age_s", 0.0):
            from dataclasses import replace

            wcfg = replace(wcfg, stale_s=float(args.max_age_s))
        wd = Watchdog(hb, cancel, wcfg, arm_on_start=args.arm_on_start, trigger_groups=trigger)
        if args.once:
            st = await wd.step()
            log.info("state %s", st)
            return 0
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, stop.set)
            except (NotImplementedError, RuntimeError):  # pragma: no cover
                pass
        log.info("watching %s (stale after %.1fs; subaccount %d)", hb, lcfg.watchdog.stale_s, lcfg.venue.sub)
        await wd.run(stop)
        return 0
    finally:
        if own:
            await rest.close()


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--live-config", default="config/live.yaml")
    p.add_argument("--heartbeat", default="", help="heartbeat file (default: live config paths.heartbeat_file)")
    p.add_argument("--once", action="store_true")
    p.add_argument("--cancel-now", action="store_true")
    p.add_argument("--arm-on-start", action="store_true",
                   help="first poll: lock onto an existing live heartbeat (cancel all at once if it is stale); "
                        "a missing / non-live heartbeat leaves it waiting")
    p.add_argument("--max-age-s", type=float, default=0.0, help="override watchdog.stale_s (seconds)")
    p.add_argument("--log-level", default="INFO")
    args = p.parse_args(argv)
    logging.basicConfig(level=args.log_level.upper(), format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    if args.live_config and not _resolve(args.live_config).is_file():
        args.live_config = "config/live.example.yaml"
        log.warning("config/live.yaml not found: using config/live.example.yaml paths")
    return asyncio.run(amain(args))


if __name__ == "__main__":
    sys.exit(main())
