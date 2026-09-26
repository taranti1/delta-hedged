#!/usr/bin/env python
"""Dead-man watchdog: cancel ALL resting Kalshi orders of the runner's subaccount when the live
runner's heartbeat stops.

Run it as its own process/service next to scripts/run_live.py (never inside it):

    python scripts/watchdog.py --live-config config/live.yaml

It reads the heartbeat file (live config paths.heartbeat_file; paper runners write their own
file). Once it has seen a fresh heartbeat of a LIVE runner of ITS subaccount (the heartbeat's
"subaccount" must equal venue.subaccount) it locks onto that runner (pid + session):
heartbeats written by any other process are ignored. The watched runner's heartbeat older than
watchdog.stale_s (default 2 s), a vanished file, or a shutdown hung longer than the runner's
shutdown_timeout_s + watchdog.stopping_grace_s triggers, with the watchdog's own signed
session: first ``PUT /portfolio/order_groups/{id}/trigger?subaccount=<n>&exchange_index=
<shard>`` for every order group the runner's heartbeat names (scoped, no trailing tail), then
every resting order of the subaccount is cancelled. On a SHARED account (venue.shared_account:
true, the deployment) that is GET /portfolio/orders?status=resting&subaccount=<n> + a batch
cancel BY ID, repeated until the list is empty: the bulk ``DELETE /portfolio/events/orders``
is never sent (Kalshi: it may also cancel orders placed in the following minute, and whether
that tail honours ``subaccount`` is unverified); only an account declared NOT shared uses the
bulk endpoint. It retries until it succeeds and repeats every watchdog.repeat_s while the
runner stays stale; each attempt is announced in <heartbeat>.cancel_all (a live runner that
finds a marker about itself, written after it started, halts). A clean runner shutdown (state
'stopped', written only after a confirmed cancel) disarms it; a new live runner re-arms it.

While it runs it writes its OWN liveness file <heartbeat>.watchdog every watchdog.beat_interval_s
(pid, subaccount, state, the runner it is armed on, last poll; EXITED when it stops): the live
runner refuses to start without a fresh one for its subaccount and blocks new orders while it
is stale (watchdog.runner_max_age_s) or stamped in the future (watchdog.max_future_s), names
another subaccount, or is not armed on it. The beat also proves CAPABILITY: at start and every
watchdog.api_probe_interval_s the watchdog reads GET /portfolio/orders?subaccount=<n>&
status=resting&limit=1 with its own key; ``api_ok`` false, or a last success older than
watchdog.api_max_age_s, counts as "not protecting" for the runner (start refused, gate closed).
It also proves it may CANCEL (review F2): at start and every watchdog.api_write_probe_interval_s
(every api_probe_interval_s while failing) DELETE /portfolio/events/orders/<fresh random uuid4>?
subaccount=<n>&exchange_index=<first of venue.exchange_indexes>: 404 = allowed; 401/403 = api_ok
false (the runner's gate closes). The id is never a real order id: nothing can be cancelled.
A runner heartbeat stamped in the future is treated like a stale one (the watchdog fires); after a
backward wall-clock step a fresh heartbeat of the watched runner resets its stored time instead.

Refusals (exit 2; launchd retries): no live config file (no fallback to the example), a
venue section that does not state venue.subaccount and venue.shared_account explicitly,
subaccount 0 unless venue.allow_primary_account with shared_account false, a shared account
without key_restricted_to_subaccount, and on a shared account no KALSHI_WATCHDOG_KEY_ID /
KALSHI_WATCHDOG_PRIVATE_KEY_PATH (its own key, restricted to the subaccount) unless
watchdog.allow_runner_key: true. It never places orders, and every write names venue.subaccount
and one of venue.exchange_indexes (the REST client refuses anything else, the bulk cancel-all
too on a shared account).

    --once              one check (+ cancel if stale) and exit (cron / manual use; no beat)
    --cancel-now        trigger the order groups named in the heartbeat file (whatever its state,
                        only groups of venue.subaccount) and cancel every resting order of the
                        subaccount at once, then exit (manual kill procedure; no beat)
    --arm-on-start      on the first poll, act on an EXISTING LIVE heartbeat of its subaccount
                        (live mode, state running/stopping): lock onto it if fresh, cancel all at
                        once if stale. A missing file or any other heartbeat leaves it waiting
                        (DISARMED) for a fresh live heartbeat.
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

from dh.live.config import LiveConfig, load_live_config, venue_scope_problems  # noqa: E402
from dh.live.monitor import read_heartbeat, watchdog_beat_path  # noqa: E402
from dh.live.watchdog import (  # noqa: E402
    Watchdog,
    rest_api_probe,
    rest_cancel_all,
    rest_scoped_cancel_all,
    rest_trigger_groups,
    rest_write_probe,
)

log = logging.getLogger("watchdog")


class ConfigRefused(Exception):
    """The watchdog refuses to run with this configuration (exit 2)."""


def _resolve(p: str) -> Path:
    q = Path(p).expanduser()
    return q if q.is_absolute() else REPO / q


def build_rest(lcfg: LiveConfig) -> Any:
    """Signed KalshiRest for the watchdog: its OWN key (watchdog.key_id_env /
    private_key_path_env). On a shared account it never falls back to the runner's key
    silently: without its variables it refuses unless watchdog.allow_runner_key is true."""
    from dh.kalshi.auth import KalshiSigner
    from dh.kalshi.config import load_config
    from dh.kalshi.rest import KalshiRest

    # load_config also loads the kalshi config's auth.env_file (if any) into the environment
    # (never overriding set variables, never ALLOW_*), so the watchdog key lookup below sees it
    kc = load_config(_resolve(lcfg.kalshi_config) if lcfg.kalshi_config else None, env=lcfg.kalshi_env or None)
    wd = lcfg.watchdog
    key_id = os.environ.get(wd.key_id_env, "")
    key_path = os.environ.get(wd.private_key_path_env, "")
    if key_id and key_path and Path(key_path).expanduser().is_file():
        signer = KalshiSigner(key_id, Path(key_path).expanduser())
    elif lcfg.venue.shared_account is not False and not wd.allow_runner_key:
        raise ConfigRefused(f"no watchdog key ({wd.key_id_env} / {wd.private_key_path_env} unset, or the key file is "
                            "missing) on a shared account: the watchdog signs with its OWN key restricted to "
                            f"subaccount {lcfg.venue.sub} (RUNBOOK 1.2 step 4); set watchdog.allow_runner_key: true "
                            "only to share the runner's key deliberately")
    else:
        signer = kc.signer()
        if signer is not None:
            log.warning("watchdog: signing with the RUNNER's key (%s): no watchdog key variables", kc.credentials_hint())
    if signer is None:
        raise ConfigRefused(f"no Kalshi credentials (set {wd.key_id_env}/{wd.private_key_path_env} or "
                            f"{kc.credentials_hint()})")
    kw = kc.rest_kwargs()
    kw.pop("cf_history_path", None)
    # every write must name the runner's subaccount and one of its shards (refused before signing
    # otherwise); a shared account can never send the bulk cancel-all
    return KalshiRest(kc.rest_url, signer, kc.limiter(), write_subaccount=lcfg.venue.sub,
                      write_shards=tuple(lcfg.venue.exchange_indexes),
                      forbid_bulk_cancel=not lcfg.venue.bulk_cancel_allowed, **kw)


async def amain(args: argparse.Namespace, rest: Any = None) -> int:
    if not args.live_config:
        log.error("--live-config is required (the runner's config/live.yaml): the watchdog never guesses its "
                  "subaccount")
        return 2
    path = _resolve(args.live_config)
    if not path.is_file():
        log.error("live config %s not found: refusing (no fallback to the example config)", path)
        return 2
    lcfg = load_live_config(path)
    problems = venue_scope_problems(lcfg.venue)
    if problems:
        log.error("refusing to run: %s", "; ".join(problems))
        return 2
    sub = lcfg.venue.sub
    bulk = lcfg.venue.bulk_cancel_allowed
    shards = [x for x in lcfg.venue.exchange_indexes if isinstance(x, int) and not isinstance(x, bool) and x >= 0]
    if not shards:
        log.error("refusing to run: venue.exchange_indexes names no exchange shard (the write-capability probe "
                  "cancels a random order id on one of them)")
        return 2
    probe_shard = shards[0]
    hb = _resolve(args.heartbeat or lcfg.paths.heartbeat_file)
    own = rest is None
    try:
        rest = rest or build_rest(lcfg)
    except ConfigRefused as exc:
        log.error("refusing to run: %s", exc)
        return 2
    # the subaccount is explicit on every request; a shared account cancels by id, never in bulk
    cancel = rest_cancel_all(rest, sub) if bulk else rest_scoped_cancel_all(rest, sub)
    trigger = rest_trigger_groups(rest, sub)
    try:
        if args.cancel_now:
            beat = read_heartbeat(hb) or {}
            hsub = beat.get("subaccount")
            groups = [g for g in beat.get("order_groups") or [] if isinstance(g, dict)]
            if groups and hsub is not None and hsub != sub:
                log.error("the heartbeat names subaccount %s, this watchdog acts for %d: its order groups are NOT "
                          "triggered (wrong --live-config / --heartbeat?)", hsub, sub)
                groups = []
            if groups:
                n = await trigger(groups)
                log.warning("manual kill: %d of %d order group(s) triggered", n, len(groups))
            ok = await cancel()
            log.warning("manual cancel-all (subaccount %d, %s): %s", sub, "bulk" if bulk else "by id",
                        "OK" if ok else "FAILED")
            return 0 if ok else 1
        wcfg = lcfg.watchdog
        if getattr(args, "max_age_s", 0.0):
            from dataclasses import replace

            wcfg = replace(wcfg, stale_s=float(args.max_age_s))
        wd = Watchdog(hb, cancel, wcfg, arm_on_start=args.arm_on_start, trigger_groups=trigger, subaccount=sub,
                      bulk=bulk, api_probe=rest_api_probe(rest, sub),
                      api_write_probe=rest_write_probe(rest, sub, probe_shard))
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
        log.info("watching %s (stale after %.1fs; subaccount %d; cancel %s; beat %s)", hb, wcfg.stale_s, sub,
                 "bulk" if bulk else "by id", watchdog_beat_path(hb))
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
    return asyncio.run(amain(args))


if __name__ == "__main__":
    sys.exit(main())
