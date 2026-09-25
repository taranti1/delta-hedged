#!/usr/bin/env python
"""Run the MarketMaker live (real orders) or in paper mode (shadow trading, no orders).

    # paper (default): the strategy trades against the LIVE book through a conservative
    # simulator; nothing is ever sent to Kalshi's order endpoints
    python scripts/run_live.py --config config/m1.yaml --live-config config/live.yaml --mode paper

    # live: needs `mode: live` in the live config AND the confirmation flag
    python scripts/run_live.py --config config/m1.yaml --live-config config/live.yaml --mode live \
        --i-understand-this-sends-real-orders

Stops on SIGINT/SIGTERM, the kill file (config paths.kill_file) or --duration: cancels every
order via REST (live), flushes the recorder and exits. Exit codes: 0 ok / kill file,
2 refused to start, 3 could not confirm all orders cancelled at shutdown (check the Kalshi
UI; the watchdog keeps trying), 4 strategy/consumer error. docs/RUNBOOK.md has the procedures.

A halt (daily loss, position or fee mismatch, the watchdog's cancel-all) and the day's P&L
survive a restart (the risk state file plus today's fills, settlements and positions at
exchange prices from Kalshi): the runner starts halted (a daily-loss halt only on the UTC day
it was decided). After investigating, --reset-daily-halt clears the carried halt/pause and
starts a fresh daily-loss budget from the current real P&L; the real P&L stays recorded
(logged loudly, recorded in the session's meta).
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from dh.live.app import StartupError, run_from_paths  # noqa: E402
from dh.live.config import LIVE_CONFIRM_FLAG, ModeError  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--config", default="config/m1.yaml", help="strategy config (dh.strategy.config)")
    p.add_argument("--live-config", default="config/live.yaml",
                   help="runner config (endpoints, paths, venue, universe); template: config/live.example.yaml")
    p.add_argument("--mode", choices=("paper", "live"), default=None,
                   help="override the live config's mode (live additionally needs mode: live in the file)")
    p.add_argument(LIVE_CONFIRM_FLAG, dest="confirmed", action="store_true",
                   help="required for --mode live: you accept that real orders will be sent")
    p.add_argument("--reset-daily-halt", action="store_true",
                   help="operator override: start without the carried-over halt / pause; the daily-loss limit counts "
                        "from the current real day P&L (a fresh budget; logged); only after investigating why it halted")
    p.add_argument("--duration", type=float, default=0.0, help="stop after N seconds (0 = until signalled)")
    p.add_argument("--log-level", default="INFO")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=args.log_level.upper(), format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    live_cfg = args.live_config
    if live_cfg and not (REPO / live_cfg).is_file() and not Path(live_cfg).is_file():
        example = "config/live.example.yaml"
        if live_cfg != "config/live.yaml":
            logging.error("live config %s not found (template: %s)", live_cfg, example)
            return 2
        # the template says `mode: paper`, so falling back to it can never enable live trading
        logging.warning("config/live.yaml not found: using %s (copy and edit it for your setup)", example)
        live_cfg = example
    try:
        if args.reset_daily_halt:
            logging.warning("--reset-daily-halt: the carried-over halt and pause are cleared; the daily-loss budget "
                            "restarts from the current real day P&L")
        return asyncio.run(run_from_paths(args.config, live_cfg, cli_mode=args.mode, confirmed=args.confirmed,
                                          duration_s=args.duration or None, reset_daily_halt=args.reset_daily_halt))
    except ModeError as exc:
        logging.error("%s", exc)
        return 2
    except StartupError as exc:
        logging.error("refusing to start: %s", exc)
        return 2


if __name__ == "__main__":
    sys.exit(main())
