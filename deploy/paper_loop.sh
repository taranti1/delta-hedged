#!/bin/bash
# Paper (shadow) trading, restarted after every exit: one session per UTC day-length run.
# Paper sends no orders (its REST client is read-only), so an automatic restart is safe here;
# the LIVE runner is never restarted automatically (docs/RUNBOOK.md).
# Stop: touch the kill file named in config/live.yaml (paths.kill_file), or kill this loop's PID
# (data/logs/paper_loop.pid) and then the runner.
set -u
cd "$(dirname "$0")/.." || exit 1
KILL_FILE=${KILL_FILE:-data/run/KILL}
# Strategy config under paper test: the live experiment's config (m1_live), so a paper copy can run beside live
# (docs/RUNBOOK.md 5.1: live needs the same strategy digest as its paper run). Override:
# STRATEGY_CONFIG=config/m1.yaml bash deploy/paper_loop.sh
STRATEGY_CONFIG=${STRATEGY_CONFIG:-config/m1_live.yaml}
echo $$ > data/logs/paper_loop.pid
while true; do
  if [ -e "$KILL_FILE" ]; then echo "$(date -u +%FT%TZ) kill file present: paper loop stops"; exit 0; fi
  echo "$(date -u +%FT%TZ) paper session starting ($STRATEGY_CONFIG)"
  .venv/bin/python scripts/run_live.py --config "$STRATEGY_CONFIG" --live-config config/paper.yaml \
      --mode paper --duration 86400
  rc=$?
  echo "$(date -u +%FT%TZ) paper session exited rc=$rc"
  if [ -e "$KILL_FILE" ]; then echo "kill file present: paper loop stops"; exit 0; fi
  # exit 2 = refused to start (e.g. transient REST failure at start-up): back off longer
  if [ "$rc" -eq 2 ]; then sleep 120; else sleep 15; fi
done
