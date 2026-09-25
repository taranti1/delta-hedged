#!/bin/bash
# Paper (shadow) trading, restarted after every exit: one session per UTC day-length run.
# Paper sends no orders (its REST client is read-only), so an automatic restart is safe here;
# the LIVE runner is never restarted automatically (docs/RUNBOOK.md).
# Stop: touch the kill file named in config/live.yaml (paths.kill_file), or kill this loop's PID
# (data/logs/paper_loop.pid) and then the runner.
set -u
cd "$(dirname "$0")/.." || exit 1
KILL_FILE=${KILL_FILE:-data/run/KILL}
echo $$ > data/logs/paper_loop.pid
while true; do
  if [ -e "$KILL_FILE" ]; then echo "$(date -u +%FT%TZ) kill file present: paper loop stops"; exit 0; fi
  echo "$(date -u +%FT%TZ) paper session starting"
  .venv/bin/python scripts/run_live.py --config config/m1.yaml --live-config config/live.yaml \
      --mode paper --duration 86400
  rc=$?
  echo "$(date -u +%FT%TZ) paper session exited rc=$rc"
  if [ -e "$KILL_FILE" ]; then echo "kill file present: paper loop stops"; exit 0; fi
  # exit 2 = refused to start (e.g. transient REST failure at start-up): back off longer
  if [ "$rc" -eq 2 ]; then sleep 120; else sleep 15; fi
done
