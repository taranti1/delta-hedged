#!/usr/bin/env bash
# Start the live watch (scripts/run_live.py) in screen session dh-watch, after the checks the
# runbook asks for (5.0, 5.2): screener beat fresh, warm file refreshed, funds OK.
# Usage: scripts/start_watch.sh [duration_s]   (default 14400 = 4 h)
set -euo pipefail
cd "$(dirname "$0")/.."
source .venv/bin/activate 2>/dev/null || true
DURATION="${1:-14400}"

SCREENS="$(screen -ls || true)"   # screen -ls exits non-zero even when sessions exist
if grep -q '\.dh-watch\b' <<<"$SCREENS"; then
  echo "dh-watch is already running (screen -r dh-watch)"; exit 1
fi
if ! grep -q '\.dh-screener\b' <<<"$SCREENS"; then
  echo "dh-screener is not running: start the screener first"; exit 1
fi
python - <<'EOF'
import json, time, sys
b = json.load(open("data/run/heartbeat.json.watchdog"))
age = time.time() - b["t"] / 1e9
if age > 10 or b.get("subaccount") != 1 or not b.get("api_ok") or not b.get("api_write_ok"):
    sys.exit(f"screener beat not healthy (age {age:.1f}s, {b.get('state')}): fix the screener first")
print(f"screener OK ({b.get('state')}, beat {age:.1f}s old)")
EOF

python -m dh.live.tools warmfile --kalshi-config config/kalshi.history.yaml
FUNDS="$(python -m dh.live.tools balance --config config/m1_live.yaml)"
echo "$FUNDS"
grep -q ' OK' <<<"$FUNDS" || { echo "funds check failed"; exit 1; }

mkdir -p data/logs
LOG="data/logs/watch_$(date -u +%Y%m%dT%H%M%SZ).log"
screen -dmS dh-watch bash -c "source .venv/bin/activate 2>/dev/null; caffeinate -i python scripts/run_live.py \
  --config config/m1_live.yaml --live-config config/live.yaml --mode live \
  --i-understand-this-sends-real-orders --duration $DURATION 2>&1 | tee -a $LOG"
echo "watch started in screen dh-watch (${DURATION}s), log $LOG"
