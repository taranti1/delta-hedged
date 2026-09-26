#!/bin/bash
# Pauses this repo's data collection when the shared disk runs low (the other trading system on
# this Mac can write ~12 GB/h). Every CHECK_S seconds: if free space < PAUSE_GB, SIGSTOP the
# recorder, the history downloader(s) and the paper loop + runner (never anything outside this
# repo, never a live runner). Resume is MANUAL once space is freed:
#   pkill -CONT -f 'scripts/record.py|download_kalshi_history|deploy/paper_loop.sh|--mode paper'
# (processes reconnect on their own; the recording shows a gap for the paused period).
# The recorder's own guard (config/feeds.yaml: shed < 30 GB, clean stop < 8 GB) stays in force.
set -u
cd "$(dirname "$0")/.." || exit 1
PAUSE_GB=${PAUSE_GB:-25}
CHECK_S=${CHECK_S:-300}
LOG=data/logs/disk_watch.log
echo $$ > data/logs/disk_watch.pid
PATTERN='scripts/record.py|download_kalshi_history|deploy/paper_loop.sh|--mode paper'
paused=0
while true; do
  free_gb=$(df -k data | tail -1 | awk '{printf "%d", $4/1048576}')
  if [ "$free_gb" -lt "$PAUSE_GB" ]; then
    if [ "$paused" -eq 0 ]; then
      echo "$(date -u +%FT%TZ) LOW DISK ${free_gb} GB < ${PAUSE_GB} GB: pausing data collection" >> "$LOG"
      for p in $(pgrep -f "$PATTERN"); do
        cmd=$(ps -o command= -p "$p")
        case "$cmd" in *caffeinate*|*"--mode live"*) continue;; esac
        kill -STOP "$p" && echo "  paused $p: ${cmd:0:100}" >> "$LOG"
      done
      osascript -e "display notification \"Paused delta-hedged data collection: ${free_gb} GB free\" with title \"Low disk\"" 2>/dev/null
      paused=1
    fi
  else
    if [ "$paused" -eq 1 ] && ! pgrep -f "$PATTERN" | xargs ps -o stat= -p 2>/dev/null | grep -q T; then
      echo "$(date -u +%FT%TZ) processes resumed manually (${free_gb} GB free)" >> "$LOG"; paused=0
    fi
  fi
  # hourly heartbeat line
  [ "$(date +%M)" -lt $((CHECK_S / 60 + 1)) ] && echo "$(date -u +%FT%TZ) free ${free_gb} GB" >> "$LOG"
  sleep "$CHECK_S"
done
