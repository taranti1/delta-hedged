# launchd agents (macOS): recorder and watchdog

Two templates, neither installed automatically: `com.dh.recorder.plist` (below) and
`com.dh.watchdog.plist` (the dead-man switch of a LIVE runner, section "Watchdog" at the end).
The live runner itself is never a launchd job: a human starts and restarts it
(docs/RUNBOOK.md 5.2).

## Recorder

`com.dh.recorder.plist` runs `scripts/record.py --config config/feeds.yaml` under
`caffeinate -i` as a per-user LaunchAgent: it starts at login, restarts on any exit
(`KeepAlive`, 30 s throttle), and logs to `data/logs/record.launchd.out`. Nothing here is
installed automatically.

## Install

```sh
cd /Users/thomast/Desktop/delta-hedged
# 1. stop a manually started recorder first (only one recorder per store: data/recorder.lock)
kill -TERM "$(cat data/logs/record.pid)"        # waits for flush; check: ps -p <pid>
# 2. install and start the agent
mkdir -p data/logs ~/Library/LaunchAgents
cp deploy/launchd/com.dh.recorder.plist ~/Library/LaunchAgents/
plutil -lint ~/Library/LaunchAgents/com.dh.recorder.plist
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.dh.recorder.plist
launchctl print gui/$(id -u)/com.dh.recorder | grep -E 'state|pid|last exit'
tail -f data/logs/record.launchd.out             # status line every 60 s
```

## Operate

```sh
launchctl kickstart -k gui/$(id -u)/com.dh.recorder   # restart (e.g. after a code update)
launchctl bootout gui/$(id -u)/com.dh.recorder        # stop and unload (SIGTERM: clean flush)
```

## Notes

* **Folder access (TCC).** The repository and the Kalshi credentials file live under
  `~/Desktop`, which macOS protects. A process started by launchd does not inherit the
  Terminal's permission: if the log shows `PermissionError: [Errno 1] Operation not
  permitted` on the repository or the env file, give the interpreter
  (`/Users/thomast/.local/share/uv/python/cpython-3.12.14-macos-aarch64-none/bin/python3.12`,
  the target of `.venv/bin/python`) Full Disk Access in System Settings > Privacy & Security,
  or move the repository out of `~/Desktop` and update the paths in the plist.
* **Sleep.** `caffeinate -i` prevents idle sleep only. Closing a laptop lid still sleeps the
  Mac (no data while asleep; the recorder reconnects and the gap is visible in the streams).
  Use `pmset -g assertions` to see the assertion.
* **Single instance.** The recorder takes an exclusive `flock` on `data/recorder.lock`; a
  second instance exits with status 2 (launchd then retries every 30 s, so stop the manual
  instance before bootstrapping, or the log fills with refusals).
* **Disk.** Measured 2026-09-25: about 250 MB/hour on disk (~6 GB/day) with the default
  `config/feeds.yaml`; see docs/RUNBOOK.md section 3 for the per-stream table.

## Watchdog

`com.dh.watchdog.plist` runs `scripts/watchdog.py --live-config config/live.yaml --arm-on-start`
under `caffeinate -i`, restarts it on any exit (`KeepAlive`, 10 s throttle, `ProcessType
Interactive`: never throttled) and logs to `data/logs/watchdog.launchd.out`. It must be up
whenever a live runner may run (docs/RUNBOOK.md 5.2 step 2 and section 6).

Before installing:
* `config/live.yaml` exists with `venue.subaccount: 1`, `venue.shared_account: true` (the
  watchdog then refuses to act for subaccount 0: exit 2, launchd retries every 10 s, fix the
  config) and the macOS runtime paths (`paths.kill_file` / `paths.heartbeat_file` empty =
  `data/run/KILL` / `data/run/heartbeat.json`).
* The watchdog key (restricted to subaccount 1, RUNBOOK 1.2) is in System 1's own env file:
  `KALSHI_WATCHDOG_KEY_ID=...` and `KALSHI_WATCHDOG_PRIVATE_KEY_PATH=...` in the file named by
  `config/kalshi.yaml` `auth.env_file` (a launchd agent does not inherit the shell environment;
  never System 2's `.secrets/prod.env`).

```sh
cd /Users/thomast/Desktop/delta-hedged
mkdir -p data/logs data/run ~/Library/LaunchAgents
cp deploy/launchd/com.dh.watchdog.plist ~/Library/LaunchAgents/
plutil -lint ~/Library/LaunchAgents/com.dh.watchdog.plist
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.dh.watchdog.plist
launchctl print gui/$(id -u)/com.dh.watchdog | grep -E 'state|pid|last exit'
tail -f data/logs/watchdog.launchd.out     # "watching .../data/run/heartbeat.json (... subaccount 1)"
```

Operate: `launchctl kickstart -k gui/$(id -u)/com.dh.watchdog` (restart; `--arm-on-start`
re-locks onto a live runner), `launchctl bootout gui/$(id -u)/com.dh.watchdog` (stop: only
while no live runner runs). The same Full Disk Access note as for the recorder applies.
