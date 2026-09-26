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
* `config/live.yaml` exists (the watchdog never falls back to the example config) and states
  `venue.subaccount: 1`, `venue.shared_account: true`, `venue.key_restricted_to_subaccount:
  true` explicitly, with the macOS runtime paths (`paths.kill_file` / `paths.heartbeat_file`
  empty = `data/run/KILL` / `data/run/heartbeat.json`). A missing file, a venue section that
  does not state the subaccount and the shared flag, or subaccount 0 is refused (exit 2;
  launchd retries every 10 s: fix the config).
* The watchdog key (restricted to subaccount 1, RUNBOOK 1.2) is in System 1's own env file:
  `KALSHI_WATCHDOG_KEY_ID=...` and `KALSHI_WATCHDOG_PRIVATE_KEY_PATH=...` in the file named by
  `config/kalshi.yaml` `auth.env_file` (a launchd agent does not inherit the shell environment;
  never System 2's `.secrets/prod.env`). On the shared account the watchdog REFUSES to run
  without them (exit 2) unless `watchdog.allow_runner_key: true` (a deliberate choice to sign
  with the runner's key).

**Liveness beat.** While it runs, the watchdog writes `data/run/heartbeat.json.watchdog` every
`watchdog.beat_interval_s` (1 s): `{"t", "pid", "subaccount", "state", "armed": [runner pid,
session], "last_poll_ns", ...}` and `"state": "EXITED"` when it stops. The live runner refuses to
start unless that beat is fresh (`watchdog.runner_max_age_s`, 10 s) and names its subaccount, and
while it trades it blocks new orders (gate `watchdog`, quotes pulled) when the beat goes stale,
names another subaccount, or (after 10 s of running) is not armed on this runner. So a watchdog
crash-looping under launchd (TCC, credentials, config refusal) is seen: the runner stops
quoting instead of trading without its dead-man switch. Check it with
`cat data/run/heartbeat.json.watchdog` (`t` in ns, `state` ARMED while a live runner runs).

**Capability, not just liveness.** At start and every `watchdog.api_probe_interval_s` (60 s) the
watchdog reads `GET /portfolio/orders?subaccount=1&status=resting&limit=1` with its OWN key (read
only; a successful cancel-all also counts). The beat carries `"api_ok"`, `"api_ok_ns"` (last
success), `"api_error"` and `"step_ok"` (false after 3 failed polls in a row). The live runner
treats `api_ok: false` (revoked or wrong watchdog key, missing permission, network down), a last
success older than `watchdog.api_max_age_s` (180 s) or `step_ok: false` like a stale beat: start
refused, gate `watchdog`. After installing the agent check `"api_ok": true` in the beat; if it
is false, `api_error` says why (a `401` = the key itself is rejected).

**Write capability (final check F2).** Reading is not cancelling: a key without the `write`
scope passes the read. At start and every `watchdog.api_write_probe_interval_s` (600 s; every
60 s while it fails) the watchdog also sends `DELETE /portfolio/events/orders/<fresh random
uuid4>?subaccount=1&exchange_index=2` (the first of `venue.exchange_indexes`) through its scoped
write client. 404 (no such order) = the key may cancel; 401 / 403 (no write scope, key not
allowed on subaccount 1), any other status, a timeout or (never expected) a 2xx = not proven:
`"api_ok": false`, `"api_write_ok": false`, reason in `"api_write_error"`. The live runner
requires `"api_write_ok": true`. The id is a new random uuid4 every time, never a real order id,
so the probe cannot cancel anything (it costs 2 write tokens every 10 minutes). After installing
the agent check `"api_write_ok": true` in the beat; a `403` there = recreate the watchdog key
with `["read","write"]` scopes restricted to subaccount 1 (docs/ACCOUNT_SETUP.md).

**Timestamps.** A beat stamped more than `watchdog.max_future_s` (2 s) in the future is not
fresh for the runner, and a RUNNER heartbeat stamped in the future is not trusted by the
watchdog: it never arms on it and it does not refresh the watched runner's liveness, so the
watchdog fires as for a stale heartbeat (a clock step costs a cancel-all, never a silent switch).
A BACKWARD wall-clock step on the host (both processes share the clock; final check F3) leaves the
stored heartbeat time in the future: the watched runner's next fresh heartbeat (same pid +
session, consistent with the new clock) resets it and the watchdog does not fire (log `wall clock
stepped back ...`); if no such heartbeat arrives within `watchdog.stale_s` (the runner died at
the step), it fires.

```sh
cd /Users/thomast/Desktop/delta-hedged
mkdir -p data/logs data/run ~/Library/LaunchAgents
cp deploy/launchd/com.dh.watchdog.plist ~/Library/LaunchAgents/
plutil -lint ~/Library/LaunchAgents/com.dh.watchdog.plist
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.dh.watchdog.plist
launchctl print gui/$(id -u)/com.dh.watchdog | grep -E 'state|pid|last exit'
tail -f data/logs/watchdog.launchd.out     # "watching .../data/run/heartbeat.json (... subaccount 1; cancel by id; beat ...)"
cat data/run/heartbeat.json.watchdog        # its own beat: refreshed every second; "api_ok": true, "api_write_ok": true
```

Operate: `launchctl kickstart -k gui/$(id -u)/com.dh.watchdog` (restart; `--arm-on-start`
re-locks onto a live runner), `launchctl bootout gui/$(id -u)/com.dh.watchdog` (stop: only
while no live runner runs). The same Full Disk Access note as for the recorder applies.
