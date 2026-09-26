"""Final-check probes: watchdog future-stamp rules and API capability probe."""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, "/Users/thomast/Desktop/delta-hedged")
from dh.core.units import NS_PER_S  # noqa: E402
from dh.kalshi.rest import KalshiHTTPError  # noqa: E402
from dh.live.config import WatchdogCfg  # noqa: E402
from dh.live.monitor import write_heartbeat  # noqa: E402
from dh.live.watchdog import Watchdog, rest_api_probe  # noqa: E402

WALL0 = 1_790_000_000 * NS_PER_S


async def _nosleep(_s):
    return None


def _hb(path, t, pid=4242, session="live-s1"):
    write_heartbeat(path, {"pid": pid, "mode": "live", "state": "running", "session": session, "subaccount": 1,
                           "shutdown_timeout_s": 10.0, "order_groups": []}, now_ns=t)


async def test_same_host_small_skew_never_fires(tmp_path: Path):
    """Runner and watchdog on one host share the wall clock: heartbeats are never in the future."""
    clock = {"t": WALL0}
    calls = []

    async def cancel():
        calls.append(clock["t"])
        return True

    hb = tmp_path / "heartbeat.json"
    wd = Watchdog(hb, cancel, WatchdogCfg(), clock_ns=lambda: clock["t"], sleep=_nosleep, subaccount=1)
    for i in range(400):  # 100 s: runner writes every 0.5 s, watchdog polls every 0.25 s
        if i % 2 == 0:
            _hb(hb, clock["t"] + 1_500_000_000)  # even a heartbeat 1.5 s ahead (sub-threshold skew)
        await wd.step()
        clock["t"] += NS_PER_S // 4
    assert wd.st.state == "ARMED" and calls == []


async def test_backward_wall_step_fires_even_though_heartbeats_stay_fresh(tmp_path: Path):
    """A 3 s BACKWARD wall-clock step on the host (both processes share it): every new heartbeat
    is fresh, but last_hb_ns (a max) is now 'in the future' -> TRIGGERED -> cancel-all + marker
    naming the live runner -> the runner halts (sticky)."""
    clock = {"t": WALL0}
    calls = []

    async def cancel():
        calls.append(clock["t"])
        return True

    hb = tmp_path / "heartbeat.json"
    wd = Watchdog(hb, cancel, WatchdogCfg(), clock_ns=lambda: clock["t"], sleep=_nosleep, subaccount=1)
    for _ in range(8):
        _hb(hb, clock["t"])
        await wd.step()
        clock["t"] += NS_PER_S // 4
    assert wd.st.state == "ARMED"
    clock["t"] -= 3 * NS_PER_S  # NTP / timed steps the wall clock back 3 s
    for _ in range(4):
        _hb(hb, clock["t"])  # the runner keeps beating, with the stepped clock
        await wd.step()
        clock["t"] += NS_PER_S // 4
    print("state", wd.st.state, "cancel-alls", len(calls), "marker", (tmp_path / "heartbeat.json.cancel_all").exists())
    assert calls, "the watchdog fired on a healthy runner after a backward clock step"


async def test_api_probe_passes_with_a_read_only_key():
    """The capability probe is a READ: a key without the write scope passes it, while every cancel
    would be refused (403)."""

    class ReadOnlyKey:
        async def get_orders(self, **kw):
            return {"orders": [], "cursor": ""}

        async def batch_cancel_orders(self, items):
            raise KalshiHTTPError("DELETE", "/portfolio/events/orders/batched", 403,
                                  {"error": {"code": "forbidden", "message": "missing write scope"}})

    probe = rest_api_probe(ReadOnlyKey(), 1)
    assert isinstance(await probe(), dict)  # api_ok: True
