"""F-verify probes: write-capability probe and backward-step reset."""
from __future__ import annotations

import sys
from pathlib import Path

import orjson
import pytest

sys.path.insert(0, "/Users/thomast/Desktop/delta-hedged")
from dh.core.units import NS_PER_S  # noqa: E402
from dh.kalshi.rest import HttpResponse, KalshiRest, UnscopedWriteError  # noqa: E402
from dh.live.config import WatchdogCfg  # noqa: E402
from dh.live.monitor import write_heartbeat  # noqa: E402
from dh.live.watchdog import Watchdog, WriteProbeError, rest_write_probe  # noqa: E402

WALL0 = 1_790_000_000 * NS_PER_S


class T:
    def __init__(self, status, body):
        self.status, self.body, self.calls = status, body, []

    async def __call__(self, method, url, headers, params, data, timeout_s):
        self.calls.append((method, url.split("/trade-api/v2", 1)[1], list(params)))
        return HttpResponse(self.status, {}, orjson.dumps(self.body) if self.body is not None else b"")


def _client(status, body):
    tr = T(status, body)
    return KalshiRest("https://fake.invalid/trade-api/v2", None, None, transport=tr, write_subaccount=1,
                      write_shards=(2,), forbid_bulk_cancel=True), tr


@pytest.mark.parametrize("status,body,ok", [
    (404, {"error": {"code": "not_found"}}, True),
    (403, {"error": {"code": "forbidden"}}, False),
    (401, {"error": {"code": "unauthorized"}}, False),
    (400, {"error": {"code": "bad_request"}}, False),
    (500, {"error": {"code": "internal"}}, False),   # unknown outcome
    (200, {"order_id": "x", "reduced_by": "1.00"}, False),
    (204, None, False),
])
async def test_write_probe_outcomes(status, body, ok):
    rest, tr = _client(status, body)
    probe = rest_write_probe(rest, 1, 2)
    if ok:
        assert (await probe())["status"] == 404
    else:
        with pytest.raises(WriteProbeError):
            await probe()
    m, path, params = tr.calls[0]
    assert m == "DELETE" and path.startswith("/portfolio/events/orders/") and path != "/portfolio/events/orders"
    assert ("subaccount", "1") in params and ("exchange_index", "2") in params


async def test_write_probe_ids_fresh_and_scope():
    rest, tr = _client(404, {"error": {}})
    probe = rest_write_probe(rest, 1, 2)
    for _ in range(50):
        await probe()
    ids = {c[1].rsplit("/", 1)[1] for c in tr.calls}
    assert len(ids) == 50
    with pytest.raises(ValueError):
        rest_write_probe(rest, 1, -1)
    # a probe built for subaccount 0 on this client is refused by the guard before sending
    with pytest.raises(UnscopedWriteError):
        await rest_write_probe(rest, 0, 2)()


def _hb(path, t, pid=4242, session="live-s1"):
    write_heartbeat(path, {"pid": pid, "mode": "live", "state": "running", "session": session, "subaccount": 1,
                           "shutdown_timeout_s": 10.0, "order_groups": []}, now_ns=t)


async def _nosleep(_s):
    return None


async def _run(tmp_path, alive_after_step: bool, step_s: float = 3.0):
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
    clock["t"] -= int(step_s * NS_PER_S)
    for i in range(40):  # 10 s after the step
        if alive_after_step and i % 2 == 0:
            _hb(hb, clock["t"])
        await wd.step()
        clock["t"] += NS_PER_S // 4
    return wd, calls


async def test_backward_step_alive_no_fire(tmp_path: Path):
    wd, calls = await _run(tmp_path, True)
    assert calls == [] and wd.st.state == "ARMED" and wd.back_steps == 1


@pytest.mark.parametrize("step_s", [3.0, 60.0])
async def test_backward_step_dead_runner_fires(tmp_path: Path, step_s):
    wd, calls = await _run(tmp_path, False, step_s)
    assert calls, "a runner that died at the step must still trigger the watchdog"
    first = calls[0]
    print("step", step_s, "fired", (first - (WALL0 + 2 * NS_PER_S - int(step_s * NS_PER_S))) / NS_PER_S, "s after the step")
