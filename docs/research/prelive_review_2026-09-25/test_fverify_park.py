"""F-verify probes: orphan delivery after the first park timeout, late ack, replay, halt w/o restricted key."""
from __future__ import annotations

import asyncio
import sys
import time

import pytest

sys.path.insert(0, "/Users/thomast/Desktop/delta-hedged")
from tests.live.fakes import FakeRest, fill_row  # noqa: E402
from tests.live.test_rereview_fixes import TK, _delivered, _fill, _replay, _runner  # noqa: E402
from dh.core.events import KalshiFill, OrderAck  # noqa: E402


async def _spin(r, secs):
    t_end = time.monotonic() + secs
    while time.monotonic() < t_end:
        await asyncio.sleep(0.01)
        r.push_side("persist")
        r.process_pending()


def _fills(s):
    return [e for e in s.events if isinstance(e, KalshiFill)]


@pytest.mark.parametrize("park,confirm", [(0.05, 0.02), (0.10, 0.05)])
async def test_orphan_delivered_once_then_late_ack_no_double(park, confirm):
    rest = FakeRest()
    t = time.time_ns() + 10**6
    rest.fills = [fill_row("f-x", "o-x", TK, created_ns=t)]
    rest.positions = {TK: "1.00"}
    s, r, rest, rec = _runner(rest, unknown_order_park_s=park, position_confirm_s=confirm, positions_interval_s=0.0)
    raw = [_fill(t, "", "o-x", "f-x")]
    r.push(raw[0])
    r.process_pending()
    await _spin(r, 1.0)
    f = _fills(s)
    print("park", park, "fills", [(x.trade_id, x.client_order_id) for x in f], "pos", s.om.position(TK),
          "gate", sorted(r.gate.reasons), "timeouts", dict(r._park_timeouts), "parked", r._parked_n)
    assert len(f) == 1 and f[0].client_order_id == "dhm1-orphan-o-x" and s.om.position(TK) == 100
    assert not [k for k in r.gate.reasons if k.startswith("halt")]
    # a LATE ack of that order (and late WS / REST copies) must not book it again
    r.push_result(OrderAck(time.time_ns(), 0, "dhm1-tok-x", "o-x", TK, 0, 0))
    r.push(_fill(time.time_ns(), "dhm1-tok-x", "o-x", "f-x"))
    r.push_side("fills", {"rows": [fill_row("f-x", "o-x", TK, created_ns=t)], "fetched_ns": time.time_ns(), "since_ns": 0})
    r.process_pending()
    assert len(_fills(s)) == 1 and s.om.position(TK) == 100
    # replay: same fills delivered
    rep = _replay(raw, rec.live())
    assert [x for x in _delivered(rep) if x[0] == "KalshiFill"] == [x for x in _delivered(s) if x[0] == "KalshiFill"]


async def test_unrestricted_key_loop_halts():
    rest = FakeRest()
    t = time.time_ns() + 10**6
    rest.fills = [fill_row("f-y", "o-y", TK, created_ns=t)]
    rest.positions = {TK: "1.00"}
    s, r, rest, rec = _runner(rest, unknown_order_park_s=0.05, position_confirm_s=0.02, positions_interval_s=0.0,
                              subaccount=0, key_restricted_to_subaccount=False, shared_account=False, allow_primary_account=True)
    r.push(_fill(t, "", "o-y", "f-y"))
    r.process_pending()
    await _spin(r, 1.5)
    halts = [k for k in r.gate.reasons if k.startswith("halt")]
    print("unrestricted: fills", len(_fills(s)), "halts", halts, "timeouts", dict(r._park_timeouts))
    assert _fills(s) == [] and halts
