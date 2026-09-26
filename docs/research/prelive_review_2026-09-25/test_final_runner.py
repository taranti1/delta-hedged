"""Final-check probes (post 560852e): parking, replay, key proof, future beats, self-wait. Offline."""
from __future__ import annotations

import asyncio
import sys
import time

import pytest

sys.path.insert(0, "/Users/thomast/Desktop/delta-hedged")
from tests.live.fakes import FakeRest, fill_row, order_row  # noqa: E402
from tests.live.test_rereview_fixes import (TK, _delivered, _drain, _fill, _replay, _runner)  # noqa: E402
from dh.core.events import KalshiFill, OrderAck  # noqa: E402
from dh.kalshi.rest import KalshiHTTPError  # noqa: E402
from dh.live.monitor import watchdog_beat_problem  # noqa: E402
from dh.live.runner import order_row_owner  # noqa: E402


def _fills(s):
    return [e for e in s.events if isinstance(e, KalshiFill)]


# NEW-1 a: our WS fill without client id, its REST copy too, both before the ack -> delivered ONCE, replay equal
async def test_ws_and_rest_copies_parked_released_once_and_replay():
    s, r, rest, rec = _runner(unknown_order_lookup_delay_s=5.0)
    t = time.time_ns() + 10**6
    raw = [_fill(t, "", "o-5", "f-5")]
    r.push(raw[0])
    r.push_side("fills", {"rows": [fill_row("f-5", "o-5", TK, created_ns=t)], "fetched_ns": t, "since_ns": 0})
    r.process_pending()
    assert r._parked_n == 2 and _fills(s) == []
    r.push_result(OrderAck(t + 5, 0, "dhm1-tok-5", "o-5", TK, 0, 100))
    r.process_pending()
    assert len(_fills(s)) == 1 and s.om.position(TK) == 100 and r._parked == {}
    # late copies (WS again / REST again) never re-deliver
    r.push(_fill(t + 6, "", "o-5", "f-5"))
    r.push_side("fills", {"rows": [fill_row("f-5", "o-5", TK, created_ns=t)], "fetched_ns": t + 7, "since_ns": 0})
    r.process_pending()
    assert len(_fills(s)) == 1
    rep = _replay(raw, rec.live())
    assert [x for x in _delivered(rep) if x[0] == "KalshiFill"] == [x for x in _delivered(s) if x[0] == "KalshiFill"]


# NEW-1 b: System 2's fill (primary order, other client id) with a RESTRICTED flag: GET order row lacks
# subaccount_number -> treated as ours by subaccount, but the client id decides -> foreign
def test_lookup_verdicts():
    assert order_row_owner(order_row("sys2-x", "o", TK), "dhm1-", 1, key_restricted=True)["verdict"] == "foreign"
    assert order_row_owner(order_row("dhm1-t-1", "o", TK), "dhm1-", 1, key_restricted=False)["verdict"] == "foreign"
    assert order_row_owner(order_row("dhm1-t-1", "o", TK), "dhm1-", 1, key_restricted=True)["verdict"] == "ours"
    assert order_row_owner(order_row("", "o", TK), "dhm1-", 1, key_restricted=True)["verdict"] == "foreign"
    assert order_row_owner(None, "dhm1-", 1, key_restricted=True)["verdict"] == "not_found"


# NEW-1 c: a proven-foreign fill never moves inventory and never pauses
async def test_foreign_by_lookup():
    rest = FakeRest()
    rest.orders["o-9"] = order_row("sys2-abc", "o-9", TK)
    s, r, rest, _ = _runner(rest)
    r.push(_fill(time.time_ns(), "", "o-9", "t9"))
    await _drain(r)
    assert _fills(s) == [] and s.om.position(TK) == 0 and "reconciling" not in r.gate.reasons


# NEW-3 runner side: future beat refused, small skew accepted
def test_future_beat_rules():
    now = time.time_ns()
    base = {"subaccount": 1, "state": "ARMED", "armed": None, "api_ok": True, "api_ok_ns": now, "step_ok": True}
    assert watchdog_beat_problem({**base, "t": now + 3600 * 10**9}, now_ns=now, subaccount=1, max_age_s=10)
    assert watchdog_beat_problem({**base, "t": now + 10**9}, now_ns=now, subaccount=1, max_age_s=10) == ""
    assert watchdog_beat_problem({**base, "t": now, "api_ok": False}, now_ns=now, subaccount=1, max_age_s=10)


# NEW-6: 401 no longer proof
@pytest.mark.parametrize("status,expect", [(200, False), (403, True), (401, False), (503, False)])
async def test_key_proof_final(status, expect):
    from dh.live.startup import verify_key_restriction

    class R:
        async def get_api_keys(self):
            raise KalshiHTTPError("GET", "/api_keys", 403, {})

        async def get_balance(self, **kw):
            if status == 200:
                return {"balance": 1}
            raise KalshiHTTPError("GET", "/portfolio/balance", status, {})

    ok, _ = await verify_key_restriction(R(), "k", 1, [])
    assert ok is expect
