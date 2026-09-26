"""Final-check probe: a fill of OUR subaccount whose order GET keeps 404-ing (e.g. the endpoint
does not see shard-2 orders) and whose REST copy is re-read by every unknown_order reconcile."""
from __future__ import annotations

import asyncio
import sys
import time

sys.path.insert(0, "/Users/thomast/Desktop/delta-hedged")
from tests.live.fakes import FakeRest, fill_row  # noqa: E402
from tests.live.test_rereview_fixes import TK, _delivered, _fill, _runner  # noqa: E402
from dh.core.events import KalshiPositionSnapshot  # noqa: E402


import pytest


@pytest.mark.parametrize("park,confirm", [(0.05, 0.02), (0.10, 0.05), (0.30, 0.05)])
async def test_unknown_order_loop(park, confirm):
    rest = FakeRest()
    t = time.time_ns() + 10**6
    rest.fills = [fill_row("f-x", "o-x", TK, created_ns=t)]
    rest.positions = {TK: "1.00"}
    s, r, rest, rec = _runner(rest, unknown_order_park_s=park, position_confirm_s=confirm, positions_interval_s=0.0)
    r.push(_fill(t, "", "o-x", "f-x"))
    r.process_pending()
    cycles_seen = []
    t_end = time.monotonic() + 3.0
    while time.monotonic() < t_end:
        await asyncio.sleep(0.01)
        r.push_side("persist")
        r.process_pending()
        if (r.metrics.get("dh_unknown_order_events_dropped_total", type="KalshiFill", source="ws") or 0) >= 1:
            cycles_seen.append(("reconciling" in r.gate.reasons, bool(r._parked)))
    drops = r.metrics.get("dh_unknown_order_events_dropped_total", type="KalshiFill", source="rest") or 0
    snaps = [e for e in s.events if isinstance(e, KalshiPositionSnapshot)]
    frac_closed = sum(1 for c, _ in cycles_seen if c) / len(cycles_seen)
    print("park", park, "confirm", confirm, "drops(rest)", drops, "delivered", _delivered(s), "snapshots", len(snaps),
          "gate closed fraction", round(frac_closed, 2), "get_order calls", len(rest.of("get_order")),
          "halted", [k for k in r.gate.reasons if k.startswith("halt")])
    assert _delivered(s) == []          # the fill never reaches the strategy
    assert not snaps                     # the mismatch is never confirmed -> never halts
    assert drops >= 2                    # dropped and re-parked again and again
