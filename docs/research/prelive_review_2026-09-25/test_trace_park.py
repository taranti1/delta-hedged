import asyncio, sys, time
sys.path.insert(0, "/Users/thomast/Desktop/delta-hedged")
from tests.live.fakes import FakeRest, fill_row
from tests.live.test_rereview_fixes import TK, _fill, _runner

async def test_trace():
    rest = FakeRest()
    t = time.time_ns() + 10**6
    rest.fills = [fill_row("f-x", "o-x", TK, created_ns=t)]
    rest.positions = {TK: "1.00"}
    s, r, rest, rec = _runner(rest, unknown_order_park_s=0.10, position_confirm_s=0.05, positions_interval_s=0.0)
    logs = []
    orig = r.jlog
    def jl(kind, ts, **p):
        if kind in ("reconcile", "order_event_unknown_dropped", "order_event_parked", "position_confirm_parked", "position_suspect"):
            logs.append((round(time.monotonic() - t0, 3), kind, p.get("action", ""), p.get("reason", p.get("source", ""))))
        return orig(kind, ts, **p)
    r.jlog = jl
    t0 = time.monotonic()
    r.push(_fill(t, "", "o-x", "f-x")); r.process_pending()
    while time.monotonic() - t0 < 1.5:
        await asyncio.sleep(0.01); r.push_side("persist"); r.process_pending()
    for l in logs: print(l)

