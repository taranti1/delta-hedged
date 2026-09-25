"""Throwaway pre-live review probes (offline; fake transports only)."""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import sys
from pathlib import Path
from typing import Any

import pytest

REPO = Path("/Users/thomast/Desktop/delta-hedged")
sys.path.insert(0, str(REPO))

from dh.kalshi.rest import HttpResponse, KalshiHTTPError, KalshiRest, UnscopedWriteError  # noqa: E402
from dh.live.config import LiveConfig, VenueCfg  # noqa: E402


# --------------------------------------------------------------------------- transport fake
class Recorder:
    def __init__(self, status: int = 200, body: bytes = b"{}") -> None:
        self.calls: list[tuple[str, str, list, bytes | None]] = []
        self.status, self.body = status, body

    async def __call__(self, method, url, headers, params, data, timeout_s):
        self.calls.append((method, url, list(params), data))
        return HttpResponse(self.status, {}, self.body)


def _rest(sub: int | None, tr: Recorder) -> KalshiRest:
    return KalshiRest("https://fake.invalid/trade-api/v2", None, None, transport=tr, write_subaccount=sub)


# 1. guard: presence of exchange_index enforced, VALUE is not (a write naming shard 0 passes)
async def test_guard_accepts_any_explicit_shard():
    tr = Recorder(body=b'{"order_id":"x"}')
    r = _rest(1, tr)
    await r.create_order({"ticker": "KXBTCD-X", "subaccount": 1, "exchange_index": 0})
    assert len(tr.calls) == 1  # shard 0 write went out: the guard checks presence, not the configured shard


# 1b. guard: subaccount 0 refused
async def test_guard_refuses_sub0():
    tr = Recorder()
    r = _rest(1, tr)
    with pytest.raises(UnscopedWriteError):
        await r.cancel_all_orders(subaccount=0)
    with pytest.raises(UnscopedWriteError):
        await r.trigger_order_group("g", subaccount=None, exchange_index=2)
    assert tr.calls == []


# 3. watchdog with an empty --live-config uses LiveConfig() defaults: subaccount 0, shared_account False
async def test_watchdog_default_config_targets_subaccount_0():
    sys.path.insert(0, str(REPO / "scripts"))
    import importlib

    wd = importlib.import_module("watchdog")
    calls: list[Any] = []

    class FakeRest:
        async def cancel_all_orders(self, *, subaccount):
            calls.append(("cancel_all", subaccount))
            return {}

        async def trigger_order_group(self, gid, *, subaccount, exchange_index):
            calls.append(("trigger", gid, subaccount, exchange_index))
            return {}

        async def close(self):
            pass

    args = argparse.Namespace(live_config="", heartbeat="/nonexistent/hb.json", once=False, cancel_now=True,
                              arm_on_start=False, max_age_s=0.0)
    rc = await wd.amain(args, rest=FakeRest())
    assert rc == 0
    assert calls == [("cancel_all", 0)]  # <-- System 2's subaccount


# 5. key restriction: an api_keys failure + balance bodies without balance_breakdown -> accepted
async def test_key_restriction_fallback_accepts_without_positive_evidence():
    from dh.live.startup import verify_key_restriction

    class R:
        async def get_api_keys(self):
            raise KalshiHTTPError("GET", "/api_keys", 503, {"error": {"code": "x"}})

    ok, why = await verify_key_restriction(R(), "kid", 1, [{"balance": 15000, "balance_dollars": "150.00"}])
    assert ok, why

    ok, why = await verify_key_restriction(R(), "kid", 1, [])
    assert not ok  # no bodies -> refused


# 5b. restricted-key rule: a primary-account fill (no subaccount field) is taken as ours
def test_primary_fill_attributed_with_restricted_flag():
    from dh.kalshi.normalize import ws_message_to_events
    from dh.live.runner import own_subaccount_ok

    msg = {"type": "fill", "sid": 1, "msg": {"trade_id": "t", "order_id": "o", "client_order_id": "sys2-abc",
                                            "market_ticker": "KXBTCD-26SEP2515-T83999.99", "side": "yes", "book_side": "bid",
                                            "action": "buy", "count_fp": "1.00", "yes_price_dollars": "0.5000",
                                            "is_taker": False, "ts": 1_700_000_000, "fee_cost": "0.0000"}}
    try:
        evs = ws_message_to_events(msg, 1)
    except Exception as exc:  # shape details may differ; the rule itself is what matters
        pytest.skip(f"normalizer shape: {exc}")
    f = evs[0]
    assert f.subaccount == 0
    assert own_subaccount_ok(f, 1, key_restricted=True)


# 7. schedule: a session closing "00:00" is dropped -> bogus closure; the 12 h plausibility check
#    looks at [start_ns, start_ns + 24h) = the PAST day when called with start = now - 1 day
def test_schedule_close_midnight_and_plausibility_window():
    from zoneinfo import ZoneInfo

    from dh.live.startup import schedule_closures

    ny = ZoneInfo("America/New_York")
    full = [{"open_time": "00:00", "close_time": "00:00"}]  # a 24 h session written as 00:00-00:00
    ok = [{"open_time": "00:00", "close_time": "23:59"}]
    week = {"start_time": "2026-01-01T00:00:00Z", "end_time": "2027-01-01T00:00:00Z",
            "monday": ok, "tuesday": ok, "wednesday": ok,
            "thursday": [{"open_time": "00:00", "close_time": "03:00"}, {"open_time": "05:00", "close_time": "00:00"}],
            "friday": ok, "saturday": ok, "sunday": ok}
    now = int(dt.datetime(2026, 9, 28, 12, 0, tzinfo=ny).timestamp() * 1e9)  # a Monday
    closures, notes = schedule_closures({"schedule": {"standard_hours": [week], "maintenance_windows": []}},
                                        now - 86_400 * 10**9, now + 8 * 86_400 * 10**9)
    long = [(a, b) for a, b, _ in closures if (b - a) > 6 * 3600 * 10**9]
    print("closures", [(dt.datetime.fromtimestamp(a / 1e9, ny).isoformat(), (b - a) / 3.6e12) for a, b, _ in closures], notes)
    assert long, "expected a >6 h bogus Thursday closure to survive the plausibility check"
    del full


# 8. sntp output variants
def test_sntp_regex_variants():
    from dh.store.recorder import _SNTP_RE

    good = "+0.073787 +/- 0.012496 time.apple.com 2620:149:a33:4000::31"
    dated = "2026-09-25 20:00:00.123456 (+0400) +0.073787 +/- 0.012496 time.apple.com 17.253.4.125 s1 no-leap"
    nosign = "0.000000 +/- 0.010000 time.apple.com 17.253.4.125"
    assert _SNTP_RE.match(good)
    assert not _SNTP_RE.match(dated)  # older ntp-sntp format -> unmeasurable -> blocks (fail closed)
    assert not _SNTP_RE.match(nosign)


# 9. a live config without a `venue:` section passes every live check and targets subaccount 0
async def test_live_config_defaults_target_subaccount_0():
    from dh.live.config import live_config_problems
    from dh.live.venue_kalshi import KalshiVenue

    cfg = LiveConfig(mode="live")
    assert live_config_problems(cfg) == []
    assert cfg.venue.sub == 0 and not cfg.venue.shared_account and not cfg.venue.key_restricted_to_subaccount
    tr = Recorder(status=204, body=b"")
    rest = _rest(cfg.venue.sub, tr)  # app.py: write_subaccount=lcfg.venue.sub
    v = KalshiVenue(rest, sink=lambda e: None, cfg=cfg.venue)
    assert await v.cancel_all_now("startup")
    assert tr.calls[0][0] == "DELETE" and ("subaccount", "0") in tr.calls[0][2]  # System 2's orders
