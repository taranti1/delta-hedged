"""Deployment on a SHARED Kalshi account (offline): System 1 on subaccount 1, markets on exchange
shard 2, another live system on subaccount 0.

* exchange_index end to end (spec -> selection -> every order write, one order group per shard);
* every write names the subaccount AND a shard, checked three ways: dynamically through the real
  KalshiRest with a capturing transport, by the REST client's own guard (write_subaccount), and
  statically over dh/live + scripts (a new write call without them fails this file);
* config refusals (subaccount 0, account_share), macOS runtime paths;
* the kill switch: order-group trigger first (runner, watchdog), never a reset afterwards;
* start-up: collateral per shard, the restricted-key verification, the first-fill probe.
"""

from __future__ import annotations

import argparse
import ast
import asyncio
import json
from dataclasses import replace
from pathlib import Path
from typing import Any

import orjson
import pytest

from dh.core.actions import (
    AmendOrder,
    CancelOrder,
    DecreaseOrder,
    PlaceOrder,
    ResetOrderGroup,
    UpdateOrderGroupLimit,
)
from dh.core.events import OrderReject
from dh.kalshi.normalize import exchange_index_of, rest_market_to_spec
from dh.kalshi.rest import HttpResponse, KalshiRest, UnscopedWriteError
from dh.live.app import LiveApp, Overrides, WsFillProbe
from dh.live.config import (
    LiveConfig,
    VenueCfg,
    account_share_problem,
    default_run_dir,
    live_config_problems,
    load_live_config,
    PathsCfg,
)
from dh.live.monitor import read_heartbeat, write_heartbeat
from dh.live.startup import required_balance_usd, select_specs, verify_key_restriction
from dh.live.venue_kalshi import KalshiVenue, balance_dollars
from dh.live.watchdog import Watchdog, rest_cancel_all, rest_trigger_groups

from ..kalshi import samples as S
from .fakes import FakeClock, FakeRest, kxbtcd_spec, order_row

REPO = Path(__file__).resolve().parents[2]
TK = "KXBTCD-26SEP2513-T84000.00"
TK2 = "KXBTCD-26SEP2513-T84250.00"


async def _nosleep(dt: float) -> None:
    return None


def venue(rest: Any = None, **cfg: Any) -> tuple[KalshiVenue, Any, list, FakeClock]:
    rest = rest if rest is not None else FakeRest()
    clock = FakeClock()
    out: list = []
    c = {"subaccount": 1, "shared_account": True, "key_restricted_to_subaccount": True, **cfg}
    v = KalshiVenue(rest, sink=out.append, cfg=VenueCfg(**c), clock_ns=clock, monotonic=clock.mono, sleep=_nosleep)
    v.register_markets([kxbtcd_spec(ticker=TK), kxbtcd_spec(ticker=TK2)])
    return v, rest, out, clock


def po(coid: str, ticker: str = TK, group: str = "dh-main") -> PlaceOrder:
    return PlaceOrder(client_order_id=coid, ticker=ticker, book_side="bid", px=4500, qty=200, order_group_id=group)


# ============================================================================ 1. exchange_index end to end
def test_market_spec_carries_the_exchange_shard():
    series = dict(S.SERIES_KXBTCD, exchange_index=2)
    event = dict(S.EVENT_KXBTCD)
    assert rest_market_to_spec(S.market(), series, event).exchange_index == 2
    # market > event > series; unknown when none of them says
    assert exchange_index_of({"exchange_index": 3}, {"exchange_index": 2}, {"exchange_index": 1}) == 3
    assert exchange_index_of({}, {"exchange_index": 2}, {"exchange_index": 1}) == 2
    assert exchange_index_of({}, None, {"exchange_index": "1"}) == 1
    assert exchange_index_of({"exchange_index": None}, {}, {}) is None
    assert exchange_index_of({"exchange_index": True}) is None and exchange_index_of({"exchange_index": -1}) is None
    m = {k: v for k, v in S.market().items() if k != "exchange_index"}
    ev = {k: v for k, v in event.items() if k != "exchange_index"}
    se = {k: v for k, v in series.items() if k != "exchange_index"}
    assert rest_market_to_spec(m, se, ev).exchange_index is None


def test_markets_on_an_unknown_or_unfunded_shard_are_never_selected():
    from dh.kalshi.fees import FeeEngine
    from dh.kalshi.metadata import MarketRegistry

    from .fakes import T0
    from .test_startup import _market

    h1 = (T0 // (3600 * 10**9) + 1) * 3600 * 10**9
    reg = MarketRegistry(FeeEngine.from_config())
    reg.add_series(dict(S.SERIES_KXBTCD, fee_type="quadratic_with_maker_fees", exchange_index=None))
    reg.add_event(dict(S.EVENT_KXBTCD, event_ticker="KXBTCD-26SEP2513", exchange_index=None))
    t1, t2, t3 = (f"KXBTCD-26SEP2513-T8{i}000.00" for i in (1, 2, 3))
    reg.add_market(_market(t1, 81000.0, h1, exchange_index=2))
    reg.add_market(_market(t2, 82000.0, h1, exchange_index=0))
    reg.add_market(_market(t3, 83000.0, h1, exchange_index=None))
    sel = select_specs(reg, T0, 7200, series=("KXBTCD",), exchange_indexes=(2,))
    assert [s.ticker for s in sel.specs] == [t1] and sel.specs[0].exchange_index == 2
    assert "not in venue.exchange_indexes" in sel.skipped[t2]
    assert "exchange shard unknown" in sel.skipped[t3]
    # even without a configured list an unknown shard is never traded
    anyshard = select_specs(reg, T0, 7200, series=("KXBTCD",))
    assert sorted(s.ticker for s in anyshard.specs) == [t1, t2]
    # a known market that moves shard is a spec change (blocked for the session)
    known = {s.ticker: s for s in anyshard.specs}
    reg.add_market(_market(t1, 81000.0, h1, exchange_index=3))
    assert select_specs(reg, T0, 7200, series=("KXBTCD",), known=known).changed == {t1: "spec changed"}


async def test_orders_carry_the_shard_and_unknown_shards_are_refused_locally():
    v, rest, out, clock = venue()
    v.shard_of["KXBTCD-OTHER-T1"] = 0  # a market on a shard this runner has not funded
    assert await v.ensure_order_group("dh-main", 2000) == "og-1"
    assert rest.of("create_order_group")[0][1] == {"subaccount": 1, "exchange_index": 2}
    v.submit([po("c-1"), po("c-x", "KXBTCD-NOSHARD-T1"), po("c-y", "KXBTCD-OTHER-T1")], clock())
    await v.wait_idle(1.0)
    (args, _), = rest.of("create_order")
    assert args[0]["exchange_index"] == 2 and args[0]["subaccount"] == 1 and args[0]["order_group_id"] == "og-1"
    rej = {e.client_order_id: e.reason for e in out if isinstance(e, OrderReject)}
    assert "exchange shard of KXBTCD-NOSHARD-T1 unknown" in rej["c-x"]
    assert "not in venue.exchange_indexes" in rej["c-y"]
    # amend: same rule as a create (shard in the body, unknown shard refused); decrease: its shard
    rest.orders["o-1"] = order_row("c-1", "o-1", TK)
    v.submit([AmendOrder("c-1", "c-1b", TK, "o-1", "bid", 4600, 200),
              AmendOrder("c-x", "c-xb", "KXBTCD-NOSHARD-T1", "o-x", "bid", 4600, 200),
              DecreaseOrder("c-1", TK, "o-1", 100)], clock())
    await v.wait_idle(1.0)
    (a_args, a_kw), = rest.of("amend_order")
    assert a_args[1]["exchange_index"] == 2 and a_kw == {"subaccount": 1}
    assert any(isinstance(e, OrderReject) and e.request == "amend" and "unknown" in e.reason for e in out)
    assert rest.of("decrease_order")[0][1]["exchange_index"] == 2


async def test_cancels_use_the_order_row_shard_else_auto_route_explicitly():
    rest = FakeRest()
    rest.orders["o-9"] = order_row("g", "o-9", "KXBTCD-OLD-T1", exchange_index=3)
    v, _, out, clock = venue(rest)
    await v.sweep_resting(None, "test")  # learns o-9's shard from the resting list
    await v.wait_idle(1.0)
    assert rest.of("cancel_order")[0][1] == {"market_ticker": "KXBTCD-OLD-T1", "subaccount": 1, "exchange_index": 3}
    # no row, no spec: the documented explicit auto-route (-1 + market_ticker); a cancel is never withheld
    v.submit([CancelOrder("h", "KXBTCD-GONE-T1", "o-10")], clock())
    await v.wait_idle(1.0)
    assert rest.of("cancel_order")[1][1] == {"market_ticker": "KXBTCD-GONE-T1", "subaccount": 1, "exchange_index": -1}
    assert v.stats.requests["cancel_autoroute"] == 1


async def test_one_order_group_per_shard_and_every_group_write_is_scoped():
    v, rest, out, clock = venue()
    v.register_markets([kxbtcd_spec(ticker="KXBTCD-SHARD3-T1", exchange_index=3)])
    v.cfg = replace(v.cfg, exchange_indexes=(2, 3))
    assert v.shards_in_use() == [2, 3]
    assert await v.ensure_order_group("dh-main", 2000) is not None
    assert [k["exchange_index"] for _, k in rest.of("create_order_group")] == [2, 3]
    assert v.groups == {"dh-main": {2: "og-1", 3: "og-2"}} and v.logical_group_of("og-2") == "dh-main"
    assert v.group_refs() == [{"logical": "dh-main", "id": "og-1", "exchange_index": 2, "subaccount": 1},
                              {"logical": "dh-main", "id": "og-2", "exchange_index": 3, "subaccount": 1}]
    v.submit([po("c-3", "KXBTCD-SHARD3-T1")], clock())
    await v.wait_idle(1.0)
    assert rest.of("create_order")[0][0][0]["order_group_id"] == "og-2"  # the group of ITS shard
    v.submit([ResetOrderGroup("dh-main"), UpdateOrderGroupLimit("dh-main", 3000)], clock())
    await v.wait_idle(1.0)
    for name in ("reset_order_group", "update_order_group_limit"):
        assert sorted((a[0], k["exchange_index"], k["subaccount"]) for a, k in rest.of(name)) == [("og-1", 2, 1), ("og-2", 3, 1)]
    assert await v._retry_group("delete", "dh-main")  # noqa: SLF001
    assert sorted(k["exchange_index"] for _, k in rest.of("delete_order_group")) == [2, 3] and v.groups == {}


async def test_kill_switch_triggers_every_group_first_and_never_resets_or_recreates():
    v, rest, out, clock = venue()
    await v.ensure_order_group("dh-main", 2000)
    v.latch_kill("kill: test")
    await v.wait_idle(1.0)
    assert rest.of("trigger_order_group") == [(("og-1",), {"subaccount": 1, "exchange_index": 2})]
    v.submit([ResetOrderGroup("dh-main")], clock())  # the strategy's cooldown reset after the trigger
    await v.wait_idle(1.0)
    assert rest.of("reset_order_group") == []
    v.latch_kill("again")  # idempotent
    await v.wait_idle(1.0)
    assert len(rest.of("trigger_order_group")) == 1
    v.register_markets([kxbtcd_spec(ticker="KXBTCD-SHARD3-T1", exchange_index=3)])
    assert await v.ensure_order_group("dh-main", 2000, exchange_index=3) is None  # no new group after a kill


def test_venue_refuses_subaccount_zero_on_a_shared_account():
    with pytest.raises(ValueError, match="venue.subaccount is not set"):
        KalshiVenue(FakeRest(), sink=lambda e: None, cfg=VenueCfg(subaccount=None, shared_account=True))
    with pytest.raises(ValueError, match="subaccount is 0"):
        KalshiVenue(FakeRest(), sink=lambda e: None, cfg=VenueCfg(subaccount=0, shared_account=True))


# ============================================================================ 2. explicit scope on EVERY write
class CaptureTransport:
    """HTTP transport of a real KalshiRest: records every request, answers like Kalshi would."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, str, list[tuple[str, str]], Any]] = []
        self.n = 0

    async def __call__(self, method, url, headers, params, data, timeout_s):
        body = orjson.loads(data) if data else None
        path = url.split("/trade-api/v2", 1)[1]
        self.calls.append((method, path, list(params), body))
        self.n += 1
        if method == "POST" and path == "/portfolio/events/orders":
            out = {"order_id": f"o-{self.n}", "client_order_id": body["client_order_id"], "fill_count": "0.00",
                   "remaining_count": body["count"], "ts_ms": 1}
        elif method == "POST" and path == "/portfolio/events/orders/batched":
            out = {"orders": [{"order_id": f"o-{self.n}-{i}", "client_order_id": b["client_order_id"], "fill_count": "0.00",
                               "remaining_count": b["count"], "ts_ms": 1} for i, b in enumerate(body["orders"])]}
        elif method == "DELETE" and path == "/portfolio/events/orders/batched":
            out = {"orders": [{"order_id": it["order_id"], "reduced_by": "2.00", "ts_ms": 1} for it in body["orders"]]}
        elif method == "POST" and path == "/portfolio/order_groups/create":
            out = {"order_group_id": f"og-{self.n}", "subaccount": body.get("subaccount"), "exchange_index": body.get("exchange_index")}
        elif method == "GET" and path == "/portfolio/order_groups":
            out = {"order_groups": []}
        elif method == "GET":
            out = {"orders": [], "cursor": ""}
        else:
            out = {"order_id": path.rsplit("/", 1)[-1], "reduced_by": "2.00", "ts_ms": 1}
        return HttpResponse(200, {}, orjson.dumps(out))


def _scopes(method: str, path: str, params: list, body: Any) -> list[dict[str, Any]]:
    """Every (subaccount, exchange_index) scope a request names: per item of a batch, else
    query parameters merged with the body's fields."""
    if isinstance(body, dict) and isinstance(body.get("orders"), list):
        return [dict(it) for it in body["orders"]]
    d = dict(params)
    if isinstance(body, dict):
        d.update({k: body[k] for k in ("subaccount", "exchange_index") if k in body})
    return [d]


def _endpoint(method: str, path: str) -> str:
    parts = ["{id}" if p.startswith(("o-", "og-", "o9")) or p.startswith("ox") else p for p in path.split("/")]
    return f"{method} {'/'.join(parts)}"


@pytest.mark.parametrize("guarded", [False, True])
async def test_every_write_path_names_the_subaccount_and_a_shard(guarded):
    """Drive every write the venue and the watchdog can send through the REAL KalshiRest; each
    request must carry subaccount=1 and an exchange_index (cancel-all: subaccount only, it has no
    shard parameter). ``guarded``: the client's own write_subaccount guard is on as well (live)."""
    t = CaptureTransport()
    rest = KalshiRest("https://x/trade-api/v2", transport=t, sleep=_nosleep, write_subaccount=1 if guarded else None)
    v, _, out, clock = venue(rest)
    assert await v.ensure_order_group("dh-main", 2000)
    v.submit([po("c-1")], clock())  # single create
    v.submit([po("c-2"), po("c-3", TK2)], clock())  # batched create
    await v.wait_idle(1.0)
    oid = next(e.order_id for e in out if getattr(e, "client_order_id", "") == "c-1" and getattr(e, "order_id", ""))
    v.submit([CancelOrder("c-1", TK, oid)], clock())  # single cancel
    v.submit([CancelOrder("c-2", TK, "o9a"), CancelOrder("c-3", TK2, "o9b")], clock())  # batched cancel
    v.submit([CancelOrder("g", "KXBTCD-GONE-T1", "o9c")], clock())  # unknown shard: explicit auto-route
    v.submit([AmendOrder("c-2", "c-2b", TK, "o9a", "bid", 4600, 200), DecreaseOrder("c-3", TK2, "o9b", 100)], clock())
    v.submit([ResetOrderGroup("dh-main"), UpdateOrderGroupLimit("dh-main", 3000)], clock())
    await v.wait_idle(1.0)
    assert await v.cancel_all_now("test")
    v.latch_kill("test")  # group trigger
    await v.wait_idle(1.0)
    assert await v._retry_group("delete", "dh-main")  # noqa: SLF001
    assert await rest_trigger_groups(rest, 1)([{"id": "og-w", "exchange_index": 2, "subaccount": 1}]) == 1
    assert await rest_cancel_all(rest, 1)()
    writes = [c for c in t.calls if c[0] != "GET"]
    seen = {_endpoint(m, p) for m, p, _, _ in writes}
    assert seen >= {
        "POST /portfolio/events/orders", "POST /portfolio/events/orders/batched", "DELETE /portfolio/events/orders/{id}",
        "DELETE /portfolio/events/orders/batched", "POST /portfolio/events/orders/{id}/amend",
        "POST /portfolio/events/orders/{id}/decrease", "DELETE /portfolio/events/orders",
        "POST /portfolio/order_groups/create", "PUT /portfolio/order_groups/{id}/reset",
        "PUT /portfolio/order_groups/{id}/limit", "PUT /portfolio/order_groups/{id}/trigger",
        "DELETE /portfolio/order_groups/{id}"}, seen
    for m, p, params, body in writes:
        for sc in _scopes(m, p, params, body):
            assert str(sc.get("subaccount")) == "1", (m, p, sc)
            if not (m == "DELETE" and p == "/portfolio/events/orders"):
                assert sc.get("exchange_index") is not None and int(sc["exchange_index"]) in (2, -1), (m, p, sc)
    assert not [e for e in out if isinstance(e, OrderReject) and e.reason == "not_sent"]


async def test_rest_guard_refuses_unscoped_writes_before_sending():
    t = CaptureTransport()
    rest = KalshiRest("https://x/trade-api/v2", transport=t, sleep=_nosleep, write_subaccount=1)
    with pytest.raises(UnscopedWriteError, match="no explicit subaccount"):
        await rest.cancel_order("o-1", market_ticker=TK, exchange_index=2)
    with pytest.raises(UnscopedWriteError, match="subaccount 0 is not this client's subaccount 1"):
        await rest.cancel_all_orders(subaccount=0)  # the other system's subaccount: never
    with pytest.raises(UnscopedWriteError, match="no explicit exchange_index"):
        await rest.trigger_order_group("og-1", subaccount=1)
    with pytest.raises(UnscopedWriteError, match=r"orders\[1\]: no explicit subaccount"):
        await rest.batch_cancel_orders([{"order_id": "a", "subaccount": 1, "exchange_index": 2}, {"order_id": "b"}])
    with pytest.raises(UnscopedWriteError, match="no explicit exchange_index"):
        await rest.create_order({"ticker": TK, "client_order_id": "c", "subaccount": 1})
    assert t.calls == []  # nothing was signed or sent
    assert await rest.cancel_all_orders(subaccount=1) is not None and t.calls[-1][2] == [("subaccount", "1")]
    with pytest.raises(ValueError, match="explicit subaccount"):
        await KalshiRest("https://x/trade-api/v2", transport=t).cancel_all_orders(subaccount=None)  # type: ignore[arg-type]


WRITE_CALLS = {"cancel_all_orders": False, "cancel_order": True, "amend_order": False, "decrease_order": True,
               "create_order_group": True, "reset_order_group": True, "trigger_order_group": True,
               "update_order_group_limit": True, "delete_order_group": True}  # name -> exchange_index keyword required
BODY_WRITES = {"create_order", "batch_create_orders", "batch_cancel_orders"}  # scope inside the body (dynamic test)


def test_no_write_call_in_dh_live_or_scripts_omits_the_subaccount_or_shard():
    """Static guard: a new REST write call in dh/live/*.py or scripts/*.py must pass subaccount
    (and exchange_index, except cancel-all whose endpoint has none; amend carries it in its body)
    as explicit keywords; raw ``.write("POST"|"PUT"|"DELETE", ...)`` calls are forbidden."""
    files = sorted((REPO / "dh" / "live").glob("*.py")) + sorted((REPO / "scripts").glob("*.py"))
    found: list[str] = []
    for f in files:
        tree = ast.parse(f.read_text(), str(f))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
                continue
            name = node.func.attr
            where = f"{f.relative_to(REPO)}:{node.lineno} {name}"
            if name == "write" and node.args and isinstance(node.args[0], ast.Constant) \
                    and node.args[0].value in ("POST", "PUT", "DELETE"):
                pytest.fail(f"raw REST write at {where}: use the typed KalshiRest methods")
            if name not in WRITE_CALLS:
                continue
            kws = {k.arg for k in node.keywords if k.arg}
            assert "subaccount" in kws, f"{where}: no explicit subaccount"
            if WRITE_CALLS[name]:
                assert "exchange_index" in kws, f"{where}: no explicit exchange_index"
            found.append(where)
    names = {w.rsplit(" ", 1)[1] for w in found}
    assert names >= set(WRITE_CALLS), names  # every kind of write call is present (and was checked) here


def test_every_rest_client_in_dh_live_and_scripts_is_read_only_or_bound_to_a_subaccount():
    """A KalshiRest built by the runner, the watchdog, the tools or any script either cannot
    write at all (read_only) or refuses writes not naming its subaccount (write_subaccount)."""
    files = sorted((REPO / "dh" / "live").glob("*.py")) + sorted((REPO / "scripts").glob("*.py"))
    n = 0
    for f in files:
        for node in ast.walk(ast.parse(f.read_text(), str(f))):
            if isinstance(node, ast.Call) and getattr(node.func, "id", getattr(node.func, "attr", "")) == "KalshiRest":
                kws = {k.arg for k in node.keywords if k.arg}
                assert kws & {"read_only", "write_subaccount"}, f"{f.relative_to(REPO)}:{node.lineno} unbound KalshiRest"
                n += 1
    assert n >= 6  # app, watchdog, tools, recorder, smoke, fee check, history download


# ============================================================================ 3. config refusals / paths
def test_shared_account_live_refusals():
    base = LiveConfig(mode="live", venue=VenueCfg(subaccount=1, shared_account=True, key_restricted_to_subaccount=True))
    assert live_config_problems(base) == []
    for sub in (None, 0):
        bad = replace(base, venue=replace(base.venue, subaccount=sub))
        assert any("dedicated subaccount" in p or "not set" in p for p in live_config_problems(bad))
    assert any("exchange_indexes" in p for p in live_config_problems(replace(base, venue=replace(base.venue, exchange_indexes=()))))
    assert any("exchange_status_interval_s" in p
               for p in live_config_problems(replace(base, venue=replace(base.venue, exchange_status_interval_s=0.0))))
    assert account_share_problem(base, 0.5) == "" and "0.5" in account_share_problem(base, 0.6)
    assert account_share_problem(replace(base, venue=replace(base.venue, shared_account=False)), 1.0) == ""


async def test_live_start_refuses_a_shared_account_with_the_whole_rate_budget(tmp_path):
    from .test_app import _setup

    rest, fake, lcfg, scfg, _ = _setup(tmp_path, "live", forbid_writes=True)
    kcfg = tmp_path / "kalshi.yaml"
    kcfg.write_text((REPO / "config" / "kalshi.example.yaml").read_text())  # account_share: 1.0
    lcfg = replace(lcfg, kalshi_config=str(kcfg), venue=replace(lcfg.venue, subaccount=1, shared_account=True,
                                                                  key_restricted_to_subaccount=True))
    app = LiveApp(scfg, lcfg, "live", Overrides(rest=rest, ws_connect=fake.connect, install_signals=False))
    assert await app.run(duration_s=0.5) == 2 and rest.calls == [] and fake.conns == []


def test_runtime_dir_defaults_per_platform(monkeypatch):
    assert default_run_dir("darwin") == "data/run" and default_run_dir("linux") == "/run/dh"
    import dh.live.config as lc

    monkeypatch.setattr(lc.sys, "platform", "darwin")
    p = PathsCfg()
    assert p.kill_file == "data/run/KILL" and p.heartbeat_file == "data/run/heartbeat.json"
    assert p.heartbeat_for("paper") == "data/run/heartbeat.paper.json"
    monkeypatch.setattr(lc.sys, "platform", "linux")
    assert PathsCfg().kill_file == "/run/dh/KILL" and PathsCfg(kill_file="/x/K").kill_file == "/x/K"


async def test_repo_relative_runtime_dir_is_created_on_demand(tmp_path, monkeypatch):
    from dh.live import app as appmod
    from dh.live.app import check_runtime_paths

    monkeypatch.setattr(appmod, "REPO_ROOT", tmp_path)
    lcfg = LiveConfig(paths=PathsCfg(data_root=str(tmp_path / "d"), kill_file="data/run/KILL",
                                     heartbeat_file="data/run/heartbeat.json"))
    locks = check_runtime_paths(lcfg, "paper")
    try:
        assert (tmp_path / "data" / "run").is_dir()
    finally:
        for lk in locks:
            lk.release()
    with pytest.raises(appmod.StartupError, match="does not exist"):  # absolute (/run/dh-like): never created
        check_runtime_paths(LiveConfig(paths=PathsCfg(data_root=str(tmp_path / "e"), kill_file=str(tmp_path / "nope" / "KILL"),
                                                      heartbeat_file=str(tmp_path / "hb.json"))), "paper")


# ============================================================================ 4. watchdog: trigger first
async def test_watchdog_triggers_the_runners_groups_first_then_cancels_all(tmp_path):
    clock = FakeClock()
    rest = FakeRest()
    rest.forbid_order_writes = False
    hbp = tmp_path / "hb.json"
    groups = [{"logical": "dh-main", "id": "og-7", "exchange_index": 2, "subaccount": 1},
              {"logical": "dh-main", "id": "og-0", "exchange_index": 2, "subaccount": 0}]  # never another subaccount's
    w = Watchdog(hbp, rest_cancel_all(rest, 1), clock_ns=clock, trigger_groups=rest_trigger_groups(rest, 1))
    write_heartbeat(hbp, {"mode": "live", "state": "running", "pid": 5, "session": "live-x", "subaccount": 1,
                          "order_groups": groups}, now_ns=clock())
    assert await w.step() == "ARMED" and len(w.st.groups) == 2
    clock.advance(3 * 10**9)
    assert await w.step() == "TRIGGERED"
    assert rest.names() == ["trigger_order_group", "cancel_all_orders"]
    assert rest.of("trigger_order_group")[0] == (("og-7",), {"subaccount": 1, "exchange_index": 2})
    assert rest.of("cancel_all_orders")[0][1] == {"subaccount": 1}
    assert json.loads((tmp_path / "hb.json.cancel_all").read_text())["groups_triggered"] == 1


@pytest.mark.parametrize("sub", [None, 0])
async def test_watchdog_script_refuses_subaccount_zero_on_a_shared_account(tmp_path, sub):
    import importlib
    import sys

    sys.path.insert(0, str(REPO / "scripts"))
    mod = importlib.import_module("watchdog")
    live = tmp_path / "live.yaml"
    live.write_text(f"venue:\n  subaccount: {'null' if sub is None else sub}\n  shared_account: true\n")
    rest = FakeRest()
    args = argparse.Namespace(live_config=str(live), heartbeat=str(tmp_path / "hb.json"), once=False, cancel_now=True,
                              arm_on_start=False, max_age_s=0.0)
    assert await mod.amain(args, rest=rest) == 2 and rest.calls == []


async def test_watchdog_cancel_now_triggers_the_heartbeat_groups(tmp_path):
    import importlib
    import sys

    sys.path.insert(0, str(REPO / "scripts"))
    mod = importlib.import_module("watchdog")
    write_heartbeat(tmp_path / "hb.json", {"mode": "live", "state": "stopping", "pid": 1, "session": "s",
                                           "order_groups": [{"id": "og-3", "exchange_index": 2, "subaccount": 1}]}, now_ns=1)
    rest = FakeRest()
    args = argparse.Namespace(live_config=str(REPO / "config" / "live.example.yaml"), heartbeat=str(tmp_path / "hb.json"),
                              once=False, cancel_now=True, arm_on_start=False, max_age_s=0.0)
    assert await mod.amain(args, rest=rest) == 0
    # shared account: trigger, then subaccount 1's resting orders listed (and cancelled by id)
    assert rest.names() == ["trigger_order_group", "iter_orders"]
    assert rest.of("trigger_order_group")[0][1] == {"subaccount": 1, "exchange_index": 2}
    assert rest.of("iter_orders")[0][1] == {"status": "resting", "subaccount": 1}


# ============================================================================ 5. runner kill path + heartbeat
async def test_runner_kill_triggers_the_group_before_the_cancel_all_and_the_heartbeat_names_it(tmp_path):
    from dh.live.monitor import KillFile

    from .test_runner import OrderingStrategy, cfg, live_runner

    s = OrderingStrategy(n=0)
    kill = tmp_path / "KILL"
    r, v, rest = live_runner(s, kill_file=KillFile(kill), heartbeat_path=tmp_path / "hb.json",
                             venue_cfg=VenueCfg(subaccount=1, shared_account=True, positions_interval_s=0.0,
                                                queue_positions_interval_s=0.0, fills_backfill_interval_s=0.0,
                                                exchange_status_interval_s=0.0, balance_interval_s=0.0))
    assert await v.ensure_order_group("dh-main", 2000)

    async def killer():
        await asyncio.sleep(0.2)
        assert read_heartbeat(tmp_path / "hb.json")["order_groups"] == [
            {"logical": "dh-main", "id": "og-1", "exchange_index": 2, "subaccount": 1}]
        kill.write_text("drill")
        await asyncio.sleep(5)

    r.add_source("killer", killer)
    assert await r.run(duration_s=3.0) == 0
    names = rest.names()
    # shared account: the group trigger, then subaccount 1's resting orders listed and cancelled by
    # id; the bulk cancel-all is never sent
    assert "cancel_all_orders" not in names
    assert names.index("trigger_order_group") < max(i for i, n in enumerate(names) if n == "iter_orders")
    assert rest.of("trigger_order_group")[0] == (("og-1",), {"subaccount": 1, "exchange_index": 2})
    assert all(k.get("subaccount") == 1 for n, _, k in rest.calls if n in ("iter_orders", "trigger_order_group"))
    assert v.kill_latched.startswith("kill:")


async def test_a_manual_halt_latches_the_kill_switch_a_timed_one_does_not():
    from dh.core.actions import CancelAll, Halt
    from dh.core.events import IndexTick

    from .test_runner import cfg, live_runner
    from .fakes import RecordingStrategy

    def respond_with(until: int):
        def respond(ev):
            if isinstance(ev, IndexTick):
                return [CancelAll(reason="x"), Halt(reason="x", scope="all", until_ts=until)]
            return []
        return respond

    for until, latched in ((0, True), (2**62, False)):
        r, v, rest = live_runner(RecordingStrategy(respond_with(until)))
        await v.ensure_order_group("dh-main", 2000)
        t = r.clock_ns()
        r.push(IndexTick(t, t, "BRTI", 84000.0, "5hz"))
        r.process_pending()
        await v.wait_idle(1.0)
        assert bool(v.kill_latched) is latched
        assert ("trigger_order_group" in rest.names()) is latched and "cancel_all_orders" in rest.names()


# ============================================================================ 6. start-up: collateral, key, probe
def test_required_balance_and_balance_parsing():
    from dh.strategy.config import load_config

    scfg = load_config(REPO / "config" / "m1.yaml")
    assert required_balance_usd(scfg.risk, 10.0) == pytest.approx(60.0)  # $50 worst case + $10
    assert balance_dollars({"balance": 12345, "balance_dollars": "123.4500"}) == pytest.approx(123.45)
    assert balance_dollars({"balance": 12345}) == pytest.approx(123.45)
    assert balance_dollars({"balance_dollars": "x"}) is None and balance_dollars(None) is None


def _live(tmp_path, **venue_kw):
    from .test_app import _setup

    rest, fake, lcfg, scfg, tickers = _setup(tmp_path, "live", forbid_writes=False)
    kcfg = tmp_path / "kalshi.yaml"  # a shared account: this process keeps to 20% of the REST budget
    kcfg.write_text((REPO / "config" / "kalshi.example.yaml").read_text().replace("account_share: 1.0", "account_share: 0.2"))
    venue_kw = {"key_restricted_to_subaccount": True, **venue_kw}
    lcfg = replace(lcfg, kalshi_config=str(kcfg), venue=replace(lcfg.venue, subaccount=1, shared_account=True, **venue_kw))
    rest.key_subaccount = 1  # System 1's own key, restricted to subaccount 1: a subaccount-0 read is refused
    return rest, fake, lcfg, scfg, tickers


async def test_startup_reads_the_shard_balance_first_and_exposes_it(tmp_path):
    rest, fake, lcfg, scfg, _ = _live(tmp_path)
    rest.balances = {2: "75.5000"}
    app = LiveApp(scfg, lcfg, "live", Overrides(rest=rest, ws_connect=fake.connect, install_signals=False))
    runner = await app.build()
    names = rest.names()
    assert rest.of("get_balance")[0][1] == {"subaccount": 1, "exchange_index": 2}
    assert "cancel_all_orders" not in names  # shared account: never the bulk cancel-all
    # read-only checks (balance, the restricted-key probe) before the clean slate (listed + cancelled by id)
    assert names.index("get_balance") < names.index("iter_orders")
    assert {"subaccount": 0} in [k for _, k in rest.of("get_balance")]  # the probe: refused (403) for this key
    # the shard's funds: balance + positions at cost + resting collateral, all scoped to shard 2
    assert rest.of("get_all_positions")[0][1] == {"count_filter": "position", "subaccount": 1, "exchange_index": 2}
    assert rest.of("iter_orders")[0][1] == {"status": "resting", "subaccount": 1, "exchange_index": 2}
    assert rest.of("create_order_group")[0][1] == {"subaccount": 1, "exchange_index": 2}
    assert app.info["balances"] == {"shards": {"2": {"available": 75.5, "positions": 0.0, "resting": 0.0, "funds": 75.5}},
                                    "required_usd": 60.0}
    runner.process_pending()
    assert runner.metrics.get("dh_balance_dollars", exchange_index="2") == 75.5 and "balance" not in runner.gate.reasons
    assert runner.metrics.get("dh_shard_funds_dollars", exchange_index="2") == 75.5
    assert all(k.get("subaccount") == 1 for n, _, k in rest.calls if n in ("cancel_all_orders", "iter_orders", "iter_fills",
                                                                         "get_all_positions", "iter_settlements"))
    assert app.info["key_restriction"]["ok"] and "HTTP 403" in app.info["key_restriction"]["evidence"]
    await runner.shutdown()
    await app.close()


@pytest.mark.parametrize("case", ["low", "unreadable", "read_fails"])
async def test_startup_refuses_an_unfunded_shard(tmp_path, case):
    rest, fake, lcfg, scfg, _ = _live(tmp_path)
    if case == "low":
        rest.balances = {2: "59.9900"}
    elif case == "unreadable":
        rest.on("get_balance", {"balance_dollars": "?"})
    else:
        from .fakes import http_error

        rest.on("get_balance", http_error(400, "invalid_subaccount", "no such subaccount", "GET"))
    app = LiveApp(scfg, lcfg, "live", Overrides(rest=rest, ws_connect=fake.connect, install_signals=False))
    assert await app.run(duration_s=0.5) == 2
    assert "cancel_all_orders" not in rest.names() and fake.conns == []  # nothing written, nothing subscribed


async def test_runner_blocks_new_orders_when_the_balance_drops_and_reopens():
    from .fakes import RecordingStrategy
    from .test_runner import live_runner

    r, v, rest = live_runner(RecordingStrategy())
    r.balance_required_usd = 60.0
    r.push_side("balance", {"balances": {2: 59.0}})
    r.process_pending()
    assert "balance" in r.gate.reasons and r.metrics.get("dh_balance_dollars", exchange_index="2") == 59.0
    r.push_side("balance", {"balances": {2: None}})  # unreadable: stays blocked
    r.process_pending()
    assert "balance" in r.gate.reasons
    r.push_side("balance", {"balances": {2: 61.0}})
    r.process_pending()
    assert "balance" not in r.gate.reasons
    rest.balances = {2: "12.0000"}  # the periodic loop's read: a defunded shard
    funds = await v.fetch_shard_funds()
    r.push_side("balance", {"balances": {sh: f["funds"] for sh, f in funds.items()},
                            "available": {sh: f["available"] for sh, f in funds.items()}})
    r.process_pending()
    assert "balance" in r.gate.reasons


async def test_our_own_positions_and_quotes_never_make_a_funded_shard_look_empty():
    """Kalshi's ``balance`` is the AVAILABLE cash: our positions (paid for) and resting quotes
    (reserved collateral) lower it. The shard's funds add them back at cost, so quoting on a
    $60-funded shard does not flap the balance gate; a real transfer out still does."""
    rest = FakeRest()
    rest.balances = {2: "20.0000"}
    rest.positions = {TK: "10.00"}
    rest.exposure = {TK: "4.5000"}  # 10 YES bought at 45c
    rest.orders["o-1"] = order_row("c-1", "o-1", TK, side="bid", px="0.4500", remaining="50.00")  # $22.50 reserved
    rest.orders["o-2"] = order_row("c-2", "o-2", "KXBTCD-OLD-T2", side="ask", px="0.6000", remaining="40.00",
                                   exchange_index=2)  # 40 x 40c = $16, a market not in our universe
    v, _, _, _ = venue(rest)
    f = (await v.fetch_shard_funds())[2]
    assert (f["available"], f["positions"], f["resting"]) == (20.0, 4.5, 38.5) and f["funds"] == pytest.approx(63.0)
    assert v.cancel_shard("KXBTCD-OLD-T2", "o-2") == 2  # the resting rows' shards were learned on the way


@pytest.mark.parametrize("case, ok", [("api_keys_restricted", True), ("api_keys_unrestricted", False),
                                      ("api_keys_other_sub", False), ("api_keys_refused", True),
                                      ("probe_answers", False), ("probe_5xx", False)])
async def test_key_restriction_is_verified(case, ok):
    """Positive proof (review M1): GET /portfolio/balance?subaccount=0 must be REFUSED (401/403)
    with the runner key; GET /api_keys is secondary (a listed unrestricted key still refuses)."""
    from .fakes import http_error

    rest = FakeRest()
    rest.key_subaccount = 1  # a key restricted to subaccount 1: the subaccount-0 probe answers 403
    bodies = [{"balance": 100, "balance_dollars": "1.00"}]
    if case == "api_keys_restricted":
        rest.api_keys = [{"api_key_id": "kid", "name": "runner", "scopes": [], "subaccount": 1}]
    elif case == "api_keys_unrestricted":
        rest.api_keys = [{"api_key_id": "kid", "name": "runner", "scopes": [], "subaccount": None}]
    elif case == "api_keys_other_sub":
        rest.api_keys = [{"api_key_id": "kid", "name": "runner", "scopes": [], "subaccount": 0}]
    elif case == "api_keys_refused":
        rest.on("get_api_keys", http_error(403, "forbidden", "restricted to a single sub-account", "GET"))
    elif case == "probe_answers":  # an unrestricted key: the subaccount-0 read answers
        rest.key_subaccount = None
        rest.on("get_api_keys", http_error(503, "unavailable", "down", "GET"))
    else:
        rest.on("get_balance", http_error(503, "unavailable", "down", "GET"))
    got, why = await verify_key_restriction(rest, "kid", 1, bodies)
    assert got is ok and why


async def test_startup_refuses_a_key_that_is_not_restricted(tmp_path):
    rest, fake, lcfg, scfg, _ = _live(tmp_path, key_restricted_to_subaccount=True)
    rest.api_keys = [{"api_key_id": "kid", "name": "system 2", "scopes": [], "subaccount": None}]

    class Signer:
        key_id = "kid"

    app = LiveApp(scfg, lcfg, "live", Overrides(rest=rest, ws_connect=fake.connect, install_signals=False, signer=Signer()))
    assert await app.run(duration_s=0.5) == 2
    assert "cancel_all_orders" not in rest.names() and "NOT restricted" in json.dumps(app.info.get("key_restriction"))


def test_ws_fill_probe_settles_the_subaccount_field_once():
    written: list = []
    seen: list = []

    class R:
        def verify_live(self, check, ok, **info):
            seen.append((check, ok, info))

    p = WsFillProbe(lambda s, t, d: written.append(d), R(), 1, True)
    p("kalshi.ws", 1, b'{"type":"orderbook_delta","sid":1,"seq":1,"msg":{}}')
    p("kalshi.ws", 2, orjson.dumps({"type": "fill", "sid": 9, "msg": {"trade_id": "t", "exchange_index": 2}}))
    p("kalshi.ws", 3, orjson.dumps({"type": "fill", "sid": 9, "msg": {"trade_id": "u", "subaccount": 1}}))
    assert len(written) == 3 and len(seen) == 1
    assert seen[0][0] == "ws_fill_subaccount_field" and seen[0][1] is True and seen[0][2]["present"] is False
    # a full-account key: only a fill of OUR order (client_order_id prefix) settles it
    seen.clear()
    q = WsFillProbe(lambda s, t, d: None, R(), 1, False, "dhm1-abc")
    q("kalshi.ws", 1, orjson.dumps({"type": "fill", "msg": {"client_order_id": "other-1"}}))
    assert seen == []
    q("kalshi.ws", 2, orjson.dumps({"type": "fill", "msg": {"client_order_id": "dhm1-abc-1"}}))
    assert seen[0][1] is False and seen[0][2]["present"] is False  # our fill, no field, unrestricted key


def test_runner_verify_live_logs_once_and_keeps_the_metric():
    from .fakes import RecordingStrategy
    from .test_runner import live_runner

    r, _, _ = live_runner(RecordingStrategy())
    logged: list = []
    r.jlog = lambda kind, ts, **p: logged.append((kind, p))  # type: ignore[method-assign]
    r.verify_live("x", True, n=1)
    r.verify_live("x", False, n=2)
    assert [k for k, _ in logged] == ["verify_live"] and r.metrics.get("dh_verify_live", check="x") == 0.0


def test_live_example_config_is_the_subaccount_deployment():
    cfg = load_live_config(REPO / "config" / "live.example.yaml")
    assert cfg.venue.sub == 1 and cfg.venue.shared_account and cfg.venue.key_restricted_to_subaccount
    assert cfg.venue.exchange_indexes == (2,)
    assert live_config_problems(replace(cfg, mode="live")) == []
