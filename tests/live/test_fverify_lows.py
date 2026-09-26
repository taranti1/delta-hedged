"""The two LOW findings of the F1-F3 verification (docs/research/prelive_review_2026-09-25/
PRELIVE_F_VERIFY.md):

  L1  an orphan fill attached to its order by a late create ack was logged twice (`orphan_fill`
      then `fill` -> two `log.fill`), so the session ledger / reconcile tools and the fill counter
      double-counted it: now logged once (with its trade id), and the tools skip a trade id
      already booked;
  L2  the order-list fallback of `KalshiVenue.find_order` scanned the market's whole order
      history on every retry: now `status=resting` first, then every status from the session
      start minus a margin (`min_ts`).
"""

from __future__ import annotations

import json
import time

from dh.backtest.kat import default_kat_config
from dh.core.actions import Log, PlaceOrder
from dh.core.events import FeedStatus, KalshiBookSnapshot, KalshiFill, OrderAck
from dh.core.units import NS_PER_S
from dh.execution.order_manager import ORPHAN_ATTACHED
from dh.live.config import LiveConfig, VenueCfg
from dh.live.monitor import JsonLog
from dh.live.replay import ledger_from_log
from dh.live.tools import cmd_reconcile

from ..strategy.mm_driver import T0 as MM_T0
from ..strategy.mm_driver import TICK, Driver, spec
from .fakes import FakeRest, http_error, order_row
from .test_rereview_fixes import TK, _drain, _fill, _runner


# ============================================================================ L1
def _placed(d: Driver) -> PlaceOrder:
    d.feed(FeedStatus(MM_T0, 0, "kalshi.ws", "connected"))
    d.feed(KalshiBookSnapshot(MM_T0, 0, TICK, 1, 1, ((4500, 10000),), ((4500, 10000),)))
    d.inputs(MM_T0)
    places = [a for a in d.advance(MM_T0 + 6 * NS_PER_S) if isinstance(a, PlaceOrder)]
    assert places
    return places[0]


def test_orphan_fill_attached_by_a_late_ack_is_logged_and_counted_once():
    d = Driver([spec()], cfg=default_kat_config())
    p = _placed(d)
    oid = "X" + p.client_order_id
    # the fill arrives BEFORE the create ack and without a client id: an orphan
    f = KalshiFill(d.now + 1, 0, p.ticker, "trade-9", oid, "", p.book_side, p.px, 100, False, 0, 0, False)
    logs1 = [a for a in d.mm.on_event(f) if isinstance(a, Log) and a.kind == "fill"]
    assert len(logs1) == 1 and logs1[0].payload["trade_id"] == "trade-9" and d.mm.stats.fills == 1
    acts = d.mm.on_event(OrderAck(d.now + 3, 0, p.client_order_id, oid, p.ticker, 0, p.qty, "create"))
    assert not [a for a in acts if isinstance(a, Log) and a.kind == "fill"]  # attached: not booked again
    assert d.mm.stats.fills == 1 and d.mm.om.position(p.ticker) == (100 if p.book_side == "bid" else -100)
    w = d.mm.om.order(p.client_order_id)
    assert w is not None and w.filled_qty == 100  # the order's own counters did take the fill


def test_order_manager_marks_the_attached_fill():
    from dh.execution.order_manager import OrderManager

    om = OrderManager()
    om.request_place(PlaceOrder("c1", "KXBTCD-T", "bid", 4500, 1000), 0)
    evs = om.on_event(KalshiFill(1, 0, "KXBTCD-T", "t1", "X1", "", "bid", 4500, 300, False, 0, 0, False))
    assert [(e.kind, e.trade_id) for e in evs] == [("orphan_fill", "t1")]
    evs = om.on_event(OrderAck(2, 0, "c1", "X1", "KXBTCD-T", 0, 1000))
    fills = [e for e in evs if e.kind == "fill"]
    assert len(fills) == 1 and fills[0].detail == ORPHAN_ATTACHED and fills[0].trade_id == "t1"


def _log_with_duplicate_fill(tmp_path, *, trade_ids: bool = True):
    path = tmp_path / "s.jsonl"
    jl = JsonLog(path, "cfg")
    t0 = time.time_ns()
    jl.write("session_start", t0)
    kw = {"trade_id": "t-1"} if trade_ids else {}
    # an older log: the orphan's line and its attach line for the same fill
    jl.write("log.fill", t0 + 1, ticker="KXBTCD-X", coid="", side="bid", px=4500, qty=200, taker=False, fee=10_000, **kw)
    jl.write("log.fill", t0 + 2, ticker="KXBTCD-X", coid="c-1", side="bid", px=4500, qty=200, taker=False, fee=10_000, **kw)
    jl.close()
    return path, t0


def test_ledger_books_a_trade_id_once(tmp_path):
    path, _ = _log_with_duplicate_fill(tmp_path)
    s = spec("KXBTCD-X")
    assert len(ledger_from_log(path, [s]).fills) == 1
    (tmp_path / "old").mkdir()
    old, _ = _log_with_duplicate_fill(tmp_path / "old", trade_ids=False)
    assert len(ledger_from_log(old, [s]).fills) == 2  # no ids (logs before the fix): cannot tell, both kept


async def test_reconcile_counts_a_trade_id_once(tmp_path):
    path, t0 = _log_with_duplicate_fill(tmp_path)
    rest = FakeRest()
    rest.fills = [{"fill_id": "t-1", "trade_id": "t-1", "ticker": "KXBTCD-X", "count_fp": "2.00", "fee_cost": "0.010000"}]
    out: list[str] = []
    assert await cmd_reconcile(str(path), LiveConfig(venue=VenueCfg(subaccount=1)), rest, out.append) == 0
    assert out[-1] == "0 tickers mismatched"


def test_json_log_fill_line_carries_the_trade_id(tmp_path):
    path, _ = _log_with_duplicate_fill(tmp_path)
    recs = [json.loads(x) for x in path.read_text().splitlines() if '"log.fill"' in x]
    assert {r["trade_id"] for r in recs} == {"t-1"}


# ============================================================================ L2
async def test_order_list_lookup_is_bounded_by_status_then_session_start():
    """The by-id read 404s (shard 2). The order is EXECUTED (not resting): the list is read with
    status=resting first, then from the session start minus the margin; never unbounded."""
    rest = FakeRest()
    rest.orders["o-8"] = order_row("dhm1-tok-8", "o-8", TK, status="executed", subaccount=1, exchange_index=2)
    rest.on("get_order", *[http_error(404, "not_found", "order not found", "GET") for _ in range(20)])
    s, r, rest, _ = _runner(rest)
    r.started_ns = 1_790_300_000 * NS_PER_S
    r.push(_fill(time.time_ns(), "", "o-8", "t8"))
    await _drain(r)
    calls = [k for _, k in rest.of("iter_orders")]
    assert calls[:2] == [{"subaccount": 1, "ticker": TK, "status": "resting"},
                         {"subaccount": 1, "ticker": TK, "min_ts": 1_790_300_000 - r.ORDER_LIST_MARGIN_S}]
    assert all("status" in c or "min_ts" in c for c in calls)  # every list read is bounded
    assert [(e.trade_id, e.client_order_id) for e in s.events if isinstance(e, KalshiFill)] == [("t8", "dhm1-tok-8")]


async def test_venue_find_order_stops_at_the_resting_page():
    from dh.live.venue_kalshi import KalshiVenue

    from .test_runner import cfg

    rest = FakeRest()
    rest.orders["o-9"] = order_row("c-9", "o-9", TK, status="resting", subaccount=1)
    rest.on("get_order", http_error(404, "not_found", "order not found", "GET"))
    v = KalshiVenue(rest, sink=lambda e: None, cfg=cfg(subaccount=1, allow_primary_account=False).venue)
    row, via = await v.find_order("o-9", ticker=TK, min_ts_s=123)
    assert row is not None and via["by_list"] is True
    assert [k for _, k in rest.of("iter_orders")] == [{"subaccount": 1, "ticker": TK, "status": "resting"}]
    row, via = await v.find_order("o-404", ticker=TK, min_ts_s=123)
    assert row is None and via["by_list"] is False
    assert [k for _, k in rest.of("iter_orders")][1:] == [{"subaccount": 1, "ticker": TK, "status": "resting"},
                                                          {"subaccount": 1, "ticker": TK, "min_ts": 123}]
