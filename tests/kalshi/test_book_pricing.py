"""NO-side price convention of Kalshi order books (asyncapi ``use_yes_price``, reconciliation #9).

Kalshi will flip the orderbook_delta default to ``use_yes_price: true`` and then remove the
flag, after which NO levels only come in yes-leg pricing (a NO bid at NO price q reported as
1 - q). The normalizer must give identical events for both wire shapes; recordings made so far
(no-leg) must replay exactly as before.
"""

from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path

import orjson
import pytest

from dh.core.book import KalshiBook
from dh.core.events import FeedStatus, KalshiBookDelta, KalshiBookSnapshot
from dh.execution.queue import book_px, resting_book
from dh.kalshi.normalize import (
    book_sides,
    no_side_yes_priced,
    normalize_rest_record,
    rest_orderbook_to_snapshot,
    snapshot_no_side_pricing,
    ws_message_to_events,
)
from dh.kalshi.sequencer import KalshiWsState, normalize_ws_frame, synthetic_status_frame
from dh.store.replay import Normalizers, RawRecord

R = 1_000


def flip(px: str) -> str:
    """Dollar price string -> 1 - price, exact to 1e-4."""
    v = 10000 - int(Decimal(px) * 10000)
    return f"{v // 10000}.{v % 10000:04d}"


def yes_leg(msg: dict) -> dict:
    """The same orderbook message as the server sends it with ``use_yes_price: true``."""
    m = json.loads(json.dumps(msg))
    body = m.get("msg") or {}
    if m.get("type") == "orderbook_snapshot" and body.get("no_dollars_fp"):
        body["no_dollars_fp"] = [[flip(p), q] for p, q in reversed(body["no_dollars_fp"])]
    elif m.get("type") == "orderbook_delta" and body.get("side") == "no":
        body["price_dollars"] = flip(body["price_dollars"])
    return m


def snap(ticker: str, yes, no, sid: int = 1, seq: int = 1) -> dict:
    body: dict = {"market_ticker": ticker, "market_id": "m"}
    if yes:
        body["yes_dollars_fp"] = [list(lv) for lv in yes]
    if no:
        body["no_dollars_fp"] = [list(lv) for lv in no]
    return {"type": "orderbook_snapshot", "sid": sid, "seq": seq, "msg": body}


def delta(ticker: str, side: str, px: str, d: str, sid: int = 1, seq: int = 2, coid: str | None = None) -> dict:
    body = {"market_ticker": ticker, "market_id": "m", "price_dollars": px, "delta_fp": d, "side": side, "ts_ms": 7}
    if coid is not None:
        body["client_order_id"] = coid
    return {"type": "orderbook_delta", "sid": sid, "seq": seq, "msg": body}


# no-leg shapes (NO prices on the NO scale). PROVING: best yes 0.45, NO bids 0.20 / 0.53 -> read
# as yes-leg the NO levels would be YES asks at 0.20 / 0.53, i.e. crossed with the 0.45 bid.
PROVING = snap("A", [("0.4400", "3.00"), ("0.4500", "10.00")], [("0.2000", "7.00"), ("0.5300", "5.00")])
THIN = snap("A", [("0.4500", "10.00")], [("0.5300", "5.00")])  # uncrossed both ways: unprovable
NO_ONLY = snap("A", [], [("0.0100", "9.00"), ("0.6000", "4.00")])
EXPECTED_PROVING = ((4400, 300), (4500, 1000)), ((2000, 700), (5300, 500))


# ---------------------------------------------------------------------------- the proof
def test_no_side_yes_priced_truth_table():
    y = ((4500, 1),)
    assert no_side_yes_priced((), ((5300, 1),)) is None and no_side_yes_priced(y, ()) is None
    assert no_side_yes_priced(y, ((2000, 1), (5300, 1))) is False  # 0.45 + 0.53 < 1, but 0.45 >= 0.20
    assert no_side_yes_priced(y, ((4700, 1), (8000, 1))) is True   # 0.45 < 0.47, but 0.45 + 0.80 >= 1
    assert no_side_yes_priced(y, ((5300, 1),)) is None              # both readings uncrossed
    assert no_side_yes_priced(y, ((5500, 1),)) is True              # read as no-leg it would be locked
    assert no_side_yes_priced(y, ((4500, 1),)) is False             # read as yes-leg it would be locked
    assert no_side_yes_priced(y, ((1000, 1), (9000, 1))) is None    # crossed either way: unprovable
    assert snapshot_no_side_pricing(PROVING["msg"]) is False
    assert snapshot_no_side_pricing(yes_leg(PROVING)["msg"]) is True
    assert snapshot_no_side_pricing(THIN["msg"]) is None and snapshot_no_side_pricing(NO_ONLY["msg"]) is None


def test_book_sides_levels_stay_ascending_on_the_no_scale():
    yes, no = book_sides([["0.4500", "1.00"]], [["0.4700", "5.00"], ["0.8000", "7.00"]], use_yes_price=True)
    assert no == ((2000, 700), (5300, 500))
    assert book_sides(None, [["0.4700", "5.00"]], use_yes_price=True) == ((), ((5300, 500),))
    assert book_sides(None, [["0.4700", "5.00"]], use_yes_price=False) == ((), ((4700, 500),))


# ---------------------------------------------------------------------------- stateless normalizer
@pytest.mark.parametrize("flag", [False, True])
def test_snapshot_both_shapes_identical_and_proof_beats_the_flag(flag):
    """A proving snapshot normalizes the same whatever the flag says; unprovable ones follow it."""
    (a,) = ws_message_to_events(PROVING, R, use_yes_price=flag)
    (b,) = ws_message_to_events(yes_leg(PROVING), R, use_yes_price=flag)
    assert (a.yes_bids, a.no_bids) == (b.yes_bids, b.no_bids) == EXPECTED_PROVING
    for m in (THIN, NO_ONLY):
        (legacy,) = ws_message_to_events(m, R, use_yes_price=False)
        (new,) = ws_message_to_events(yes_leg(m), R, use_yes_price=True)
        assert legacy == new
    (wrong,) = ws_message_to_events(yes_leg(NO_ONLY), R, use_yes_price=False)  # unprovable + wrong flag
    assert wrong.no_bids == ((4000, 400), (9900, 900))  # mirrored: the flag is all there is


def test_delta_both_shapes_identical_including_own_order_annotation():
    # our resting ask: sell YES at 0.39 == a NO bid at 0.61 on the NO book
    m = delta("A", "no", "0.6100", "3.00", coid="dh-ask-7")
    (legacy,) = ws_message_to_events(m, R, use_yes_price=False)
    (new,) = ws_message_to_events(yes_leg(m), R, use_yes_price=True)
    assert yes_leg(m)["msg"]["price_dollars"] == "0.3900"
    assert legacy == new == KalshiBookDelta(ts=R, ts_exch=7_000_000, ticker="A", sid=1, seq=2, side="no", px=6100,
                                            delta=300, own_client_order_id="dh-ask-7")
    assert (new.side, new.px) == (resting_book("ask"), book_px("ask", 3900))  # queue / own-footprint level
    y = delta("A", "yes", "0.4500", "-1.00", coid="dh-bid-1")  # YES side: identical in both shapes
    assert yes_leg(y) == y
    (bid,) = ws_message_to_events(y, R, use_yes_price=True)
    assert (bid.side, bid.px) == (resting_book("bid"), book_px("bid", 4500))


def test_book_reconstruction_identical_for_both_shapes():
    msgs = [PROVING,
            delta("A", "no", "0.5300", "-5.00", seq=2),             # best NO bid pulled
            delta("A", "no", "0.5100", "2.00", seq=3, coid="own"),  # our ask at YES 0.49
            delta("A", "yes", "0.4600", "1.00", seq=4)]
    books = []
    for shape, flag in ((lambda x: x, False), (yes_leg, True)):
        b = KalshiBook("A")
        for m in msgs:
            for ev in ws_message_to_events(shape(m), R, use_yes_price=flag):
                if isinstance(ev, KalshiBookSnapshot):
                    b.apply_snapshot(ev)
                else:
                    assert b.apply_delta(ev)
        books.append((dict(b.yes_bids), dict(b.no_bids), b.best_bid(), b.best_ask(), b.ask_qty(4900)))
    assert books[0] == books[1] == ({4400: 300, 4500: 1000, 4600: 100}, {2000: 700, 5100: 200}, 4600, 4900, 200)


# ---------------------------------------------------------------------------- REST
def test_rest_orderbook_no_leg_by_default_and_proof_converts_a_yes_leg_book():
    body = {"orderbook_fp": {"yes_dollars": [["0.4400", "3.00"], ["0.4500", "10.00"]],
                             "no_dollars": [["0.2000", "7.00"], ["0.5300", "5.00"]]}}
    ev = rest_orderbook_to_snapshot("A", body, R)
    assert (ev.yes_bids, ev.no_bids) == EXPECTED_PROVING
    yl = {"orderbook_fp": {"yes_dollars": body["orderbook_fp"]["yes_dollars"],
                           "no_dollars": [["0.4700", "5.00"], ["0.8000", "7.00"]]}}
    assert rest_orderbook_to_snapshot("A", yl, R) == ev  # proven yes-leg: converted, not mirrored
    thin = {"orderbook_fp": {"yes_dollars": [["0.4500", "1.00"]], "no_dollars": [["0.5300", "5.00"]]}}
    assert rest_orderbook_to_snapshot("A", thin, R).no_bids == ((5300, 500),)  # documented no-leg
    assert rest_orderbook_to_snapshot("A", thin, R, use_yes_price=True).no_bids == ((4700, 500),)
    rec = {"method": "GET", "path": "/markets/orderbooks", "status": 200,
           "body": {"orderbooks": [dict(yl, ticker="A"), dict(thin, ticker="B")]}}
    a, b = normalize_rest_record(rec, R)
    assert (a.no_bids, b.no_bids) == (EXPECTED_PROVING[1], ((5300, 500),))


# ---------------------------------------------------------------------------- sequencer
def run(frames, state):
    out = []
    for i, fr in enumerate(frames):
        out.extend(normalize_ws_frame(fr if isinstance(fr, bytes) else orjson.dumps(fr), R + i, state))
    return out


def subscribed(sid=1):
    return {"id": 1, "type": "subscribed", "msg": {"channel": "orderbook_delta", "sid": sid}}


def test_connection_declaration_drives_unprovable_messages_and_resets_on_disconnect():
    frames = [synthetic_status_frame("connected", "wss://x", use_yes_price=True), subscribed(),
              yes_leg(snap("B", [], [("0.6000", "4.00")], seq=1)), yes_leg(delta("B", "no", "0.6100", "3.00", seq=2)),
              synthetic_status_frame("disconnected", "boom"),
              synthetic_status_frame("connected", "wss://x"), subscribed(),  # undeclared (legacy)
              snap("B", [], [("0.6000", "4.00")], seq=1), delta("B", "no", "0.6100", "3.00", seq=2)]
    st = KalshiWsState()  # replay default: undeclared connections are no-leg
    evs = run(frames, st)
    books = [(e.no_bids if isinstance(e, KalshiBookSnapshot) else (e.px, e.delta)) for e in evs
             if isinstance(e, (KalshiBookSnapshot, KalshiBookDelta))]
    assert books == [((6000, 400),), (6100, 300)] * 2
    assert st.counters["pricing_mismatches"] == 0 and st.conn_use_yes_price is None


def test_proof_mismatch_is_followed_reported_and_resyncs_books_built_on_the_wrong_scale():
    frames = [subscribed(),
              yes_leg(snap("B", [], [("0.6000", "4.00")], seq=1)),   # unprovable: read with the (wrong) default
              yes_leg(dict(PROVING, seq=2)),                        # proves yes-leg
              yes_leg(delta("B", "no", "0.6100", "3.00", seq=3)),   # B invalid until resynced
              yes_leg(delta("A", "no", "0.2000", "-7.00", seq=4)),  # A: proven scale
              yes_leg(snap("B", [], [("0.6000", "4.00")], seq=5)),  # resync snapshot, proven scale
              yes_leg(delta("B", "no", "0.6100", "3.00", seq=6))]
    st = KalshiWsState(use_yes_price=False)
    evs = run(frames, st)
    status = [(e.stream, e.status) for e in evs if isinstance(e, FeedStatus)]
    assert status == [("kalshi.ws:orderbook_delta", "error"), ("kalshi.book:B", "gap"), ("kalshi.book:B", "resynced"),
                      ("kalshi.ws", "resynced")]
    err = next(e for e in evs if isinstance(e, FeedStatus) and e.status == "error")
    assert "use_yes_price=True" in err.detail and "sid=1" in err.detail
    assert st.take_resync_requests() == [(1, ("B",))]
    assert st.counters["pricing_mismatches"] == 1 and st.counters["suppressed_deltas"] == 1
    book = [e for e in evs if isinstance(e, (KalshiBookSnapshot, KalshiBookDelta))]
    assert [(e.ticker, e.no_bids if isinstance(e, KalshiBookSnapshot) else (e.px, e.delta)) for e in book] == [
        ("B", ((4000, 400),)),            # wrong (unprovable, default) ... then invalidated
        ("A", EXPECTED_PROVING[1]),
        ("A", (2000, -700)),
        ("B", ((6000, 400),)),            # resynced on the proven scale
        ("B", (6100, 300))]
    assert st.book_yes_priced(1) is True
    run([subscribed()], st)  # sid reused on a new subscription: the proof is forgotten
    assert st.book_yes_priced(1) is False


# ---------------------------------------------------------------------------- real recorded frames
SLICE = orjson.loads((Path(__file__).parent / "fixtures" / "live_book_slice_2026-09-25.json").read_bytes())


def slice_frames(*, to_yes_leg: bool = False, declare: bool | None = None) -> list[tuple[int, bytes]]:
    """The recorded slice with a contiguous seq on the orderbook sid (it keeps 4 of the sid's ~470
    markets), optionally rewritten to the yes-leg shape and/or with a declaring 'connected' record."""
    out, seq = [], 0
    for t, raw in SLICE["frames"]:
        m = orjson.loads(raw)
        if "seq" in m:
            seq += 1
            m["seq"] = seq
        if to_yes_leg:
            m = yes_leg(m)
        if declare is not None and m.get("type") == "dh.feed_status":
            m["use_yes_price"] = declare
        out.append((t, orjson.dumps(m)))
    return out


def oracle_states(frames: list[tuple[int, bytes]]) -> list[tuple[str, dict, dict]]:
    """Independent legacy reading of RECORDED (no-leg) frames: prices as on the wire."""
    books: dict[str, tuple[dict, dict]] = {}
    out = []
    for _, raw in frames:
        m = json.loads(raw)
        b = m.get("msg") or {}
        if m["type"] == "orderbook_snapshot":
            books[b["market_ticker"]] = tuple({int(Decimal(p) * 10000): int(Decimal(q) * 100) for p, q in b.get(k) or []}
                                              for k in ("yes_dollars_fp", "no_dollars_fp"))
        elif m["type"] == "orderbook_delta":
            side = books[b["market_ticker"]][0 if b["side"] == "yes" else 1]
            px = int(Decimal(b["price_dollars"]) * 10000)
            side[px] = side.get(px, 0) + int(Decimal(b["delta_fp"]) * 100)
            if not side[px]:
                del side[px]
        else:
            continue
        yes, no = books[b["market_ticker"]]
        out.append((b["market_ticker"], dict(yes), dict(no)))
    return out


def replay(frames: list[tuple[int, bytes]], **kw) -> tuple[list, list[tuple[str, dict, dict]], Normalizers]:
    """dh.store.replay's kalshi.ws path -> (events, book state after every book event)."""
    norm = Normalizers(**kw)
    evs = [e for i, (t, raw) in enumerate(frames) for e in norm(RawRecord(t, "kalshi.ws", i, raw))]
    books: dict[str, KalshiBook] = {}
    states = []
    for e in evs:
        if isinstance(e, KalshiBookSnapshot):
            books.setdefault(e.ticker, KalshiBook(e.ticker)).apply_snapshot(e)
        elif isinstance(e, KalshiBookDelta):
            assert books[e.ticker].apply_delta(e)
        else:
            continue
        b = books[e.ticker]
        states.append((e.ticker, dict(b.yes_bids), dict(b.no_bids)))
    return evs, states, norm


def test_real_recorded_frames_same_books_through_every_path():
    recorded = slice_frames()
    oracle = oracle_states(recorded)
    snaps = [orjson.loads(r)["msg"] for _, r in recorded if b'"orderbook_snapshot"' in r]
    # B83950 and T83899.99 prove no-leg; B84250 (NO only) and T83799.99 (YES only) prove nothing
    assert [snapshot_no_side_pricing(s) for s in snaps] == [False, None, None, False]
    assert len(oracle) == 567 and {t for t, _, _ in oracle} == set(SLICE["markets"])

    legacy, legacy_states, _ = replay(recorded)  # what every replay of data/raw does (no declaration, default)
    assert legacy_states == oracle
    book_events = [e for e in legacy if isinstance(e, (KalshiBookSnapshot, KalshiBookDelta))]
    assert len(book_events) == len(oracle) and not any(isinstance(e, FeedStatus) and e.status in ("error", "gap") for e in legacy)

    paths = {
        "recorded, explicit no-leg": (recorded, {"kalshi_use_yes_price": False}),
        "yes-leg, declared by the connected record (new recorder)": (slice_frames(to_yes_leg=True, declare=True), {}),
        "yes-leg, replay told": (slice_frames(to_yes_leg=True), {"kalshi_use_yes_price": True}),
        "yes-leg, nothing declared, wrong default: snapshots prove it": (slice_frames(to_yes_leg=True), {}),
        "recorded, wrong replay flag: snapshots prove it": (recorded, {"kalshi_use_yes_price": True}),
    }
    for name, (frames, kw) in paths.items():
        evs, states, norm = replay(frames, **kw)
        assert states == oracle, name
        assert [e for e in evs if isinstance(e, (KalshiBookSnapshot, KalshiBookDelta))] == book_events, name
        mismatches = norm.states["kalshi.ws"].counters["pricing_mismatches"]
        assert mismatches == (1 if "prove it" in name else 0), name
