from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from dh.kalshi.wire import iso_to_ns

from . import samples as S

ROOT = Path(__file__).resolve().parents[2]


def load_script(name: str):
    spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / f"{name}.py")
    mod = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(mod)
    return mod


dl = load_script("download_kalshi_history")

E0 = dict(S.EVENT_KXBTCD, event_ticker="KXBTCD-25JUL0117", strike_date="2025-07-01T21:00:00Z")
E1 = dict(S.EVENT_KXBTCD, event_ticker="KXBTCD-25AUG0417", strike_date="2025-08-04T21:00:00Z")
E2 = dict(S.EVENT_KXBTCD, event_ticker="KXBTCD-25AUG0517", strike_date="2025-08-05T21:00:00Z")
DAY1 = dict(open_time="2025-08-04T20:00:00Z", close_time="2025-08-04T21:00:00Z",
            expected_expiration_time="2025-08-04T21:00:00Z", settlement_ts="2025-08-04T21:05:00Z")
M1A = S.settled_market(ticker="KXBTCD-25AUG0417-T114999.99", event_ticker=E1["event_ticker"], **DAY1)
M1B = S.settled_market(ticker="KXBTCD-25AUG0417-T119999.99", event_ticker=E1["event_ticker"], volume_fp="0.00", result="no",
                       settlement_value_dollars="0.0000", **DAY1)
M2 = S.settled_market(event_ticker=E2["event_ticker"])


def trade(tid: str, ticker: str, t: str, taker: str = "yes") -> dict[str, Any]:
    return dict(S.trade_row(tid, t, taker=taker), ticker=ticker)


class FakeRest:
    def __init__(self) -> None:
        self.calls: list[tuple] = []

    async def get_historical_cutoff(self):
        return {"market_settled_ts": "2025-08-05T00:00:00Z", "trades_created_ts": "2025-08-05T20:30:00Z",
                "orders_updated_ts": "2025-08-05T00:00:00Z"}

    async def iter_events(self, **kw):
        self.calls.append(("events", kw["series_ticker"], kw["status"], kw.get("min_close_ts")))
        for e in (E0, E2, E1):
            yield dict(e)

    async def iter_historical_markets(self, **kw):
        self.calls.append(("hmarkets", kw["event_ticker"]))
        for m in {E1["event_ticker"]: [M1A, M1B]}.get(kw["event_ticker"], []):
            yield m

    async def iter_markets(self, **kw):
        self.calls.append(("markets", kw["event_ticker"]))
        for m in {E2["event_ticker"]: [M2]}.get(kw["event_ticker"], []):
            yield m

    async def iter_trades(self, *, ticker, historical=False, **kw):
        self.calls.append(("trades", ticker, historical))
        data = {
            (M1A["ticker"], True): [trade("a2", M1A["ticker"], "2025-08-04T20:40:00Z", "no"), trade("a1", M1A["ticker"], "2025-08-04T20:10:00Z")],
            (M2["ticker"], True): [trade("b1", M2["ticker"], "2025-08-05T20:10:00Z"), trade("b2", M2["ticker"], "2025-08-05T20:29:59.5Z")],
            (M2["ticker"], False): [trade("b2", M2["ticker"], "2025-08-05T20:29:59.5Z"), trade("b3", M2["ticker"], "2025-08-05T20:45:00Z", "no")],
        }
        for t in data.get((ticker, historical), []):
            yield t

    async def get_historical_candlesticks(self, ticker, start_ts, end_ts, period):
        self.calls.append(("hcandles", ticker))
        return {"ticker": ticker, "candlesticks": [{"end_period_ts": start_ts + 60, "yes_bid": {"open": "0.40", "low": "0.40", "high": "0.45", "close": "0.45"},
                                                   "yes_ask": {"open": "0.42", "low": "0.42", "high": "0.47", "close": "0.47"},
                                                   "price": {"open": "0.41", "low": None, "high": None, "close": "0.46", "mean": "0.433333", "previous": None},
                                                   "volume": "10.00", "open_interest": "8.00"}]}

    async def get_market_candlesticks(self, series, ticker, start_ts, end_ts, period):
        self.calls.append(("candles", series, ticker))
        return {"ticker": ticker, "candlesticks": [{"end_period_ts": start_ts + 60, "yes_bid": {"open_dollars": "0.4500", "low_dollars": "0.4500", "high_dollars": "0.4600", "close_dollars": "0.4600"},
                                                   "yes_ask": {"open_dollars": "0.4700", "low_dollars": "0.4700", "high_dollars": "0.4800", "close_dollars": "0.4800"},
                                                   "price": {"open_dollars": "0.4600", "close_dollars": "0.4600"}, "volume_fp": "3.00", "open_interest_fp": "3.00"}]}

    async def get_cfbenchmarks_history(self, index_id, *, timespan=None, timestamp=None, extra_params=None):
        self.calls.append(("cf", index_id, timespan, timestamp))
        return {"payload": [{"value": "115100.5", "time": int(timestamp) - 1000}, {"value": "115101.5", "time": int(timestamp)}]}

    async def iter_event_fee_changes(self, *a, **kw):
        for ch in ({"id": "f1", "event_ticker": E1["event_ticker"], "series_ticker": "KXBTCD", "fee_type_override": "quadratic",
                    "fee_multiplier_override": 0.5, "scheduled_ts": "2025-08-01T00:00:00Z"},
                   {"id": "f2", "event_ticker": "KXOTHER-1", "series_ticker": "KXOTHER", "fee_type_override": None,
                    "fee_multiplier_override": None, "scheduled_ts": "2025-08-01T00:00:00Z"}):
            yield ch

    async def iter_incentive_programs(self, **kw):
        yield {"id": "i1", "market_id": "m", "market_ticker": M2["ticker"], "incentive_type": "liquidity", "incentive_description": "x",
               "start_date": "2025-08-01T00:00:00Z", "end_date": "2025-09-01T00:00:00Z", "period_reward": 1000000, "paid_out": False}
        yield {"id": "i2", "market_ticker": "KXETHD-X", "period_reward": 1}

    async def get_series(self, s):
        return {"series": S.SERIES_KXBTCD}

    async def get_series_fee_changes(self, s, show_historical=False):
        return {"series_fee_change_arr": []}


async def test_download_pipeline_and_resume(tmp_path: Path):
    rest = FakeRest()
    start, end = iso_to_ns("2025-08-01T00:00:00Z"), iso_to_ns("2025-08-06T00:00:00Z")
    stats = await dl.download(rest, ["KXBTCD"], start, end, tmp_path, datasets=dl.DATASETS, have_auth=True, log=lambda *_: None)
    assert stats == {"events": 2, "events_skipped": 0, "markets": 3, "trades": 5, "candles": 2, "brti_ticks": 4, "errors": 0}
    # E1 settled before the market cutoff -> historical markets; E2 -> live markets
    assert ("hmarkets", E1["event_ticker"]) in rest.calls and ("markets", E2["event_ticker"]) in rest.calls
    assert ("trades", M1B["ticker"], True) not in rest.calls  # zero-volume market skipped
    assert ("trades", M1A["ticker"], False) not in rest.calls  # closed before the trades cutoff
    assert {("trades", M2["ticker"], True), ("trades", M2["ticker"], False)} <= set(rest.calls)  # straddles the cutoff

    t = pq.read_table(tmp_path / "trades" / "series=KXBTCD" / f"{E2['event_ticker']}.parquet")
    assert t.schema.equals(dl.TRADE_SCHEMA)
    rows = t.to_pylist()
    assert [(r["trade_id"], r["source"]) for r in rows] == [("b1", "historical"), ("b2", "historical"), ("b3", "live")]
    b3 = rows[2]
    assert (b3["yes_px"], b3["qty"], b3["taker_outcome_side"], b3["taker_book_side"]) == (4600, 300, "no", "ask")
    assert b3["ts_ms"] == iso_to_ns("2025-08-05T20:45:00Z") // 1_000_000 and b3["is_block_trade"] is False
    assert rows[1]["ts_ms"] == iso_to_ns("2025-08-05T20:29:59.5Z") // 1_000_000
    t1 = pq.read_table(tmp_path / "trades" / "series=KXBTCD" / f"{E1['event_ticker']}.parquet").to_pylist()
    assert [r["trade_id"] for r in t1] == ["a1", "a2"]  # sorted by time

    m = pq.read_table(tmp_path / "markets" / "series=KXBTCD" / f"{E1['event_ticker']}.parquet")
    assert m.schema.equals(dl.MARKET_SCHEMA)
    mr = {r["ticker"]: r for r in m.to_pylist()}
    assert mr[M1A["ticker"]]["settlement_px"] == 10_000 and mr[M1B["ticker"]]["settlement_px"] == 0
    assert mr[M1A["ticker"]]["expiration_value"] == "115123.45" and mr[M1A["ticker"]]["source"] == "historical"
    assert mr[M1A["ticker"]]["floor_strike"] == 114999.99 and mr[M1A["ticker"]]["volume"] == 123400

    ev = pq.read_table(tmp_path / "events" / "series=KXBTCD" / "events.parquet").to_pylist()
    assert [e["event_ticker"] for e in ev] == [E1["event_ticker"], E2["event_ticker"]]  # E0 out of range
    c1 = pq.read_table(tmp_path / "candles" / "series=KXBTCD" / f"{E1['event_ticker']}.parquet").to_pylist()
    assert c1[0]["price_mean"] == "0.433333" and c1[0]["volume"] == 1000 and c1[0]["source"] == "historical"
    c2 = pq.read_table(tmp_path / "candles" / "series=KXBTCD" / f"{E2['event_ticker']}.parquet").to_pylist()
    assert c2[0]["yes_bid_close"] == "0.4600" and c2[0]["source"] == "live"
    br = pq.read_table(tmp_path / "brti" / "series=KXBTCD" / f"{E2['event_ticker']}.parquet").to_pylist()
    assert [r["value"] for r in br] == [115100.5, 115101.5]
    cfs = [c for c in rest.calls if c[0] == "cf"]
    exp_ms = [iso_to_ns(f"2025-08-0{d}T21:00:00Z") // 1_000_000 for d in (4, 5)]
    assert [(c[2], c[3]) for c in cfs] == [("660s", str(e + 300_000)) for e in exp_ms]
    assert (tmp_path / "brti" / "series=KXBTCD" / f"{E2['event_ticker']}.raw.json").is_file()
    fees = pq.read_table(tmp_path / "fees" / "event_fee_changes.parquet").to_pylist()
    assert [(f["id"], f["fee_multiplier_override"]) for f in fees] == [("f1", "0.5")]
    inc = pq.read_table(tmp_path / "incentives" / "incentive_programs.parquet").to_pylist()
    assert [i["id"] for i in inc] == ["i1"] and inc[0]["period_reward_centicents"] == 1_000_000

    # second run: everything checkpointed, no market/trade requests
    rest2 = FakeRest()
    stats2 = await dl.download(rest2, ["KXBTCD"], start, end, tmp_path, datasets=dl.DATASETS, have_auth=True, log=lambda *_: None)
    assert stats2["events_skipped"] == 2 and stats2["events"] == 0
    assert not any(c[0] in ("trades", "markets", "hmarkets", "cf") for c in rest2.calls)
    assert ("events", "KXBTCD", "settled", start // 10**9) in rest.calls
    rest3 = FakeRest()
    await dl.download(rest3, ["KXBTCD"], start, end, tmp_path, datasets=["markets"], event_close_filter=False, log=lambda *_: None)
    assert ("events", "KXBTCD", "settled", None) in rest3.calls


def test_scripts_help_runs_offline():
    for name in ("download_kalshi_history", "smoke_kalshi", "verify_fee_schedule"):
        r = subprocess.run([sys.executable, str(ROOT / "scripts" / f"{name}.py"), "--help"], capture_output=True, text=True, timeout=60)
        assert r.returncode == 0, (name, r.stderr)
        assert "usage" in r.stdout.lower()
