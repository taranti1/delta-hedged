"""E0 at scale (dh.research.e0_history): the cell reduction reproduces the per-print E0 statistic
exactly, and the policy-C (last-in-queue) flag marks the last print before a trade-through."""
from __future__ import annotations

import numpy as np
import pandas as pd

from dh.research.e0_history import c_flags, event_cells, seg_table
from dh.research.exp0_maker_pnl import prepare, segment_table


def _tape(n=400, n_markets=3, seed=5):
    rng = np.random.default_rng(seed)
    T = 1_790_000_000_000
    mk = pd.DataFrame({"ticker": [f"KXBTCD-E-T{i}" for i in range(n_markets)], "result": ["yes", "no", "no"][:n_markets],
                       "strike_type": "greater", "floor_strike": [80000.0 + 100 * i for i in range(n_markets)],
                       "cap_strike": np.nan, "close_ts_ms": T, "event_ticker": "KXBTCD-E"})
    tr = pd.DataFrame({"ticker": rng.choice(mk.ticker, n), "ts_ms": np.sort(rng.integers(T - 3_600_000, T, n)),
                       "yes_px": rng.integers(1, 99, n) * 100, "qty": rng.integers(1, 50, n) * 100,
                       "taker_outcome_side": rng.choice(["yes", "no"], n), "is_block_trade": False})
    return tr, mk


def test_cells_reproduce_per_print_e0():
    tr, mk = _tape()
    cells, st = event_cells(tr, mk, "KXBTCD", {"KXBTCD": ("quadratic", 1.0)}, None,
                            (np.array([], dtype=np.int64), np.array([], dtype=np.int64)), (np.array([]), np.array([])))
    assert st["trades"] == len(tr) and cells.k.sum() == len(tr)
    a = seg_table(cells, ["series"], n_boot=50, min_events=1).iloc[0]
    # the per-print module on the same prints (one cluster: identical point estimate)
    df = prepare(tr.rename(columns={"taker_outcome_side": "taker_side"}),
                 mk.rename(columns={"close_ts_ms": "expiration_ts_ms"}), fee_types={"*": ("quadratic", 1.0)})
    b = segment_table(df.assign(all="all"), ["all"], n_boot=50, min_events=1).iloc[0]
    assert abs(a.maker_gross_c - b.maker_gross_c) < 1e-9 and abs(a.maker_net_c - b.maker_net_c) < 1e-9
    assert a.maker_fee_c == 0.0  # quadratic: no maker fee


def test_maker_fee_series_is_charged():
    tr, mk = _tape()
    cells, _ = event_cells(tr, mk, "KXBTCD", {"KXBTCD": ("quadratic_with_maker_fees", 1.0)}, None,
                           (np.array([], dtype=np.int64), np.array([], dtype=np.int64)), (np.array([]), np.array([])))
    a = seg_table(cells, ["series"], n_boot=50, min_events=1).iloc[0]
    assert a.maker_fee_c > 0 and abs(a.maker_gross_c - a.maker_net_c - a.maker_fee_c) < 1e-9


def test_c_flag_is_the_last_print_before_a_trade_through():
    t = pd.DataFrame({"ticker": ["A"] * 6, "taker_side": ["yes", "yes", "yes", "no", "no", "yes"],
                      "yes_px": [5000, 5000, 5100, 4900, 4900, 5100], "ts_ms": [0, 100, 200, 300, 5000, 9000]})
    # buys: 5000, 5000 (next buy 5100 within 2 s -> through), 5100 (next buy at 9000: > 2 s) ...
    assert c_flags(t, 2000).tolist() == [False, True, False, False, False, False]
    # sells: 4900 then 4900 (not lower) -> no trade-through
    t2 = t.assign(yes_px=[5000, 5000, 5100, 4900, 4800, 5100])
    assert c_flags(t2, 5000).tolist() == [False, True, False, True, False, False]


def test_same_millisecond_sweep_is_ordered_by_price():
    """A taker buy sweeping 50c then 51c in one ms: the 50c print is the last-in-queue (C) fill,
    whatever order the tape lists the two prints in."""
    T = 1_790_000_000_000
    mk = pd.DataFrame({"ticker": ["KXBTCD-E-T1"], "result": ["no"], "strike_type": "greater", "floor_strike": [1.0],
                       "cap_strike": [np.nan], "close_ts_ms": [T]})
    tr = pd.DataFrame({"ticker": ["KXBTCD-E-T1"] * 2, "ts_ms": [T - 10_000] * 2, "yes_px": [5100, 5000], "qty": [100, 100],
                       "taker_outcome_side": ["yes", "yes"], "is_block_trade": False})
    none = (np.array([], dtype=np.int64), np.array([], dtype=np.int64))
    cells, _ = event_cells(tr, mk, "KXBTCD", {"KXBTCD": ("quadratic", 1.0)}, None, none, (np.array([]), np.array([])))
    c = cells[cells.c_fill]
    assert c.k.sum() == 1 and c.px_b.tolist() == ["50-65c"]
