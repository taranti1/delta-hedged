"""M1.2 settlement check (dh.research.settlement_check): exact integer-cent arithmetic, window
selection and the pass-rate summary, on synthetic prints with a known convention."""
from __future__ import annotations

import numpy as np
import pandas as pd

from dh.research.settlement_check import (
    PRODUCTION,
    cents,
    check,
    expirations_from_markets,
    round_half_up_cents,
    rounding_rule_check,
    summarize,
)


def test_cents_and_rounding():
    assert cents("83950.62") == 8395062 and cents(83950.62) == 8395062
    assert cents("83950.625") is None and cents("") is None and cents(None) is None
    assert round_half_up_cents(60 * 100 + 30, 60) == 101  # exactly half a cent -> up
    assert round_half_up_cents(60 * 100 + 29, 60) == 100
    assert round_half_up_cents(60 * 100 + 31, 60) == 101


def _world(n_exp=5, seed=3):
    """Random-walk prints; published values follow [T-60, T) rounded half up."""
    rng = np.random.default_rng(seed)
    T0 = 1_790_000_000
    secs = np.arange(T0 - 200, T0 + 900 * n_exp + 400)
    c = 8_400_000 + np.cumsum(rng.integers(-300, 301, len(secs)))
    prints = dict(zip(secs.tolist(), c.tolist()))
    rows = []
    for k in range(n_exp):
        T = T0 + 900 * k
        tot = sum(prints[s] for s in range(T - 60, T))
        ev = round_half_up_cents(tot, 60)
        for series in ("KXBTC15M", "KXBTCD"):
            rows.append({"ticker": f"{series}-{k}-X", "event_ticker": f"{series}-{k}", "series_ticker": series,
                         "close_ts_ms": T * 1000, "expected_expiration_ts_ms": (T + 300) * 1000, "result": "no",
                         "expiration_value": f"{ev / 100:.2f}"})
    return prints, pd.DataFrame(rows)


def test_check_identifies_the_production_window():
    prints, mk = _world()
    ex = expirations_from_markets(mk)
    assert len(ex) == 5 and (ex.ev_values == 1).all() and (ex.series == "KXBTC15M+KXBTCD").all()
    res = check(ex, prints)
    s = summarize(res).set_index("window")
    assert s.loc[PRODUCTION, "pass_rate"] == 1.0 and s.loc[PRODUCTION, "events_checked"] == 5
    assert s.loc[PRODUCTION, "max_abs_diff"] <= 0.005 + 1e-12
    assert s.loc["close_(T-60,T]", "pass_rate"] < 1.0  # the neighbouring window does not match
    assert s.loc["expected_[E-60,E)", "pass_rate"] < 1.0


def test_missing_print_is_not_observable_not_a_mismatch():
    prints, mk = _world(n_exp=2)
    T = int(mk.close_ts_ms.min()) // 1000
    del prints[T - 30]
    res = check(expirations_from_markets(mk), prints)
    assert np.isnan(res.loc[0, f"{PRODUCTION}:match"]) and res.loc[0, f"{PRODUCTION}:missing"] == 1
    assert res.loc[1, f"{PRODUCTION}:match"] == 1.0


def test_rounding_rule_check_counts_exact_ties():
    T = 1_790_000_000
    prints = {s: 100 for s in range(T - 60, T)}
    prints[T - 60] = 130  # sum = 6030 -> average 100.5 cents exactly
    mk = pd.DataFrame([{"ticker": "A-1-X", "event_ticker": "A-1", "series_ticker": "A", "close_ts_ms": T * 1000,
                        "expected_expiration_ts_ms": (T + 300) * 1000, "result": "yes", "expiration_value": "1.01"}])
    res = check(expirations_from_markets(mk), prints)
    out = rounding_rule_check(res, prints)
    assert {k: out[k] for k in ("ties", "half_up", "half_down", "half_even", "truncate")} == \
        {"ties": 1, "half_up": 1, "half_down": 0, "half_even": 0, "truncate": 0}
    assert cents("77,362.10") == 7736210
