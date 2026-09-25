"""Segment searches under multiplicity control (audit C5) and E0's exact maker fee (M3).

E0: >= 200 settlement events per segment, Holm across every segment, simultaneous bounds.
E6/E7: Holm within each table family, and 'quote' only after confirmation on a disjoint later
sample. Null inputs (zero edge everywhere) must never ACCEPT or recommend 'quote'."""

from __future__ import annotations

import json
import math

import numpy as np
import pandas as pd
import pytest
from scipy.stats import norm

from dh.research import exp0_maker_pnl as e0
from dh.research.exp67_segments import ledger_status, segment_tables, split_by_expiration


# ------------------------------------------------------------------ E0
def _null_trades(seed: int, n_events: int = 400):
    """Trades at the exact fair price (zero maker edge; net < 0 after fees), several strikes."""
    rng = np.random.default_rng(seed)
    tr, mk = [], []
    sig = 0.5 / math.sqrt(365 * 24 * 3600) * 60000
    for e in range(n_events):
        T = 1_790_000_000_000 + e * 3_600_000
        path = 60000 + np.cumsum(rng.normal(0, sig, 3600))
        for k in range(-3, 4):
            K = 60000 + 150 * k
            tk = f"KXBTCD-E{e}-T{K}"
            mk.append(dict(ticker=tk, event_ticker=f"KXBTCD-E{e}", expiration_ts_ms=T,
                           result="yes" if path[-1] > K else "no", strike_type="greater", floor_strike=float(K),
                           cap_strike=np.nan))
            ts = np.sort(rng.integers(0, 3590, rng.poisson(20)))
            p = norm.cdf((path[ts] - K) / (sig * np.sqrt(3600 - ts)))
            keep = (p > 0.03) & (p < 0.97)
            for t_, x in zip(ts[keep], np.round(p[keep] * 1e4).astype(int)):
                tr.append(dict(ticker=tk, ts_ms=T - 3_600_000 + int(t_) * 1000, yes_px=int(x),
                               qty=100 * int(rng.integers(1, 20)), taker_side="yes" if rng.random() < 0.5 else "no"))
    return pd.DataFrame(tr), pd.DataFrame(mk)


@pytest.mark.parametrize("seed", [1, 2, 3, 4])
def test_e0_null_never_accepts_and_examines_only_200_event_segments(tmp_path, seed):
    tr, mk = _null_trades(seed)
    tabs = e0.run(tr, mk, None, tmp_path, n_boot=200)
    rows = pd.concat([t for t in tabs.values() if len(t)], ignore_index=True)
    assert (rows["events"] >= 200).all()
    assert not (rows["net_lo_c"] > 0.15).any()  # simultaneous bounds
    v = json.loads((tmp_path / "exp0_verdict.json").read_text())
    assert not v["verdict"].startswith("ACCEPT")


def test_e0_clusters_by_expiration_across_series():
    tr = pd.DataFrame([dict(ticker=t, ts_ms=1000, yes_px=5000, qty=100, taker_side="yes")
                       for t in ("KXBTCD-A-T1", "KXBTC-A-B1", "KXBTC15M-A-T1")])
    mk = pd.DataFrame([dict(ticker=t, event_ticker=t.rsplit("-", 1)[0], expiration_ts_ms=3_600_000, result="no",
                            strike_type="greater", floor_strike=1.0, cap_strike=np.nan)
                       for t in ("KXBTCD-A-T1", "KXBTC-A-B1", "KXBTC15M-A-T1")])
    df = e0.prepare(tr, mk)
    assert df["cluster"].nunique() == 1  # three series, one expiration -> one settlement event


def test_e0_maker_fee_is_the_exact_rounded_order_fee_by_series():
    tr = pd.DataFrame([dict(ticker="KXBTCD-A-T1", ts_ms=1000, yes_px=5000, qty=100, taker_side="yes"),
                       dict(ticker="KXBTC-A-B1", ts_ms=1000, yes_px=5000, qty=100, taker_side="yes")])
    mk = pd.DataFrame([dict(ticker=t, event_ticker="A", expiration_ts_ms=3_600_000, result="no", strike_type="greater",
                            floor_strike=1.0, cap_strike=np.nan) for t in ("KXBTCD-A-T1", "KXBTC-A-B1")])
    df = e0.prepare(tr, mk, fee_types={"KXBTCD": ("quadratic_with_maker_fees", 1.0), "KXBTC": ("quadratic", 1.0)})
    fee = dict(zip(df.ticker, df.maker_fee))
    # 1 contract at 50c: exact 0.4375c, the order pays it rounded up to the cent
    assert fee["KXBTCD-A-T1"] == pytest.approx(0.01) and fee["KXBTC-A-B1"] == pytest.approx(0.0)
    assert e0.prepare(tr, mk, maker_rate=0.0).maker_fee.max() == 0.0  # legacy flag still maps to a fee type


# ------------------------------------------------------------------ E6 / E7
def _ledger(rng, n_events=300, edge_bucket=None, edge_c=0.0, t0=0):
    rows = []
    for e in range(n_events):
        p_true = rng.uniform(0.05, 0.95)
        settle = float(rng.random() < p_true)
        n = 1 + rng.poisson(12)
        side = rng.choice([-1, 1], n)
        tau = rng.uniform(20, 3800, n)
        net = 100 * side * (settle - p_true)
        if edge_bucket is not None:
            net = net + np.where((tau >= edge_bucket[0]) & (tau < edge_bucket[1]), edge_c, 0.0)
        rows.append(pd.DataFrame({"event": f"exp {t0 + e}", "expiration_ns": (t0 + e) * 3600 * 10**9,
                                  "contracts": rng.integers(1, 10, n).astype(float), "net_c_per_ct": net, "tau_s": tau,
                                  "z": rng.normal(0, 1.2, n), "px": np.clip(p_true + rng.normal(0, 0.03, n), 0.02, 0.98),
                                  "fee": 0.0, "gross_edge_c": 0.0, "settle": settle}))
    d = pd.concat(rows, ignore_index=True)
    d["net"] = d.net_c_per_ct * d.contracts / 100
    return d


@pytest.mark.parametrize("seed", range(6))
def test_e67_null_never_recommends_quote_even_with_confirmation(seed):
    rng = np.random.default_rng(seed)
    sel = _ledger(rng)
    conf = _ledger(rng, t0=1000)
    tabs = segment_tables({"B": sel, "C": sel.sample(frac=0.7, random_state=seed)}, 12.5,
                          confirm={"B": conf, "C": conf.sample(frac=0.7, random_state=seed)}, confirm_days=12.5)
    for t in tabs.values():
        assert not (t.get("recommendation", pd.Series(dtype=str)) == "quote").any()


def test_e67_real_edge_is_quoted_only_after_confirmation():
    rng = np.random.default_rng(11)
    edge = ((600.0, 1800.0), 12.0)  # +12c/contract in the 10-30m bucket
    sel = _ledger(rng, 400, *edge)
    conf = _ledger(rng, 400, *edge, t0=1000)
    no_conf = segment_tables({"B": sel, "C": sel}, 12.5)["tau"]
    rec = no_conf.groupby("tau_bucket", observed=True)["recommendation"].first()
    assert rec["10-30m"] == "candidate (not confirmed)"
    tau = segment_tables({"B": sel, "C": sel}, 12.5, confirm={"B": conf, "C": conf}, confirm_days=12.5)["tau"]
    rec = tau.groupby("tau_bucket", observed=True)["recommendation"].first()
    assert rec["10-30m"] == "quote" and (rec.drop("10-30m") != "quote").all()


def test_e67_chronological_split_keeps_settlement_events_whole():
    d = pd.DataFrame({"event": ["exp a", "exp a", "exp b"], "expiration_ns": [10, 10, 30], "ts": [1, 12, 25]})
    a, b = split_by_expiration({"B": d}, 20)
    assert list(a["B"]["event"]) == ["exp a", "exp a"] and list(b["B"]["event"]) == ["exp b"]


def test_ledger_status_columns_reach_the_verdict():
    d = pd.DataFrame({"event": ["exp a"], "synthetic": [True], "fv_status": ["out_of_sample"], "flow_status": ["defaults"]})
    assert ledger_status({"B": d}) == (True, False, "")
    old = d.drop(columns=["fv_status", "flow_status"])
    syn, ins, why = ledger_status({"B": old})
    assert ins and "status unknown" in why
    ins2 = ledger_status({"B": d.assign(fv_status="in_sample")})
    assert ins2[1] and "FV parameters in sample" in ins2[2]
