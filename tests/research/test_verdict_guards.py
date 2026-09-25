"""Verdict guards (audit C2, M1, M7, M8, m9): every experiment's ACCEPT needs results under BOTH
fill policies B and C, no in-sample or synthetic inputs, P&L without shrinking volume, and
multiplicity control; the guarded text never carries the rule's own outcome. Null (zero-effect)
inputs must not ACCEPT on any verdict path."""

from __future__ import annotations

import json

import numpy as np
import pandas as pd

from dh.research import exp2_nowcast, exp3_toxicity, exp4_queue, exp9_multistrike, replay_grid
from dh.research.exp_common import Report
from dh.research.replay_env import Universe

NS = 10**9
T0 = 1_800_000_000 * NS
T1 = T0 + 4 * 86400 * NS


def _ledger(rng, shift_c: float, n_ev: int = 40, per: int = 10, t0: int = T0, t1: int = T1, sd: float = 3.0):
    ev = np.repeat([f"exp {i:03d}" for i in range(n_ev)], per)
    ts = np.sort(rng.integers(t0, t1, len(ev)))
    d = pd.DataFrame({"ts": ts, "event": ev, "ticker": ev, "contracts": 5.0, "net_c_per_ct": rng.normal(shift_c, sd, len(ev)),
                      "fee": 0.01, "settle": 1.0, "px": 0.5, "side": 1, "is_taker": False, "gross_edge_c": 1.0,
                      "tau_s": rng.uniform(60, 3600, len(ev)), "rv_1h": rng.uniform(0.3, 0.9, len(ev)),
                      "trade_id": [f"t{i}" for i in range(len(ev))]})
    d["net"] = d["net_c_per_ct"] * d["contracts"] / 100
    return d


def _run(name, p, df, days=4.0, collectors=None):
    s = {"net_c_per_contract": float((df.net_c_per_ct * df.contracts).sum() / df.contracts.sum()),
         "net_usd_per_day": float(df.net.sum()) / days, "contracts_per_day": float(df.contracts.sum()) / days,
         "fills_per_day": len(df) / days, "days": days, "warnings": []}
    return replay_grid.GridRun(name, p, df, s, collectors if collectors is not None else [pd.DataFrame()])


def _verdict(out, name):
    return json.loads((out / f"{name}_verdict.json").read_text())


# ------------------------------------------------------------------ Report guards (M7, m9)
def test_report_caps_accept_for_synthetic_in_sample_and_missing_policies(tmp_path):
    base = dict(decision_events=50, policies=["B", "C"])
    assert Report("x", "x", tmp_path, verdict="ACCEPT", **base).final_verdict() == "ACCEPT"
    for kw in ({"synthetic": True}, {"in_sample": True, "in_sample_why": "FV parameters in sample"},
               {"policies": ["B"]}, {"policies": ["A"]}):
        r = Report("x", "x", tmp_path, verdict="ACCEPT (rule text)", **{**base, **kw})
        fv = r.final_verdict()
        assert fv.startswith("INCONCLUSIVE") and "ACCEPT" not in fv, kw
        r.write()
        rec = _verdict(tmp_path, "x")
        assert rec["rule_outcome"].startswith("ACCEPT") and rec["outcome"] == "INCONCLUSIVE"
    # a REJECT is not capped by synthetic / in-sample status (only by evidence guards)
    assert Report("x", "x", tmp_path, verdict="REJECT", synthetic=True, in_sample=True, **base).final_verdict() == "REJECT"


# ------------------------------------------------------------------ C2 + M8: E4
def test_e4_needs_b_and_c_and_holm_across_preregistered_variants(tmp_path, monkeypatch):
    rng = np.random.default_rng(1)
    uni = Universe(root=str(tmp_path), t0=T0, t1=T1)

    def rv(root, t0, t1, variants, policies=("B", "C"), **kw):
        return [_run(v.name, p, _ledger(rng, 1.0 if v.name == "hysteresis_strong" else 0.0)) for v in variants for p in policies]

    monkeypatch.setattr(exp4_queue, "run_variants", rv)
    r = exp4_queue.run(str(tmp_path), T0, T1, tmp_path / "b", policies=["B"], universe=uni,
                       grid='{"hysteresis_strong": {}}')
    assert r["verdict"].startswith("INCONCLUSIVE") and "B and C" in r["verdict"]
    # Holm: three pre-registered variants; a p-value of 0.02 is significant alone but not after Holm
    tab = pd.DataFrame([{"variant": v, "policy": p, "d_p": pv, "d_net_vs_ref_c": 0.5, "d_usd_per_day_vs_ref": 1.0}
                        for p in ("B", "C") for v, pv in (("config", 0.02), ("hysteresis_strong", 0.5), ("age_only_5s", 0.6))])
    winners, _ = exp4_queue.e4_decision(tab, list(exp4_queue.KEEP_PRIORITY))
    assert winners == []
    tab.loc[tab.variant == "config", "d_p"] = 0.001
    assert exp4_queue.e4_decision(tab, list(exp4_queue.KEEP_PRIORITY))[0] == ["config"]


def test_e4_null_never_accepts(tmp_path, monkeypatch):
    rng = np.random.default_rng(2)
    uni = Universe(root=str(tmp_path), t0=T0, t1=T1)
    monkeypatch.setattr(exp4_queue, "run_variants", lambda root, t0, t1, variants, policies=("B", "C"), **kw:
                        [_run(v.name, p, _ledger(rng, 0.0)) for v in variants for p in policies])
    r = exp4_queue.run(str(tmp_path), T0, T1, tmp_path / "n", policies=["B", "C"], universe=uni)
    assert not r["verdict"].startswith("ACCEPT")


# ------------------------------------------------------------------ C2 + M8: E9
def _ps(rng, n=2000):
    t = T0 + np.arange(n) * 60 * NS
    return pd.DataFrame({"ts": t, "delta_btc": np.cumsum(rng.normal(0, 0.01, n)), "gross_delta_btc": 1.0,
                         "abs_position_ct": 1.0, "collateral_usd": 1.0, "net_position_ct": 0.0})


def test_e9_decides_on_paired_cis_not_point_estimates(tmp_path, monkeypatch):
    """'all' beats the reference on BOTH point estimates ($/day higher, turnover per contract
    lower) but by far less than the noise: the old point-estimate rule said IMPROVES."""
    rng = np.random.default_rng(3)
    uni = Universe(root=str(tmp_path), t0=T0, t1=T1)
    ref = _ledger(rng, 0.0, sd=6.0)
    alt = _ledger(rng, 0.0, sd=6.0)
    alt["net_c_per_ct"] += (ref.net_c_per_ct.mean() - alt.net_c_per_ct.mean()) + 0.05  # point estimate +0.05c
    alt["net"] = alt["net_c_per_ct"] * alt["contracts"] / 100
    ps_ref, ps_alt = _ps(rng), _ps(rng)
    ps_alt["delta_btc"] = ps_ref["delta_btc"] * 0.99 + rng.normal(0, 0.01, len(ps_ref))  # slightly less turnover (noisy)
    ps_alt["delta_btc"] *= np.abs(np.diff(ps_ref.delta_btc)).sum() * 0.99 / np.abs(np.diff(ps_alt.delta_btc)).sum()

    def rv(root, t0, t1, variants, policies=("B", "C"), **kw):
        return [_run(v.name, p, (alt if v.name == "all" else ref).copy(), collectors=[ps_alt if v.name == "all" else ps_ref])
                for v in variants for p in policies]

    monkeypatch.setattr(exp9_multistrike, "run_variants", rv)
    r = exp9_multistrike.run(str(tmp_path), T0, T1, tmp_path / "e9", policies=["B", "C"], universe=uni, counts=[1, 0])
    t = r["table"].set_index(["variant", "policy"])
    assert t.loc[("all", "B"), "usd_per_day"] > t.loc[("nearest_1", "B"), "usd_per_day"]  # point estimates say better
    assert t.loc[("all", "B"), "delta_turnover_per_ct"] < t.loc[("nearest_1", "B"), "delta_turnover_per_ct"]
    assert not r["rule_outcome"].startswith("ACCEPT") and "IMPROVES" not in r["verdict"]
    r = exp9_multistrike.run(str(tmp_path), T0, T1, tmp_path / "e9a", policies=["A"], universe=uni, counts=[1, 0])
    assert not r["verdict"].startswith("ACCEPT")


# ------------------------------------------------------------------ C2 + M1: E2
def _hook(**over):
    row = {"policy": "B", "d_net_c": 0.5, "d_net_lo_c": 0.2, "d_net_hi_c": 0.8, "contracts_day_base": 100.0,
           "contracts_day_nowcast": 100.0, "usd_day_base": 10.0, "usd_day_nowcast": 11.0, "events": 40}
    rows = [dict(row, **over), dict(row, policy="C", **over)]
    return pd.DataFrame(rows)


def test_e2_hook_needs_b_c_and_no_volume_or_dollar_drop():
    assert exp2_nowcast.hook_decision(_hook())[0]
    assert not exp2_nowcast.hook_decision(_hook(contracts_day_nowcast=40.0))[0]  # c/contract up, volume down
    assert not exp2_nowcast.hook_decision(_hook(usd_day_nowcast=9.0))[0]  # $/day down
    assert not exp2_nowcast.hook_decision(_hook(profitable_ct_day_base=60.0, profitable_ct_day_nowcast=50.0))[0]
    only_b = _hook().iloc[:1]
    assert not exp2_nowcast.hook_decision(only_b)[0]


def test_e2_null_hook_never_accepts(tmp_path, monkeypatch):
    rng = np.random.default_rng(4)
    uni = Universe(root=str(tmp_path), t0=T0, t1=T1)
    monkeypatch.setattr(replay_grid, "run_variants", lambda root, t0, t1, variants, policies=("B", "C"), **kw:
                        [_run(v.name, p, _ledger(rng, 0.0, t0=t0, t1=t1)) for v in variants for p in policies])
    monkeypatch.setattr(exp2_nowcast, "build_nowcast_panel", lambda *a, **k: exp2_nowcast.PanelResult(
        pd.DataFrame({"t": [T0], "g_med": [0.0], "y_0.5s": [0.0]}), pd.DataFrame(), pd.DataFrame()))
    monkeypatch.setattr(exp2_nowcast, "iter_index_and_venues", lambda *a, **k: iter(()))
    monkeypatch.setattr(exp2_nowcast, "evaluate_nowcasts", lambda panel, hs, **k: (pd.DataFrame(
        [{"horizon": f"{h:g}s", "model": "ridge", "rmse_gain_pct": 12.0} for h in hs]), pd.DataFrame({"t": [T0]})))
    r = exp2_nowcast.run(str(tmp_path), T0, T1, tmp_path / "e2", policies=["B", "C"], universe=uni)
    assert not r["verdict"].startswith("ACCEPT")


# ------------------------------------------------------------------ C2 + M1: E3
def test_e3_cancel_rule_needs_b_and_c_and_dollars_per_day():
    ok = dict(d_net_lo_c=0.2, d_net_c=0.3, fill_loss_pct=10.0, usd_day_guard=11.0, usd_day_base=10.0,
              profitable_ct_day_base=50.0, profitable_ct_day_guard=50.0)
    rows = pd.DataFrame([dict(ok, policy="B"), dict(ok, policy="C")])
    assert exp3_toxicity.e3_verdict(rows, pd.DataFrame(), None).startswith("ACCEPT")
    assert not exp3_toxicity.e3_verdict(rows.iloc[:1], pd.DataFrame(), None).startswith("ACCEPT")
    worse = rows.assign(usd_day_guard=9.0)
    assert not exp3_toxicity.e3_verdict(worse, pd.DataFrame(), None).startswith("ACCEPT")
    fewer_good = rows.assign(profitable_ct_day_guard=40.0)  # profitable contracts/day fell (TEST_MATRIX convention)
    assert not exp3_toxicity.e3_verdict(fewer_good, pd.DataFrame(), None).startswith("ACCEPT")
    lift = pd.DataFrame([{"policy": "B", "model": "logistic", "lift_hi": -0.001}])
    assert exp3_toxicity.e3_verdict(pd.DataFrame(), lift, None).startswith("REJECT")  # no OOS lift


def test_e3_rule_is_trained_on_policy_b_fills_only_with_label_embargo():
    t_split = T0 + 3600 * NS
    b = pd.DataFrame({"ts": [t_split - 120 * NS, t_split - 30 * NS, t_split + 5 * NS], "x": [1, 2, 3]})
    tr = exp3_toxicity.rule_training_fills({"A": b, "C": b, "B": b}, t_split)
    assert list(tr["x"]) == [1]  # the fill 30 s before the split has a 60 s label running into the scored half
    assert exp3_toxicity.rule_training_fills({"A": b, "C": b}, t_split).empty
