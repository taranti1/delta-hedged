"""Experiment 5 on realized fills (dh.research.exp5_hedge): known-answer tests of the hedge
evaluator on constructed fill streams with a known optimal hedge, the null cases that must never
ACCEPT, and the end-to-end run on the synthetic recording (replayed fills and session fills)."""

from __future__ import annotations

import importlib.util
import json
import math
import shutil
import sys
from dataclasses import replace
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from dh.core.events import KalshiFill
from dh.core.units import NS_PER_S
from dh.execution.latency import LatencyModel
from dh.research import exp5_hedge as e5
from dh.research.exp_common import SYNTHETIC_BANNER, Report
from dh.strategy.config import HedgeCfg, StrategyConfig
from dh.strategy.hedging import no_trade_band

NS = NS_PER_S
T0 = 1_800_000_000 * NS
REPO = Path(__file__).resolve().parents[2]
ZERO_LAT = LatencyModel.fixed(0.0, 0.0, 0.0, md_ms=0.0)
HCFG = HedgeCfg(band_min_btc=0.0, min_order_btc=0.001)


def _inputs(label: str = "B", *, n_events: int = 30, D0: float = 0.5, kalshi: str = "linear", sigma: float = 10.0,
            ev_len_s: int = 600, fill_at_s: int = 100, seed: int = 0) -> e5.HedgeInputs:
    """Back-to-back events of ev_len_s; one fill per event at fill_at_s carrying a CONSTANT delta D0
    until expiry. BTC: Gaussian random walk (sigma $/sqrt s) ticked every second. Kalshi leg:
    'linear' = D0 (S_T - S_fill) (a perfectly hedgeable exposure), 'noise' = independent of BTC."""
    rng = np.random.default_rng(seed)
    n_s = n_events * ev_len_s + 1
    ts = T0 + np.arange(n_s, dtype=np.int64) * NS
    S = 84_000.0 + np.concatenate([[0.0], np.cumsum(rng.normal(0.0, sigma, n_s - 1))])
    grid, fills, events = [], [], []
    for i in range(n_events):
        start = i * ev_len_s
        f, T = start + fill_at_s, start + ev_len_s
        e = f"exp {i:03d}"
        fills.append((int(ts[f]), e, "K", 1.0, D0, D0))
        for s in range(f, T):
            grid.append((int(ts[s]), e, D0, float(S[s]), sigma, float(T - s)))
        net = D0 * (S[T] - S[f]) if kalshi == "linear" else float(rng.normal(0.0, 50.0))
        events.append((e, int(ts[T]), float(net), 10.0, True, ""))
    return e5.HedgeInputs(label, pd.DataFrame(grid, columns=e5.GRID_COLS), pd.DataFrame(fills, columns=e5.FILL_COLS),
                          pd.DataFrame({"ts": ts, "value": S}), pd.DataFrame(events, columns=e5.EVENT_COLS))


def _run(inputs, policies, *, half_spread_bps: float = 0.0, lam_cfg: float = 1e-3):
    per_event, trades = e5.evaluate(inputs, policies, HCFG, latency=ZERO_LAT, half_spread_bps=half_spread_bps)
    return per_event, trades, e5.summarize(per_event, lam_cfg, (), n_boot=400)


def _row(tab, policy, name):
    r = tab[(tab.policy == policy) & (tab.hedge_policy == name)]
    assert len(r) == 1, (policy, name)
    return r.iloc[0]


# ------------------------------------------------------------------ known answers
def test_zero_cost_hedge_of_an_injected_linear_delta_removes_all_variance(tmp_path):
    """Injected: Kalshi P&L = D0 (S_T - S_fill), delta D0 until expiry. With zero cost and zero
    latency the optimal hedge is -D0 from the fill to settlement; hedging each fill and the band at
    c = 0 (B = 0) both reach it, so every event nets exactly 0 while no hedge carries D0^2 sigma^2 tau
    of variance. With a risk aversion at which that variance dominates the sampling noise of the
    mean (lam/2 var ~ 312 $ vs a mean s.e. of ~20 $), under B and C (same fills) the rule ACCEPTs and
    a non-synthetic, out-of-sample, 30-event report keeps the ACCEPT."""
    ins = [_inputs("B"), _inputs("C")]
    dec = e5.HedgePolicy("band", 0.0, 5e-2, 0.0, tag="configured")
    pols = [e5.HedgePolicy("none"), e5.HedgePolicy("per_fill", 0.0), dec]
    per_event, trades, tab = _run(ins, pols, lam_cfg=5e-2)
    for L in ("B", "C"):
        none, pf, band = (_row(tab, L, p.name) for p in pols)
        pe = per_event[(per_event.policy == L)]
        for p in (pf, band):
            assert pe.loc[pe.hedge_policy == p.hedge_policy, "total_usd"].abs().max() < 1e-6
            assert p.var_ratio_vs_none < 1e-12 and p.d_util_lo > 0 and p.hedge_fees_c == 0
        exp_var = 0.5**2 * 10.0**2 * 500  # D0^2 sigma^2 (T - fill)
        assert none.sd_usd_event**2 == pytest.approx(exp_var, rel=0.35)
        # the band's first trade is the full hedge -D0 at the fill's decision step, then the unwind
        tb = trades[(trades.policy == L) & (trades.hedge_policy == dec.name)]
        assert np.allclose(tb.groupby("event")["trade_btc"].first(), -0.5)
        assert all(r == ["band", "unwind"] for r in tb.groupby("event")["reason"].apply(list))
    d = tab[tab.hedge_policy == dec.name]
    verdict = e5.e5_verdict(d)
    assert verdict.startswith("ACCEPT")
    rep = Report("e5_hedge", "x", tmp_path, verdict=verdict, decision_events=int(d.events.min()), policies=["B", "C"],
                 in_sample=False)
    assert rep.final_verdict().startswith("ACCEPT")
    rep_syn = Report("e5_hedge", "x", tmp_path, verdict=verdict, decision_events=30, policies=["B", "C"], synthetic=True)
    assert rep_syn.final_verdict().startswith("INCONCLUSIVE")  # a synthetic recording caps it


def test_band_trades_to_the_known_band_edge():
    """Constant delta D0 = 5 BTC, cost c = 5 bp: the mean-variance band B = 2 c S / (lam sigma^2 h)
    is known at the first decision step; the evaluator trades back to its edge, -(D0 - B), and then
    holds (B widens as h shrinks) until the unwind at settlement."""
    D0, lam, fee = 5.0, 1e-2, 5.0
    inp = _inputs("B", n_events=3, D0=D0, kalshi="linear")
    pol = e5.HedgePolicy("band", fee, lam)
    _, trades = e5.simulate_hedge(inp, pol, HCFG, latency=ZERO_LAT, half_spread_bps=0.0)
    g0 = inp.grid.groupby("event").first()
    for e, tb in trades.groupby("event"):
        r = g0.loc[e]
        B = no_trade_band(replace(HCFG, enabled=True, fee_bps_maker=fee, half_spread_bps=0.0), spot=r.S,
                          sigma_abs_per_sqrt_s=r.sigma_1s, h_eff_s=r.tau_s, lam=lam)
        assert 0 < B < D0
        assert list(tb.reason) == ["band", "unwind"]
        assert tb.trade_btc.iloc[0] == pytest.approx(-(D0 - B), rel=1e-9)
        assert tb.trade_btc.iloc[1] == pytest.approx(D0 - B, rel=1e-9)


def test_pending_hedge_is_not_double_counted_when_the_ack_beats_the_fill():
    """The venue's REST 'filled' update (response 0 ms) arrives before its ws HedgeFill (50 ms): the
    in-flight hedge must be counted exactly once, so the band still hedges once (-D0) and unwinds,
    with nothing left over."""
    inp = _inputs("B", n_events=5)
    lat = LatencyModel.fixed(0.0, 0.0, 50.0, md_ms=0.0)
    for pol in (e5.HedgePolicy("band", 0.0, 1e-3), e5.HedgePolicy("per_fill", 0.0)):
        pe, tr = e5.simulate_hedge(inp, pol, HCFG, latency=lat, half_spread_bps=0.0)
        assert all(r == [pol.kind if pol.kind == "band" else "fill", "unwind"]
                   for r in tr.groupby("event")["reason"].apply(list)), pol.name
        assert np.allclose(pe.turnover_btc, 1.0) and (pe.residual_btc.abs() < 1e-12).all()


def test_zero_delta_flow_makes_no_hedge_optimal_and_never_accepts(tmp_path):
    """Null: fills with zero delta. No policy has anything to hedge: zero turnover, the utility gain
    is exactly 0 and the rule REJECTs (hedge stays disabled); never an ACCEPT."""
    ins = [_inputs("B", D0=0.0, kalshi="noise"), _inputs("C", D0=0.0, kalshi="noise", seed=1)]
    dec = e5.HedgePolicy("band", 0.0, 1e-3, 0.0, tag="configured")
    pols = [e5.HedgePolicy("none"), e5.HedgePolicy("per_fill", 0.0), e5.HedgePolicy("band", 5.0, 1e-2), dec]
    per_event, trades, tab = _run(ins, pols)
    assert len(trades) == 0 and (tab.turnover_btc_per_event == 0).all() and (tab.d_util_usd == 0).all()
    v = e5.e5_verdict(tab[tab.hedge_policy == dec.name])
    assert v.startswith("REJECT") and "never trades" in v
    assert Report("x", "x", tmp_path, verdict=v, decision_events=30, policies=["B", "C"]).final_verdict().startswith("REJECT")


def test_hedging_a_delta_the_kalshi_leg_does_not_carry_never_accepts():
    """Null: the fills report a delta but their P&L is independent of BTC (a mis-specified delta):
    hedging adds variance, the utility gain is negative and the rule never ACCEPTs."""
    ins = [_inputs("B", D0=5.0, kalshi="noise"), _inputs("C", D0=5.0, kalshi="noise", seed=3)]
    dec = e5.HedgePolicy("band", 0.0, 1e-2, 0.0, tag="configured")
    _, _, tab = _run(ins, [e5.HedgePolicy("none"), dec], lam_cfg=1e-2)
    d = tab[tab.hedge_policy == dec.name]
    assert (d.turnover_btc_per_event > 0).all() and (d.d_util_hi < 0).all() and (d.var_ratio_vs_none > 1).all()
    v = e5.e5_verdict(d)
    assert v.startswith("REJECT") and not v.startswith("ACCEPT")


def test_a_cost_above_break_even_never_hedges():
    """The band edge grows linearly in the cost c: above the break-even c* = |D| lam sigma^2 h / (2 S)
    (the band edge at the widest reachable |D|) the band never trades and equals no hedge, while
    hedging each fill at that cost only loses."""
    D0, lam, sigma = 5.0, 1e-2, 10.0
    inp = _inputs("B", D0=D0, sigma=sigma)
    S_max, h_max = float(inp.grid.S.max()), float(inp.grid.tau_s.max())
    c_star_bps = 1e4 * D0 * lam * sigma**2 * h_max / (2 * S_max)  # ~ 150 bp here
    lo, hi = c_star_bps / 30, 1.5 * c_star_bps
    pols = [e5.HedgePolicy("none"), e5.HedgePolicy("band", lo, lam), e5.HedgePolicy("band", hi, lam),
            e5.HedgePolicy("per_fill", hi)]
    per_event, trades, tab = _run([inp], pols, lam_cfg=lam)
    assert _row(tab, "B", pols[1].name).turnover_btc_per_event > 0
    never = _row(tab, "B", pols[2].name)
    assert never.turnover_btc_per_event == 0 and never.d_util_usd == 0
    pe = per_event.set_index(["hedge_policy", "event"])["total_usd"]
    assert (pe.loc[pols[2].name].values == pe.loc["none"].values).all()
    pf = _row(tab, "B", pols[3].name)
    assert pf.hedge_fees_c > 0 and pf.mean_usd_event < _row(tab, "B", "none").mean_usd_event


def test_policy_grid_holds_the_configured_decision_policy():
    cfg = StrategyConfig()
    g = e5.policy_grid(cfg, fees_bps=(1.0,), lams=(1e-3,), decision_fee_bps=0.8)
    names = [p.name for p in g]
    dec = e5.decision_policy(cfg, 0.8)
    assert names[0] == "none" and dec.name in names and dec.band_min_btc == cfg.hedge.band_min_btc
    assert dec.lam == pytest.approx(cfg.lam) and any(p.kind == "per_fill" and p.fee_bps == 0.8 for p in g)
    assert {p.lam for p in g if p.kind == "band"} == {1e-3, cfg.lam}
    with pytest.raises(ValueError):
        e5.HedgePolicy("continuous")


def test_paired_stat_ci_covers_a_zero_utility_gain_and_detects_a_real_one():
    rng = np.random.default_rng(5)
    a = rng.normal(0, 10, 60)
    ci0 = e5.paired_stat_ci(lambda x, y: e5._util(y, 1e-3) - e5._util(x, 1e-3), a, a.copy())
    assert (ci0.mean, ci0.lo, ci0.hi) == (0.0, 0.0, 0.0)
    ci = e5.paired_stat_ci(lambda x, y: e5._util(y, 1e-2) - e5._util(x, 1e-2), a, 0.2 * a)
    assert ci.lo > 0 and ci.clusters == 60


# ------------------------------------------------------------------ end to end (SYNTHETIC recording)
def _load_cli():
    spec = importlib.util.spec_from_file_location("_run_experiment_cli_e5", REPO / "scripts" / "run_experiment.py")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


def test_e5_flags_are_rejected_by_other_commands():
    cli = _load_cli()
    with pytest.raises(SystemExit):
        cli.main(["e4", "--root", "/nonexistent", "--hedge-fee-bps", "1"])
    with pytest.raises(SystemExit):
        cli.main(["all", "--root", "/nonexistent", "--session-fills", "live"])


def test_e5_runs_on_the_synthetic_recording_via_the_cli(tiny_rec, tmp_path):
    cli = _load_cli()
    out = tmp_path / "e5"
    assert cli.main(["e5", "--root", tiny_rec.root, "--config", "synthetic", "--out", str(out), "--jobs", "2",
                     "--hedge-scales", "1,25", "--hedge-fees-bps", "1,12", "--hedge-lams", "1e-2"]) == 0
    md = (out / "e5_hedge.md").read_text()
    assert SYNTHETIC_BANNER in md and "**Decision rule" in md and "band_configured" in md
    assert "fill-intensity parameters" in md and "prior" in md
    v = json.loads((out / "e5_hedge_verdict.json").read_text())
    assert v["outcome"] == "INCONCLUSIVE" and v["policies"] == ["B", "C"]  # 2 events < 20; synthetic
    assert not v["verdict"].startswith("ACCEPT")
    tab = pd.read_csv(out / "e5_hedge_policies.csv")
    assert tab["synthetic"].all() and set(tab.scale) == {1.0, 25.0} and set(tab.policy) == {"B", "C"}
    ev = pd.read_csv(out / "e5_hedge_events.csv")
    none = ev[ev.hedge_policy == "none"]
    assert len(none) and (none.hedge_pnl == 0).all() and (none.turnover_btc == 0).all()
    # the same fills under every hedge policy: the Kalshi leg is identical across policies
    assert ev.groupby(["policy", "scale", "event"])["kalshi_usd"].nunique().max() == 1


def test_session_fills_reproduce_the_replayed_hedge_inputs(tiny_rec, synth_cfg, tmp_path):
    """The session-fill path (FvProbe + positions rebuilt from fills) reproduces the replay path's
    per-event Kalshi leg exactly and its delta path closely, for the same fills (a replay ledger
    CSV, and the same fills written as a paper session's events.paper)."""
    from dh.research.replay_env import build_universe, run_replay
    from dh.store.recorder import Recorder

    uni = build_universe(tiny_rec.root, tiny_rec.t0, tiny_rec.t1)
    res = run_replay(tiny_rec.root, tiny_rec.t0, tiny_rec.t1, synth_cfg, "B", universe=uni,
                     collectors=[e5.HedgeInputCollector])
    rep_inp = e5.inputs_from_replay("B", res.extras["collectors"][0], res.df, tiny_rec.t1)
    assert len(res.df) and rep_inp.included["event"].nunique() >= 1
    led = tmp_path / "ledger_B.csv"
    res.df.to_csv(led, index=False)
    fl = e5.session_fill_table(tiny_rec.root, tiny_rec.t0, tiny_rec.t1, uni, str(led))
    ses = e5.session_inputs(tiny_rec.root, tiny_rec.t0, tiny_rec.t1, synth_cfg, uni, fl, label="B", source="ledger")
    a = rep_inp.included.set_index("event")
    b = ses.included.set_index("event")
    assert set(a.index) == set(b.index)
    assert np.allclose(a.loc[b.index, "kalshi_net_usd"], b["kalshi_net_usd"], atol=1e-9)
    for e in a.index:
        ga = rep_inp.grid[rep_inp.grid.event == e].sort_values("ts")
        gb = ses.grid[ses.grid.event == e].sort_values("ts")
        m = pd.merge_asof(ga, gb[["ts", "D_btc"]], on="ts", suffixes=("", "_s"), direction="nearest",
                          tolerance=NS)
        m = m.dropna()
        assert len(m) > 20
        assert np.corrcoef(m.D_btc, m.D_btc_s)[0, 1] > 0.9, e
        assert (m.D_btc - m.D_btc_s).abs().median() < 0.2 * m.D_btc.abs().median() + 1e-6
    # a paper session: the same fills as KalshiFill events on events.paper of a copy of the recording
    root = tmp_path / "rec"
    shutil.copytree(tiny_rec.root, root)
    rec = Recorder(root, start=False)
    for i, r in enumerate(fl.itertuples()):
        rec.write_event("events.paper", KalshiFill(int(r.ts), int(r.ts), r.ticker, f"t{i}", f"o{i}", f"c{i}",
                                                   "bid" if r.side > 0 else "ask", int(round(r.px * 10_000)),
                                                   int(round(r.contracts * 100)), False, int(round(r.fee * 1e6)), 0))
    rec.close()
    paper = e5.session_fill_table(root, tiny_rec.t0, tiny_rec.t1, uni, "paper")
    assert len(paper) == len(fl) and np.allclose(paper.sort_values(["ts", "ticker"]).px, fl.sort_values(["ts", "ticker"]).px)
    assert math.isclose(float(paper.contracts.sum()), float(fl.contracts.sum()))
