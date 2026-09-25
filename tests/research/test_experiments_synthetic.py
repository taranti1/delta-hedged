"""Known-answer tests: each experiment recovers the SIGN of an effect injected into a synthetic
recording (pipeline validation only; never evidence of edge), plus an end-to-end smoke run of
every experiment runner and of the CLI on the tiny recording."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import numpy as np
import pytest

from dh.research import exp2_nowcast as e2
from dh.research import exp3_toxicity as e3
from dh.research import exp8_taker as e8
from dh.research.exp_common import SYNTHETIC_BANNER
from dh.research.replay_env import build_universe
from dh.research.synth_recording import SynthRecordingConfig, write_synthetic_recording
from dh.sim.synthetic import SynthConfig

REPO = Path(__file__).resolve().parents[2]


def _rec(tmp_path_factory, name: str, *, n_events: int = 1, spacing: int = 300, seed: int = 5, **synth):
    base = dict(n_strikes_each_side=2, mm_lag_s=1.5, informed=True, informed_edge_ticks=1.0, vol_ann=0.6,
                mm_update_prob=0.3, noise_taker_rate_per_s=0.08)
    base.update(synth)
    return write_synthetic_recording(tmp_path_factory.mktemp(name), SynthRecordingConfig(
        n_events=n_events, event_spacing_s=spacing, seed=seed, synth=SynthConfig(**base)))


def _gain(info, h: str) -> float:
    uni = build_universe(info.root, info.t0, info.t1, own_fill_scan=False)
    pr = e2.build_nowcast_panel(e2.iter_index_and_venues(info.root, info.t0, info.t1), info.t0, info.t1,
                                expirations=uni.expirations())
    m, _ = e2.evaluate_nowcasts(pr.panel, models=("ridge",))
    return float(m[(m.horizon == h) & (m.model == "ridge")].rmse_gain_pct.iloc[0])


def test_e2_nowcast_gain_follows_benchmark_publication_delay(tiny_rec, tmp_path_factory):
    """Injected: the benchmark prints 250 ms after the venues move -> venues predict the next prints.
    Removing the delay (benchmark as fast as the venues) must shrink the gain."""
    uni = build_universe(tiny_rec.root, tiny_rec.t0, tiny_rec.t1)
    pr = e2.build_nowcast_panel(e2.iter_index_and_venues(tiny_rec.root, tiny_rec.t0, tiny_rec.t1), tiny_rec.t0,
                                tiny_rec.t1, expirations=uni.expirations())
    m, preds = e2.evaluate_nowcasts(pr.panel, models=("ridge",))
    g = m.set_index(["horizon", "model"]).rmse_gain_pct
    assert g[("0.2s", "ridge")] > 10 and g[("0.2s", "median_mid")] > 0 and g[("0.5s", "ridge")] > 0
    assert g[("0.2s", "last_print")] == 0
    assert 0.5 < e2.fit_beta(pr.panel, 0.5) < 1.5  # the P&L hook's venue-gap weight
    wa = e2.window_average_eval(pr.window_rows, pr.panel, preds, "ridge_y_1s")
    assert len(wa) and set(wa.tau_bucket.astype(str)) <= {"0-15s", "15-30s", "30-60s", "60-120s"}
    fast = _rec(tmp_path_factory, "e2_fast", brti_delay_ms=30)
    assert _gain(fast, "0.2s") < g[("0.2s", "ridge")] - 10


@pytest.mark.slow
def test_e8_taking_edge_follows_maker_lag(tmp_path_factory):
    """Injected: background makers quote off BTC lagged by mm_lag_s (no informed takers to clean up).
    A 3 s lag must create many more +EV taking opportunities than a 0.2 s lag, and the fills of
    those takes must mark out positively 1 s later (after the exact taker fee)."""
    out = {}
    for lag in (0.2, 3.0):
        info = _rec(tmp_path_factory, f"e8_lag{lag}", mm_lag_s=lag, informed=False, mm_update_prob=0.5)
        r = e8.scan(info.root, info.t0, info.t1, _cfg(), policy="B")
        s = e8.summarize(r)
        out[lag] = (s[s.threshold_c == 0.0].iloc[0], r.takes)
    assert out[3.0][0].opportunities > 3 * max(out[0.2][0].opportunities, 1)
    f = out[3.0][1][out[3.0][1].contracts > 0]
    assert len(f) >= 10 and np.average(f.net_1s_c, weights=f.contracts) > 0


@pytest.mark.slow
def test_e3_informed_flow_makes_shadow_fills_toxic(tmp_path_factory):
    """Injected: latency takers pick off stale quotes (including ours). Same seed and price path with
    and without informed flow: shadow fills must mark out worse with it."""
    res = {}
    for inf in (False, True):
        info = _rec(tmp_path_factory, f"e3_inf{inf}", n_events=2, informed=inf, informed_edge_ticks=0.5, vol_ann=0.9)
        df, s = e3.replay_fill_table(info.root, info.t0, info.t1, _cfg(), "B")
        assert len(df) >= 15 and {"adv_ext_1s", "queue_at_place_ct", "mkpx_10s_c", "toxic"} <= set(df.columns)
        res[inf] = (np.average(df.net_mk_10s_c, weights=df.contracts), s["markout_10s_c"])
    assert res[True][0] < res[False][0]
    assert res[True][1] < res[False][1]


def _cfg():
    from dh.research.synth_recording import synth_strategy_config

    return synth_strategy_config("KXBTCD")


def _load_cli():
    spec = importlib.util.spec_from_file_location("_run_experiment_cli", REPO / "scripts" / "run_experiment.py")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.mark.slow
def test_every_experiment_runs_end_to_end_on_the_synthetic_recording(tiny_rec, tmp_path):
    cli = _load_cli()
    base = ["--root", tiny_rec.root, "--config", "synthetic"]
    pol = ["--policies", "B", "--jobs", "2"]
    runs = {
        "e1": [],
        "e2": ["--no-pnl", *pol],
        "e3": pol,
        "e4": ["--grid", '{"config": {}}', *pol],
        "e67": pol,
        "e8": pol,
        "e9": ["--strikes", "1,0", *pol],
        "e10": ["--multipliers", "1,4", *pol],
    }
    for name, extra in runs.items():
        assert cli.main([name, *base, "--out", str(tmp_path / name), *extra]) == 0
    reports = {p.parent.name: p.read_text() for p in tmp_path.glob("*/*.md")}
    assert set(reports) == set(runs)
    for name, md in reports.items():
        assert SYNTHETIC_BANNER in md and "**Verdict:**" in md and "**Decision rule" in md, name
        assert "**Verdict:** ACCEPT" not in md, name  # synthetic data, policy B only: never an ACCEPT
    assert "recenter_always" in reports["e4"] and "nearest_1" in reports["e9"] and "x4" in reports["e10"]
    for name in ("e3", "e4", "e8", "e9", "e10"):  # regime splits (audit m8)
        reg = next((tmp_path / name).glob("*regimes.csv"), None)
        assert reg is not None, name
    for p in tmp_path.glob("*/*.csv"):
        assert p.read_text().startswith("synthetic"), p  # leading synthetic column (header only if empty)


class _ConstModel:
    """Stub classifier: P(toxic) = p for every row (exercise the guard hook deterministically)."""

    def __init__(self, p: float) -> None:
        self.p = p

    def predict_proba(self, X):
        X = np.asarray(X)
        return np.column_stack([np.full(len(X), 1 - self.p), np.full(len(X), self.p)])


@pytest.mark.slow
def test_toxicity_guard_hook_pulls_quotes_only_when_scored_toxic(tiny_rec, synth_cfg):
    from dh.research.replay_env import run_replay

    base = run_replay(tiny_rec.root, tiny_rec.t0, tiny_rec.t1, synth_cfg, "B")
    never = run_replay(tiny_rec.root, tiny_rec.t0, tiny_rec.t1, synth_cfg, "B", strategy_factory=e3.ToxicityGuardMM,
                       factory_kwargs={"rule": e3.ToxicityRule(_ConstModel(0.0), e3.FEATURES, 0.5)})
    always = run_replay(tiny_rec.root, tiny_rec.t0, tiny_rec.t1, synth_cfg, "B", strategy_factory=e3.ToxicityGuardMM,
                        factory_kwargs={"rule": e3.ToxicityRule(_ConstModel(1.0), e3.FEATURES, 0.5)})
    assert len(base.df) > 0
    assert never.df[["ts", "ticker", "px", "contracts"]].equals(base.df[["ts", "ticker", "px", "contracts"]])
    assert len(always.df) == 0 and always.summary["mm_quotes_placed"] == 0
    assert always.summary["mm_top_reasons"].get("toxicity_pull", 0) > 0
