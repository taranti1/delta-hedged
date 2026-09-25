"""E8 threshold policies and exact fees (audit M2, M3), E3 match-time features (m1), CLI latency
threading and flag checking (C3). SYNTHETIC recordings: pipeline checks only."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

import dh.research.replay_env as re_
import dh.research.replay_grid as rg
from dh.research import exp3_toxicity as e3
from dh.research import exp8_taker as e8
from dh.research.synth_recording import SynthRecordingConfig, write_synthetic_recording
from dh.sim.synthetic import SynthConfig

REPO = Path(__file__).resolve().parents[2]
NS = 10**9


@pytest.fixture(scope="module")
def taker_rec(tmp_path_factory):
    return write_synthetic_recording(tmp_path_factory.mktemp("taker_rec"), SynthRecordingConfig(
        n_events=2, event_spacing_s=300, seed=5,
        synth=SynthConfig(n_strikes_each_side=2, mm_lag_s=1.5, informed=False, vol_ann=0.6, mm_update_prob=0.3,
                          noise_taker_rate_per_s=0.08)))


@pytest.mark.slow
def test_e8_decides_on_the_scan_of_its_own_threshold_with_exact_order_fees(taker_rec, synth_cfg, tmp_path):
    res = e8.run(taker_rec.root, taker_rec.t0, taker_rec.t1, tmp_path, cfg=synth_cfg, policies=("B", "C"),
                 thresholds_c=(0.0, 0.5))
    own = e8.scan(taker_rec.root, taker_rec.t0, taker_rec.t1, synth_cfg, policy="B", min_threshold_c=0.5)
    want = e8.summarize(own, thresholds_c=(0.5,)).iloc[0]
    got = res["summary"].query("policy == 'B' and threshold_c == 0.5").iloc[0]
    assert got.opportunities == want.opportunities and got.contracts == want.contracts
    zero = e8.scan(taker_rec.root, taker_rec.t0, taker_rec.t1, synth_cfg, policy="B", min_threshold_c=0.0)
    assert e8.summarize(zero, thresholds_c=(0.5,)).iloc[0].opportunities != want.opportunities  # the old decision row
    # exact per-order fees: with whole-cent prices and whole contracts every order's fee is whole cents
    tk = own.takes[own.takes.contracts > 0]
    assert len(tk)
    fee_cents = tk.fee_c * tk.contracts
    assert np.allclose(fee_cents, np.round(fee_cents), atol=1e-6)
    assert tk.event.str.startswith("exp ").all()


@pytest.mark.slow
def test_e3_features_are_captured_at_match_time(tiny_rec, synth_cfg):
    df, summ = e3.replay_fill_table(tiny_rec.root, tiny_rec.t0, tiny_rec.t1, synth_cfg, "B")
    assert len(df) and (df["at_match"] == 1).all()
    # match time on the recorded timeline = exchange-time fill stamp + the market-data offset (< delivery)
    off = (df["t_match"] - df["ts"]).to_numpy()
    assert np.allclose(off, off[0]) and 0 < off[0] < 100_000_000


def _load_cli():
    spec = importlib.util.spec_from_file_location("_cli_fix", REPO / "scripts" / "run_experiment.py")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


def test_cli_threads_latency_to_every_replay_and_rejects_unused_flags(tmp_path, monkeypatch):
    seen = []
    monkeypatch.setattr(re_, "build_universe", lambda root, t0, t1, **k: re_.Universe(root=root, t0=t0, t1=t1))

    def fake_run_replay(root, t0, t1, cfg, policy, latency=None, **kw):
        seen.append(latency)
        return re_.ReplayResult(pd.DataFrame(columns=["settle", "contracts", "net_c_per_ct", "event", "net"]),
                                {"days": 1.0, "warnings": []}, None, {"collectors": [pd.DataFrame()]})

    monkeypatch.setattr(rg, "run_replay", fake_run_replay)
    cli = _load_cli()
    T0 = 1_800_000_000 * NS
    common = ["--root", str(tmp_path), "--t0", str(T0), "--t1", str(T0 + 3600 * NS)]
    for name, extra in (("e4", ["--grid", '{"config": {}}']), ("e10", ["--multipliers", "1,2"])):
        seen.clear()
        cli.main([name, *common, "--out", str(tmp_path / name), "--latency-ms", "250,250,80,40", *extra])
        assert seen and all(x is not None for x in seen)
        d = seen[0].dists
        assert d["submit"].median() == 250 and d["ws"].median() == 80 and d["md"].median() == 40
    with pytest.raises(SystemExit):
        cli.main(["e3", *common, "--grid", '{"config": {}}'])
    with pytest.raises(SystemExit):
        cli.main(["e1", *common, "--policies", "B"])
