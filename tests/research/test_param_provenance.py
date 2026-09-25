"""Provenance of the fill-intensity and adverse-selection (toxicity) parameters (look-ahead guard,
docs/research/EXPERIMENTS_RUNBOOK.md section 2a): every parameter set records its fitting window,
dataset and method (or that it is a prior never fitted on data), and every experiment labels it
in-sample / out-of-sample / prior against its evaluation window exactly like the fair-value
parameters; fitted parameters that overlap the window cap an ACCEPT at INCONCLUSIVE."""

from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import yaml

from dh.core.units import NS_PER_S
from dh.research.calibrate_flow import calibrate_split, dataset_hash, load_segments, write_split_report
from dh.research.exp_common import Report
from dh.research.replay_env import (
    Universe,
    flow_status,
    input_labels,
    inputs_meta,
    inputs_status,
    provenance_status,
    status_columns,
)
from dh.strategy.config import AdverseSelCfg, FillModelCfg, ParamProvenance, StrategyConfig, load_config, utc_from_ns
from dh.strategy.fill_model import AdverseSelectionModel, FillIntensityModel, SegmentFlow

REPO = Path(__file__).resolve().parents[2]
NS = NS_PER_S
T0 = 1_800_000_000 * NS  # 2027-01-15T08:00Z
T1 = T0 + 86_400 * NS
H_MS = 3_600_000


def _iso(t_ns: int) -> str:
    return utc_from_ns(t_ns)


def _cfg(fill: ParamProvenance | None = None, adverse: ParamProvenance | None = None) -> StrategyConfig:
    c = StrategyConfig()
    if fill is not None:
        c = replace(c, fill=replace(c.fill, provenance=fill))
    if adverse is not None:
        c = replace(c, adverse=replace(c.adverse, provenance=adverse))
    return c


def _uni() -> Universe:
    return Universe(root="/nonexistent", t0=T0, t1=T1)


# ------------------------------------------------------------------ where the provenance lives
def test_m1_config_marks_fill_and_adverse_parameters_as_priors():
    cfg = load_config(REPO / "config" / "m1.yaml")
    for sec in (cfg.fill, cfg.adverse):
        p = sec.provenance
        assert p.status == "prior" and p.is_prior and p.fitted_to_utc == "" and "[ESTIMATE]" in p.method
    raw = yaml.safe_load((REPO / "config" / "m1.yaml").read_text())
    assert raw["fill"]["provenance"]["status"] == "prior" and raw["adverse"]["provenance"]["status"] == "prior"
    # the dataclass defaults are priors too (research configs built in code)
    assert FillModelCfg().provenance.is_prior and AdverseSelCfg().provenance.is_prior


def test_fitted_provenance_loads_from_yaml_and_normalizes_utc(tmp_path):
    p = tmp_path / "s.yaml"
    # unquoted timestamps: YAML parses them into datetimes; they are normalized to ISO-8601 UTC
    p.write_text("adverse:\n  a0: 0.004\n  provenance:\n    status: fitted\n    fitted_from_utc: 2026-10-01T00:00:00Z\n"
                 "    fitted_to_utc: 2026-10-08\n    dataset_id: sha256:abc\n    method: E3 walk-forward\n")
    c = load_config(p)
    pv = c.adverse.provenance
    assert (pv.status, pv.fitted_from_utc, pv.fitted_to_utc) == ("fitted", "2026-10-01T00:00:00Z", "2026-10-08T00:00:00Z")
    assert pv.fitted_to_ns == pd.Timestamp("2026-10-08T00:00Z").value
    assert c.fill.provenance.is_prior and c.digest() != StrategyConfig().digest()
    with pytest.raises(ValueError):
        ParamProvenance(status="estimated")
    assert ParamProvenance.from_dict(pv.as_dict()) == pv


def test_model_classes_expose_the_provenance_in_use():
    fm, am = FillIntensityModel(FillModelCfg()), AdverseSelectionModel(AdverseSelCfg())
    assert fm.provenance.is_prior and am.provenance.is_prior
    seg = {("*", "*", "bid"): SegmentFlow(1.0, 10.0, 1.0)}
    unknown = FillIntensityModel(FillModelCfg(), dict(seg))
    assert unknown.provenance.status == "fitted" and unknown.provenance.fitted_to_utc == ""  # unknown window
    fitted = ParamProvenance.fitted("2026-10-01", "2026-10-08", dataset_id="sha256:x", method="calibrate_flow")
    assert FillIntensityModel(FillModelCfg(), dict(seg), segments_provenance=fitted).provenance == fitted
    assert AdverseSelectionModel(AdverseSelCfg(), {("*", "*", "bid"): (0.0, 0.0, 0.0)},
                                 coefs_provenance=fitted).provenance == fitted


# ------------------------------------------------------------------ labels against the window
@pytest.mark.parametrize("section", ["fill", "adverse"])
def test_fill_and_adverse_parameters_are_labelled_against_the_window(section):
    name = {"fill": "fill-intensity parameters", "adverse": "adverse-selection parameters"}[section]

    def lab(prov):
        cfg = _cfg(**{section: prov})
        info, warn = input_labels(_uni(), T0, cfg)[section]
        meta, warns = inputs_meta(_uni(), T0, cfg)
        ins, why = inputs_status(_uni(), T0, cfg)
        return info, warn, meta[name], ins, why, status_columns(_uni(), T0, cfg)[f"{section}_status"]

    info, warn, row, ins, why, col = lab(ParamProvenance(method="M1 placeholder"))
    assert info["status"] == "prior" and info["in_sample"] is False and warn is None and not ins
    assert "prior" in row and "counts as out-of-sample" in row and col == "prior"
    before = ParamProvenance.fitted(_iso(T0 - 30 * 86_400 * NS), _iso(T0), dataset_id="sha256:aa", method="m")
    info, warn, row, ins, why, col = lab(before)
    assert info["status"] == "out_of_sample" and not ins and warn is None and "out-of-sample" in row
    assert "sha256:aa" in row and col == "out_of_sample"
    overlap = ParamProvenance.fitted(_iso(T0 - 86_400 * NS), _iso(T0 + 3600 * NS), dataset_id="sha256:bb")
    info, warn, row, ins, why, col = lab(overlap)
    assert info["status"] == "in_sample" and ins and "IN-SAMPLE" in row and "overlaps the window" in row
    assert name in why and f"in-sample {name.split()[0]}" in warn and col == "in_sample"
    later = ParamProvenance.fitted(_iso(T1), _iso(T1 + 86_400 * NS))
    info, *_ , ins, why, col = lab(later)
    assert info["status"] == "in_sample" and "after the window" in info["label"] and ins
    no_end = ParamProvenance("fitted", fitted_from_utc=_iso(T0 - 86_400 * NS))
    info, warn, row, ins, why, col = lab(no_end)
    assert info["status"] == "unknown" and ins and "UNKNOWN fit window" in row and col == "unknown"


def test_in_sample_fill_or_adverse_parameters_cap_an_accept(tmp_path):
    """The null of this guard: identical evidence, the ONLY difference being the fitting window of the
    adverse-selection parameters -> ACCEPT stands with priors / an earlier fit, INCONCLUSIVE with a
    fit overlapping the window. A REJECT is never capped by it (exactly like the FV parameters)."""
    base = dict(decision_events=50, policies=["B", "C"])
    for prov, capped in ((ParamProvenance(), False),
                         (ParamProvenance.fitted(_iso(T0 - 7 * 86_400 * NS), _iso(T0)), False),
                         (ParamProvenance.fitted(_iso(T0), _iso(T1)), True)):
        ins, why = inputs_status(_uni(), T0, _cfg(adverse=prov))
        r = Report("x", "x", tmp_path, verdict="ACCEPT (rule)", in_sample=ins, in_sample_why=why, **base)
        fv = r.final_verdict()
        assert fv.startswith("INCONCLUSIVE") == capped, prov
        if capped:
            assert "adverse-selection parameters" in fv and "ACCEPT" not in fv
        rej = Report("x", "x", tmp_path, verdict="REJECT", in_sample=ins, in_sample_why=why, **base)
        assert rej.final_verdict() == "REJECT"


def test_provenance_status_without_provenance_is_unknown():
    info, warn = provenance_status(None, "custom coefficients", T0)
    assert info["status"] == "unknown" and info["in_sample"] and warn
    info, _ = provenance_status({"status": "prior"}, "x", T0)
    assert info["status"] == "prior" and not info["in_sample"]


# ------------------------------------------------------------------ fitters write provenance
def _flow_sample(n_markets: int, t_open_ms: int = 1_790_000_000_000):
    mk, rows = [], []
    for i in range(n_markets):
        o = t_open_ms + i * H_MS
        mk.append(dict(ticker=f"M{i:03d}", event_ticker=f"EM{i:03d}", strike_type="greater", floor_strike=100_000.0,
                       cap_strike=np.nan, result="no", open_ts_ms=o, close_ts_ms=o + H_MS))
        for k in range(60):
            rows.append(dict(ticker=f"M{i:03d}", ts_ms=o + 60_000 * k + 20_000, yes_px=5000, qty=100,
                             taker_outcome_side="no"))
    btc = pd.DataFrame({"ts_ms": [t_open_ms - 60_000], "price": [100_000.0]})
    return pd.DataFrame(rows), pd.DataFrame(mk), btc


def test_calibrate_flow_records_fitting_window_dataset_hash_and_method(tmp_path):
    trades, markets, btc = _flow_sample(10)
    t_open = 1_790_000_000_000
    res = calibrate_split(trades, markets, btc, train_frac=0.7, btc_bar_ms=0, prior_s=1e-9)
    tr, al = ParamProvenance.from_dict(res.provenance), ParamProvenance.from_dict(res.provenance_all)
    assert tr.status == al.status == "fitted"
    assert tr.fitted_from_ns == al.fitted_from_ns == t_open * 1_000_000  # first market open
    assert tr.fitted_to_ns == res.train_end_ms * 1_000_000 and al.fitted_to_ns == res.all_end_ms * 1_000_000
    assert tr.dataset_id.startswith("sha256:") and tr.dataset_id != al.dataset_id
    assert "gamma-Poisson" in tr.method and "chronological" in tr.method
    again = calibrate_split(trades, markets, btc, train_frac=0.7, btc_bar_ms=0, prior_s=1e-9)
    assert again.provenance == res.provenance  # deterministic
    moved = trades.assign(qty=np.where(np.arange(len(trades)) == 0, 200, trades.qty))
    assert calibrate_split(moved, markets, btc, train_frac=0.7, btc_bar_ms=0).provenance["dataset_id"] != tr.dataset_id
    assert dataset_hash(trades.head(0), markets.head(0)).startswith("sha256:")
    write_split_report(res, tmp_path)
    seg, meta = load_segments(tmp_path / "flow_segments.json")
    assert ParamProvenance.from_dict(meta["provenance"]) == al
    _, meta_tr = load_segments(tmp_path / "flow_segments_train.json")
    assert ParamProvenance.from_dict(meta_tr["provenance"]) == tr
    assert "Provenance of `flow_segments.json`" in (tmp_path / "flow_calibration.md").read_text()
    # the replay check reads the provenance: a replay starting at the fit end is out of sample
    info, warn = flow_status(seg, meta, "flow_segments.json", al.fitted_to_ns)
    assert info["flow_in_sample"] is False and warn is None and "sha256:" in info["flow_segments"]
    info, warn = flow_status(seg, meta, "flow_segments.json", al.fitted_to_ns - NS)
    assert info["flow_in_sample"] is True and info["flow_status"] == "in_sample" and "in-sample flow" in warn


def test_e3_cancel_rule_provenance_is_out_of_sample_for_its_scoring_window():
    from dh.research.exp3_toxicity import rule_provenance

    t_split = T0 + 43_200 * NS
    train = pd.DataFrame({"ts": [T0 + NS, T0 + 2 * NS], "ticker": ["A", "B"], "side": [1, -1], "px": [0.4, 0.6],
                          "contracts": [5.0, 5.0], "toxic": [1.0, 0.0]})
    prov = rule_provenance(train, T0, t_split, "rec")
    p = ParamProvenance.from_dict(prov)
    assert (p.fitted_from_ns, p.fitted_to_ns) == (T0, t_split) and "2 policy-B shadow fills" in p.dataset_id
    assert provenance_status(prov, "cancel rule", t_split, T1)[0]["status"] == "out_of_sample"
    assert provenance_status(prov, "cancel rule", T0, T1)[0]["status"] == "in_sample"


# ------------------------------------------------------------------ replay wiring (SYNTHETIC recording)
def test_replay_labels_and_ledgers_carry_fill_and_adverse_status(tiny_rec, synth_cfg, tmp_path):
    from dh.research.flow_recording import fit_flow
    from dh.research.replay_env import run_replay

    t0 = tiny_rec.t0 + 30 * NS
    t1 = t0 + 30 * NS
    fitted = ParamProvenance.fitted(utc_from_ns(tiny_rec.t0), utc_from_ns(tiny_rec.t1), dataset_id="sha256:rec")
    cfg = replace(synth_cfg, adverse=replace(synth_cfg.adverse, provenance=fitted))
    fit_flow(tiny_rec.root, tiny_rec.t0, tiny_rec.t1, tmp_path / "flow", train_frac=0.5)
    meta = json.loads((tmp_path / "flow" / "flow_segments.json").read_text())["meta"]
    assert meta["provenance"]["status"] == "fitted" and "recording" in meta["provenance"]["dataset_id"]
    rr = run_replay(tiny_rec.root, t0, t1, cfg, "B", flow_segments=tmp_path / "flow" / "flow_segments.json",
                    keep_objects=True)
    s = rr.summary
    assert s["adverse_params_in_sample"] is True and "IN-SAMPLE" in s["adverse_params"]
    assert s["fill_params_in_sample"] is False and "prior" in s["fill_params"]
    assert any(w.startswith("in-sample adverse-selection") for w in s["warnings"])
    assert s["status_adverse_status"] == "in_sample" and s["status_fill_status"] == "prior"
    assert rr.mm.flow.provenance == ParamProvenance.from_dict(meta["provenance"])  # bound segments' provenance
    if len(rr.df):
        assert (rr.df["adverse_status"] == "in_sample").all() and (rr.df["fill_status"] == "prior").all()
