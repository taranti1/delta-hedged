"""dh.research.exp_common: time parsing, event-clustered CIs, policy flags, synthetic banner."""

from __future__ import annotations

import math

import numpy as np
import pandas as pd
import pytest

from dh.research.exp_common import (
    SYNTHETIC_BANNER,
    Report,
    cluster_mean_ci,
    flag_only_A,
    paired_diff_ci,
    parse_policies,
    parse_time,
    tau_bucket,
    verdict_from_ci,
)
from dh.research.replay_grid import interp_capacity, scaled_cfg
from dh.strategy.config import StrategyConfig


def test_parse_time_formats():
    t = parse_time("2026-09-24T12:00:00Z")
    assert t == 1_790_251_200 * 10**9
    assert parse_time("2026-09-24T12") == t and parse_time("2026-09-24 12:00") == t
    assert parse_time(str(1_790_251_200)) == t and parse_time("1790251200000") == t
    assert parse_time(1_790_251_200_000_000_000) == t
    assert parse_time("2026-09-24T12:00:00.5Z") == t + 500_000_000
    with pytest.raises(ValueError):
        parse_time("yesterday")
    assert parse_policies("b, c,B") == ["B", "C"] and parse_policies(["optimistic"]) == ["A"]


def test_cluster_ci_is_contract_weighted_and_clustered():
    # 10 events, each with one +1c and one -1c fill: mean 0; weights change the mean exactly
    df = pd.DataFrame({"v": [1.0, -1.0] * 10, "w": [3.0, 1.0] * 10, "e": np.repeat(np.arange(10), 2)})
    ci = cluster_mean_ci(df.v, df.w, df.e, n_boot=200)
    assert ci.mean == pytest.approx(0.5) and ci.clusters == 10
    assert ci.lo == pytest.approx(0.5) and ci.hi == pytest.approx(0.5)  # every event has the same mean
    one = cluster_mean_ci([1.0, 2.0], [1, 1], ["a", "a"])
    assert one.mean == 1.5 and math.isnan(one.lo)


def test_paired_difference_resamples_events_jointly():
    rng = np.random.default_rng(1)
    ev = np.repeat(np.arange(30), 5)
    shock = rng.normal(0, 5, 30)[ev]  # common event shock: paired design removes it
    a = pd.DataFrame({"net_c_per_ct": shock + rng.normal(0, 0.1, len(ev)), "contracts": 1.0, "event": ev})
    b = a.assign(net_c_per_ct=a.net_c_per_ct + 0.3)
    d = paired_diff_ci(a, b, "net_c_per_ct", n_boot=300)
    assert d.mean == pytest.approx(0.3) and 0.2 < d.lo < 0.3 < d.hi < 0.4


def test_flag_only_A_and_verdicts():
    t = pd.DataFrame({"bucket": ["x", "x", "x", "y", "y"], "policy": ["A", "B", "C", "A", "B"],
                      "net_lo_c": [0.2, 0.1, -0.1, 0.3, 0.4]})
    f = flag_only_A(t, ["bucket"])
    assert f.loc[f.bucket == "x", "holds_only_under_A"].all() and not f.loc[f.bucket == "y", "holds_only_under_A"].any()
    assert verdict_from_ci(0.1, 0.5) == "ACCEPT" and verdict_from_ci(-0.5, -0.1) == "REJECT"
    assert verdict_from_ci(-0.1, 0.1) == "INCONCLUSIVE"
    assert list(tau_bucket([10, 45, 100, 400, 700, 4000]).astype(str)) == ["<30s", "30-60s", "1-5m", "5-10m", "10-30m", ">30m"]


def test_report_synthetic_banner(tmp_path):
    r = Report("ex", "Example", tmp_path, synthetic=True, rule="accept if x", meta={"root": "r"})
    r.verdict = "ACCEPT"
    r.table("t", pd.DataFrame({"a": [1.23456, math.nan], "b": ["p", "q"]}), "note")
    md = r.write().read_text()
    assert SYNTHETIC_BANNER in md.splitlines()[2] and "not a trading decision" in md and "| a | b |" in md
    csv = pd.read_csv(tmp_path / "ex_t.csv")
    assert list(csv.columns[:1]) == ["synthetic"] and csv.synthetic.all()


def test_capacity_interpolation_and_scaling():
    assert interp_capacity([1, 10], [2.0, 0.0], 1.0) == pytest.approx(math.sqrt(10))
    assert interp_capacity([1, 10], [2.0, 1.5], 1.0) == 10
    assert math.isnan(interp_capacity([1, 10], [0.5, 0.2], 1.0))
    cfg = StrategyConfig()
    c5 = scaled_cfg(cfg, 5)
    assert c5.quoting.clip_contracts == 5 * cfg.quoting.clip_contracts
    assert c5.risk.max_pos_per_market == 5 * cfg.risk.max_pos_per_market
    assert scaled_cfg(cfg, 5, scale_limits=False).risk == cfg.risk


def test_verdicts_need_enough_settlement_events(tmp_path):
    """A decision over fewer than MIN_DECISION_EVENTS settlement events is reported INCONCLUSIVE
    (event-bootstrap CIs over a handful of clusters are far too narrow); E6/E7 buckets likewise."""
    from dh.research.exp67_segments import add_recommendation
    from dh.research.exp_common import MIN_DECISION_EVENTS, n_events

    r = Report("ex", "Example", tmp_path, rule="accept if x")
    r.verdict = "ACCEPT"
    r.decision_events = 4
    assert r.final_verdict().startswith("INCONCLUSIVE") and "Rule outcome on this sample: ACCEPT" in r.final_verdict()
    assert "settlement events behind the decision | 4" in r.write().read_text()
    r.decision_events = MIN_DECISION_EVENTS
    assert r.final_verdict() == "ACCEPT"
    assert n_events(pd.DataFrame({"event": ["a", "b"]}), pd.DataFrame({"event": ["b", "c"]}), None) == 3
    t = pd.DataFrame({"tau_bucket": ["x", "x", "y", "y", "z"], "policy": ["B", "C", "B", "C", "B"],
                      "events": [25, 25, 4, 4, 30], "net_lo_c": [0.1, 0.2, 0.1, 0.2, 0.5], "net_hi_c": [1, 1, 1, 1, 1]})
    rec = add_recommendation(t, ["tau_bucket"]).groupby("tau_bucket")["recommendation"].first().to_dict()
    assert rec == {"x": "quote", "y": "insufficient data", "z": "insufficient data"}  # z: C missing
