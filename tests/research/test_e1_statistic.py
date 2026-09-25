"""E1 decides on the lag coefficient of the PAST external move, beyond the receive latency, with
time blocks shared across markets, month stability and the 20-event guard (audit C1).

Panels are built directly (the verdict code is exp1_staleness.run with build_panel stubbed):
Kalshi mid y, external fair value x, 100 ms grid, one market per settlement event."""

from __future__ import annotations

import math

import numpy as np
import pandas as pd
import pytest

import dh.research.exp1_staleness as e1
from dh.research.replay_env import Universe

NS = 10**9
T0 = 1_800_000_000 * NS  # 2027-01-15


def _panel(n_events: int, *, lag_s: float, rng, minutes: int = 10, step_ms: int = 100, move_sd: float = 0.002,
           quote_noise: float = 0.005, model_err: float = 0.015, start: int = T0, lag_events=None) -> pd.DataFrame:
    rows = []
    n = minutes * 60 * 1000 // step_ms
    lag = int(round(lag_s * 1000 / step_ms))
    a = math.exp(-step_ms / 1000 / 600)
    for k in range(n_events):
        t = start + k * 3600 * NS + np.arange(n) * step_ms * 1_000_000
        v = np.clip(0.5 + np.cumsum(rng.normal(0, move_sd, n + lag)), 0.02, 0.98)  # efficient probability
        eps = np.zeros(n)
        for i in range(1, n):
            eps[i] = a * eps[i - 1] + rng.normal(0, model_err * math.sqrt(1 - a * a))
        this_lag = lag if (lag_events is None or k in lag_events) else 0
        y = v[lag - this_lag: lag - this_lag + n] + rng.normal(0, quote_noise, n)  # Kalshi mid, stale by this_lag
        x = v[lag: lag + n] + eps  # research fair value from the external composite (+ persistent model error)
        rows.append(pd.DataFrame({"t": t, "ticker": f"M{k}", "event": f"E{k}", "T": t[-1] + 120 * NS, "x": x, "y": y,
                                  "S": 1.0}))
    return pd.concat(rows, ignore_index=True)


def _run(monkeypatch, tmp_path, panel, md_ms=None):
    """exp1_staleness.run on the panel; the receive latency defaults to the recording's measurement
    (no recording here: the documented 25 ms fallback)."""
    monkeypatch.setattr(e1, "build_panel", lambda *a, **k: panel)
    t1 = int(panel.t.max()) + NS
    uni = Universe(root=str(tmp_path), t0=int(panel.t.min()), t1=t1)
    kw = {} if md_ms is None else {"md_latency_ms": md_ms}
    res = e1.run(str(tmp_path), uni.t0, uni.t1, tmp_path / "out", universe=uni, vol_ann=0.4, n_boot=100, **kw)
    md = (tmp_path / "out" / "e1_staleness.md").read_text()
    res.setdefault("verdict", next(x for x in md.splitlines() if x.startswith("**Verdict")).split("** ", 1)[1])
    return res


def test_zero_lag_market_is_never_accepted(monkeypatch, tmp_path):
    """NULL: quote noise and persistent model error, no lag, 22 events: the old gap-closure rule
    accepted this; the lag-coefficient rule rejects it."""
    res = _run(monkeypatch, tmp_path, _panel(22, lag_s=0.0, rng=np.random.default_rng(0)))
    assert not res["verdict"].startswith("ACCEPT")
    assert res["rule_outcome"].startswith("REJECT")


def test_injected_lag_is_accepted_and_needs_twenty_events(monkeypatch, tmp_path):
    res = _run(monkeypatch, tmp_path, _panel(22, lag_s=1.5, rng=np.random.default_rng(1)))
    assert res["verdict"].startswith("ACCEPT"), res["verdict"]
    small = _run(monkeypatch, tmp_path, _panel(6, lag_s=1.5, rng=np.random.default_rng(1)))
    assert small["rule_outcome"].startswith("ACCEPT") and small["verdict"].startswith("INCONCLUSIVE")


def test_pure_receive_latency_is_not_staleness(monkeypatch, tmp_path):
    """Kalshi delivered 300 ms late (market efficient): measured from t + receive latency there is
    no response; measured from t (no anchor) the delay would look like a lag."""
    panel = _panel(22, lag_s=0.3, rng=np.random.default_rng(2), quote_noise=0.001)
    res = _run(monkeypatch, tmp_path, panel, md_ms=300.0)
    assert not res["verdict"].startswith("ACCEPT")
    naive = e1.lead_lag(panel, anchor_s=0.0, horizons=(0.25,), n_boot=50)
    assert naive[0].c_move_ci[0] > 0  # without the anchor the delivery delay shows up as a response


def test_accept_requires_every_calendar_month(monkeypatch, tmp_path):
    rng = np.random.default_rng(3)
    jan = _panel(22, lag_s=1.5, rng=rng, start=T0)
    feb = _panel(22, lag_s=0.0, rng=rng, start=T0 + 31 * 86400 * NS)
    res = _run(monkeypatch, tmp_path, pd.concat([jan, feb], ignore_index=True))
    assert res["verdict"].startswith("INCONCLUSIVE") and "not stable" in res["verdict"]
    assert set(res["by_month"]["month"]) == {"2027-01", "2027-02"}


def test_inference_clusters_are_time_blocks_shared_by_markets():
    # two markets on the same clock share each 60 s block: 10 min -> 10 blocks, not 20
    p = _panel(1, lag_s=0.0, rng=np.random.default_rng(4))
    p2 = p.assign(ticker="M_other", event="E_other")
    both = pd.concat([p, p2], ignore_index=True)
    r = e1.lead_lag(both, horizons=(0.5,), n_boot=20)[0]
    assert r.units == 10


@pytest.mark.parametrize("seed", [5, 6])
def test_null_verdict_path_never_accepts_across_seeds(monkeypatch, tmp_path, seed):
    res = _run(monkeypatch, tmp_path, _panel(21, lag_s=0.0, rng=np.random.default_rng(seed)))
    assert not res["verdict"].startswith("ACCEPT")
