from __future__ import annotations

import math

import numpy as np
import pytest

from dh.models.calibration import (
    TAIL_BINS,
    block_bootstrap_counts,
    block_bootstrap_mean,
    brier,
    brier_terms,
    by_bucket,
    ece,
    log_loss,
    log_loss_terms,
    murphy,
    reliability,
    wilson_ci,
)


def test_brier_and_log_loss_known_values():
    p = [0.9, 0.2, 0.5]
    y = [1, 0, 1]
    assert brier(p, y) == pytest.approx((0.01 + 0.04 + 0.25) / 3)
    assert log_loss(p, y) == pytest.approx(-(math.log(0.9) + math.log(0.8) + math.log(0.5)) / 3)
    # clipping keeps confident wrong forecasts finite
    assert log_loss([0.0], [1]) == pytest.approx(-math.log(1e-6))
    assert np.allclose(brier_terms(p, y), [0.01, 0.04, 0.25])
    assert log_loss_terms([1.0], [0], eps=1e-3)[0] == pytest.approx(-math.log(1e-3))
    assert brier(p, y, w=[1, 0, 0]) == pytest.approx(0.01)
    with pytest.raises(ValueError):
        brier([0.1, 0.2], [1])


def test_calibrated_forecasts_have_small_ece_and_consistent_reliability():
    rng = np.random.default_rng(0)
    p = rng.random(200_000)
    y = (rng.random(p.size) < p).astype(float)
    assert ece(p, y, bins=10) < 0.01
    t = reliability(p, y, bins=10)
    assert len(t) == 10 and np.all(np.abs(t["gap"]) < 0.01)
    assert np.all((t["y_lo"] <= t["y_mean"]) & (t["y_mean"] <= t["y_hi"]))
    # miscalibrated (overconfident) forecasts are detected
    q = np.clip(0.5 + 1.5 * (p - 0.5), 0, 1)
    assert ece(q, y, bins=10) > 0.05
    # tail bins accepted
    assert reliability(p, y, bins=TAIL_BINS)["n"].sum() == p.size


def test_murphy_decomposition_identity():
    rng = np.random.default_rng(1)
    levels = np.array([0.05, 0.2, 0.5, 0.7, 0.95])
    p = levels[rng.integers(0, 5, 50_000)]
    y = (rng.random(p.size) < np.clip(p + 0.05, 0, 1)).astype(float)
    m = murphy(p, y, bins=[0, 0.1, 0.3, 0.6, 0.8, 1.0])
    assert m["brier"] == pytest.approx(m["reliability"] - m["resolution"] + m["uncertainty"], abs=1e-12)
    assert abs(m["within_bin"]) < 1e-12
    assert m["reliability"] == pytest.approx(0.05**2, rel=0.2)


def test_wilson_interval():
    lo, hi = wilson_ci(np.array([0, 5, 50]), np.array([10, 10, 100]))
    assert lo[0] == 0.0 and hi[0] == pytest.approx(0.2775, abs=1e-3)
    assert lo[1] == pytest.approx(0.2366, abs=1e-3) and hi[1] == pytest.approx(0.7634, abs=1e-3)
    lo0, hi0 = wilson_ci(0, 0)
    assert math.isnan(float(lo0)) and math.isnan(float(hi0))


def test_by_bucket():
    p = np.array([0.1, 0.1, 0.9, 0.9])
    y = np.array([0, 1, 1, 1])
    out = by_bucket(p, y, {"g": ["a", "a", "b", "b"]})
    a = out.set_index("g").loc["a"]
    assert a["n"] == 2 and a["brier"] == pytest.approx((0.01 + 0.81) / 2)
    assert a["citl"] == pytest.approx(0.5 - 0.1)


def test_block_bootstrap_is_deterministic_and_covers_estimate():
    rng = np.random.default_rng(5)
    days = np.repeat(np.arange(200), 50)
    day_effect = rng.standard_normal(200)[days]
    v = day_effect + rng.standard_normal(days.size)
    est, lo, hi = block_bootstrap_mean(v, days, n_boot=500, seed=3)
    est2, lo2, hi2 = block_bootstrap_mean(v, days, n_boot=500, seed=3)
    assert (est, lo, hi) == (est2, lo2, hi2)
    assert lo < est < hi
    # block CI is wider than the naive iid CI because of the day effect
    naive = 1.96 * v.std() / math.sqrt(v.size)
    assert (hi - lo) / 2 > 3 * naive
    e, l, h = block_bootstrap_counts([1, 2, 3], [10, 10, 10], n_boot=200, seed=1)
    assert e == pytest.approx(0.2) and l <= e <= h
