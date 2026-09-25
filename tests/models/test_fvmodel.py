from __future__ import annotations

import math

import numpy as np
import pytest

from dh.core.market import MarketSpec
from dh.core.units import NS_PER_S
from dh.models.fairvalue import digital
from dh.models.fvmodel import RECOMMENDED_CONFIG_PATH, FairValueModel, TailSchedule, load_recommended_config
from dh.models.tails import GAUSS, StudentT
from dh.models.vol import SeasonalVol
from dh.settlement.window import pre_window_state

T = 1_790_301_600 * NS_PER_S


def _cfg(kind="student_t"):
    return {
        "vol": {
            "half_lives_s": [600, 86400],
            "weights_by_horizon_s": {"120": [0.9, 0.1], "3600": [0.3, 0.7]},
            "min_dt_s": 60.0,
            "max_dt_s": 600.0,
        },
        "seasonal": SeasonalVol(factors=tuple([1.0] * 24), layout="time_of_day", bucket_s=3600).to_dict(),
        "tail": {"kind": kind, "by_horizon_s": {"120": {"nu": 4.0, "scale": 0.98}, "3600": {"nu": 3.0, "scale": 1.02}}},
    }


def _spec(K):
    return MarketSpec(ticker="KXBTCD-X", event_ticker="E", series_ticker="KXBTCD", strike_type="greater",
                      floor_strike=K, cap_strike=None, open_ts=0, close_ts=T, expiration_ts=T)


def test_tail_schedule_interpolation_and_clamping():
    ts = TailSchedule(knots=((120.0, 4.0, 0.98), (3600.0, 3.0, 1.02)))
    assert ts.params(10.0) == (4.0, 0.98)
    assert ts.params(1e6) == (3.0, 1.02)
    nu, c = ts.params(1860.0)
    assert nu == pytest.approx(3.5) and c == pytest.approx(1.0)
    tail, c = ts.at(1860.0)
    assert isinstance(tail, StudentT) and tail.nu == pytest.approx(3.5)
    g, c1 = TailSchedule(kind="gauss").at(100.0)
    assert g is GAUSS and c1 == 1.0
    with pytest.raises(ValueError):
        TailSchedule(knots=((10.0, 3.0, 1.0), (5.0, 3.0, 1.0)))


def test_fair_value_model_prices_with_config_parameters():
    fv = FairValueModel.from_config(_cfg())
    rng = np.random.default_rng(0)
    t = T - 2 * 86400 * NS_PER_S
    p = 84_000.0
    sig = 6e-5
    while t < T - 3600 * NS_PER_S:
        fv.update(t, p)
        t += 60 * NS_PER_S
        p *= math.exp(sig * math.sqrt(60) * rng.standard_normal())
    assert fv.ready
    now = T - 3600 * NS_PER_S
    ws = pre_window_state(_spec(p).settlement, T, now)
    d = fv.price(_spec(p + 200.0), ws, p, now)
    # reproduce by hand: horizon 3600 s -> weights (0.3, 0.7), t(3), scale 1.02
    s = fv.vol.ewma_sigmas()
    sig_blend = math.sqrt(0.3 * s[0] ** 2 + 0.7 * s[1] ** 2)
    ref = digital(_spec(p + 200.0), ws, p, p * sig_blend * 1.02, StudentT(3.0))
    assert d.p_yes == pytest.approx(ref.p_yes, rel=1e-12)
    assert d.delta == pytest.approx(ref.delta, rel=1e-12)
    # gauss config path
    fvg = FairValueModel.from_config(_cfg("gauss"))
    assert fvg.tails.kind == "gauss"


def test_recommended_config_loads_if_present():
    if not RECOMMENDED_CONFIG_PATH.exists():
        pytest.skip("research config not generated yet")
    cfg = load_recommended_config()
    fv = FairValueModel.from_config(cfg)
    assert len(fv.vol.cfg.half_lives_s) == len(cfg["vol"]["half_lives_s"])
    for h in (60.0, 600.0, 3600.0):
        tail, c = fv.tails.at(h)
        assert 0.5 < c < 2.0
