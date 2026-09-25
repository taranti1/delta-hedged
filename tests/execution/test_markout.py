from __future__ import annotations

import math

import numpy as np
import pytest

from dh.execution.markout import AsOf, compute_markouts, fee_cents_per_contract, markout_matrix, summarize
from tests.execution.helpers import S, T, fill


def test_asof_lookup():
    f = AsOf([10, 20, 30], [0.1, 0.2, 0.3])
    assert math.isnan(f(9)) and f(10) == 0.1 and f(25) == 0.2 and f(30) == 0.3 and math.isnan(f(31))
    np.testing.assert_allclose(f(np.array([10, 29])), [0.1, 0.2])
    with pytest.raises(ValueError):
        AsOf([2, 1], [0.0, 0.0])


def test_signed_markouts_bid_ask_settlement_and_fees():
    ts = np.array([0, 1 * S, 5 * S, 60 * S]) + 100 * S
    fv = (ts, np.array([0.50, 0.52, 0.47, 0.60]))
    fills = [fill(100 * S, "b", "x", "bid", 5000, 200, "t1", fee=8750),  # bought 2 YES at 50c
             fill(100 * S, "a", "y", "ask", 5000, 100, "t2")]  # sold 1 YES at 50c
    mk = compute_markouts(fills, fv, horizons_s=(0.5, 1, 5, 60, 61), settlement={T: 1.0})
    b, a = mk
    assert b.markouts_c[:4] == pytest.approx((0.0, 2.0, -3.0, 10.0)) and math.isnan(b.markouts_c[4])
    assert a.markouts_c[:4] == pytest.approx((0.0, -2.0, 3.0, -10.0))
    assert b.settle_c == pytest.approx(50.0) and a.settle_c == pytest.approx(-50.0)
    assert b.fee_c == pytest.approx(0.4375) and b.net(1) == pytest.approx(2.0 - 0.4375)
    assert fee_cents_per_contract(17_500, 100) == pytest.approx(1.75)
    m, w = markout_matrix(mk)
    assert m.shape == (2, 6) and list(w) == [2.0, 1.0]


def test_time_basis_dict_fv_and_summary():
    f1 = fill(10 * S, "b", "x", "bid", 4000, 300, "t1", ts_exch=9 * S)
    f2 = fill(10 * S, "b", "x", "bid", 4000, 100, "t2", ts_exch=9 * S, ticker="OTHER")
    fv = {T: lambda t: 0.40 if t < 10 * S else 0.45}
    mk_exch = compute_markouts([f1, f2], fv, horizons_s=(0.5, 1))
    mk_recv = compute_markouts([f1], fv, horizons_s=(0.5, 1), time_basis="recv")
    assert mk_exch[0].markouts_c == pytest.approx((0.0, 5.0)) and mk_recv[0].markouts_c == pytest.approx((5.0, 5.0))
    assert all(math.isnan(x) for x in mk_exch[1].markouts_c)  # no fair value for OTHER
    per_ticker = compute_markouts([f1], lambda tk, t: 0.5, horizons_s=(1,), per_ticker=True)
    assert per_ticker[0].markouts_c == pytest.approx((10.0,))
    s = summarize(mk_exch)
    assert s["1s"]["mean_c"] == pytest.approx(5.0) and s["1s"]["n"] == 1 and s["1s"]["contracts"] == 3.0
    assert s["settle"]["n"] == 0
    both = summarize(compute_markouts([f1, dataclass_replace(f1, trade_id="t3", qty=100, yes_px=4200)], fv,
                                      horizons_s=(1,)))
    # contract-weighted: (3 * 5c + 1 * 3c) / 4
    assert both["1s"]["mean_c"] == pytest.approx((3 * 5.0 + 1 * 3.0) / 4)


def dataclass_replace(obj, **kw):
    import dataclasses

    return dataclasses.replace(obj, **kw)
