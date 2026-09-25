from __future__ import annotations

import pytest

from dh.execution.latency import Empirical, Fixed, LatencyModel, LogNormal

MS = 1_000_000


def test_fixed_and_zero():
    lat = LatencyModel.fixed(10, 20, 5, cancel_ms=8, md_ms=3)
    assert lat.submit_ns() == 10 * MS and lat.cancel_ns() == 8 * MS
    assert lat.response_ns() == 20 * MS and lat.ws_ns() == 5 * MS and lat.md_ns() == 3 * MS
    assert lat.md_offset_ns == 3 * MS
    z = LatencyModel.zero()
    assert z.submit_ns() == z.cancel_ns() == z.response_ns() == z.ws_ns() == z.md_offset_ns == 0


def test_same_seed_same_sequence_and_streams_independent():
    a = LatencyModel(42, submit=LogNormal(30, 0.5), ws=LogNormal(10, 0.8))
    b = LatencyModel(42, submit=LogNormal(30, 0.5), ws=LogNormal(10, 0.8))
    sa = [a.submit_ns() for _ in range(50)]
    # interleave many ws draws in b: submit stream must be unaffected (pathwise comparability)
    sb = []
    for _ in range(50):
        for _ in range(3):
            b.ws_ns()
        sb.append(b.submit_ns())
    assert sa == sb
    c = LatencyModel(43, submit=LogNormal(30, 0.5))
    assert [c.submit_ns() for _ in range(50)] != sa


def test_multiplier_and_fork():
    base = LatencyModel(1, submit=LogNormal(30, 0.5))
    f1 = base.fork(9)
    f2 = base.fork(9)
    x1 = [f1.submit_ns() for _ in range(20)]
    assert x1 == [f2.submit_ns() for _ in range(20)]
    # forking does not consume the parent's streams
    p1 = [base.submit_ns() for _ in range(5)]
    p2 = [LatencyModel(1, submit=LogNormal(30, 0.5)).submit_ns() for _ in range(1)]
    assert p1[0] == p2[0]
    scaled = base.fork(9, multiplier=1.5)
    y = [scaled.submit_ns() for _ in range(20)]
    assert all(abs(b - 1.5 * a) <= 1 for a, b in zip(x1, y))


def test_distributions():
    import numpy as np

    rng = np.random.default_rng(0)
    ln = LogNormal(20, 0.5, floor_ms=5, cap_ms=40)
    xs = [ln.sample(rng) for _ in range(2000)]
    assert min(xs) >= 5 and max(xs) <= 40
    assert 15 < float(np.median(xs)) < 25 and ln.median() == 20
    emp = Empirical([1, 2, 3, 100])
    ys = {emp.sample(rng) for _ in range(200)}
    assert ys == {1.0, 2.0, 3.0, 100.0} and emp.median() == 2.5
    assert Fixed(7).sample(rng) == 7
    with pytest.raises(ValueError):
        Empirical([])
    with pytest.raises(ValueError):
        Fixed(-1)
    with pytest.raises(ValueError):
        LatencyModel(0, multiplier=0)
