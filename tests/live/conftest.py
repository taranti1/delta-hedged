"""tests/live fixtures: the test machine has no chronyd, and a live runner's clock gate fails
closed without one (review N5), so every test gets a synthetic, healthy chrony sample unless it
injects its own sampler (LiveRunner(clock_sampler=...) / Overrides(clock_sampler=...))."""

from __future__ import annotations

import time

import pytest


def healthy_clock_sample() -> dict:
    return {"wall_ns": time.time_ns(), "mono_ns": time.monotonic_ns(), "src": "chronyc", "offset_s": 0.0002,
            "est_error_s": 0.001, "synced": True}


@pytest.fixture(autouse=True)
def _healthy_clock(monkeypatch):
    import dh.store.recorder as rec

    monkeypatch.setattr(rec, "sample_clock", healthy_clock_sample)
