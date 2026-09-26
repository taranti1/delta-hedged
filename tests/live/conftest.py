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


def healthy_watchdog_beat(subaccount: int, now_ns: int) -> dict:
    return {"t": int(now_ns), "pid": 1, "subaccount": int(subaccount), "state": "ARMED", "armed": None,
            "last_poll_ns": int(now_ns)}


@pytest.fixture(autouse=True)
def _healthy_watchdog_and_disk(request, monkeypatch):
    """A live LiveApp refuses to start without a fresh watchdog beat for its subaccount and with
    < 10 GB free on the data root's disk (review M3, disk guard), and its runner gates on both.
    Tests get a healthy watchdog beat (stamped with the app's own clock) and plenty of disk,
    unless marked ``real_guards`` (the tests of those guards)."""
    if request.node.get_closest_marker("real_guards"):
        return
    import dh.live.app as app

    monkeypatch.setattr(app, "watchdog_reader_for", lambda path, sub, clock: (lambda: healthy_watchdog_beat(sub, clock())))
    monkeypatch.setattr(app, "data_root_free_gb", lambda root: 500.0)
