"""Suite-wide isolation from host-local configuration.

``config/kalshi.yaml`` (git-ignored) is a HOST copy of the example: on the recording Mac it
points ``auth.env_file`` at real credentials and sets ``account_share`` and the fee balance
precision. ``dh.kalshi.config.load_config()`` without a path prefers it, so tests would
silently depend on (and load credentials from) whatever host they run on. Every test sees the
committed example instead; a test that wants a specific file passes its path explicitly.
"""

from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def _hermetic_kalshi_config(monkeypatch, tmp_path_factory):
    import dh.kalshi.config as kcfg

    monkeypatch.setattr(kcfg, "DEFAULT_CONFIG", tmp_path_factory.getbasetemp() / "no-host-kalshi.yaml")


@pytest.fixture(autouse=True)
def _no_network_clock_samples(monkeypatch):
    """dh.store.recorder.sample_clock falls back to a network `sntp` query on macOS (no
    chronyc): tests must not depend on NTP reachability or its latency."""
    import dh.store.recorder as rec

    monkeypatch.setattr(rec, "SNTP_ENABLED", False)
