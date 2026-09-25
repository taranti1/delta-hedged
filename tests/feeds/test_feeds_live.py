"""Live venue checks (need outbound network; skipped by default).

    python -m pytest -m network tests/feeds/test_feeds_live.py -q

Each case runs scripts/smoke_feeds.py for one enabled feed of config/feeds.yaml.
"""

from __future__ import annotations

import pytest

from dh.feeds.registry import load_feeds_config
from tests.feeds.test_feeds_scripts import REPO, smoke

_CFG = load_feeds_config(REPO / "config" / "feeds.yaml")
_ENABLED = [k for k, v in (_CFG.get("feeds") or {}).items() if (v or {}).get("enabled", True)]


@pytest.mark.network
@pytest.mark.parametrize("feed", _ENABLED)
def test_live_feed_smoke(feed):
    assert smoke.main(["--venues", feed, "--seconds", "20", "--min-updates", "5"]) == 0
