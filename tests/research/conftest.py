"""Shared synthetic recordings for tests/research (SYNTHETIC DATA: pipeline validation only)."""

from __future__ import annotations

import pytest

from dh.research.synth_recording import SynthRecordingConfig, synth_strategy_config, write_synthetic_recording
from dh.sim.synthetic import SynthConfig

TINY = SynthRecordingConfig(n_events=2, event_spacing_s=240, strike_delay_s=20.0, seed=11,
                            synth=SynthConfig(n_strikes_each_side=2, mm_lag_s=1.5, informed=True, informed_edge_ticks=1.0,
                                              vol_ann=0.6, mm_update_prob=0.3, noise_taker_rate_per_s=0.08))


@pytest.fixture(scope="session")
def tiny_rec(tmp_path_factory):
    """2 events x 240 s, 5 strikes each, strikes published 20 s after open."""
    root = tmp_path_factory.mktemp("tiny_rec")
    return write_synthetic_recording(root, TINY)


@pytest.fixture(scope="session")
def synth_cfg():
    return synth_strategy_config("KXBTCD")
