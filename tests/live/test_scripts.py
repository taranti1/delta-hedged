"""scripts/run_live.py and scripts/watchdog.py: CLI safety rules (no network needed)."""

from __future__ import annotations

import importlib
import subprocess
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]


def _run_live():
    sys.path.insert(0, str(REPO / "scripts"))
    return importlib.import_module("run_live")


@pytest.mark.parametrize("script", ["scripts/run_live.py", "scripts/watchdog.py"])
def test_help(script):
    r = subprocess.run([sys.executable, script, "--help"], cwd=REPO, capture_output=True, text=True, timeout=60)
    assert r.returncode == 0 and "usage" in r.stdout.lower()


def test_tools_help():
    r = subprocess.run([sys.executable, "-m", "dh.live.tools", "--help"], cwd=REPO, capture_output=True, text=True, timeout=60)
    assert r.returncode == 0 and "backfill" in r.stdout


def test_live_mode_refused_without_config_and_flag(tmp_path, caplog):
    mod = _run_live()
    # the example config says mode: paper -> --mode live is refused even with the flag
    assert mod.main(["--mode", "live", "--i-understand-this-sends-real-orders",
                     "--live-config", "config/live.example.yaml"]) == 2
    live = tmp_path / "live.yaml"
    live.write_text("mode: live\n")
    # mode: live in the config but no confirmation flag -> refused
    assert mod.main(["--live-config", str(live)]) == 2
    assert mod.main(["--mode", "live", "--live-config", str(live)]) == 2
    assert "i-understand-this-sends-real-orders" in caplog.text


def test_missing_nondefault_live_config(tmp_path):
    mod = _run_live()
    assert mod.main(["--live-config", str(tmp_path / "nope.yaml")]) == 2


def test_confirm_flag_parses():
    mod = _run_live()
    a = mod.parse_args(["--mode", "live", "--i-understand-this-sends-real-orders"])
    assert a.confirmed and a.mode == "live"
    assert not mod.parse_args([]).confirmed and mod.parse_args([]).mode is None
