"""Shared fixtures for dh.kalshi tests (offline; spec-shaped payloads only)."""

from __future__ import annotations

import asyncio
import functools
import inspect
import signal
import threading
from pathlib import Path

import pytest
import yaml
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

ROOT = Path(__file__).resolve().parents[2]
ASYNCAPI = ROOT / "docs" / "kalshi_specs" / "asyncapi.yaml"

# Per-test time budget (s). Every test here finishes in well under a second; a hang must fail
# its own test instead of blocking the suite. Override per test with @pytest.mark.timeout_s(n).
TEST_TIMEOUT_S = 15.0


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line("markers", "timeout_s(seconds): per-test time budget (tests/kalshi/conftest.py)")


class _HardTimeout(BaseException):
    """Raised from SIGALRM when a test outlives 2x its budget (e.g. a loop that never yields)."""


@pytest.hookimpl(wrapper=True)
def pytest_runtest_call(item: pytest.Item):
    """pytest-timeout-like guard without the dependency, two layers:

    * coroutine tests run under ``asyncio.timeout(budget)``: a task that spins or awaits
      forever (the event loop still runs) fails with a clear message, and teardown proceeds;
    * any test (sync or async) gets a SIGALRM backstop at 2x the budget, re-armed every second,
      for code that blocks the loop / thread itself (POSIX main thread only).
    """
    marker = item.get_closest_marker("timeout_s")
    budget = float(marker.args[0]) if marker else TEST_TIMEOUT_S
    fn = getattr(item, "obj", None)
    if inspect.iscoroutinefunction(fn):

        @functools.wraps(fn)
        async def timeboxed(*args, **kwargs):
            cm = asyncio.timeout(budget)
            try:
                async with cm:
                    return await fn(*args, **kwargs)
            except TimeoutError:
                if cm.expired():
                    pytest.fail(f"{item.name}: no result within {budget:g}s (hang?)", pytrace=False)
                raise

        item.obj = timeboxed  # pytest-asyncio wraps item.obj at runtest time
    alarm = hasattr(signal, "setitimer") and threading.current_thread() is threading.main_thread()
    if alarm:

        def _on_alarm(signum, frame):
            raise _HardTimeout(f"{item.name}: still running after {2 * budget:g}s (blocked event loop?)")

        old = signal.signal(signal.SIGALRM, _on_alarm)
        signal.setitimer(signal.ITIMER_REAL, 2 * budget, 1.0)
    try:
        return (yield)
    finally:
        if alarm:
            signal.setitimer(signal.ITIMER_REAL, 0)
            signal.signal(signal.SIGALRM, old)
        if fn is not None:
            item.obj = fn


@pytest.fixture(scope="session")
def asyncapi_examples() -> dict[str, list[dict]]:
    """message name -> list of example payloads, straight from the vendored asyncapi.yaml."""
    spec = yaml.safe_load(ASYNCAPI.read_text(encoding="utf-8"))
    out: dict[str, list[dict]] = {}
    for name, m in spec["components"]["messages"].items():
        for ex in m.get("examples") or []:
            out.setdefault(name, []).append(ex["payload"])
    return out


@pytest.fixture(scope="session")
def rsa_key() -> rsa.RSAPrivateKey:
    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


@pytest.fixture(scope="session")
def rsa_pem(rsa_key: rsa.RSAPrivateKey) -> bytes:
    return rsa_key.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.TraditionalOpenSSL, serialization.NoEncryption()
    )
