"""Monitoring: JSON-lines logging, Prometheus-style metrics, and the manual kill file.

Kept dependency-free (no prometheus_client needed): metrics are served as plain text in the
Prometheus exposition format by a tiny aiohttp endpoint started by the live runner.
"""

from __future__ import annotations

import json
import os
import time
from collections import defaultdict
from pathlib import Path
from typing import Any

import orjson


class JsonLog:
    """Append-only JSON-lines log with the config digest and git SHA on every line."""

    def __init__(self, path: str | Path, config_digest: str, git_sha: str = "") -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._f = open(self.path, "ab", buffering=0)
        self.cfg = config_digest
        self.sha = git_sha

    def write(self, kind: str, ts_ns: int, **payload: Any) -> None:
        rec = {"k": kind, "t": ts_ns, "cfg": self.cfg, "sha": self.sha, **payload}
        self._f.write(orjson.dumps(rec, default=str) + b"\n")

    def close(self) -> None:
        self._f.close()


class Metrics:
    """Counters and gauges with labels; exposition() renders Prometheus text format."""

    def __init__(self) -> None:
        self.counters: dict[tuple[str, tuple], float] = defaultdict(float)
        self.gauges: dict[tuple[str, tuple], float] = {}

    def inc(self, name: str, value: float = 1.0, **labels: str) -> None:
        self.counters[(name, tuple(sorted(labels.items())))] += value

    def set(self, name: str, value: float, **labels: str) -> None:
        self.gauges[(name, tuple(sorted(labels.items())))] = value

    def exposition(self) -> str:
        lines = []
        for (name, labels), v in sorted(self.counters.items()):
            lines.append(f"{name}{_fmt(labels)} {v}")
        for (name, labels), v in sorted(self.gauges.items()):
            lines.append(f"{name}{_fmt(labels)} {v}")
        return "\n".join(lines) + "\n"


def _fmt(labels: tuple) -> str:
    if not labels:
        return ""
    return "{" + ",".join(f'{k}="{v}"' for k, v in labels) + "}"


class KillFile:
    """Manual kill switch: if the file exists, the runner cancels everything and halts."""

    def __init__(self, path: str | Path = "/run/dh/KILL") -> None:
        self.path = Path(path)

    def triggered(self) -> bool:
        return self.path.exists()

    def reason(self) -> str:
        try:
            return self.path.read_text().strip() or "manual kill file"
        except OSError:
            return "manual kill file"


def git_sha(repo: str | Path = ".") -> str:
    head = Path(repo) / ".git" / "HEAD"
    try:
        ref = head.read_text().strip()
        if ref.startswith("ref:"):
            return (Path(repo) / ".git" / ref.split(" ", 1)[1]).read_text().strip()[:12]
        return ref[:12]
    except OSError:
        return os.environ.get("DH_GIT_SHA", "unknown")


def heartbeat_file(path: str | Path) -> None:
    """Touch a heartbeat file (read by the watchdog process)."""
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps({"t": time.time_ns()}))
