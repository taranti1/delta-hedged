"""Monitoring: JSON-lines logging, Prometheus-style metrics, the manual kill file, the
heartbeat file read by the watchdog, and the local metrics HTTP endpoint.

Kept dependency-free (no prometheus_client needed): metrics are served as plain text in the
Prometheus exposition format by a tiny aiohttp endpoint started by the live runner
(``MetricsServer``, bound to 127.0.0.1 by default).
"""

from __future__ import annotations

import json
import os
import time
from collections import defaultdict
from collections.abc import Callable
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
        self.lines = 0

    def write(self, kind: str, ts_ns: int, /, **payload: Any) -> None:
        """One line {"k": kind, "t": ts_ns, "cfg", "sha", **payload}; payload keys that collide
        with the envelope are prefixed with '_' (never silently overwritten)."""
        rec: dict[str, Any] = {"k": kind, "t": ts_ns, "cfg": self.cfg, "sha": self.sha}
        for key, v in payload.items():
            rec["_" + key if key in ("k", "t", "cfg", "sha") else key] = v
        self._f.write(orjson.dumps(rec, default=str, option=orjson.OPT_NON_STR_KEYS) + b"\n")
        self.lines += 1

    def close(self) -> None:
        if not self._f.closed:
            self._f.close()


class Metrics:
    """Counters, gauges and simple summaries with labels; exposition() renders the
    Prometheus text format (summaries as <name>_count / _sum / _max)."""

    def __init__(self) -> None:
        self.counters: dict[tuple[str, tuple], float] = defaultdict(float)
        self.gauges: dict[tuple[str, tuple], float] = {}

    def inc(self, name: str, value: float = 1.0, /, **labels: str) -> None:
        self.counters[(name, tuple(sorted(labels.items())))] += value

    def set(self, name: str, value: float, /, **labels: str) -> None:
        self.gauges[(name, tuple(sorted(labels.items())))] = value

    def observe(self, name: str, value: float, /, **labels: str) -> None:
        """Summary: count, sum and running max of observations."""
        key = tuple(sorted(labels.items()))
        self.counters[(name + "_count", key)] += 1
        self.counters[(name + "_sum", key)] += value
        mk = (name + "_max", key)
        if value > self.gauges.get(mk, float("-inf")):
            self.gauges[mk] = value

    def get(self, name: str, /, **labels: str) -> float | None:
        key = (name, tuple(sorted(labels.items())))
        if key in self.counters:
            return self.counters[key]
        return self.gauges.get(key)

    def exposition(self) -> str:
        lines = []
        for (name, labels), v in sorted(self.counters.items()):
            lines.append(f"{name}{_fmt(labels)} {_num(v)}")
        for (name, labels), v in sorted(self.gauges.items()):
            lines.append(f"{name}{_fmt(labels)} {_num(v)}")
        return "\n".join(lines) + "\n"


def _num(v: float) -> str:
    f = float(v)
    if f != f:
        return "NaN"
    if f == float("inf"):
        return "+Inf"
    if f == float("-inf"):
        return "-Inf"
    if f.is_integer() and abs(f) < 1e15:
        return str(int(f))
    return repr(f)


def _esc(v: Any) -> str:
    return str(v).replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


def _fmt(labels: tuple) -> str:
    if not labels:
        return ""
    return "{" + ",".join(f'{k}="{_esc(v)}"' for k, v in labels) + "}"


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
    write_heartbeat(path, {})


def write_heartbeat(path: str | Path, payload: dict[str, Any], now_ns: int | None = None) -> None:
    """Atomically replace the heartbeat file with {"t": now_ns, **payload} (tmp + rename, so
    the watchdog never reads a half-written file)."""
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    rec = {"t": time.time_ns() if now_ns is None else int(now_ns), **payload}
    tmp = p.with_name(p.name + f".tmp{os.getpid()}")
    tmp.write_text(json.dumps(rec))
    os.replace(tmp, p)


def cancel_all_marker_path(heartbeat: str | Path) -> Path:
    """File the watchdog writes after every cancel-all it sends (the live runner reads it: new
    orders are held for the cancel-all tail and its order view is reconciled)."""
    p = Path(heartbeat)
    return p.with_name(p.name + ".cancel_all")


def write_json_atomic(path: str | Path, payload: dict[str, Any]) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_name(p.name + f".tmp{os.getpid()}")
    tmp.write_text(json.dumps(payload))
    os.replace(tmp, p)


def read_heartbeat(path: str | Path) -> dict[str, Any] | None:
    """Parsed heartbeat ({"t": ns, ...}); falls back to the file mtime when the content is
    unreadable; None when the file does not exist."""
    p = Path(path)
    try:
        st = p.stat()
    except OSError:
        return None
    try:
        rec = json.loads(p.read_text())
        if isinstance(rec, dict) and isinstance(rec.get("t"), int):
            return rec
    except (OSError, ValueError):
        pass
    return {"t": int(st.st_mtime_ns), "unparsed": True}


class MetricsServer:
    """``GET /metrics`` (Prometheus text) and ``GET /health`` (JSON) on host:port (aiohttp).

    ``health`` returns a JSON-serializable dict; the status is 200 when it has
    ``ok: true`` and 503 otherwise, so a load balancer / curl -f can alert on it.
    """

    def __init__(self, metrics: Metrics, host: str = "127.0.0.1", port: int = 9108,
                 health: Callable[[], dict[str, Any]] | None = None,
                 before_scrape: Callable[[], None] | None = None) -> None:
        self.metrics = metrics
        self.host = host
        self.port = port
        self.health = health
        self.before_scrape = before_scrape
        self._runner: Any = None
        self.bound_port: int | None = None

    async def start(self) -> int:
        from aiohttp import web

        app = web.Application()
        app.router.add_get("/metrics", self._metrics)
        app.router.add_get("/health", self._health)
        self._runner = web.AppRunner(app, access_log=None)
        await self._runner.setup()
        site = web.TCPSite(self._runner, self.host, self.port)
        await site.start()
        sockets = getattr(site._server, "sockets", None) or []  # noqa: SLF001 - aiohttp exposes no public accessor
        self.bound_port = sockets[0].getsockname()[1] if sockets else self.port
        return self.bound_port

    async def _metrics(self, request: Any) -> Any:
        from aiohttp import web

        if self.before_scrape is not None:
            try:
                self.before_scrape()
            except Exception:  # noqa: BLE001 - a scrape must never kill the runner
                pass
        return web.Response(text=self.metrics.exposition(), content_type="text/plain", charset="utf-8",
                            headers={"X-Prometheus-Format": "0.0.4"})

    async def _health(self, request: Any) -> Any:
        from aiohttp import web

        h = self.health() if self.health is not None else {"ok": True}
        return web.json_response(h, status=200 if h.get("ok", True) else 503, dumps=lambda o: json.dumps(o, default=str))

    async def stop(self) -> None:
        if self._runner is not None:
            await self._runner.cleanup()
            self._runner = None
