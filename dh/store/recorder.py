"""Append-only raw capture: zstd-compressed JSON-lines segments, one per stream per UTC hour.

Layout (``root`` is the data directory, e.g. ``data``)::

    <root>/raw/<stream>/<YYYY-MM-DD>/<HH>.jsonl.zst        first segment of that hour
    <root>/raw/<stream>/<YYYY-MM-DD>/<HH>.p<N>.jsonl.zst   later parts of the same hour
                                                           (process restart, clock step)
    <root>/raw/<stream>/<YYYY-MM-DD>/<HH>[.p<N>].idx.json  sidecar index, written on close

The hour of a record is the UTC hour of its receive time ``t``. Files are created with
``O_EXCL`` and only ever appended to by the process that created them: existing files are
never reopened, rewritten or truncated.

Record format (one line of UTF-8 JSON terminated by ``\\n``)::

    {"t": <recv_ns>, "s": "<stream>", "q": <per-process record seq>, "d": "<raw frame text>"}

``d`` holds the frame exactly as received (``raw.decode('utf-8')``; ``d.encode('utf-8')``
restores the original bytes). Frames that are not valid UTF-8 are stored as
``"b": "<base64>"`` instead of ``"d"``. ``q`` increases by one per record across all streams of
one recorder process (it restarts at 0 in a new process).

Crash safety: records are buffered in memory and written every ``flush_interval_s`` (default
1 s) as ONE INDEPENDENT ZSTD FRAME per segment per flush, followed by ``flush()`` to the OS.
Concatenated zstd frames form a valid zstd stream (``zstdcat`` works). A crash loses at most
the unflushed interval; a frame cut mid-write is detected by the reader, which still
recovers every complete line before the cut (dh.store.replay). Segments are fsync'ed on
rotation, on close and every ``fsync_interval_s``.

Thread/async safety: ``write`` only appends to an in-memory list under a lock (cheap, safe
from any thread or the asyncio loop); a single background thread (or an explicit ``flush``)
serializes, compresses and writes. There is exactly one writer per process.

Streams used by the collector (scripts/record.py): one per venue connection
(``coinbase.ws``, ...), ``kalshi.ws``, ``kalshi.rest.*``, ``status`` (FeedStatus events of
sources that cannot write markers into their own stream, encoded with dh.store.codec) and
``clock`` (clock-health samples, see ``ClockSampler``).
"""

from __future__ import annotations

import asyncio
import base64
import ctypes
import ctypes.util
import logging
import os
import re
import shutil
import socket
import subprocess
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import orjson
import zstandard as zstd

from dh.core.events import Event
from dh.core.units import NS_PER_S
from dh.store.codec import encode_event

log = logging.getLogger(__name__)

HOUR_NS = 3600 * NS_PER_S
FORMAT = "dh-raw-jsonl-zstd/1"
_STREAM_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:\-]{0,127}$")
SEGMENT_SUFFIX = ".jsonl.zst"
INDEX_SUFFIX = ".idx.json"


def valid_stream_name(stream: str) -> bool:
    return bool(_STREAM_RE.match(stream)) and ".." not in stream


def utc_day_hour(t_ns: int) -> tuple[str, str]:
    """recv_ns -> ('YYYY-MM-DD', 'HH') in UTC."""
    tm = time.gmtime(t_ns // NS_PER_S)
    return f"{tm.tm_year:04d}-{tm.tm_mon:02d}-{tm.tm_mday:02d}", f"{tm.tm_hour:02d}"


def encode_record(t: int, stream: str, q: int, raw: bytes) -> bytes:
    """One record line (with trailing newline)."""
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        return orjson.dumps({"t": t, "s": stream, "q": q, "b": base64.b64encode(raw).decode("ascii")}) + b"\n"
    return orjson.dumps({"t": t, "s": stream, "q": q, "d": text}) + b"\n"


def _fsync_dir(path: Path) -> None:
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


class _Segment:
    """One open, append-only segment file."""

    def __init__(self, raw_dir: Path, stream: str, hour: int, now_ns: int) -> None:
        day, hh = utc_day_hour(hour * HOUR_NS)
        d = raw_dir / stream / day
        d.mkdir(parents=True, exist_ok=True)
        part = 0
        while True:
            stem = hh if part == 0 else f"{hh}.p{part}"
            path = d / (stem + SEGMENT_SUFFIX)
            try:
                self.f = open(path, "xb")  # O_EXCL: never reuse an existing file
                break
            except FileExistsError:
                part += 1
        _fsync_dir(d)
        self.stream, self.hour, self.path, self.stem, self.part = stream, hour, path, stem, part
        self.count = 0
        self.frames = 0
        self.raw_bytes = 0
        self.file_bytes = 0
        self.first_t = self.last_t = 0
        self.min_t = self.max_t = 0
        self.min_q = self.max_q = -1
        self.opened_ns = now_ns
        self.closed = False

    def write_frame(self, frame: bytes, recs: list[tuple[str, int, int, bytes]], n_raw: int) -> None:
        self.f.write(frame)
        self.f.flush()
        self.frames += 1
        self.file_bytes += len(frame)
        self.raw_bytes += n_raw
        for _, t, q, _ in recs:
            if self.count == 0:
                self.first_t = self.min_t = self.max_t = t
                self.min_q = self.max_q = q
            else:
                self.min_t = min(self.min_t, t)
                self.max_t = max(self.max_t, t)
                self.min_q = min(self.min_q, q)
                self.max_q = max(self.max_q, q)
            self.last_t = t
            self.count += 1

    def fsync(self) -> None:
        if not self.closed:
            os.fsync(self.f.fileno())

    def close(self, now_ns: int) -> None:
        if self.closed:
            return
        self.f.flush()
        os.fsync(self.f.fileno())
        self.f.close()
        self.closed = True
        idx = {
            "format": FORMAT,
            "stream": self.stream,
            "segment": self.path.name,
            "hour_start_ns": self.hour * HOUR_NS,
            "first_t": self.first_t,
            "last_t": self.last_t,
            "min_t": self.min_t,
            "max_t": self.max_t,
            "count": self.count,
            "min_q": self.min_q,
            "max_q": self.max_q,
            "frames": self.frames,
            "raw_bytes": self.raw_bytes,
            "file_bytes": self.file_bytes,
            "opened_ns": self.opened_ns,
            "closed_ns": now_ns,
            "pid": os.getpid(),
            "host": socket.gethostname(),
        }
        ipath = self.path.with_name(self.stem + INDEX_SUFFIX)
        tmp = ipath.with_name(ipath.name + f".tmp{os.getpid()}")
        with open(tmp, "wb") as f:
            f.write(orjson.dumps(idx, option=orjson.OPT_INDENT_2 | orjson.OPT_SORT_KEYS))
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, ipath)
        _fsync_dir(ipath.parent)


@dataclass
class StreamStats:
    count: int = 0
    bytes: int = 0
    first_t: int = 0
    last_t: int = 0


@dataclass
class RecorderStats:
    records: int = 0
    bytes: int = 0
    flushes: int = 0
    frames: int = 0
    segments_opened: int = 0
    segments_closed: int = 0
    write_errors: int = 0
    last_error: str = ""
    streams: dict[str, StreamStats] = field(default_factory=dict)


class Recorder:
    """Append-only raw capture (see module docstring).

    Usage::

        rec = Recorder("data")            # starts the background flusher thread
        rec.write("coinbase.ws", recv_ns, raw_bytes)
        ...
        rec.close()                        # final flush + fsync + sidecar indexes
    """

    def __init__(
        self,
        root: str | Path,
        *,
        flush_interval_s: float = 1.0,
        fsync_interval_s: float = 30.0,
        zstd_level: int = 3,
        start: bool = True,
        clock_ns: Callable[[], int] = time.time_ns,
        close_grace_s: float = 5.0,
        max_pending_bytes: int = 1 << 30,
    ) -> None:
        self.root = Path(root)
        self.raw_dir = self.root / "raw"
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.flush_interval_s = flush_interval_s
        self.fsync_interval_s = fsync_interval_s
        self.close_grace_ns = int(max(close_grace_s, 3 * flush_interval_s) * NS_PER_S)
        self.max_pending_bytes = max_pending_bytes
        self._clock = clock_ns
        self._cctx = zstd.ZstdCompressor(level=zstd_level, write_content_size=True, write_checksum=True)
        self._lock = threading.Lock()  # guards _pending, _q, stats
        self._io_lock = threading.RLock()  # serializes flush/close (single writer)
        self._pending: list[tuple[str, int, int, bytes]] = []
        self._pending_bytes = 0
        self._q = 0
        self._segments: dict[str, _Segment] = {}
        self._valid_streams: set[str] = set()
        self._closed = False
        self._stop = threading.Event()
        self._last_fsync = time.monotonic()
        self._warned_backlog = False
        self.stats = RecorderStats()
        self._thread: threading.Thread | None = None
        if start:
            self.start()

    # ------------------------------------------------------------------ public API
    def start(self) -> None:
        if self._thread is None:
            self._thread = threading.Thread(target=self._run, name="dh-recorder", daemon=True)
            self._thread.start()

    def write(self, stream: str, recv_ns: int, raw: bytes) -> None:
        """Queue one raw frame. Never blocks on I/O. Thread-safe."""
        if stream not in self._valid_streams:
            if not valid_stream_name(stream):
                raise ValueError(f"invalid stream name {stream!r}")
            self._valid_streams.add(stream)
        if not isinstance(raw, (bytes, bytearray, memoryview)):
            raise TypeError(f"raw must be bytes, got {type(raw).__name__}")
        raw = bytes(raw)
        with self._lock:
            if self._closed:
                raise RuntimeError("recorder is closed")
            q = self._q
            self._q += 1
            self._pending.append((stream, int(recv_ns), q, raw))
            self._pending_bytes += len(raw)
            st = self.stats.streams.get(stream)
            if st is None:
                st = self.stats.streams[stream] = StreamStats(first_t=int(recv_ns))
            st.count += 1
            st.bytes += len(raw)
            st.last_t = int(recv_ns)
            self.stats.records += 1
            self.stats.bytes += len(raw)
            backlog = self._pending_bytes
        if backlog > self.max_pending_bytes and not self._warned_backlog:
            self._warned_backlog = True
            log.error("recorder backlog %.0f MB: disk too slow?", backlog / 1e6)

    def write_event(self, stream: str, ev: Event) -> None:
        """Record a normalized event (codec-encoded) at its own ``ts``."""
        self.write(stream, ev.ts, encode_event(ev))

    def flush(self, fsync: bool = False) -> None:
        """Write everything queued so far (one zstd frame per touched segment)."""
        with self._io_lock:
            with self._lock:
                batch, self._pending = self._pending, []
                self._pending_bytes = 0
                self._warned_backlog = False
            if batch:
                by_stream: dict[str, list[tuple[str, int, int, bytes]]] = {}
                for rec in batch:
                    by_stream.setdefault(rec[0], []).append(rec)
                for stream in sorted(by_stream):
                    try:
                        self._write_stream(stream, by_stream[stream])
                    except Exception as exc:  # noqa: BLE001 - never let one stream kill capture
                        self.stats.write_errors += 1
                        self.stats.last_error = f"{stream}: {type(exc).__name__}: {exc}"
                        log.exception("recorder: failed writing %d records of %s", len(by_stream[stream]), stream)
                self.stats.flushes += 1
            if fsync:
                for seg in self._segments.values():
                    seg.fsync()
                self._last_fsync = time.monotonic()

    def close_idle_segments(self, now_ns: int | None = None) -> None:
        """Close (fsync + index) segments whose hour ended more than the grace period ago."""
        now_ns = self._clock() if now_ns is None else now_ns
        with self._io_lock:
            for stream, seg in list(self._segments.items()):
                if (seg.hour + 1) * HOUR_NS + self.close_grace_ns <= now_ns:
                    seg.close(now_ns)
                    self.stats.segments_closed += 1
                    del self._segments[stream]

    def close(self) -> None:
        """Stop the flusher, write everything, fsync and write sidecar indexes."""
        with self._lock:
            if self._closed:
                return
            self._closed = True
        self._stop.set()
        if self._thread is not None and self._thread is not threading.current_thread():
            self._thread.join(timeout=30)
        with self._io_lock:
            self.flush(fsync=True)
            now = self._clock()
            for seg in self._segments.values():
                seg.close(now)
                self.stats.segments_closed += 1
            self._segments.clear()

    def __enter__(self) -> Recorder:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def open_segments(self) -> dict[str, Path]:
        return {s: seg.path for s, seg in self._segments.items()}

    # ------------------------------------------------------------------ internals
    def _write_stream(self, stream: str, recs: list[tuple[str, int, int, bytes]]) -> None:
        seg = self._segments.get(stream)
        buf = bytearray()
        chunk: list[tuple[str, int, int, bytes]] = []
        n_raw = 0
        for rec in recs:
            h = rec[1] // HOUR_NS
            if seg is None or h != seg.hour:
                if chunk and seg is not None:
                    self._emit(seg, buf, chunk, n_raw)
                    buf, chunk, n_raw = bytearray(), [], 0
                now = self._clock()
                if seg is not None:
                    seg.close(now)
                    self.stats.segments_closed += 1
                seg = _Segment(self.raw_dir, stream, h, now)
                self._segments[stream] = seg
                self.stats.segments_opened += 1
            buf += encode_record(rec[1], stream, rec[2], rec[3])
            chunk.append(rec)
            n_raw += len(rec[3])
        if chunk and seg is not None:
            self._emit(seg, buf, chunk, n_raw)

    def _emit(self, seg: _Segment, buf: bytearray, chunk: list[tuple[str, int, int, bytes]], n_raw: int) -> None:
        frame = self._cctx.compress(bytes(buf))
        seg.write_frame(frame, chunk, n_raw)
        self.stats.frames += 1

    def _run(self) -> None:
        while not self._stop.wait(self.flush_interval_s):
            try:
                do_fsync = self.fsync_interval_s > 0 and time.monotonic() - self._last_fsync >= self.fsync_interval_s
                self.flush(fsync=do_fsync)
                self.close_idle_segments()
            except Exception as exc:  # noqa: BLE001
                self.stats.write_errors += 1
                self.stats.last_error = f"{type(exc).__name__}: {exc}"
                log.exception("recorder flush failed")


# ============================================================================ clock health
class _Timeval(ctypes.Structure):
    _fields_ = [("tv_sec", ctypes.c_long), ("tv_usec", ctypes.c_long)]


class _Timex(ctypes.Structure):  # struct timex (linux/timex.h), native alignment
    _fields_ = [
        ("modes", ctypes.c_uint),
        ("offset", ctypes.c_long),
        ("freq", ctypes.c_long),
        ("maxerror", ctypes.c_long),
        ("esterror", ctypes.c_long),
        ("status", ctypes.c_int),
        ("constant", ctypes.c_long),
        ("precision", ctypes.c_long),
        ("tolerance", ctypes.c_long),
        ("time", _Timeval),
        ("tick", ctypes.c_long),
        ("ppsfreq", ctypes.c_long),
        ("jitter", ctypes.c_long),
        ("shift", ctypes.c_int),
        ("stabil", ctypes.c_long),
        ("jitcnt", ctypes.c_long),
        ("calcnt", ctypes.c_long),
        ("errcnt", ctypes.c_long),
        ("stbcnt", ctypes.c_long),
        ("tai", ctypes.c_int),
        ("_pad", ctypes.c_int * 11),
    ]


STA_UNSYNC = 0x0040
STA_NANO = 0x2000


def _adjtimex() -> dict[str, Any] | None:
    """Read-only adjtimex(2) (modes=0 needs no privileges). Linux only."""
    try:
        libname = ctypes.util.find_library("c")
        libc = ctypes.CDLL(libname, use_errno=True)
        tx = _Timex()
        tx.modes = 0
        state = libc.adjtimex(ctypes.byref(tx))
    except Exception:  # noqa: BLE001
        return None
    if state < 0:
        return None
    scale = 1e-9 if tx.status & STA_NANO else 1e-6
    return {
        "state": int(state),  # 0 TIME_OK ... 5 TIME_ERROR (clock not synchronized)
        "synced": not (tx.status & STA_UNSYNC) and state != 5,
        "offset_s": tx.offset * scale,
        "maxerror_s": tx.maxerror * 1e-6,
        "esterror_s": tx.esterror * 1e-6,
        "freq_ppm": tx.freq / 65536.0,
        "status": int(tx.status),
    }


def _run_cmd(args: list[str], timeout: float = 2.0) -> str | None:
    if shutil.which(args[0]) is None:
        return None
    try:
        r = subprocess.run(args, capture_output=True, text=True, timeout=timeout, check=False)
    except (OSError, subprocess.SubprocessError):
        return None
    return r.stdout if r.returncode == 0 and r.stdout.strip() else None


def _chronyc() -> dict[str, Any] | None:
    out = _run_cmd(["chronyc", "-c", "tracking"])
    if not out:
        return None
    f = out.strip().splitlines()[0].split(",")
    d: dict[str, Any] = {"raw": out.strip()}
    try:
        # CSV: refid, name, stratum, ref time, system time offset (s), last offset, rms offset,
        # freq ppm, residual freq, skew, root delay, root dispersion, update interval, leap
        d.update(
            stratum=int(f[2]),
            offset_s=float(f[4]),
            last_offset_s=float(f[5]),
            rms_offset_s=float(f[6]),
            root_delay_s=float(f[10]),
            root_dispersion_s=float(f[11]),
            leap=f[13] if len(f) > 13 else "",
        )
        d["synced"] = d["leap"] != "Not synchronised"
        # max plausible error vs UTC ~ |offset| + root_delay/2 + root_dispersion
        d["est_error_s"] = abs(d["offset_s"]) + d["root_delay_s"] / 2 + d["root_dispersion_s"]
    except (IndexError, ValueError):
        pass
    return d


_OFFSET_RE = re.compile(r"Offset:\s*([+-]?[0-9.]+)\s*(ns|us|µs|ms|s)\b")


def _timedatectl() -> dict[str, Any] | None:
    out = _run_cmd(["timedatectl", "timesync-status"])
    d: dict[str, Any] = {}
    if out:
        d["raw"] = out.strip()[:1000]
        m = _OFFSET_RE.search(out)
        if m:
            mult = {"ns": 1e-9, "us": 1e-6, "µs": 1e-6, "ms": 1e-3, "s": 1.0}[m.group(2)]
            d["offset_s"] = float(m.group(1)) * mult
    show = _run_cmd(["timedatectl", "show"])
    if show:
        for line in show.splitlines():
            if line.startswith("NTPSynchronized="):
                d["synced"] = line.split("=", 1)[1].strip() == "yes"
    return d or None


def sample_clock() -> dict[str, Any]:
    """Best-effort clock-health sample. ``src`` is the most informative source found:
    'chronyc' > 'timedatectl' > 'adjtimex' > 'unknown'. Offsets in seconds."""
    rec: dict[str, Any] = {"wall_ns": time.time_ns(), "mono_ns": time.monotonic_ns()}
    chrony = _chronyc()
    tdc = None if chrony else _timedatectl()
    adj = _adjtimex()
    if chrony:
        rec.update(src="chronyc", offset_s=chrony.get("offset_s"), est_error_s=chrony.get("est_error_s"), synced=chrony.get("synced"))
        rec["chronyc"] = chrony
    elif tdc:
        rec.update(src="timedatectl", offset_s=tdc.get("offset_s"), est_error_s=None, synced=tdc.get("synced"))
        rec["timedatectl"] = tdc
    elif adj:
        rec.update(src="adjtimex", offset_s=adj.get("offset_s"), est_error_s=adj.get("esterror_s"), synced=adj.get("synced"))
    else:
        rec.update(src="unknown", offset_s=None, est_error_s=None, synced=None)
    if adj:
        rec["adjtimex"] = adj
    return rec


class ClockSampler:
    """Records ``sample_clock()`` to stream ``clock`` every ``interval_s`` seconds."""

    def __init__(self, recorder: Recorder, stream: str = "clock", interval_s: float = 60.0) -> None:
        self.recorder, self.stream, self.interval_s = recorder, stream, interval_s
        self.last: dict[str, Any] | None = None

    def sample_once(self) -> dict[str, Any]:
        rec = sample_clock()
        self.last = rec
        self.recorder.write(self.stream, time.time_ns(), orjson.dumps(rec))
        return rec

    async def run(self) -> None:
        while True:
            try:
                rec = await asyncio.to_thread(sample_clock)
                self.last = rec
                self.recorder.write(self.stream, time.time_ns(), orjson.dumps(rec))
            except Exception:  # noqa: BLE001
                log.exception("clock sample failed")
            await asyncio.sleep(self.interval_s)
