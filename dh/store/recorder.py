"""Append-only raw capture: zstd-compressed JSON-lines segments, one per stream per UTC hour.

Layout (``root`` is the data directory, e.g. ``data``)::

    <root>/raw/<stream>/<YYYY-MM-DD>/<HH>.jsonl.zst        first segment of that hour
    <root>/raw/<stream>/<YYYY-MM-DD>/<HH>.p<N>.jsonl.zst   later parts of the same hour
                                                           (process restart, clock step)
    <root>/raw/<stream>/<YYYY-MM-DD>/<HH>[.p<N>].idx.json  sidecar index, written on close

The hour of a record is the UTC hour of its receive time ``t``. Files are created with
``O_EXCL`` and only ever appended to by the process that created them: existing files are
never reopened, rewritten or truncated (the one exception: the writer cuts the partial tail of
a frame IT failed to write back off its own open segment, see "Full disk" below).

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

Full disk: the store shares its filesystem with other systems. A frame that cannot be written
in full (ENOSPC, EDQUOT, EIO) is cut back off the segment (``ftruncate`` to the previous frame
boundary), so a segment written by a live process always ends on a frame boundary; if even
that fails the segment is abandoned (closed without index, later records go to a new part).
The records of the failed frame are dropped (``stats.dropped_records``) and after ENOSPC /
EDQUOT writing pauses ``disk_full_backoff_s`` (records stay queued). ``DiskGuard`` keeps the
disk from getting there: below ``min_free_gb_shed`` it stops recording the non-essential
``shed_streams`` (``Recorder.set_shed``; ``meta`` records mark each transition) until free
space is back above the threshold plus a hysteresis, and below ``min_free_gb_stop`` the
collector flushes, fsyncs and closes the store and exits with ``EXIT_LOW_DISK`` (5). Kalshi
streams and ``meta`` / ``clock`` / ``status`` are never shed.

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
import errno
import logging
import os
import re
import shutil
import socket
import subprocess
import threading
import time
from collections.abc import Callable, Iterable
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
_DISK_FULL_ERRNOS = frozenset({errno.ENOSPC, errno.EDQUOT})


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
                # O_EXCL: never reuse an existing file. Unbuffered: each frame is one write
                # loop, so a failed write leaves nothing buffered and can be cut back exactly.
                self.f = open(path, "xb", buffering=0)
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
        self.abandoned = False

    def write_frame(self, frame: bytes, recs: list[tuple[str, int, int, bytes]], n_raw: int) -> None:
        """Append one complete zstd frame. If the write fails part-way (ENOSPC, EIO, ...), the
        bytes already written are cut off again (the file ends on the previous frame boundary)
        and the error is re-raised; if the cut itself fails the segment is abandoned."""
        view = memoryview(frame)
        off = 0
        try:
            while off < len(frame):
                n = self.f.write(view[off:])
                if not n:
                    raise OSError(errno.EIO, f"short write to {self.path}")
                off += n
        except BaseException:
            self._cut_back()
            raise
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

    def _cut_back(self) -> None:
        """Remove a partially written frame: truncate to the last complete frame and seek there."""
        try:
            os.ftruncate(self.f.fileno(), self.file_bytes)
            self.f.seek(self.file_bytes)
        except (OSError, ValueError):
            log.exception("recorder: cannot cut the partial frame off %s: abandoning the segment", self.path)
            self.abandon()

    def abandon(self) -> None:
        """Stop using this segment without an index (after an unrecoverable write error). The
        reader still recovers every complete record; the next record of the hour opens a new
        part. Never raises."""
        if self.closed:
            return
        self.closed = self.abandoned = True
        try:
            self.f.close()
        except OSError:
            pass

    def fsync(self) -> None:
        if not self.closed:
            os.fsync(self.f.fileno())

    def close(self, now_ns: int) -> None:
        if self.closed:
            return
        self.closed = True  # never retried: a failure below leaves the segment without an index
        try:
            os.fsync(self.f.fileno())
        finally:
            self.f.close()
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
    shed: int = 0  # records NOT recorded because the stream was shed (low disk)


@dataclass
class RecorderStats:
    records: int = 0
    bytes: int = 0
    flushes: int = 0
    frames: int = 0
    segments_opened: int = 0
    segments_closed: int = 0
    segments_abandoned: int = 0
    write_errors: int = 0
    last_error: str = ""
    dropped_records: int = 0  # queued but lost to a write failure (the frame was cut back off)
    disk_full_errors: int = 0  # ENOSPC / EDQUOT write failures
    shed_records: int = 0  # not recorded: stream shed by DiskGuard (low disk)
    shed_streams: tuple[str, ...] = ()
    free_gb: float | None = None  # last DiskGuard measurement of the store's filesystem
    total_gb: float | None = None
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
        disk_full_backoff_s: float = 5.0,
    ) -> None:
        self.root = Path(root)
        self.raw_dir = self.root / "raw"
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.flush_interval_s = flush_interval_s
        self.fsync_interval_s = fsync_interval_s
        self.close_grace_ns = int(max(close_grace_s, 3 * flush_interval_s) * NS_PER_S)
        self.max_pending_bytes = max_pending_bytes
        self.disk_full_backoff_s = disk_full_backoff_s
        self._disk_full_until = 0.0  # time.monotonic() until which writes pause after ENOSPC
        self._shed: frozenset[str] = frozenset()  # streams not recorded (DiskGuard)
        self._written = 0  # records written by the current _write_stream call
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
            st = self.stats.streams.get(stream)
            if st is None:
                st = self.stats.streams[stream] = StreamStats()
            if stream in self._shed:  # low disk (DiskGuard): not recorded, no q consumed
                st.shed += 1
                self.stats.shed_records += 1
                return
            q = self._q
            self._q += 1
            self._pending.append((stream, int(recv_ns), q, raw))
            self._pending_bytes += len(raw)
            if st.count == 0:
                st.first_t = int(recv_ns)
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
        self._flush(fsync, retry_now=False)

    def _flush(self, fsync: bool, retry_now: bool) -> None:
        with self._io_lock:
            if not retry_now and time.monotonic() < self._disk_full_until:
                # disk full: keep the records queued (bounded) and retry after the back-off
                with self._lock:
                    if self._pending_bytes > self.max_pending_bytes:
                        self.stats.dropped_records += len(self._pending)
                        self._pending, self._pending_bytes = [], 0
                return
            with self._lock:
                batch, self._pending = self._pending, []
                self._pending_bytes = 0
                self._warned_backlog = False
            if batch:
                by_stream: dict[str, list[tuple[str, int, int, bytes]]] = {}
                for rec in batch:
                    by_stream.setdefault(rec[0], []).append(rec)
                disk_full = False
                for stream in sorted(by_stream):
                    recs = by_stream[stream]
                    if disk_full:  # the rest of this batch cannot be written either
                        self.stats.dropped_records += len(recs)
                        continue
                    self._written = 0
                    try:
                        self._write_stream(stream, recs)
                    except Exception as exc:  # noqa: BLE001 - never let one stream kill capture
                        self.stats.write_errors += 1
                        self.stats.dropped_records += len(recs) - self._written
                        self.stats.last_error = f"{stream}: {type(exc).__name__}: {exc}"
                        seg = self._segments.get(stream)
                        if seg is not None and seg.closed:  # abandoned: next record opens a new part
                            del self._segments[stream]
                        if isinstance(exc, OSError) and exc.errno in _DISK_FULL_ERRNOS:
                            disk_full = True
                            self.stats.disk_full_errors += 1
                            self._disk_full_until = time.monotonic() + self.disk_full_backoff_s
                            log.error("recorder: DISK FULL writing %s (%s): %d records dropped, writes paused %.0f s",
                                      stream, exc, len(recs) - self._written, self.disk_full_backoff_s)
                        else:
                            log.exception("recorder: failed writing %d records of %s", len(recs), stream)
                self.stats.flushes += 1
            if fsync:
                for seg in list(self._segments.values()):
                    try:
                        seg.fsync()
                    except OSError as exc:
                        self.stats.write_errors += 1
                        self.stats.last_error = f"fsync {seg.path}: {exc}"
                        log.error("recorder: fsync %s failed: %s", seg.path, exc)
                self._last_fsync = time.monotonic()

    def _close_segment(self, seg: _Segment, now_ns: int) -> None:
        try:
            seg.close(now_ns)
            self.stats.segments_closed += 1
        except Exception as exc:  # noqa: BLE001 - the data is on disk; only the index is missing
            self.stats.write_errors += 1
            self.stats.last_error = f"close {seg.path}: {type(exc).__name__}: {exc}"
            log.exception("recorder: closing %s failed (segment left without index)", seg.path)

    def close_idle_segments(self, now_ns: int | None = None) -> None:
        """Close (fsync + index) segments whose hour ended more than the grace period ago."""
        now_ns = self._clock() if now_ns is None else now_ns
        with self._io_lock:
            for stream, seg in list(self._segments.items()):
                if (seg.hour + 1) * HOUR_NS + self.close_grace_ns <= now_ns:
                    self._close_segment(seg, now_ns)
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
            self._flush(fsync=True, retry_now=True)  # last chance, even during a disk-full back-off
            now = self._clock()
            for seg in self._segments.values():
                self._close_segment(seg, now)
            self._segments.clear()

    def set_shed(self, streams: Iterable[str]) -> frozenset[str]:
        """Stop recording ``streams`` (an empty iterable resumes all): their records are dropped
        in ``write`` (counted in ``stats.shed_records`` and ``StreamStats.shed``; no ``q`` is
        consumed). Streams that are never shed (``never_shed``: ``kalshi.*``, ``meta``,
        ``clock``, ``status``) are ignored. Returns the effective set. Driven by ``DiskGuard``."""
        eff = frozenset(s for s in streams if not never_shed(s))
        with self._lock:
            self._shed = eff
            self.stats.shed_streams = tuple(sorted(eff))
        return eff

    @property
    def shed_streams(self) -> frozenset[str]:
        return self._shed

    def __enter__(self) -> Recorder:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def stream_stats(self) -> dict[str, StreamStats]:
        """Consistent copy of the per-stream counters (records queued so far)."""
        with self._lock:
            return {k: StreamStats(v.count, v.bytes, v.first_t, v.last_t, v.shed) for k, v in self.stats.streams.items()}

    def open_segments(self) -> dict[str, Path]:
        return {s: seg.path for s, seg in self._segments.items()}

    # ------------------------------------------------------------------ internals
    def _write_stream(self, stream: str, recs: list[tuple[str, int, int, bytes]]) -> None:
        seg = self._segments.get(stream)
        if seg is not None and seg.closed:  # abandoned after a write error
            del self._segments[stream]
            seg = None
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
                    self._close_segment(seg, now)
                    self._segments.pop(stream, None)
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
        try:
            seg.write_frame(frame, chunk, n_raw)
        except BaseException:
            if seg.abandoned:
                self.stats.segments_abandoned += 1
            raise
        self.stats.frames += 1
        self._written += len(chunk)

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


# ============================================================================ disk guard
GB = 10**9  # decimal gigabytes, as Finder and `df -H` report them
EXIT_LOW_DISK = 5  # scripts/record.py exit status: stopped, or refused to start, below min_free_gb_stop
# Non-essential for M1 (perps/options context and the minor BRTI constituents). Kalshi, the
# largest BRTI constituents (coinbase, kraken, bitstamp), clock and meta keep recording.
DEFAULT_SHED_STREAMS = ("deribit.options", "deribit.ws", "okx.ws", "hyperliquid.ws", "gemini.ws", "cryptocom.ws")
NEVER_SHED = frozenset({"meta", "clock", "status"})
NEVER_SHED_PREFIXES = ("kalshi.",)


def never_shed(stream: str) -> bool:
    """Kalshi streams (the traded venue) and the recorder's own meta/clock/status: always recorded."""
    return stream in NEVER_SHED or stream.startswith(NEVER_SHED_PREFIXES)


def disk_usage_path(root: str | Path) -> Path:
    """Directory whose filesystem holds the store: ``<root>/raw`` when it exists (it may be a
    symlink to another volume), else the nearest existing ancestor of ``root``."""
    p = Path(root).absolute()
    if (p / "raw").exists():
        return p / "raw"
    while not p.exists() and p.parent != p:
        p = p.parent
    return p


@dataclass(frozen=True)
class DiskCheck:
    action: str  # 'ok' | 'shed' | 'unshed' | 'stop' | 'error' (measurement failed)
    free_gb: float | None
    total_gb: float | None
    shed: bool  # shed state after the check


ShedCallback = Callable[[tuple[str, ...], dict[str, Any]], None]


class DiskGuard:
    """Free-space guard for the filesystem holding the store (shared with other systems).

    ``check()`` measures free space (``shutil.disk_usage``, space available to this user) and
    applies two thresholds, in decimal GB:

    * ``free < min_free_gb_shed``: stop recording ``shed_streams`` (``Recorder.set_shed``). A
      ``meta`` record ``{"kind": "disk_shed", "streams": [...], "last_t": {...}, ...}`` is written
      first, so replay knows these streams stopped deliberately (not a feed gap); then
      ``before_shed(streams, info)`` runs (the collector writes a 'disconnected' marker into each
      live venue stream so replay invalidates its books). Recording resumes once
      ``free >= min_free_gb_shed + hysteresis_gb``: ``meta`` ``{"kind": "disk_unshed", ...}``
      (with the number of records not recorded), then ``after_unshed(streams, info)`` (the
      collector reconnects those feeds for a fresh book snapshot).
    * ``free < min_free_gb_stop``: ``meta`` ``{"kind": "disk_stop", ...}`` and action ``'stop'``
      (terminal): the caller stops its sources, flushes, fsyncs and closes the store and exits
      with ``EXIT_LOW_DISK``. Before start-up, ``measure()`` + ``below_stop()`` refuse to start.

    Streams for which ``never_shed`` is true (``kalshi.*``, ``meta``, ``clock``, ``status``) are
    removed from ``shed_streams`` with a warning. A threshold of 0 disables that level.
    """

    def __init__(
        self,
        root: str | Path,
        recorder: Recorder | None = None,
        *,
        min_free_gb_shed: float = 30.0,
        min_free_gb_stop: float = 8.0,
        hysteresis_gb: float = 5.0,
        shed_streams: Iterable[str] | None = DEFAULT_SHED_STREAMS,
        interval_s: float = 60.0,
        usage: Callable[[Path], Any] | None = None,
        meta_stream: str = "meta",
        clock_ns: Callable[[], int] = time.time_ns,
    ) -> None:
        self.root = Path(root)
        self.recorder = recorder
        self.min_free_gb_shed = float(min_free_gb_shed)
        self.min_free_gb_stop = float(min_free_gb_stop)
        self.hysteresis_gb = float(hysteresis_gb)
        self.interval_s = float(interval_s)
        if self.min_free_gb_shed < 0 or self.min_free_gb_stop < 0 or self.hysteresis_gb < 0:
            raise ValueError("min_free_gb_shed, min_free_gb_stop and shed_hysteresis_gb must be >= 0")
        if self.min_free_gb_shed > 0 and self.min_free_gb_shed <= self.min_free_gb_stop:
            raise ValueError(f"min_free_gb_shed ({self.min_free_gb_shed:g}) must be above min_free_gb_stop "
                             f"({self.min_free_gb_stop:g}), or 0 to disable shedding")
        if not self.interval_s > 0:
            raise ValueError("disk_check_interval_s must be > 0")
        if isinstance(shed_streams, (str, bytes)):
            raise ValueError(f"shed_streams must be a list of stream names, got {shed_streams!r}")
        streams: list[str] = []
        for s in DEFAULT_SHED_STREAMS if shed_streams is None else shed_streams:
            s = str(s)
            if not valid_stream_name(s):
                raise ValueError(f"shed_streams: invalid stream name {s!r}")
            if never_shed(s):
                log.warning("disk guard: %s is never shed (kalshi.*, meta, clock, status): ignored in shed_streams", s)
            elif s not in streams:
                streams.append(s)
        self.shed_streams: tuple[str, ...] = tuple(streams)
        # late-bound so tests can monkeypatch shutil.disk_usage
        self._usage = usage if usage is not None else (lambda p: shutil.disk_usage(p))
        self.meta_stream = meta_stream
        self._clock = clock_ns
        self.before_shed: ShedCallback | None = None
        self.after_unshed: ShedCallback | None = None
        self.shed = False
        self.stopped = False
        self.shed_since_ns = 0
        self.shed_episodes = 0
        self._shed_records0 = 0
        self.last: DiskCheck | None = None
        self._measure_failed = False

    @classmethod
    def from_config(cls, rcfg: dict[str, Any] | None, root: str | Path, recorder: Recorder | None = None,
                    **kw: Any) -> DiskGuard:
        """From the ``recorder:`` section of config/feeds.yaml (defaults for missing keys)."""
        r = rcfg or {}
        return cls(root, recorder,
                   min_free_gb_shed=float(r.get("min_free_gb_shed", 30.0)),
                   min_free_gb_stop=float(r.get("min_free_gb_stop", 8.0)),
                   hysteresis_gb=float(r.get("shed_hysteresis_gb", 5.0)),
                   shed_streams=r.get("shed_streams"),
                   interval_s=float(r.get("disk_check_interval_s", 60.0)),
                   **kw)

    @property
    def resume_gb(self) -> float:
        return self.min_free_gb_shed + self.hysteresis_gb

    def path(self) -> Path:
        return disk_usage_path(self.root)

    def measure(self) -> tuple[float, float]:
        """(free GB, total GB) of the store's filesystem. Raises OSError."""
        u = self._usage(self.path())
        return u.free / GB, u.total / GB

    def below_stop(self, free_gb: float) -> bool:
        return free_gb < self.min_free_gb_stop

    def describe(self) -> str:
        return (f"shed {len(self.shed_streams)} streams below {self.min_free_gb_shed:g} GB free "
                f"(resume at {self.resume_gb:g} GB), stop below {self.min_free_gb_stop:g} GB, "
                f"every {self.interval_s:g} s")

    def info(self, free_gb: float, total_gb: float) -> dict[str, Any]:
        return {"free_gb": round(free_gb, 3), "total_gb": round(total_gb, 3),
                "min_free_gb_shed": self.min_free_gb_shed, "resume_above_gb": self.resume_gb,
                "min_free_gb_stop": self.min_free_gb_stop, "path": str(self.path()), "pid": os.getpid()}

    def check(self) -> DiskCheck:
        """Measure and apply the thresholds (see the class docstring). Never raises OSError: a
        failed measurement is logged and returns action 'error' with the state unchanged."""
        rec = self.recorder
        if rec is None:
            raise RuntimeError("DiskGuard.check() needs a recorder (use measure() before start-up)")
        try:
            free, total = self.measure()
        except OSError as exc:
            if not self._measure_failed:
                log.warning("disk guard: cannot measure free space of %s: %s", self.path(), exc)
            self._measure_failed = True
            self.last = DiskCheck("error", None, None, self.shed)
            return self.last
        self._measure_failed = False
        rec.stats.free_gb, rec.stats.total_gb = free, total
        action = "ok"
        if self.stopped or self.below_stop(free):
            action = "stop"
            if not self.stopped:
                self.stopped = True
                self._meta("disk_stop", free, total, shed=self.shed, exit_code=EXIT_LOW_DISK)
                log.error("disk: %.1f GB free on %s, below min_free_gb_stop %g GB: stopping the recorder "
                          "(flush, fsync, close; exit %d)", free, self.path(), self.min_free_gb_stop, EXIT_LOW_DISK)
        elif not self.shed and free < self.min_free_gb_shed:
            action = "shed"
            self._do_shed(free, total)
        elif self.shed and free >= self.resume_gb:
            action = "unshed"
            self._do_unshed(free, total)
        else:
            log.debug("disk: %.1f GB free (shed=%s)", free, self.shed)
        self.last = DiskCheck(action, free, total, self.shed)
        return self.last

    # ------------------------------------------------------------------ internals
    def _meta(self, kind: str, free: float, total: float, **extra: Any) -> dict[str, Any]:
        assert self.recorder is not None
        body = {"kind": kind, **self.info(free, total), **extra}
        self.recorder.write(self.meta_stream, self._clock(), orjson.dumps(body))
        return body

    def _do_shed(self, free: float, total: float) -> None:
        rec = self.recorder
        assert rec is not None
        streams = self.shed_streams
        stats = rec.stream_stats()
        last_t = {s: stats[s].last_t for s in streams if s in stats}
        info = self._meta("disk_shed", free, total, streams=list(streams), last_t=last_t)
        if self.before_shed is not None and streams:
            try:
                self.before_shed(streams, info)
            except Exception:  # noqa: BLE001 - shedding itself must happen regardless
                log.exception("disk guard: before_shed callback failed")
        rec.set_shed(streams)
        self.shed = True
        self.shed_since_ns = self._clock()
        self.shed_episodes += 1
        self._shed_records0 = rec.stats.shed_records
        log.warning("disk: %.1f GB free on %s, below min_free_gb_shed %g GB: NOT recording %s until %g GB free",
                    free, self.path(), self.min_free_gb_shed, ",".join(streams) or "(no streams configured)",
                    self.resume_gb)

    def _do_unshed(self, free: float, total: float) -> None:
        rec = self.recorder
        assert rec is not None
        streams = self.shed_streams
        missed = rec.stats.shed_records - self._shed_records0
        info = self._meta("disk_unshed", free, total, streams=list(streams), shed_since_ns=self.shed_since_ns,
                          not_recorded=missed)
        rec.set_shed(())
        self.shed = False
        if self.after_unshed is not None and streams:
            try:
                self.after_unshed(streams, info)
            except Exception:  # noqa: BLE001
                log.exception("disk guard: after_unshed callback failed")
        log.warning("disk: %.1f GB free on %s, back above %g GB: recording %s again (%d records were not recorded)",
                    free, self.path(), self.resume_gb, ",".join(streams), missed)


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


_SNTP_RE = re.compile(r"^\s*([+-][0-9.]+)\s+\+/-\s+([0-9.]+)\s+(\S+)")
SNTP_DEFAULT_SERVER = "time.apple.com"
SNTP_ENABLED = True  # the test suite turns this off (a network round trip per sample)


def _ntp_server() -> str:
    """First 'server' of /etc/ntp.conf (macOS writes the System Settings time server there)."""
    try:
        for line in Path("/etc/ntp.conf").read_text().splitlines():
            f = line.split()
            if len(f) >= 2 and f[0] == "server":
                return f[1]
    except OSError:
        pass
    return SNTP_DEFAULT_SERVER


def _sntp(server: str | None = None, timeout_s: int = 2) -> dict[str, Any] | None:
    """macOS fallback (no chronyc / timedatectl / adjtimex): one query-only SNTP exchange
    (``sntp -t <timeout> <server>``; without -s/-S sntp never sets the clock, no root needed).
    Output's last line '+0.034883 +/- 0.021637 time.apple.com <addr>': offset = server - local
    (positive = local clock BEHIND true time, the same sign as chronyc's 'slow' offset), and
    the +/- error bound. One network round trip (~50 ms, up to a few s on a timeout)."""
    if not SNTP_ENABLED:
        return None
    srv = server or _ntp_server()
    out = _run_cmd(["sntp", "-t", str(int(timeout_s)), srv], timeout=3.0 * timeout_s + 2)
    if not out:
        return None
    for line in reversed(out.strip().splitlines()):
        m = _SNTP_RE.match(line)
        if m:
            return {"offset_s": float(m.group(1)), "est_error_s": float(m.group(2)), "server": m.group(3),
                    "raw": line.strip()[:200]}
    return None


def sample_clock() -> dict[str, Any]:
    """Best-effort clock-health sample. ``src`` is the most informative source found:
    'chronyc' > 'timedatectl' > 'adjtimex' > 'sntp' (macOS: query-only SNTP offset against
    the configured time server; ``synced`` stays None: it measures, it does not tell whether
    the OS disciplines the clock) > 'unknown'. Offsets in seconds, positive = local clock
    behind. The live runner trusts 'chronyc'/'timedatectl', and on macOS also 'sntp'
    (dh.live.runner.trusted_clock_sources)."""
    rec: dict[str, Any] = {"wall_ns": time.time_ns(), "mono_ns": time.monotonic_ns()}
    chrony = _chronyc()
    tdc = None if chrony else _timedatectl()
    adj = _adjtimex()
    sn = _sntp() if not (chrony or tdc or adj) else None
    if chrony:
        rec.update(src="chronyc", offset_s=chrony.get("offset_s"), est_error_s=chrony.get("est_error_s"), synced=chrony.get("synced"))
        rec["chronyc"] = chrony
    elif tdc:
        rec.update(src="timedatectl", offset_s=tdc.get("offset_s"), est_error_s=None, synced=tdc.get("synced"))
        rec["timedatectl"] = tdc
    elif adj:
        rec.update(src="adjtimex", offset_s=adj.get("offset_s"), est_error_s=adj.get("esterror_s"), synced=adj.get("synced"))
    elif sn:
        rec.update(src="sntp", offset_s=sn["offset_s"], est_error_s=sn["est_error_s"], synced=None)
        rec["sntp"] = sn
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
