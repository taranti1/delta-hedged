"""Deterministic replay of the raw store (see dh.store.recorder for the on-disk format).

``iter_raw(root, streams, t0_ns, t1_ns)``
    k-way merge of the selected streams' records with receive time in [t0_ns, t1_ns),
    ordered by (t, stream rank, q) where the rank is the stream's position in ``streams``
    (patterns like ``kalshi.rest.*`` expand to sorted matches). Within one stream the file
    (arrival) order is always preserved, even across a backwards clock step.

``iter_events(root, streams, t0_ns, t1_ns, warmup_ns=0)``
    ``iter_raw`` + on-the-fly normalization with one explicit NormalizerState per stream:
      * external venues (``coinbase.ws``, ``deribit.options``, ...) -> dh.feeds normalizers
      * ``kalshi.ws``      -> dh.kalshi.normalize.ws_message_to_events(json, t)
      * ``kalshi.rest.*``  -> dh.kalshi.normalize.normalize_rest_record(json, t)
      * ``status``, ``events.*`` -> codec-encoded events (dh.store.codec)
      * ``clock``          -> no events (clock-health samples; read them with iter_raw)
    With ``warmup_ns > 0`` records in [t0 - warmup, t0) are normalized silently and, at t0,
    the tracked state is emitted as synthesized snapshots (dh.feeds.books.BookTracker.
    state_events) before the events from t0 on.

Determinism: the output is a pure function of the files and arguments (no clock, no
randomness, no dict-order dependence); two runs yield identical sequences.

Tolerance: a segment whose last zstd frame was cut by a crash yields every complete record
before the cut; the damage is counted in ``ReadStats``.
"""

from __future__ import annotations

import calendar
import fnmatch
import heapq
import importlib
import json
import logging
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, NamedTuple

import orjson
import zstandard as zstd

from dh.core.events import Event, FeedStatus
from dh.core.units import NS_PER_S
from dh.feeds.base import safe_normalize
from dh.feeds.registry import has_normalizer, normalizer_for
from dh.store.codec import decode_event
from dh.store.recorder import HOUR_NS, INDEX_SUFFIX, SEGMENT_SUFFIX

log = logging.getLogger(__name__)

DAY_NS = 24 * HOUR_NS
READ_CHUNK = 1 << 20


class ReplayError(RuntimeError):
    """Replay cannot proceed (missing normalizer module, unknown stream requested...)."""


class RawRecord(NamedTuple):
    t: int  # receive time, ns since epoch
    stream: str
    q: int  # per-process record sequence
    data: bytes  # raw frame bytes exactly as received


@dataclass
class ReadStats:
    files: int = 0
    records: int = 0
    truncated_files: list[str] = field(default_factory=list)
    corrupt_files: list[str] = field(default_factory=list)
    bad_lines: int = 0
    time_order_violations: int = 0  # t decreased within a stream (clock steps)


# ============================================================================ files
def raw_dir(root: str | Path) -> Path:
    return Path(root) / "raw"


def list_streams(root: str | Path) -> list[str]:
    base = raw_dir(root)
    if not base.is_dir():
        return []
    return sorted(p.name for p in base.iterdir() if p.is_dir())


def resolve_streams(root: str | Path, streams: Iterable[str] | None) -> list[str]:
    """Expand patterns (fnmatch) against available streams, keeping first-mention order."""
    available = list_streams(root)
    if streams is None:
        return available
    out: list[str] = []
    for s in streams:
        if any(c in s for c in "*?["):
            for m in sorted(fnmatch.filter(available, s)):
                if m not in out:
                    out.append(m)
        elif s not in out:
            out.append(s)
    return out


def _day_start_ns(day: str) -> int | None:
    try:
        y, m, d = (int(x) for x in day.split("-"))
        return calendar.timegm((y, m, d, 0, 0, 0, 0, 0, 0)) * NS_PER_S
    except ValueError:
        return None


def parse_segment_name(name: str) -> tuple[int, int] | None:
    """'13.jsonl.zst' -> (13, 0); '13.p2.jsonl.zst' -> (13, 2)."""
    if not name.endswith(SEGMENT_SUFFIX):
        return None
    stem = name[: -len(SEGMENT_SUFFIX)]
    hh, _, part = stem.partition(".p")
    if not hh.isdigit() or len(hh) != 2 or (part and not part.isdigit()):
        return None
    return int(hh), int(part or 0)


def segment_files(root: str | Path, stream: str, t0_ns: int | None = None, t1_ns: int | None = None) -> list[tuple[int, int, Path]]:
    """[(hour_start_ns, part, path)] of a stream overlapping [t0, t1), chronological."""
    base = raw_dir(root) / stream
    if not base.is_dir():
        return []
    out: list[tuple[int, int, Path]] = []
    for day_dir in base.iterdir():
        ds = _day_start_ns(day_dir.name) if day_dir.is_dir() else None
        if ds is None:
            continue
        if t0_ns is not None and ds + DAY_NS <= t0_ns:
            continue
        if t1_ns is not None and ds >= t1_ns:
            continue
        for f in day_dir.iterdir():
            parsed = parse_segment_name(f.name)
            if parsed is None:
                continue
            hs = ds + parsed[0] * HOUR_NS
            if t0_ns is not None and hs + HOUR_NS <= t0_ns:
                continue
            if t1_ns is not None and hs >= t1_ns:
                continue
            out.append((hs, parsed[1], f))
    out.sort(key=lambda x: (x[0], x[1], x[2].name))
    return out


def read_index(segment: Path) -> dict[str, Any] | None:
    """Sidecar index of a closed segment (None while the segment is open / after a crash)."""
    stem = segment.name[: -len(SEGMENT_SUFFIX)]
    p = segment.with_name(stem + INDEX_SUFFIX)
    try:
        return orjson.loads(p.read_bytes())
    except (OSError, orjson.JSONDecodeError):
        return None


def _parse_line(line: bytes) -> RawRecord:
    o = orjson.loads(line)
    if "d" in o:
        data = o["d"].encode("utf-8")
    else:
        import base64

        data = base64.b64decode(o["b"])
    return RawRecord(int(o["t"]), o["s"], int(o["q"]), data)


def read_segment(path: str | Path, stats: ReadStats | None = None) -> Iterator[RawRecord]:
    """Stream records from one segment, frame by frame, tolerating a truncated/corrupt tail."""
    path = Path(path)
    st = stats if stats is not None else ReadStats()
    st.files += 1
    dctx = zstd.ZstdDecompressor()
    obj = dctx.decompressobj()
    in_frame = False
    pending = b""
    corrupt = False
    with open(path, "rb") as f:
        while not corrupt:
            data = f.read(READ_CHUNK)
            if not data:
                break
            while data:
                try:
                    out = obj.decompress(data)
                except zstd.ZstdError as exc:
                    log.warning("%s: corrupt zstd data (%s); keeping records before it", path, exc)
                    st.corrupt_files.append(str(path))
                    corrupt = True
                    break
                in_frame = True
                if out:
                    pending += out
                    if b"\n" in out:
                        *lines, pending = pending.split(b"\n")
                        for line in lines:
                            if not line:
                                continue
                            try:
                                rec = _parse_line(line)
                            except (orjson.JSONDecodeError, KeyError, ValueError, TypeError):
                                st.bad_lines += 1
                                continue
                            st.records += 1
                            yield rec
                if obj.eof:
                    data = obj.unused_data
                    obj = dctx.decompressobj()
                    in_frame = False
                    if pending:  # a frame always ends with a newline
                        st.bad_lines += 1
                        pending = b""
                else:
                    data = b""
    if (in_frame or pending) and not corrupt:
        st.truncated_files.append(str(path))
        log.warning("%s: truncated final frame (crash?); recovered records before the cut", path)


def iter_stream(
    root: str | Path, stream: str, t0_ns: int, t1_ns: int, stats: ReadStats | None = None
) -> Iterator[RawRecord]:
    """One stream's records in [t0, t1), in file (arrival) order."""
    st = stats if stats is not None else ReadStats()
    last_t: int | None = None
    for _, _, path in segment_files(root, stream, t0_ns, t1_ns):
        for rec in read_segment(path, st):
            if last_t is not None and rec.t < last_t:
                st.time_order_violations += 1
            last_t = rec.t
            if t0_ns <= rec.t < t1_ns:
                yield rec


def iter_raw(
    root: str | Path,
    streams: Iterable[str] | None,
    t0_ns: int,
    t1_ns: int,
    stats: ReadStats | None = None,
) -> Iterator[RawRecord]:
    """k-way merge by (t, stream rank, q); per-stream arrival order preserved."""
    names = resolve_streams(root, streams)
    st = stats if stats is not None else ReadStats()
    heap: list[tuple[int, int, int, int, RawRecord, Iterator[RawRecord]]] = []
    tie = 0
    for rank, s in enumerate(names):
        it = iter_stream(root, s, t0_ns, t1_ns, st)
        rec = next(it, None)
        if rec is not None:
            heap.append((rec.t, rank, rec.q, tie, rec, it))
            tie += 1
    heapq.heapify(heap)
    while heap:
        _, rank, _, _, rec, it = heapq.heappop(heap)
        yield rec
        nxt = next(it, None)
        if nxt is not None:
            heapq.heappush(heap, (nxt.t, rank, nxt.q, tie, nxt, it))
            tie += 1


# ============================================================================ normalization
def _loads(data: bytes) -> Any:
    try:
        return orjson.loads(data)
    except orjson.JSONDecodeError:
        return json.loads(data)


def _kalshi_fn(name: str) -> Callable[..., list[Event]]:
    try:
        mod = importlib.import_module("dh.kalshi.normalize")
    except Exception as exc:  # noqa: BLE001
        raise ReplayError(
            "replaying 'kalshi.*' streams needs dh.kalshi.normalize "
            f"(ws_message_to_events / normalize_rest_record), which failed to import: {exc!r}"
        ) from exc
    fn = getattr(mod, name, None)
    if fn is None:
        raise ReplayError(f"dh.kalshi.normalize has no {name}(); cannot replay Kalshi records")
    return fn


class Normalizers:
    """Per-stream normalizer dispatch with explicit state (one instance per replay)."""

    def __init__(self, strict: bool = False) -> None:
        self.strict = strict
        self._fns: dict[str, Callable[[RawRecord], list[Event]]] = {}
        self.states: dict[str, Any] = {}
        self._warned: set[str] = set()

    def __call__(self, rec: RawRecord) -> list[Event]:
        fn = self._fns.get(rec.stream)
        if fn is None:
            fn = self._fns[rec.stream] = self._resolve(rec.stream)
        return fn(rec)

    def _resolve(self, stream: str) -> Callable[[RawRecord], list[Event]]:
        if stream == "clock":
            return lambda rec: []
        if stream == "status" or stream.startswith("events."):
            return _decode_codec
        if stream == "kalshi.ws":
            ws_fn = _kalshi_fn("ws_message_to_events")
            return lambda rec: _guard(stream, rec, lambda: ws_fn(_loads(rec.data), rec.t))
        if stream.startswith("kalshi.rest"):
            rest_fn = _kalshi_fn("normalize_rest_record")
            return lambda rec: _guard(stream, rec, lambda: rest_fn(_loads(rec.data), rec.t))
        if stream.startswith("kalshi."):
            raise ReplayError(f"no normalizer for Kalshi stream {stream!r}")
        if has_normalizer(stream):
            norm, state = normalizer_for(stream)
            self.states[stream] = state
            return lambda rec: safe_normalize(norm, rec.data, rec.t, state)
        if self.strict:
            raise ReplayError(f"no normalizer for stream {stream!r}")
        if stream not in self._warned:
            self._warned.add(stream)
            log.warning("replay: no normalizer for stream %r; its records are skipped", stream)
        return lambda rec: []


def _decode_codec(rec: RawRecord) -> list[Event]:
    try:
        return [decode_event(rec.data)]
    except Exception as exc:  # noqa: BLE001
        return [FeedStatus(rec.t, 0, rec.stream, "error", f"undecodable event: {exc}"[:300])]


def _guard(stream: str, rec: RawRecord, fn: Callable[[], list[Event]]) -> list[Event]:
    try:
        return list(fn())
    except ReplayError:
        raise
    except Exception as exc:  # noqa: BLE001 - one bad frame must not stop a replay
        return [FeedStatus(rec.t, 0, stream, "error", f"normalize {type(exc).__name__}: {exc}"[:300])]


def iter_records_events(
    root: str | Path,
    streams: Iterable[str] | None,
    t0_ns: int,
    t1_ns: int,
    stats: ReadStats | None = None,
    normalizers: Normalizers | None = None,
) -> Iterator[tuple[RawRecord, list[Event]]]:
    """(record, events) pairs in merge order: for tools needing per-stream attribution."""
    norm = normalizers if normalizers is not None else Normalizers()
    for rec in iter_raw(root, streams, t0_ns, t1_ns, stats):
        yield rec, norm(rec)


def iter_events(
    root: str | Path,
    streams: Iterable[str] | None,
    t0_ns: int,
    t1_ns: int,
    *,
    warmup_ns: int = 0,
    stats: ReadStats | None = None,
    normalizers: Normalizers | None = None,
) -> Iterator[Event]:
    """Normalized events for [t0, t1) in deterministic merge order (see module docstring)."""
    if warmup_ns <= 0:
        for _, evs in iter_records_events(root, streams, t0_ns, t1_ns, stats, normalizers):
            yield from evs
        return
    from dh.feeds.books import BookTracker

    tracker = BookTracker()
    primed = False
    for rec, evs in iter_records_events(root, streams, t0_ns - warmup_ns, t1_ns, stats, normalizers):
        if not primed:
            if rec.t < t0_ns:
                for ev in evs:
                    tracker.on_event(ev)
                continue
            primed = True
            yield from tracker.state_events(t0_ns)
        yield from evs
    if not primed:
        yield from tracker.state_events(t0_ns)


def day_bounds(day: str) -> tuple[int, int]:
    """'YYYY-MM-DD' -> [start_ns, end_ns) in UTC."""
    ds = _day_start_ns(day)
    if ds is None:
        raise ValueError(f"bad day {day!r}")
    return ds, ds + DAY_NS
