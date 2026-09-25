"""Feed-client framework for external BTC venues (spot BRTI constituents, perps, options).

Two halves with a hard boundary between them:

1. **Transport** (``FeedClient.run``): connect, subscribe, keep alive, watch for staleness,
   reconnect with exponential backoff + jitter, and hand every received frame to
   ``emit_raw(stream, recv_ns, raw_bytes)`` *verbatim*. ``recv_ns = time.time_ns()`` is taken
   immediately after the websocket library returns the frame, before any parsing.

2. **Normalization** (``FeedClient.normalize``): a PURE static function
   ``normalize(raw, recv_ns, state) -> list[Event]``. Everything it depends on is either in the
   frame, the receive time, or the explicit, serializable ``NormalizerState``. Live trading
   and replay call the very same function on the very same byte sequence, so a replay
   reproduces the live event stream exactly (same books, same gaps, same statuses).

Control records ("markers")
---------------------------
So that each raw stream is self-describing, a client also writes a few control records into
its own stream through ``emit_raw``. They are JSON objects whose first key is ``"_dh"`` (venue
frames never start with ``{"_dh":``)::

    {"_dh":"status","conn":3,"status":"connected","url":"wss://...","detail":""}
    {"_dh":"status","conn":3,"status":"disconnected","detail":"ConnectionClosedError: ..."}
    {"_dh":"status","conn":3,"status":"stale","detail":"no frame for 12.0s"}
    {"_dh":"status","conn":3,"status":"resumed","detail":"..."}
    {"_dh":"sent","conn":3,"msg":"<outbound command text>"}          # audit of subscriptions
    {"_dh":"rest","conn":3,"kind":"order_book","symbol":"btcusd","url":"https://...",
     "status":200,"req_ns":..., "body":"<raw response text>"}          # REST snapshot payloads

``handle_marker`` turns status markers into ``FeedStatus`` events (``resumed`` ->
``FeedStatus('connected', detail='resumed ...')``) and resets per-connection normalizer state
on ``connected``; REST payloads are handed to the venue's ``on_rest`` handler. Because the
markers are in the venue stream itself, replaying *only* ``coinbase.ws`` reproduces its
disconnects, staleness and resyncs deterministically.

Status semantics (``FeedStatus.stream``)
  * ``<stream>`` (e.g. ``coinbase.ws``): transport status: connected / disconnected / stale /
    error (venue error messages, normalizer exceptions).
  * ``<venue>.book:<symbol>`` (e.g. ``kraken.book:BTC/USD``): book integrity. ``gap`` means the
    local book can no longer be trusted (sequence gap, checksum mismatch, ...). After a
    ``gap`` the normalizer emits **no deltas** for that book until a fresh snapshot, which is
    emitted followed by ``resynced``. Consumers therefore never apply deltas to a broken book.

The recorder's separate ``status`` stream (see ``dh.store.recorder``) is used only by sources
that cannot write markers into their own stream (the Kalshi client, the collector
supervisor), so no status is ever recorded twice.

Units: external venues use floats: price in USD (or USDT for USDT-margined perps, treated as
USD), size in **BTC** (contract sizes are converted in the venue normalizer), timestamps in
integer ns since the Unix epoch.
"""

from __future__ import annotations

import asyncio
import calendar
import logging
import random
import time
from abc import ABC, abstractmethod
from collections.abc import Callable, Coroutine, Iterable
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, ClassVar

import orjson
from sortedcontainers import SortedDict

from dh.core.events import Event, ExtBookSnapshot, FeedStatus
from dh.core.units import NS_PER_MS, NS_PER_S

log = logging.getLogger(__name__)

EmitRaw = Callable[[str, int, bytes], None]
StatusCallback = Callable[[FeedStatus], None]
EventCallback = Callable[[Event], None]

MARKER_PREFIX = b'{"_dh":'
NS_PER_US = 1_000
HISTORY_SLACK_NS = 2 * NS_PER_S  # trades older than (connect time - slack) are snapshot history


# ============================================================================ time helpers
def ms_to_ns(ms: int | float | str) -> int:
    """Milliseconds since epoch (int/float/str) -> integer ns."""
    if isinstance(ms, str):
        if "." in ms:
            return int(Decimal(ms) * NS_PER_MS)
        return int(ms) * NS_PER_MS
    if isinstance(ms, float):
        return int(round(ms * NS_PER_MS))
    return int(ms) * NS_PER_MS


def us_to_ns(us: int | str) -> int:
    """Microseconds since epoch -> ns."""
    return int(us) * NS_PER_US


def parse_rfc3339_ns(s: str) -> int:
    """'2023-02-09T20:32:50.714964855Z' (any fraction length, Z or +HH:MM) -> ns since epoch.

    Also accepts a space instead of 'T'. Pure and exact (no float arithmetic).
    """
    if len(s) < 19:
        raise ValueError(f"bad timestamp {s!r}")
    y, mo, d = int(s[0:4]), int(s[5:7]), int(s[8:10])
    h, mi, sec = int(s[11:13]), int(s[14:16]), int(s[17:19])
    rest = s[19:]
    frac_ns = 0
    if rest[:1] == ".":
        i = 1
        n = len(rest)
        while i < n and rest[i].isdigit():
            i += 1
        digits = rest[1:i]
        frac_ns = int((digits + "000000000")[:9]) if digits else 0
        rest = rest[i:]
    rest = rest.strip()
    off_s = 0
    if rest and rest[0] in "+-":
        sign = 1 if rest[0] == "+" else -1
        hh, mm = int(rest[1:3]), int(rest[4:6]) if len(rest) >= 6 else 0
        off_s = sign * (hh * 3600 + mm * 60)
    secs = calendar.timegm((y, mo, d, h, mi, sec, 0, 0, 0)) - off_s
    return secs * NS_PER_S + frac_ns


def to_dec(x: Any) -> Decimal:
    """Wire number (str / int / float / Decimal) -> exact Decimal (floats via shortest repr)."""
    if isinstance(x, Decimal):
        return x
    if isinstance(x, str):
        return Decimal(x)
    if isinstance(x, float):
        return Decimal(repr(x))
    return Decimal(int(x))


# ============================================================================ normalizer state
class DepthBook:
    """Exact local price-level book kept by a normalizer where the protocol requires it
    (checksum validation, truncation of depth-limited books, snapshot alignment).

    Keys and values are ``Decimal`` so prices compare exactly and the venue's text precision
    is preserved (Kraken's checksum is computed over the decimal text). Serializable via
    ``to_dict`` / ``from_dict`` (decimal strings).
    """

    __slots__ = ("depth", "bids", "asks")

    def __init__(self, depth: int = 0) -> None:
        self.depth = depth  # 0 = unlimited
        self.bids: SortedDict = SortedDict()  # Decimal price -> Decimal size (ascending)
        self.asks: SortedDict = SortedDict()

    def clear(self) -> None:
        self.bids.clear()
        self.asks.clear()

    def side(self, side: str) -> SortedDict:
        return self.bids if side == "b" else self.asks

    def set(self, side: str, price: Decimal, size: Decimal) -> None:
        """Absolute size at price; size == 0 removes the level."""
        book = self.bids if side == "b" else self.asks
        if size == 0:
            book.pop(price, None)
        else:
            book[price] = size

    def truncate(self) -> list[tuple[str, float, float]]:
        """Drop levels beyond ``depth``; returns the removals as ExtBookDelta changes."""
        out: list[tuple[str, float, float]] = []
        if self.depth <= 0:
            return out
        while len(self.bids) > self.depth:  # worst bids are the lowest prices
            p, _ = self.bids.popitem(0)
            out.append(("b", float(p), 0.0))
        while len(self.asks) > self.depth:  # worst asks are the highest prices
            p, _ = self.asks.popitem(-1)
            out.append(("a", float(p), 0.0))
        return out

    def top_bids(self, n: int) -> list[tuple[Decimal, Decimal]]:
        items = self.bids.items()
        return [(p, q) for p, q in reversed(items[-n:])] if n else []

    def top_asks(self, n: int) -> list[tuple[Decimal, Decimal]]:
        return list(self.asks.items()[:n]) if n else []

    def snapshot_levels(self) -> tuple[tuple[tuple[float, float], ...], tuple[tuple[float, float], ...]]:
        """(bids best-first, asks best-first) as float tuples for ExtBookSnapshot."""
        bids = tuple((float(p), float(q)) for p, q in reversed(self.bids.items()))
        asks = tuple((float(p), float(q)) for p, q in self.asks.items())
        return bids, asks

    def to_dict(self) -> dict[str, Any]:
        return {
            "depth": self.depth,
            "bids": [[format(p, "f"), format(q, "f")] for p, q in self.bids.items()],
            "asks": [[format(p, "f"), format(q, "f")] for p, q in self.asks.items()],
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> DepthBook:
        b = cls(int(d.get("depth", 0)))
        b.bids = SortedDict({Decimal(p): Decimal(q) for p, q in d.get("bids", [])})
        b.asks = SortedDict({Decimal(p): Decimal(q) for p, q in d.get("asks", [])})
        return b

    def __eq__(self, other: object) -> bool:
        return (
            isinstance(other, DepthBook)
            and self.depth == other.depth
            and list(self.bids.items()) == list(other.bids.items())
            and list(self.asks.items()) == list(other.asks.items())
        )


@dataclass
class NormalizerState:
    """Explicit, serializable state of one stream's normalizer.

    Everything a normalizer remembers between frames lives here (never in globals or class
    attributes), so ``to_dict()`` at any point plus the remaining frames reproduces the rest
    of the event stream exactly.
    """

    stream: str = ""
    venue: str = ""
    conn: int = 0  # number of 'connected' markers seen
    conn_ns: int = 0  # recv_ns of the latest 'connected' marker (0 = none seen)
    seq: dict[str, int] = field(default_factory=dict)  # last sequence number per key
    valid: dict[str, bool] = field(default_factory=dict)  # book key -> deltas may be emitted
    books: dict[str, DepthBook] = field(default_factory=dict)  # local books where required
    cursors: dict[str, Any] = field(default_factory=dict)  # trade de-dup cursors per symbol
    fields: dict[str, dict[str, Any]] = field(default_factory=dict)  # merged ticker fields
    buffers: dict[str, list[Any]] = field(default_factory=dict)  # frames awaiting a snapshot
    meta: dict[str, Any] = field(default_factory=dict)  # venue settings learned from the stream
    stats: dict[str, int] = field(default_factory=dict)  # integrity counters (gaps, checksums)

    def bump(self, key: str, n: int = 1) -> None:
        self.stats[key] = self.stats.get(key, 0) + n

    def to_dict(self) -> dict[str, Any]:
        """Deep, JSON-serializable copy."""
        return {
            "stream": self.stream,
            "venue": self.venue,
            "conn": self.conn,
            "conn_ns": self.conn_ns,
            "seq": dict(self.seq),
            "valid": dict(self.valid),
            "books": {k: b.to_dict() for k, b in self.books.items()},
            "cursors": orjson.loads(orjson.dumps(self.cursors)),
            "fields": orjson.loads(orjson.dumps(self.fields)),
            "buffers": orjson.loads(orjson.dumps(self.buffers)),
            "meta": orjson.loads(orjson.dumps(self.meta)),
            "stats": dict(self.stats),
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> NormalizerState:
        return cls(
            stream=d.get("stream", ""),
            venue=d.get("venue", ""),
            conn=int(d.get("conn", 0)),
            conn_ns=int(d.get("conn_ns", 0)),
            seq={k: int(v) for k, v in d.get("seq", {}).items()},
            valid={k: bool(v) for k, v in d.get("valid", {}).items()},
            books={k: DepthBook.from_dict(v) for k, v in d.get("books", {}).items()},
            cursors=orjson.loads(orjson.dumps(d.get("cursors", {}))),
            fields=orjson.loads(orjson.dumps(d.get("fields", {}))),
            buffers=orjson.loads(orjson.dumps(d.get("buffers", {}))),
            meta=orjson.loads(orjson.dumps(d.get("meta", {}))),
            stats={k: int(v) for k, v in d.get("stats", {}).items()},
        )

    def to_json(self) -> bytes:
        return orjson.dumps(self.to_dict(), option=orjson.OPT_SORT_KEYS)


# ============================================================================ normalizer helpers
def book_key(state: NormalizerState, symbol: str) -> str:
    """Stream id used in FeedStatus for a book: '<venue>.book:<symbol>'."""
    return f"{state.venue}.book:{symbol}"


def book_gap(state: NormalizerState, symbol: str, ts: int, detail: str) -> list[Event]:
    """Invalidate a book (suppress deltas until the next snapshot) and report the gap once.

    Further gaps detected while the book is already waiting for a snapshot are counted in
    ``state.stats['gaps']`` but not re-reported.
    """
    key = book_key(state, symbol)
    state.bump("gaps")
    state.valid[key] = False
    in_gap = state.meta.setdefault("in_gap", {})
    if in_gap.get(key):
        return []
    in_gap[key] = True
    return [FeedStatus(ts=ts, ts_exch=0, stream=key, status="gap", detail=detail)]


def book_snapshot_ok(state: NormalizerState, symbol: str, ts: int, detail: str = "snapshot") -> list[Event]:
    """Mark a book valid after a snapshot; emits 'resynced' if it recovers from a gap."""
    key = book_key(state, symbol)
    state.valid[key] = True
    state.meta.setdefault("snap_conn", {})[symbol] = state.conn
    if state.meta.get("in_gap", {}).pop(key, False):
        state.bump("resyncs")
        return [FeedStatus(ts=ts, ts_exch=0, stream=key, status="resynced", detail=detail)]
    return []


def book_valid(state: NormalizerState, symbol: str) -> bool:
    return state.valid.get(book_key(state, symbol), False)


def invalidate_all_books(state: NormalizerState) -> None:
    """Connection boundary: every book needs a fresh snapshot (not a gap: expected)."""
    for k in list(state.valid):
        state.valid[k] = False


def is_history(state: NormalizerState, ts_exch: int) -> bool:
    """True for trades executed before the current connection was opened (snapshot replay of
    recent trades that some venues send on subscribe). Deterministic: uses recorded times."""
    return bool(state.conn_ns) and bool(ts_exch) and ts_exch < state.conn_ns - HISTORY_SLACK_NS


def trade_seen(state: NormalizerState, symbol: str, ts_exch: int, trade_id: str, keep: int = 64) -> bool:
    """De-duplicate trades by id with a bounded memory (last ``keep`` ids per symbol).

    Returns True if the trade was already emitted. Ids are compared as strings; ordering is
    not assumed (some venues' ids are UUIDs or hashes).
    """
    if not trade_id:
        return False
    cur = state.cursors.setdefault(symbol, [])
    if trade_id in cur:
        return True
    cur.append(trade_id)
    if len(cur) > keep:
        del cur[: len(cur) - keep]
    return False


def snapshot_event(
    ts: int,
    ts_exch: int,
    venue: str,
    symbol: str,
    bids: Iterable[tuple[float, float]],
    asks: Iterable[tuple[float, float]],
    seq: int = 0,
    depth_limited: bool = True,
) -> ExtBookSnapshot:
    """Build an ExtBookSnapshot with bids sorted best (highest) first and asks best (lowest)
    first, zero sizes dropped, duplicate prices collapsed (last wins)."""
    b: dict[float, float] = {}
    for p, s in bids:
        if s > 0:
            b[p] = s
        else:
            b.pop(p, None)
    a: dict[float, float] = {}
    for p, s in asks:
        if s > 0:
            a[p] = s
        else:
            a.pop(p, None)
    return ExtBookSnapshot(
        ts=ts,
        ts_exch=ts_exch,
        venue=venue,
        symbol=symbol,
        bids=tuple(sorted(b.items(), key=lambda x: -x[0])),
        asks=tuple(sorted(a.items())),
        seq=seq,
        depth_limited=depth_limited,
    )


# ============================================================================ markers
def make_marker(kind: str, conn: int, **fields: Any) -> bytes:
    """Serialize a control record. ``_dh`` is always the first key (fast prefix test)."""
    d: dict[str, Any] = {"_dh": kind, "conn": conn}
    d.update(fields)
    return orjson.dumps(d)


def is_marker(raw: bytes) -> bool:
    return raw[:7] == MARKER_PREFIX


RestHandler = Callable[[dict[str, Any], int, NormalizerState], list[Event]]


def handle_marker(
    raw: bytes, recv_ns: int, state: NormalizerState, on_rest: RestHandler | None = None
) -> list[Event]:
    """Generic marker handling shared by every normalizer."""
    m = orjson.loads(raw)
    kind = m.get("_dh")
    if kind == "status":
        st = m.get("status", "")
        detail = str(m.get("detail", ""))
        if st == "connected":
            state.conn += 1
            state.conn_ns = recv_ns
            state.seq.clear()
            invalidate_all_books(state)
            for b in state.books.values():
                b.clear()
            state.buffers.clear()
            return [FeedStatus(recv_ns, 0, state.stream, "connected", detail or str(m.get("url", "")))]
        if st == "disconnected":
            invalidate_all_books(state)
            return [FeedStatus(recv_ns, 0, state.stream, "disconnected", detail)]
        if st == "stale":
            return [FeedStatus(recv_ns, 0, state.stream, "stale", detail)]
        if st == "resumed":
            return [FeedStatus(recv_ns, 0, state.stream, "connected", "resumed " + detail)]
        if st == "error":
            return [FeedStatus(recv_ns, 0, state.stream, "error", detail)]
        return []
    if kind == "rest":
        if on_rest is None:
            return []
        return on_rest(m, recv_ns, state)
    if kind == "sent":
        sent = state.meta.setdefault("sent_count", 0)
        state.meta["sent_count"] = sent + 1
        return []
    return []


def safe_normalize(
    fn: Callable[[bytes, int, NormalizerState], list[Event]], raw: bytes, recv_ns: int, state: NormalizerState
) -> list[Event]:
    """Run a normalizer; an exception becomes a deterministic FeedStatus('error') event.

    Used identically by live clients and by replay, so a malformed frame produces the same
    event in both.
    """
    try:
        return fn(raw, recv_ns, state)
    except Exception as exc:  # noqa: BLE001 - any parse failure must not kill the stream
        state.bump("normalize_errors")
        snippet = raw[:160].decode("utf-8", "replace")
        return [FeedStatus(recv_ns, 0, state.stream, "error", f"normalize {type(exc).__name__}: {exc} | {snippet}")]


# ============================================================================ transport
@dataclass
class FeedConfig:
    """Per-feed runtime configuration (config/feeds.yaml ``feeds.<name>``)."""

    stream: str | None = None
    url: str | None = None
    symbols: tuple[str, ...] | None = None
    channels: tuple[str, ...] | None = None
    options: dict[str, Any] = field(default_factory=dict)
    enabled: bool = True
    stale_after_s: float | None = None
    dead_after_s: float | None = None
    resnapshot_interval_s: float | None = None  # None = class default, 0 = never
    keepalive_interval_s: float | None = None
    max_frame_bytes: int | None = None
    backoff_initial_s: float = 1.0
    backoff_max_s: float = 60.0
    stable_after_s: float = 60.0  # a connection alive this long resets the backoff
    open_timeout_s: float = 15.0
    resync_min_interval_s: float = 5.0
    online_normalize: bool = True
    proxy: str | bool | None = True  # websockets proxy: True = from environment, None = direct

    @classmethod
    def from_mapping(cls, m: dict[str, Any] | None) -> FeedConfig:
        m = dict(m or {})
        known = {f for f in cls.__dataclass_fields__}  # type: ignore[attr-defined]
        kw: dict[str, Any] = {}
        extra: dict[str, Any] = dict(m.pop("options", {}) or {})
        for k, v in m.items():
            if k in known:
                kw[k] = tuple(v) if k in ("symbols", "channels") and v is not None else v
            else:
                extra[k] = v
        kw["options"] = extra
        return cls(**kw)


class Backoff:
    """Exponential backoff with jitter: n-th delay ~ U(cap/2, cap), cap = min(max, initial*2^n)."""

    def __init__(self, initial_s: float = 1.0, max_s: float = 60.0, rng: random.Random | None = None) -> None:
        self.initial_s, self.max_s = initial_s, max_s
        self.n = 0
        self.rng = rng or random.Random()

    def next(self) -> float:
        cap = min(self.max_s, self.initial_s * (2**self.n))
        self.n += 1
        return self.rng.uniform(cap / 2, cap)

    def reset(self) -> None:
        self.n = 0


@dataclass
class FeedMetrics:
    frames: int = 0
    bytes: int = 0
    connects: int = 0
    disconnects: int = 0
    reconnects: int = 0
    gaps: int = 0
    resyncs: int = 0
    stale: int = 0
    errors: int = 0
    last_rx_ns: int = 0
    last_error: str = ""


class FeedDead(RuntimeError):
    """Raised by the watchdog when a connection must be torn down (no data)."""


class ResyncReconnect(RuntimeError):
    """Raised to force a reconnect as the resync/resnapshot mechanism."""


class FeedClient(ABC):
    """Base class for one venue connection that records one raw stream.

    Subclasses declare class attributes (venue, default_stream, default_url, ...), build
    their subscription commands, and implement the pure static ``normalize``.
    """

    venue: ClassVar[str] = ""
    default_stream: ClassVar[str] = ""
    default_url: ClassVar[str] = ""
    default_symbols: ClassVar[tuple[str, ...]] = ()
    default_channels: ClassVar[tuple[str, ...]] = ()
    stale_after_s: ClassVar[float] = 15.0
    dead_after_s: ClassVar[float] = 60.0
    keepalive_interval_s: ClassVar[float | None] = None
    resnapshot_interval_s: ClassVar[float] = 3600.0
    max_frame_bytes: ClassVar[int] = 16 * 2**20
    ws_ping_interval_s: ClassVar[float | None] = 20.0
    subscribe_delay_s: ClassVar[float] = 0.0
    implemented: ClassVar[bool] = True
    geo_note: ClassVar[str] = ""

    def __init__(
        self,
        cfg: FeedConfig | None = None,
        *,
        status_cb: StatusCallback | None = None,
        on_event: EventCallback | None = None,
        rng: random.Random | None = None,
    ) -> None:
        self.cfg = cfg or FeedConfig()
        self.name: str = self.cfg.stream or self.default_stream
        self.url: str = self.cfg.url or self.default_url
        self.symbols: tuple[str, ...] = tuple(self.cfg.symbols or self.default_symbols)
        self.channels: tuple[str, ...] = tuple(self.cfg.channels or self.default_channels)
        self.options: dict[str, Any] = dict(self.cfg.options)
        self.stale_after = self.cfg.stale_after_s or self.stale_after_s
        self.dead_after = self.cfg.dead_after_s or self.dead_after_s
        self.keepalive_interval = (
            self.cfg.keepalive_interval_s if self.cfg.keepalive_interval_s is not None else self.keepalive_interval_s
        )
        rs = self.cfg.resnapshot_interval_s
        self.resnapshot_interval = self.resnapshot_interval_s if rs is None else rs
        self.status_cb = status_cb
        self.on_event = on_event
        self.clock_ns: Callable[[], int] = time.time_ns  # live-only decisions (injectable in tests)
        self.state = self.new_state(self.name)
        self.metrics = FeedMetrics()
        self.conn_id = 0
        self._rng = rng or random.Random()
        self._emit_raw: EmitRaw | None = None
        self._ws: Any = None
        self._stopping = False
        self._stale = False
        self._resync_reason: str | None = None
        self._reconnect_reason: str | None = None
        self._last_resync_mono = 0.0
        self._resync_count_conn = 0

    # ------------------------------------------------------------------ pure side
    @classmethod
    def new_state(cls, stream: str | None = None) -> NormalizerState:
        return NormalizerState(stream=stream or cls.default_stream, venue=cls.venue)

    @staticmethod
    @abstractmethod
    def normalize(raw: bytes, recv_ns: int, state: NormalizerState) -> list[Event]:
        """Raw frame -> normalized events. Pure given (raw, recv_ns, state)."""

    # ------------------------------------------------------------------ venue hooks
    def subscribe_messages(self) -> list[str]:
        """Commands sent right after connecting (after ``subscribe_delay_s``)."""
        return []

    def resubscribe_messages(self) -> list[str] | None:
        """Commands that force a fresh book snapshot on the same connection (periodic
        resnapshot and gap resync). ``None`` = reconnect instead."""
        return None

    def keepalive_message(self) -> str | None:
        """Application-level ping sent every ``keepalive_interval_s`` (not recorded)."""
        return None

    def control_replies(self, raw: bytes) -> list[str]:
        """Immediate protocol replies to a received frame (e.g. heartbeat answers)."""
        return []

    def extra_tasks(self) -> list[Coroutine[Any, Any, None]]:
        """Additional per-connection coroutines (REST snapshots, instrument discovery)."""
        return []

    def on_events(self, events: list[Event], raw: bytes, recv_ns: int) -> None:
        """Hook after online normalization of a frame (venue control logic)."""

    # ------------------------------------------------------------------ helpers for venues
    def _status(self, ev: FeedStatus) -> None:
        if self.status_cb is not None:
            try:
                self.status_cb(ev)
            except Exception:  # noqa: BLE001
                log.exception("status callback failed")

    def _emit(self, recv_ns: int, raw: bytes) -> list[Event]:
        """Record a frame, then normalize it online with the live state (same code as replay)."""
        assert self._emit_raw is not None
        self._emit_raw(self.name, recv_ns, raw)
        self.metrics.frames += 1
        self.metrics.bytes += len(raw)
        if not self.cfg.online_normalize:
            return []
        events = safe_normalize(self.normalize, raw, recv_ns, self.state)
        for ev in events:
            if isinstance(ev, FeedStatus):
                if ev.status == "gap":
                    self.metrics.gaps += 1
                    if self._resync_reason is None:
                        self._resync_reason = f"{ev.stream}: {ev.detail}"
                elif ev.status == "resynced":
                    self.metrics.resyncs += 1
                elif ev.status == "error":
                    self.metrics.errors += 1
                    self.metrics.last_error = ev.detail[:200]
                self._status(ev)
            if self.on_event is not None:
                try:
                    self.on_event(ev)
                except Exception:  # noqa: BLE001
                    log.exception("%s: on_event callback failed", self.name)
        try:
            self.on_events(events, raw, recv_ns)
        except Exception:  # noqa: BLE001
            log.exception("%s: on_events hook failed", self.name)
        return events

    def _marker(self, kind: str, **fields: Any) -> list[Event]:
        return self._emit(time.time_ns(), make_marker(kind, self.conn_id, **fields))

    async def send(self, msg: str, record: bool = True) -> None:
        """Send a command on the live connection; recorded as a 'sent' marker by default."""
        if self._ws is None:
            raise ConnectionError(f"{self.name}: not connected")
        await self._ws.send(msg)
        if record:
            self._marker("sent", msg=msg)

    def record_rest(self, kind: str, url: str, status: int, body: bytes, req_ns: int, **extra: Any) -> list[Event]:
        """Record a REST response into this stream (normalized by the venue's on_rest)."""
        text = body.decode("utf-8", "replace")
        return self._emit(
            time.time_ns(),
            make_marker("rest", self.conn_id, kind=kind, url=url, status=status, req_ns=req_ns, body=text, **extra),
        )

    def stop(self) -> None:
        """Request a graceful stop; ``run`` returns after closing the connection."""
        self._stopping = True
        ws = self._ws
        if ws is not None:
            try:
                asyncio.get_running_loop().create_task(ws.close())
            except RuntimeError:
                pass

    # ------------------------------------------------------------------ runtime
    async def run(self, emit_raw: EmitRaw) -> None:
        """Connect/record forever: reconnect with exponential backoff + jitter.

        ``emit_raw(stream, recv_ns, raw)`` receives every frame and control marker.
        Returns only after ``stop()``; cancellation propagates.
        """
        if not self.implemented:
            raise NotImplementedError(f"{type(self).__name__} is an interface stub")
        self._emit_raw = emit_raw
        backoff = Backoff(self.cfg.backoff_initial_s, self.cfg.backoff_max_s, self._rng)
        while not self._stopping:
            started = time.monotonic()
            reason = "closed"
            conn_before = self.conn_id
            try:
                await self._run_connection()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - every failure leads to a reconnect
                reason = f"{type(exc).__name__}: {exc}"[:300]
                self.metrics.last_error = reason
                log.warning("%s: connection ended: %s", self.name, reason)
                if self.conn_id == conn_before and not self._stopping:
                    # never connected: record the failed attempt so outages are visible in data
                    self._marker("status", status="error", detail=f"connect failed: {reason}"[:300])
            if self._stopping:
                break
            if time.monotonic() - started >= self.cfg.stable_after_s:
                backoff.reset()
            delay = backoff.next()
            self.metrics.reconnects += 1
            log.info("%s: reconnecting in %.1fs (%s)", self.name, delay, reason)
            await asyncio.sleep(delay)

    def _ws_connect(self) -> Any:
        from websockets.asyncio.client import connect as ws_connect

        return ws_connect(
            self.url,
            max_size=self.cfg.max_frame_bytes or self.max_frame_bytes,
            ping_interval=self.ws_ping_interval_s,
            ping_timeout=self.ws_ping_interval_s,
            open_timeout=self.cfg.open_timeout_s,
            close_timeout=3,
            max_queue=1024,
            proxy=self.cfg.proxy,
        )

    async def _run_connection(self) -> None:
        reason = "closed"
        async with self._ws_connect() as ws:
            self._ws = ws
            self.conn_id += 1
            self._stale = False
            self._resync_reason = None
            self._reconnect_reason = None
            self._resync_count_conn = 0
            self.metrics.connects += 1
            self.metrics.last_rx_ns = time.time_ns()
            self._marker("status", status="connected", url=self.url)
            tasks: list[asyncio.Task[Any]] = []
            try:
                tasks.append(asyncio.create_task(self._reader(ws), name=f"{self.name}:reader"))
                tasks.append(asyncio.create_task(self._watchdog(), name=f"{self.name}:watchdog"))
                if self.keepalive_interval:
                    tasks.append(asyncio.create_task(self._keepalive(), name=f"{self.name}:keepalive"))
                if self.resnapshot_interval and self.resnapshot_interval > 0:
                    tasks.append(asyncio.create_task(self._resnapshot_loop(), name=f"{self.name}:resnapshot"))
                if self.subscribe_delay_s:
                    await asyncio.sleep(self.subscribe_delay_s)
                for msg in self.subscribe_messages():
                    await self.send(msg)
                for coro in self.extra_tasks():
                    tasks.append(asyncio.create_task(coro, name=f"{self.name}:extra"))
                reader = tasks[0]
                pending: set[asyncio.Task[Any]] = set(tasks)
                while pending:
                    done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
                    for t in done:
                        exc = t.exception()
                        if exc is not None:
                            raise exc
                        if t is reader:  # pragma: no cover - recv() raises on close
                            raise ConnectionError("reader ended")
            except asyncio.CancelledError:
                reason = "cancelled"
                raise
            except Exception as exc:
                reason = f"{type(exc).__name__}: {exc}"[:300]
                raise
            finally:
                for t in tasks:
                    t.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                self._ws = None
                self.metrics.disconnects += 1
                self._marker("status", status="disconnected", detail=reason)

    async def _reader(self, ws: Any) -> None:
        while True:
            raw = await ws.recv(decode=False)
            recv_ns = time.time_ns()
            if not isinstance(raw, bytes):  # pragma: no cover - decode=False returns bytes
                raw = bytes(raw) if not isinstance(raw, str) else raw.encode()
            if self._stale:
                self._stale = False
                self._emit(recv_ns, make_marker("status", self.conn_id, status="resumed", detail=""))
            self.metrics.last_rx_ns = recv_ns
            self._emit(recv_ns, raw)
            for reply in self.control_replies(raw):
                await self.send(reply)
            # websockets returns buffered frames without suspending: yield once per frame so a
            # busy feed never starves the strategy consumer, order requests or heartbeats
            await asyncio.sleep(0)
            if self._reconnect_reason is not None:
                reason, self._reconnect_reason = self._reconnect_reason, None
                raise ResyncReconnect(reason)
            if self._resync_reason is not None:
                await self._do_resync()

    async def resync(self, reason: str) -> None:
        """Obtain a fresh snapshot: resubscribe in place, or reconnect when the venue has no
        in-place mechanism. Venues with REST snapshots override this."""
        msgs = self.resubscribe_messages()
        if msgs is None:
            raise ResyncReconnect(f"resync by reconnect: {reason}")
        log.info("%s: resync (%s)", self.name, reason)
        for m in msgs:
            await self.send(m)

    def request_reconnect(self, reason: str) -> None:
        """Ask the reader loop to drop the connection after the current frame."""
        self._reconnect_reason = reason

    async def _do_resync(self) -> None:
        reason = self._resync_reason or ""
        now = time.monotonic()
        if now - self._last_resync_mono < self.cfg.resync_min_interval_s:
            return  # debounce; the flag stays set and is retried on a later frame
        self._resync_reason = None
        self._last_resync_mono = now
        self._resync_count_conn += 1
        if self._resync_count_conn > 3:  # repeated in-place resyncs failing: start over
            raise ResyncReconnect(f"repeated gaps, reconnecting: {reason}")
        await self.resync(reason)

    async def _watchdog(self) -> None:
        while True:
            await asyncio.sleep(0.5)
            age_s = (time.time_ns() - self.metrics.last_rx_ns) / NS_PER_S
            if not self._stale and age_s > self.stale_after:
                self._stale = True
                self.metrics.stale += 1
                self._marker("status", status="stale", detail=f"no frame for {age_s:.1f}s")
            if age_s > self.dead_after:
                raise FeedDead(f"no frame for {age_s:.1f}s")

    async def _keepalive(self) -> None:
        assert self.keepalive_interval
        while True:
            await asyncio.sleep(self.keepalive_interval)
            msg = self.keepalive_message()
            if msg is not None:
                await self.send(msg, record=False)

    async def _resnapshot_loop(self) -> None:
        while True:
            await asyncio.sleep(self.resnapshot_interval)
            await self.resync("periodic resnapshot")

    # ------------------------------------------------------------------ misc
    def describe(self) -> str:
        return f"{self.name} {self.url} symbols={','.join(self.symbols)} channels={','.join(self.channels)}"


def dumps(obj: Any) -> str:
    """Compact JSON text for outbound commands."""
    return orjson.dumps(obj).decode()
