"""Conservative recorded exposure: valid books intersected with feed/reference freshness.

No snapshot means no coverage. Disconnects and sequence gaps invalidate the affected
books until another snapshot; genuine quiet periods count while the public connection
and reference remain fresh. Intervals are half-open receive-time milliseconds.
"""
from __future__ import annotations

from collections.abc import Iterable

from dh.core.events import FeedStatus, KalshiBookSnapshot


def merge_intervals(intervals: Iterable[tuple[int, int]]) -> list[tuple[int, int]]:
    out: list[tuple[int, int]] = []
    for a, b in sorted(intervals):
        if b <= a:
            continue
        if out and a <= out[-1][1]:
            out[-1] = (out[-1][0], max(b, out[-1][1]))
        else:
            out.append((a, b))
    return out


def intersect(a: list[tuple[int, int]], b: list[tuple[int, int]]) -> list[tuple[int, int]]:
    out = []
    i = j = 0
    while i < len(a) and j < len(b):
        lo, hi = max(a[i][0], b[j][0]), min(a[i][1], b[j][1])
        if lo < hi:
            out.append((lo, hi))
        if a[i][1] <= b[j][1]:
            i += 1
        else:
            j += 1
    return out


def healthy_book_intervals(records, normalize, t0_ms: int, t1_ms: int, *, max_silence_ms: int = 5000):
    """Input includes WS frames and normalized status records, ordered by receipt."""
    active: dict[str, int] = {}
    books: dict[str, list[tuple[int, int]]] = {}
    feed = []
    last_frame = None
    def close(ticker, at):
        start = active.pop(ticker, None)
        if start is not None:
            books.setdefault(ticker, []).append((max(start, t0_ms), min(at, t1_ms)))
    for rec in records:
        at = rec.t // 1_000_000
        if rec.stream == "kalshi.ws":
            if last_frame is not None and at - last_frame > max_silence_ms:
                for ticker in list(active):
                    close(ticker, last_frame + max_silence_ms)
            last_frame = at
            feed.append((max(at, t0_ms), min(at + max_silence_ms, t1_ms)))
        for ev in normalize(rec):
            if isinstance(ev, KalshiBookSnapshot):
                active.setdefault(ev.ticker, at)
            elif isinstance(ev, FeedStatus) and ev.status in ("gap", "disconnected", "stale", "error"):
                if ev.stream.startswith("kalshi.book:"):
                    close(ev.stream.split(":", 1)[1], at)
                elif ev.stream.startswith("kalshi"):
                    for ticker in list(active):
                        close(ticker, at)
    for ticker in list(active):
        close(ticker, t1_ms)
    fresh = merge_intervals(feed)
    return {k: intersect(merge_intervals(v), fresh) for k, v in books.items()}
