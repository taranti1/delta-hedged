"""Compaction of the raw store into Parquet for research (DuckDB / polars).

Two products per stream per UTC day (derived data: safe to delete and rebuild):

  <root>/parquet/raw/<stream>/<YYYY-MM-DD>.parquet
      lossless copy of the raw records: t (int64 ns), q (int64), d (string, raw frame text)
  <root>/parquet/events/<stream>/<YYYY-MM-DD>/<EventType>.parquet
      normalized events (dh.store.replay.iter_events), one table per event type, columns =
      the dataclass fields. Level tuples become lists of structs, e.g. ExtBookSnapshot.bids ->
      list<struct<price: double, size: double>>, ExtBookDelta.changes ->
      list<struct<side: string, price: double, size: double>>.

Example (DuckDB):
  SELECT ts, venue, symbol, price, size, aggressor
  FROM 'data/parquet/events/coinbase.ws/2026-09-25/ExtTrade.parquet' ORDER BY ts;

Events are normalized with a fresh normalizer state at 00:00 UTC; pass ``warmup_ns`` (e.g. one
hour) to start books from the previous day's last snapshot.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from dh.core.events import EVENT_TYPES
from dh.store.replay import day_bounds, iter_events, iter_stream

ROW_GROUP = 200_000

# Named struct fields for known level-tuple shapes (by annotation).
_TUPLE_NAMES: dict[tuple[str, ...], tuple[str, ...]] = {
    ("float", "float"): ("price", "size"),
    ("str", "float", "float"): ("side", "price", "size"),
    ("int", "int"): ("px", "qty"),
    ("int", "int", "int"): ("start", "end", "step"),
}
_STR_ALIASES = {"str", "YesNo", "BookSide", "Aggressor"}


def _scalar_type(t: str) -> pa.DataType:
    if t == "int":
        return pa.int64()
    if t == "float":
        return pa.float64()
    if t == "bool":
        return pa.bool_()
    return pa.string()


def arrow_type(annotation: str) -> tuple[pa.DataType, tuple[str, ...] | None]:
    """Dataclass annotation string -> (arrow type, struct field names or None)."""
    a = annotation.replace(" ", "")
    if a.endswith("|None"):
        a = a[: -len("|None")]
    if a.startswith("tuple[tuple[") and a.endswith(",...]"):
        inner = a[len("tuple[tuple[") : -len("],...]")].split(",")
        names = _TUPLE_NAMES.get(tuple(inner), tuple(f"f{i}" for i in range(len(inner))))
        return pa.list_(pa.struct([pa.field(n, _scalar_type(t)) for n, t in zip(names, inner)])), names
    if a in _STR_ALIASES or a.startswith("Literal["):
        return pa.string(), None
    if a in ("int", "float", "bool"):
        return _scalar_type(a), None
    return pa.string(), None


def event_schema(cls: type) -> tuple[pa.Schema, dict[str, tuple[str, ...]]]:
    fields = []
    structs: dict[str, tuple[str, ...]] = {}
    for name, f in cls.__dataclass_fields__.items():  # type: ignore[attr-defined]
        ann = f.type if isinstance(f.type, str) else getattr(f.type, "__name__", str(f.type))
        typ, names = arrow_type(ann)
        if names is not None:
            structs[name] = names
        fields.append(pa.field(name, typ))
    return pa.schema(fields), structs


class _TypeWriter:
    def __init__(self, path: Path, cls: type) -> None:
        self.path = path
        self.schema, self.structs = event_schema(cls)
        self.names = [f.name for f in self.schema]
        self.rows: dict[str, list[Any]] = {n: [] for n in self.names}
        self.n = 0
        self.writer: pq.ParquetWriter | None = None

    def add(self, ev: Any) -> None:
        for n in self.names:
            v = getattr(ev, n)
            names = self.structs.get(n)
            if names is not None:
                v = [dict(zip(names, t)) for t in v]
            self.rows[n].append(v)
        self.n += 1
        if self.n >= ROW_GROUP:
            self.flush()

    def flush(self) -> None:
        if self.n == 0:
            return
        table = pa.Table.from_pydict(self.rows, schema=self.schema)
        if self.writer is None:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self.writer = pq.ParquetWriter(self.path, self.schema, compression="zstd")
        self.writer.write_table(table)
        self.rows = {n: [] for n in self.names}
        self.n = 0

    def close(self) -> None:
        self.flush()
        if self.writer is not None:
            self.writer.close()


_DAY_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def compact_raw(root: str | Path, stream: str, day: str, out_root: str | Path | None = None) -> Path | None:
    """Raw records of one stream-day -> Parquet (t, q, d). Returns the path (None if empty)."""
    if not _DAY_RE.match(day):
        raise ValueError(f"bad day {day!r}")
    t0, t1 = day_bounds(day)
    out = Path(out_root or Path(root) / "parquet") / "raw" / stream / f"{day}.parquet"
    schema = pa.schema([("t", pa.int64()), ("q", pa.int64()), ("d", pa.large_string())])
    writer: pq.ParquetWriter | None = None
    cols: dict[str, list[Any]] = {"t": [], "q": [], "d": []}
    n_total = 0

    def _flush() -> None:
        nonlocal writer
        if not cols["t"]:
            return
        if writer is None:
            out.parent.mkdir(parents=True, exist_ok=True)
            writer = pq.ParquetWriter(out, schema, compression="zstd")
        writer.write_table(pa.Table.from_pydict(cols, schema=schema))
        for v in cols.values():
            v.clear()

    for rec in iter_stream(root, stream, t0, t1):
        cols["t"].append(rec.t)
        cols["q"].append(rec.q)
        cols["d"].append(rec.data.decode("utf-8", "surrogateescape"))
        n_total += 1
        if len(cols["t"]) >= ROW_GROUP:
            _flush()
    _flush()
    if writer is not None:
        writer.close()
    return out if n_total else None


def compact_events(
    root: str | Path, stream: str, day: str, out_root: str | Path | None = None, warmup_ns: int = 0
) -> dict[str, Path]:
    """Normalized events of one stream-day -> one Parquet file per event type."""
    if not _DAY_RE.match(day):
        raise ValueError(f"bad day {day!r}")
    t0, t1 = day_bounds(day)
    base = Path(out_root or Path(root) / "parquet") / "events" / stream / day
    writers: dict[str, _TypeWriter] = {}
    for ev in iter_events(root, [stream], t0, t1, warmup_ns=warmup_ns):
        name = type(ev).__name__
        w = writers.get(name)
        if w is None:
            w = writers[name] = _TypeWriter(base / f"{name}.parquet", EVENT_TYPES[name])
        w.add(ev)
    for w in writers.values():
        w.close()
    return {n: w.path for n, w in sorted(writers.items())}
