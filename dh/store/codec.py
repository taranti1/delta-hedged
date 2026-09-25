"""Event codec: dh.core.events dataclasses <-> JSON bytes (via EVENT_TYPES).

Encoded form: ``{"type": "<ClassName>", "<field>": value, ...}``. Tuples become JSON arrays and
are turned back into (nested) tuples on decode, so ``decode_event(encode_event(ev)) == ev``.
Used for the recorder's ``status`` stream, ``events.*`` streams (normalized events written by
a live runner) and Parquet compaction.
"""

from __future__ import annotations

from typing import Any

import orjson

from dh.core.events import EVENT_TYPES, Event


def _fields(cls: type) -> tuple[str, ...]:
    return tuple(cls.__dataclass_fields__)  # type: ignore[attr-defined]


_FIELDS: dict[type, tuple[str, ...]] = {cls: _fields(cls) for cls in EVENT_TYPES.values()}


def event_to_dict(ev: Event) -> dict[str, Any]:
    cls = type(ev)
    names = _FIELDS.get(cls) or _fields(cls)
    d: dict[str, Any] = {"type": cls.__name__}
    for n in names:
        d[n] = getattr(ev, n)
    return d


def _tuplify(v: Any) -> Any:
    if isinstance(v, list):
        return tuple(_tuplify(x) for x in v)
    return v


def event_from_dict(d: dict[str, Any]) -> Event:
    d = dict(d)
    name = d.pop("type")
    try:
        cls = EVENT_TYPES[name]
    except KeyError:
        raise ValueError(f"unknown event type {name!r}") from None
    return cls(**{k: _tuplify(v) for k, v in d.items()})


def encode_event(ev: Event) -> bytes:
    return orjson.dumps(event_to_dict(ev))


def decode_event(raw: bytes | str) -> Event:
    return event_from_dict(orjson.loads(raw))
