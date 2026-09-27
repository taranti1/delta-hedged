"""Streaming readers for losslessly archived UTF-8 logs."""
from __future__ import annotations

import gzip
import io
from contextlib import contextmanager
from pathlib import Path


@contextmanager
def open_text(path: str | Path):
    """Read plain, .gz, or .zst text without materializing the file in memory."""
    path = Path(path)
    if path.suffix == ".gz":
        with gzip.open(path, "rt", encoding="utf-8") as stream:
            yield stream
    elif path.suffix == ".zst":
        import zstandard

        with path.open("rb") as raw:
            with zstandard.ZstdDecompressor().stream_reader(raw) as reader:
                with io.TextIOWrapper(reader, encoding="utf-8") as stream:
                    yield stream
    else:
        with path.open(encoding="utf-8") as stream:
            yield stream
