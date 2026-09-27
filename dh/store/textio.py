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
            with io.TextIOWrapper(io.BufferedReader(_CheckedZstd(raw, zstandard)), encoding="utf-8") as stream:
                yield stream
    else:
        with path.open(encoding="utf-8") as stream:
            yield stream


class _CheckedZstd(io.RawIOBase):
    """Streaming .zst decoder that raises on a truncated final frame (the library's stream
    reader ends quietly when the input stops mid-frame, which could drop a log's tail)."""

    def __init__(self, raw, zstandard, chunk: int = 1 << 20) -> None:
        self._raw, self._zstd, self._chunk = raw, zstandard, chunk
        self._dobj = zstandard.ZstdDecompressor().decompressobj()
        self._buf = b""
        self._frame_open = False

    def readable(self) -> bool:
        return True

    def readinto(self, b) -> int:
        while not self._buf:
            data = self._raw.read(self._chunk)
            if not data:
                if self._frame_open:
                    raise ValueError(f"truncated zstd stream: {getattr(self._raw, 'name', '?')}")
                return 0
            while data:
                self._frame_open = True
                self._buf += self._dobj.decompress(data)
                data = b""
                if self._dobj.eof:
                    self._frame_open = False
                    data = self._dobj.unused_data
                    self._dobj = self._zstd.ZstdDecompressor().decompressobj()
        n = min(len(b), len(self._buf))
        b[:n] = self._buf[:n]
        self._buf = self._buf[n:]
        return n
