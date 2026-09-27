"""Lossless archival of closed paper logs, with round-trip verification.

This deliberately does not touch recorder segments, active logs, live logs, or
Parquet datasets. Their ownership/reader protocols are different. Run one worker
at a low scheduling priority on a host that also runs trading workloads.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import stat
import time
import uuid

import zstandard as zstd

CHUNK = 1024 * 1024


def fingerprint(path: Path) -> tuple[int, ...]:
    s = path.lstat()
    if not stat.S_ISREG(s.st_mode) or s.st_nlink != 1:
        raise ValueError(f"not a single-link regular file: {path}")
    return s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns, s.st_ctime_ns


def _sync_dir(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def archive_log(path: Path, *, level: int = 6, pause_s: float = .002) -> dict:
    """Replace a terminal paper-session JSON log with a byte-exact zstd archive.

    A durable receipt precedes removal of the source. Failures keep the source;
    an already existing archive is never overwritten. The terminal marker and
    repeated identity checks prevent archival of a growing/current session.
    """
    path = Path(path).absolute()
    if path.is_symlink() or path.parent.is_symlink() or path.resolve() != path:
        raise ValueError("symlink paths are not supported")
    if path.parent.name != "paper_logs" or not path.name.startswith("paper-") or path.suffix != ".jsonl":
        raise ValueError("only paper_logs/paper-*.jsonl session logs may be archived")
    before = fingerprint(path)
    metadata = path.stat()
    with path.open("rb") as src:
        src.seek(max(0, before[2] - 65536))
        tail = src.read()
    if not tail.endswith(b"\n") or json.loads(tail.splitlines()[-1]).get("k") != "session_end":
        raise ValueError("log has no terminal session_end record; leave it untouched")
    dest = path.with_suffix(path.suffix + ".zst")
    receipt = dest.with_name(dest.name + ".archive.json")
    if dest.exists() or receipt.exists():
        raise FileExistsError(f"archive or receipt already exists for {path}")
    tmp = dest.with_name(dest.name + ".tmp-" + uuid.uuid4().hex)
    receipt_tmp = receipt.with_name(receipt.name + ".tmp-" + uuid.uuid4().hex)
    raw_hash = hashlib.sha256()
    raw_size = 0
    try:
        with path.open("rb") as src, tmp.open("xb") as dst:
            os.fchmod(dst.fileno(), stat.S_IMODE(metadata.st_mode))
            with zstd.ZstdCompressor(level=level, threads=0, write_checksum=True).stream_writer(dst, closefd=False) as writer:
                while chunk := src.read(CHUNK):
                    raw_hash.update(chunk)
                    raw_size += len(chunk)
                    writer.write(chunk)
                    if pause_s:
                        time.sleep(pause_s)
            dst.flush()
            os.fsync(dst.fileno())
        if fingerprint(path) != before or raw_size != before[2]:
            raise RuntimeError("source changed while compressing; source retained")
        verified = hashlib.sha256()
        verified_size = 0
        with tmp.open("rb") as packed, zstd.ZstdDecompressor().stream_reader(packed) as reader:
            while chunk := reader.read(CHUNK):
                verified.update(chunk)
                verified_size += len(chunk)
                if pause_s:
                    time.sleep(pause_s)
        if verified_size != raw_size or verified.digest() != raw_hash.digest():
            raise RuntimeError("archive round-trip verification failed; source retained")
        packed_size = tmp.stat().st_size
        if packed_size >= raw_size:
            raise ValueError("compression does not save space; source retained")
        if fingerprint(path) != before:
            raise RuntimeError("source changed while verifying; source retained")
        result = {"format": "dh-paper-log-archive/1", "source": str(path), "archive": str(dest),
                  "sha256_uncompressed": raw_hash.hexdigest(), "original_bytes": raw_size,
                  "compressed_bytes": packed_size, "saved_bytes": raw_size - packed_size,
                  "original_mtime_ns": metadata.st_mtime_ns, "original_atime_ns": metadata.st_atime_ns,
                  "original_mode": stat.S_IMODE(metadata.st_mode), "verified_round_trip": True,
                  "created_ns": time.time_ns()}
        with receipt_tmp.open("x") as f:
            json.dump(result, f, indent=2)
            f.write("\n")
            f.flush()
            os.fsync(f.fileno())
        # link provides atomic no-clobber publication, unlike replace().
        os.link(tmp, dest)
        tmp.unlink()
        os.link(receipt_tmp, receipt)
        receipt_tmp.unlink()
        _sync_dir(path.parent)
        if fingerprint(path) != before:
            raise RuntimeError("source changed before retirement; source and archive both retained")
        path.unlink()
        _sync_dir(path.parent)
        return result
    finally:
        tmp.unlink(missing_ok=True)
        receipt_tmp.unlink(missing_ok=True)


def restore_log(archive: Path) -> Path:
    """Restore original bytes/permissions/times without deleting the archive."""
    archive = Path(archive).absolute()
    if archive.resolve() != archive or not archive.name.endswith(".jsonl.zst"):
        raise ValueError("expected a non-symlink .jsonl.zst archive")
    receipt = json.loads(archive.with_name(archive.name + ".archive.json").read_text())
    dest = archive.with_suffix("")
    if dest.exists():
        raise FileExistsError(dest)
    tmp = dest.with_name(dest.name + ".restore-" + uuid.uuid4().hex)
    digest = hashlib.sha256()
    n = 0
    try:
        with archive.open("rb") as src, zstd.ZstdDecompressor().stream_reader(src) as reader, tmp.open("xb") as dst:
            while chunk := reader.read(CHUNK):
                digest.update(chunk)
                n += len(chunk)
                dst.write(chunk)
            dst.flush()
            os.fsync(dst.fileno())
        if n != receipt["original_bytes"] or digest.hexdigest() != receipt["sha256_uncompressed"]:
            raise RuntimeError("archive does not match receipt")
        os.chmod(tmp, receipt["original_mode"])
        os.utime(tmp, ns=(receipt["original_atime_ns"], receipt["original_mtime_ns"]))
        os.link(tmp, dest)
        _sync_dir(dest.parent)
        return dest
    finally:
        tmp.unlink(missing_ok=True)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("paths", nargs="+", type=Path)
    parser.add_argument("--restore", action="store_true")
    parser.add_argument("--execute", action="store_true", help="otherwise list candidates only")
    args = parser.parse_args(argv)
    if hasattr(os, "nice"):
        try:
            os.nice(10)
        except PermissionError:
            pass  # some sandboxes deny even lowering priority; chunk sleeps still throttle I/O
    for path in args.paths:
        if not args.execute:
            print(json.dumps({"path": str(path), "action": "restore" if args.restore else "archive", "dry_run": True}), flush=True)
        elif args.restore:
            print(json.dumps({"restored": str(restore_log(path))}), flush=True)
        else:
            print(json.dumps(archive_log(path)), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
