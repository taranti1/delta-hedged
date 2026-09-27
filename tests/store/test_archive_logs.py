import hashlib
import json

import pytest
import zstandard as zstd

from dh.store.archive_logs import archive_log, restore_log


def log_file(tmp_path, closed=True):
    folder = tmp_path / "paper_logs"
    folder.mkdir()
    p = folder / "paper-example.jsonl"
    p.write_bytes(b'{"k":"log.fv","F":0.51}\n' * 2000 + (b'{"k":"session_end"}\n' if closed else b''))
    return p


def test_roundtrip_exact_bytes_and_metadata(tmp_path):
    p = log_file(tmp_path)
    raw, metadata = p.read_bytes(), p.stat()
    result = archive_log(p, pause_s=0)
    assert not p.exists()
    assert result["sha256_uncompressed"] == hashlib.sha256(raw).hexdigest()
    assert result["saved_bytes"] > 0
    archive = p.with_suffix(".jsonl.zst")
    assert restore_log(archive) == p
    assert p.read_bytes() == raw
    assert p.stat().st_mtime_ns == metadata.st_mtime_ns
    assert p.stat().st_mode == metadata.st_mode
    with pytest.raises(FileExistsError):
        archive_log(p, pause_s=0)


def test_active_log_untouched(tmp_path):
    p = log_file(tmp_path, closed=False)
    raw = p.read_bytes()
    with pytest.raises(ValueError, match="terminal"):
        archive_log(p, pause_s=0)
    assert p.read_bytes() == raw
    assert list(p.parent.iterdir()) == [p]


def test_symlink_untouched(tmp_path):
    p = log_file(tmp_path)
    link = p.with_name("paper-link.jsonl")
    link.symlink_to(p)
    with pytest.raises(ValueError, match="symlink"):
        archive_log(link, pause_s=0)
    assert p.exists()


def test_corrupt_archive_never_restores(tmp_path):
    p = log_file(tmp_path)
    archive_log(p, pause_s=0)
    archive = p.with_suffix(".jsonl.zst")
    archive.write_bytes(zstd.ZstdCompressor().compress(b"different data"))
    with pytest.raises(RuntimeError, match="receipt"):
        restore_log(archive)
    assert not p.exists()


def test_source_change_during_verification_retains_source(tmp_path, monkeypatch):
    import dh.store.archive_logs as module
    p = log_file(tmp_path)
    real_fingerprint = module.fingerprint
    calls = 0
    def changed(path):
        nonlocal calls
        calls += 1
        if calls == 3:
            with path.open("ab") as f:
                f.write(b'{"k":"more"}\n')
        return real_fingerprint(path)
    monkeypatch.setattr(module, "fingerprint", changed)
    with pytest.raises(RuntimeError, match="source changed"):
        archive_log(p, pause_s=0)
    assert p.exists()
    assert not p.with_suffix(".jsonl.zst").exists()


def test_preexisting_archive_never_overwritten(tmp_path):
    p = log_file(tmp_path)
    archive = p.with_suffix(".jsonl.zst")
    archive.write_bytes(b"keep me")
    with pytest.raises(FileExistsError):
        archive_log(p, pause_s=0)
    assert archive.read_bytes() == b"keep me"
    assert p.exists()
