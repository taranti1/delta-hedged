"""Content-addressed E0 input inventory; metadata checks also detect concurrent writers."""
from __future__ import annotations
import hashlib
import subprocess
from pathlib import Path


def implementation_manifest(repo: Path | None = None) -> dict:
    """Effective source/model/cost identities, including uncommitted changes.

    Deliberately excludes host-only connection configs, secrets, and runtime data.
    A commit id alone does not identify the code used from a dirty working tree.
    """
    repo = repo or Path(__file__).resolve().parents[2]
    paths = set((repo / "dh").rglob("*.py")) | set((repo / "scripts").rglob("*.py"))
    paths.update((repo / "dh" / "models" / "data").glob("*.json"))
    paths.update(repo / p for p in ("config/fees.yaml", "config/m1.yaml", "pyproject.toml"))
    files = []
    for p in sorted(paths):
        if p.is_file() and not p.is_symlink():
            files.append({"path": str(p.relative_to(repo)), "sha256": hashlib.sha256(p.read_bytes()).hexdigest()})
    try:
        result = subprocess.run(["git", "rev-parse", "HEAD"], cwd=repo, capture_output=True, text=True, check=True)
        sha = result.stdout.strip()
    except (OSError, subprocess.SubprocessError):
        sha = None
    return {"git_sha": sha, "files": files}


def input_manifest(root: Path) -> dict:
    rows = []
    for folder in ("trades", "markets", "brti", "series", "fees", "events"):
        for p in sorted((root / folder).rglob("*")):
            if not p.is_file() or p.suffix not in (".json", ".parquet"):
                continue
            before = p.stat()
            h = hashlib.sha256()
            with p.open("rb") as f:
                for chunk in iter(lambda: f.read(1024 * 1024), b""):
                    h.update(chunk)
            after = p.stat()
            if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
                raise ValueError(f"input changed while hashing: {p}")
            rows.append({"path": str(p.relative_to(root)), "bytes": after.st_size,
                         "mtime_ns": after.st_mtime_ns, "sha256": h.hexdigest()})
    return {"version": 2, "files": rows, "implementation": implementation_manifest()}


def manifest_matches(root: Path, manifest: dict) -> bool:
    return input_manifest(root) == manifest
