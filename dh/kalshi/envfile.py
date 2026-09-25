"""KEY=VALUE environment files for credentials (``auth.env_file`` in config/kalshi.yaml).

Lets this system reuse an existing, host-local secrets file (for example the one another
system on the same host already maintains) WITHOUT copying the key id or the private key:
the file is read programmatically at start-up and its variables are put into ``os.environ``
so the usual ``auth.key_id_env`` / ``auth.private_key_path_env`` lookups find them.

Rules (deliberately small; no shell semantics):
  * one ``KEY=VALUE`` per line; blank lines and lines starting with ``#`` are ignored; an
    optional leading ``export`` is accepted;
  * KEY must match ``[A-Za-z_][A-Za-z0-9_]*``; other lines are skipped (and counted);
  * VALUE: surrounding whitespace stripped; one pair of matching single or double quotes is
    removed; an unquoted value ends at `` #`` (inline comment). No ``$VAR`` expansion, no
    escapes, no multi-line values;
  * variables ALREADY set in the environment are never overridden (the environment wins);
  * variables whose name starts with ``ALLOW_`` (any case) are NEVER loaded: in the other
    system such names are safety switches (e.g. permission to trade) that must not leak
    into this process by way of a shared credentials file.

Values are never logged, printed or put in exception messages: only variable NAMES appear
in the returned report. A missing or unreadable file raises ``EnvFileError`` naming the path.
"""

from __future__ import annotations

import os
import re
import stat
from collections.abc import MutableMapping
from dataclasses import dataclass, field
from pathlib import Path

_KEY_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
BLOCKED_PREFIXES = ("ALLOW_",)


class EnvFileError(RuntimeError):
    """The env file could not be read (message names the path, never a value)."""


@dataclass
class EnvFileReport:
    """What load_env_file did, by variable NAME only (safe to log)."""

    path: str
    loaded: list[str] = field(default_factory=list)  # set into the environment
    kept: list[str] = field(default_factory=list)  # already set in the environment: not overridden
    blocked: list[str] = field(default_factory=list)  # ALLOW_* names: never loaded
    invalid_lines: int = 0  # lines that are neither KEY=VALUE nor comments
    world_readable: bool = False  # group/other permission bits set on the file

    def summary(self) -> str:
        parts = [f"env file {self.path}: loaded {self.loaded or '[]'}"]
        if self.kept:
            parts.append(f"kept existing {self.kept}")
        if self.blocked:
            parts.append(f"blocked {self.blocked}")
        if self.invalid_lines:
            parts.append(f"{self.invalid_lines} unparsed lines")
        if self.world_readable:
            parts.append("WARNING: file is readable by group/others (chmod 600)")
        return "; ".join(parts)


def is_blocked(name: str) -> bool:
    """True for names this loader must never load (``ALLOW_*``, case-insensitive)."""
    up = name.upper()
    return any(up.startswith(p) for p in BLOCKED_PREFIXES)


def _unquote(value: str) -> str:
    v = value.strip()
    if len(v) >= 2 and v[0] == v[-1] and v[0] in ("'", '"'):
        return v[1:-1]
    # unquoted: an inline comment starts at whitespace + '#'
    m = re.search(r"\s#", v)
    if m:
        v = v[: m.start()].rstrip()
    return v


def parse_env_text(text: str) -> tuple[dict[str, str], int]:
    """Parse env-file text -> ({name: value}, number of unparsed lines). Later lines win."""
    out: dict[str, str] = {}
    bad = 0
    for line in text.splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        if s.startswith("export ") or s.startswith("export\t"):
            s = s[7:].lstrip()
        if "=" not in s:
            bad += 1
            continue
        key, value = s.split("=", 1)
        key = key.strip()
        if not _KEY_RE.match(key):
            bad += 1
            continue
        out[key] = _unquote(value)
    return out, bad


def resolve_env_path(path: str | os.PathLike[str], base: Path | None = None) -> Path:
    """Expand ``~`` and resolve a relative path against ``base`` (default: the CWD)."""
    p = Path(path).expanduser()
    if not p.is_absolute() and base is not None:
        p = base / p
    return p


def load_env_file(
    path: str | os.PathLike[str],
    environ: MutableMapping[str, str] | None = None,
    *,
    base: Path | None = None,
) -> EnvFileReport:
    """Load ``path`` into ``environ`` (default ``os.environ``) without overriding variables
    that are already set and never loading ``ALLOW_*`` names. Returns a names-only report."""
    env = os.environ if environ is None else environ
    p = resolve_env_path(path, base)
    try:
        text = p.read_text(encoding="utf-8")
        mode = p.stat().st_mode
    except OSError as exc:
        raise EnvFileError(f"cannot read env file {p}: {type(exc).__name__}") from None
    values, bad = parse_env_text(text)
    rep = EnvFileReport(path=str(p), invalid_lines=bad, world_readable=bool(mode & (stat.S_IRWXG | stat.S_IRWXO)))
    for name, value in values.items():
        if is_blocked(name):
            rep.blocked.append(name)
        elif name in env:
            rep.kept.append(name)
        else:
            env[name] = value
            rep.loaded.append(name)
    return rep
