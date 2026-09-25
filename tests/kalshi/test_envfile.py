"""auth.env_file support: KEY=VALUE loader (no real secrets anywhere in these tests)."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from dh.kalshi.config import EXAMPLE_CONFIG, load_config
from dh.kalshi.envfile import EnvFileError, is_blocked, load_env_file, parse_env_text

FAKE_ID = "fake-key-id-0000"


def test_parse_rules():
    text = "\n".join([
        "# comment",
        "",
        "PLAIN=abc",
        "export EXPORTED=def",
        'DQ="quoted value"',
        "SQ='single # not a comment'",
        "INLINE=val # trailing comment",
        "HASHED=a#b",
        "EMPTY=",
        "  SPACED  =  padded  ",
        "not a pair",
        "1BAD=x",
        "DUP=first",
        "DUP=second",
    ])
    vals, bad = parse_env_text(text)
    assert vals == {"PLAIN": "abc", "EXPORTED": "def", "DQ": "quoted value", "SQ": "single # not a comment",
                    "INLINE": "val", "HASHED": "a#b", "EMPTY": "", "SPACED": "padded", "DUP": "second"}
    assert bad == 2


def test_load_never_overrides_and_never_loads_allow(tmp_path: Path):
    f = tmp_path / "x.env"
    f.write_text("A_ID=from-file\nALREADY=from-file\nALLOW_LIVE_TRADING=true\nallow_lower=1\nB_PATH='/k.pem'\n")
    f.chmod(0o600)
    env = {"ALREADY": "from-env"}
    rep = load_env_file(f, env)
    assert env == {"ALREADY": "from-env", "A_ID": "from-file", "B_PATH": "/k.pem"}
    assert rep.loaded == ["A_ID", "B_PATH"] and rep.kept == ["ALREADY"]
    assert rep.blocked == ["ALLOW_LIVE_TRADING", "allow_lower"] and not rep.world_readable
    s = rep.summary()
    assert "from-file" not in s and "/k.pem" not in s and "A_ID" in s  # names only, never values
    assert is_blocked("ALLOW_X") and is_blocked("Allow_x") and not is_blocked("XALLOW_")


def test_world_readable_flag_and_missing_file(tmp_path: Path):
    f = tmp_path / "x.env"
    f.write_text("K=secret-value\n")
    f.chmod(0o644)
    assert load_env_file(f, {}).world_readable
    missing = tmp_path / "nope.env"
    with pytest.raises(EnvFileError) as ei:
        load_env_file(missing, {})
    assert str(missing) in str(ei.value)


def test_load_config_env_file_with_custom_var_names(monkeypatch, tmp_path: Path, rsa_pem):
    key = tmp_path / "k.pem"
    key.write_bytes(rsa_pem)
    envf = tmp_path / "prod.env"
    envf.write_text(f'MY_KEY_ID={FAKE_ID}\nMY_KEY_PATH="{key}"\nALLOW_SOMETHING=yes\n')
    envf.chmod(0o600)
    for v in ("MY_KEY_ID", "MY_KEY_PATH", "ALLOW_SOMETHING"):
        monkeypatch.setenv(v, "x")  # registers the original state so the loader's writes are undone
        monkeypatch.delenv(v)
    cfg_path = tmp_path / "kalshi.yaml"
    cfg_path.write_text(EXAMPLE_CONFIG.read_text()
                        .replace("env_file: null", f"env_file: {envf}")
                        .replace("key_id_env: KALSHI_KEY_ID", "key_id_env: MY_KEY_ID")
                        .replace("private_key_path_env: KALSHI_PRIVATE_KEY_PATH", "private_key_path_env: MY_KEY_PATH")
                        .replace("account_share: 1.0", "account_share: 0.2"))
    cfg = load_config(cfg_path)
    assert cfg.key_id == FAKE_ID and cfg.private_key_path == str(key) and cfg.has_credentials
    assert cfg.signer() is not None and FAKE_ID not in repr(cfg.signer())
    assert "ALLOW_SOMETHING" not in os.environ
    assert cfg.env_file == str(envf) and cfg.env_file_report.loaded == ["MY_KEY_ID", "MY_KEY_PATH"]
    assert "MY_KEY_ID" in cfg.credentials_hint() and FAKE_ID not in cfg.credentials_hint()
    assert cfg.account_share == 0.2 and cfg.limiter().account_share == 0.2
    # an explicitly set environment variable wins over the file
    monkeypatch.setenv("MY_KEY_ID", "from-env")
    assert load_config(cfg_path).key_id == "from-env"


def test_bad_account_share_rejected(tmp_path: Path):
    p = tmp_path / "k.yaml"
    p.write_text(EXAMPLE_CONFIG.read_text().replace("account_share: 1.0", "account_share: 1.5"))
    with pytest.raises(ValueError):
        load_config(p)
