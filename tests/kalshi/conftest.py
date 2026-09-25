"""Shared fixtures for dh.kalshi tests (offline; spec-shaped payloads only)."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

ROOT = Path(__file__).resolve().parents[2]
ASYNCAPI = ROOT / "docs" / "kalshi_specs" / "asyncapi.yaml"


@pytest.fixture(scope="session")
def asyncapi_examples() -> dict[str, list[dict]]:
    """message name -> list of example payloads, straight from the vendored asyncapi.yaml."""
    spec = yaml.safe_load(ASYNCAPI.read_text(encoding="utf-8"))
    out: dict[str, list[dict]] = {}
    for name, m in spec["components"]["messages"].items():
        for ex in m.get("examples") or []:
            out.setdefault(name, []).append(ex["payload"])
    return out


@pytest.fixture(scope="session")
def rsa_key() -> rsa.RSAPrivateKey:
    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


@pytest.fixture(scope="session")
def rsa_pem(rsa_key: rsa.RSAPrivateKey) -> bytes:
    return rsa_key.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.TraditionalOpenSSL, serialization.NoEncryption()
    )
