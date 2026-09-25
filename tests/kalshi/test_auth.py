from __future__ import annotations

import base64
from pathlib import Path

import pytest
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec, padding

from dh.kalshi.auth import (
    HEADER_KEY,
    HEADER_SIGNATURE,
    HEADER_TIMESTAMP,
    WS_PATH,
    KalshiSigner,
    full_path,
    sign_path,
)


def _verify(pub, sig_b64: str, message: bytes, salt_length: int = 32) -> None:
    pub.verify(
        base64.b64decode(sig_b64),
        message,
        padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=salt_length),
        hashes.SHA256(),
    )


def test_headers_sign_timestamp_method_path_without_query(rsa_key, rsa_pem):
    s = KalshiSigner("key-123", rsa_pem)
    h = s.headers("post", "/trade-api/v2/portfolio/events/orders?subaccount=0", now_ms=1_700_000_000_123)
    assert h[HEADER_KEY] == "key-123"
    assert h[HEADER_TIMESTAMP] == "1700000000123"
    _verify(rsa_key.public_key(), h[HEADER_SIGNATURE], b"1700000000123POST/trade-api/v2/portfolio/events/orders")
    # a signature over the query-string form must NOT verify
    with pytest.raises(InvalidSignature):
        _verify(
            rsa_key.public_key(),
            h[HEADER_SIGNATURE],
            b"1700000000123POST/trade-api/v2/portfolio/events/orders?subaccount=0",
        )


def test_salt_length_is_digest_length(rsa_key, rsa_pem):
    s = KalshiSigner("k", rsa_pem)
    sig = s.headers("GET", "/trade-api/v2/exchange/status", now_ms=1)[HEADER_SIGNATURE]
    _verify(rsa_key.public_key(), sig, b"1GET/trade-api/v2/exchange/status", salt_length=32)
    with pytest.raises(InvalidSignature):
        _verify(rsa_key.public_key(), sig, b"1GET/trade-api/v2/exchange/status", salt_length=64)


def test_ws_handshake_headers(rsa_key, rsa_pem):
    s = KalshiSigner("k", rsa_pem)
    h = s.ws_headers(now_ms=42)
    assert WS_PATH == "/trade-api/ws/v2"
    _verify(rsa_key.public_key(), h[HEADER_SIGNATURE], b"42GET/trade-api/ws/v2")


def test_default_timestamp_is_ms(rsa_pem):
    h = KalshiSigner("k", rsa_pem).headers("GET", "/trade-api/v2/markets")
    assert len(h[HEADER_TIMESTAMP]) == 13 and h[HEADER_TIMESTAMP].isdigit()


def test_paths():
    assert full_path("https://external-api.kalshi.com/trade-api/v2", "/markets/X") == "/trade-api/v2/markets/X"
    assert full_path("https://h/trade-api/v2/", "markets") == "/trade-api/v2/markets"
    assert sign_path("https://h/trade-api/v2/markets?limit=5#x") == "/trade-api/v2/markets"
    with pytest.raises(ValueError):
        sign_path("markets")


def test_key_loading_variants(rsa_pem, tmp_path: Path):
    p = tmp_path / "k.pem"
    p.write_bytes(rsa_pem)
    for src in (rsa_pem, rsa_pem.decode(), p, str(p)):
        s = KalshiSigner("k", src)
        assert b"PUBLIC KEY" in s.public_key_pem()
    s = KalshiSigner("k", p)
    assert "PRIVATE" not in repr(s) and "redacted" in repr(s)
    with pytest.raises(ValueError):
        KalshiSigner("", rsa_pem)
    with pytest.raises(ValueError):
        KalshiSigner("k", b"-----BEGIN RSA PRIVATE KEY-----\nnot a key\n-----END RSA PRIVATE KEY-----\n")


def test_non_rsa_key_rejected():
    ec_pem = ec.generate_private_key(ec.SECP256R1()).private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
    )
    with pytest.raises(TypeError):
        KalshiSigner("k", ec_pem)


def test_from_env(monkeypatch, rsa_pem, tmp_path: Path):
    p = tmp_path / "k.pem"
    p.write_bytes(rsa_pem)
    monkeypatch.setenv("KALSHI_KEY_ID", "abc")
    monkeypatch.setenv("KALSHI_PRIVATE_KEY_PATH", str(p))
    assert KalshiSigner.from_env().key_id == "abc"
    monkeypatch.delenv("KALSHI_KEY_ID")
    with pytest.raises(RuntimeError):
        KalshiSigner.from_env()
