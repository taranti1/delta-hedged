"""Kalshi API-key request signing (REST and WebSocket handshake).

Every authenticated request carries three headers (openapi securitySchemes):

  KALSHI-ACCESS-KEY        the API key id
  KALSHI-ACCESS-TIMESTAMP  request time, Unix epoch **milliseconds**, as a decimal string
  KALSHI-ACCESS-SIGNATURE  base64( RSA-PSS( SHA256, MGF1(SHA256), salt_length = 32 bytes
                           (= digest length) ) over the UTF-8 bytes of
                           f"{timestamp_ms}{METHOD}{path}" )

``path`` is the full request path from the host root, including the ``/trade-api/v2``
prefix, WITHOUT the query string (the official SDK signs ``URL.pathname``), e.g.
``/trade-api/v2/portfolio/events/orders``. The WebSocket handshake is signed the same way
with ``GET`` and ``/trade-api/ws/v2`` (the headers are sent on the HTTP upgrade request).

The private key never appears in reprs, logs or exceptions raised by this module.
"""

from __future__ import annotations

import base64
import os
import time
from pathlib import Path
from urllib.parse import urlsplit

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey

REST_PATH_PREFIX = "/trade-api/v2"
WS_PATH = "/trade-api/ws/v2"

HEADER_KEY = "KALSHI-ACCESS-KEY"
HEADER_TIMESTAMP = "KALSHI-ACCESS-TIMESTAMP"
HEADER_SIGNATURE = "KALSHI-ACCESS-SIGNATURE"

_PSS = padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH)


def _load_key(private_key_pem: bytes | str | Path) -> RSAPrivateKey:
    if isinstance(private_key_pem, Path):
        data = private_key_pem.expanduser().read_bytes()
    elif isinstance(private_key_pem, bytes):
        data = private_key_pem
    elif isinstance(private_key_pem, str):
        if "-----BEGIN" in private_key_pem:
            data = private_key_pem.encode("utf-8")
        else:  # a filesystem path given as str
            data = Path(private_key_pem).expanduser().read_bytes()
    else:
        raise TypeError("private_key_pem must be PEM bytes/str or a Path to a PEM file")
    try:
        key = serialization.load_pem_private_key(data, password=None)
    except ValueError as exc:  # do not echo key material
        raise ValueError("could not parse the Kalshi private key (expected unencrypted PEM)") from exc
    if not isinstance(key, RSAPrivateKey):
        raise TypeError("Kalshi API keys are RSA keys; got a different key type")
    return key


def sign_path(path_or_url: str) -> str:
    """Path component to sign: strips scheme/host and the query string/fragment."""
    if "://" in path_or_url:
        path = urlsplit(path_or_url).path
    else:
        path = path_or_url.split("?", 1)[0].split("#", 1)[0]
    if not path.startswith("/"):
        raise ValueError(f"signing path must be absolute from the host root: {path_or_url!r}")
    return path


def full_path(base_url: str, rel_path: str) -> str:
    """Join a base URL's path ('https://h/trade-api/v2') and a relative API path ('/markets')."""
    base = urlsplit(base_url).path.rstrip("/")
    if not rel_path.startswith("/"):
        rel_path = "/" + rel_path
    return sign_path(base + rel_path)


class KalshiSigner:
    """Signs Kalshi requests. Thread/async safe (stateless apart from the loaded key)."""

    __slots__ = ("key_id", "_key")

    def __init__(self, key_id: str, private_key_pem: bytes | str | Path) -> None:
        """key_id: API key id; private_key_pem: PEM bytes/str, or a Path (or path str) to it."""
        if not key_id:
            raise ValueError("key_id is required")
        self.key_id = key_id
        self._key = _load_key(private_key_pem)

    @classmethod
    def from_env(
        cls,
        key_id_var: str = "KALSHI_KEY_ID",
        key_path_var: str = "KALSHI_PRIVATE_KEY_PATH",
    ) -> KalshiSigner:
        """Build from environment variables (key id + path to the PEM file)."""
        key_id = os.environ.get(key_id_var, "")
        key_path = os.environ.get(key_path_var, "")
        if not key_id or not key_path:
            raise RuntimeError(f"set {key_id_var} and {key_path_var} to use authenticated endpoints")
        return cls(key_id, Path(key_path))

    def sign(self, message: bytes) -> str:
        """base64 RSA-PSS(SHA256, MGF1-SHA256, salt=digest length) signature of message."""
        sig = self._key.sign(message, _PSS, hashes.SHA256())
        return base64.b64encode(sig).decode("ascii")

    def headers(self, method: str, path: str, now_ms: int | None = None) -> dict[str, str]:
        """Auth headers for one request.

        method: HTTP method (any case; signed upper-case). path: full path incl. the
        '/trade-api/v2' prefix; a query string, if present, is stripped before signing.
        now_ms: request time in Unix ms (default: wall clock; injectable for tests).
        """
        ts = str(int(time.time() * 1000) if now_ms is None else int(now_ms))
        msg = f"{ts}{method.upper()}{sign_path(path)}".encode()
        return {
            HEADER_KEY: self.key_id,
            HEADER_TIMESTAMP: ts,
            HEADER_SIGNATURE: self.sign(msg),
        }

    def ws_headers(self, now_ms: int | None = None, path: str = WS_PATH) -> dict[str, str]:
        """Headers for the authenticated WebSocket handshake (GET /trade-api/ws/v2)."""
        return self.headers("GET", path, now_ms=now_ms)

    def public_key_pem(self) -> bytes:
        """Public half (PEM) — safe to print; useful to confirm which key is loaded."""
        return self._key.public_key().public_bytes(
            serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo
        )

    def __repr__(self) -> str:
        return f"KalshiSigner(key_id={self.key_id!r}, key=<redacted>)"
