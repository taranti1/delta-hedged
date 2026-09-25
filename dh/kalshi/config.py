"""Kalshi adapter configuration (config/kalshi.yaml, falling back to config/kalshi.example.yaml).

Secrets are never stored in the config: the key id / private-key PATH come from the
environment (KALSHI_KEY_ID, KALSHI_PRIVATE_KEY_PATH) or from placeholder fields.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from dh.kalshi.auth import KalshiSigner
from dh.kalshi.fees import FeeEngine, FeeRates
from dh.kalshi.rate_limit import BucketLimit, KalshiRateLimiter

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG = REPO_ROOT / "config" / "kalshi.yaml"
EXAMPLE_CONFIG = REPO_ROOT / "config" / "kalshi.example.yaml"


@dataclass
class KalshiConfig:
    env: str
    rest_url: str
    ws_url: str
    series: list[str]
    index_ids: list[str]
    key_id: str = ""
    private_key_path: str = ""
    rest: dict[str, Any] = field(default_factory=dict)
    ws: dict[str, Any] = field(default_factory=dict)
    rate_limits: dict[str, Any] = field(default_factory=dict)
    fees: dict[str, Any] = field(default_factory=dict)
    history: dict[str, Any] = field(default_factory=dict)
    source: str = ""

    @property
    def has_credentials(self) -> bool:
        return bool(self.key_id and self.private_key_path and Path(self.private_key_path).expanduser().is_file())

    def signer(self) -> KalshiSigner | None:
        """KalshiSigner if credentials are configured, else None (public endpoints only)."""
        if not self.has_credentials:
            return None
        return KalshiSigner(self.key_id, Path(self.private_key_path).expanduser())

    def limiter(self) -> KalshiRateLimiter:
        rl = self.rate_limits or {}
        kw: dict[str, Any] = {}
        if "read" in rl:
            kw["read"] = BucketLimit.from_json(rl["read"])
        if "write" in rl:
            kw["write"] = BucketLimit.from_json(rl["write"])
        return KalshiRateLimiter(**kw)

    def fee_engine(self) -> FeeEngine:
        path = self.fees.get("config") or "config/fees.yaml"
        p = Path(path)
        if not p.is_absolute():
            p = REPO_ROOT / p
        rates = FeeRates.from_yaml(p)
        bp = self.fees.get("balance_precision_dollars")
        if bp:
            from dataclasses import replace

            from dh.core.units import micros_from_dollars

            rates = replace(rates, balance_precision_micros=micros_from_dollars(str(bp)))
        return FeeEngine(rates, apply_fee_waiver=bool(self.fees.get("apply_fee_waiver", False)))

    def rest_kwargs(self) -> dict[str, Any]:
        r = self.rest or {}
        return {
            "timeout_s": float(r.get("timeout_s", 10)),
            "write_timeout_s": float(r.get("write_timeout_s", 5)),
            "max_get_retries": int(r.get("max_get_retries", 4)),
            "backoff_base_s": float(r.get("backoff_base_s", 0.25)),
            "backoff_max_s": float(r.get("backoff_max_s", 8)),
            "cf_history_path": str(r.get("cf_history_path", "/cfbenchmarks/history/values")),
            "trust_env": bool(r.get("use_env_proxy", True)),
        }


def load_config(path: str | Path | None = None, env: str | None = None) -> KalshiConfig:
    """Load the YAML config. env overrides the file's `env` ('prod' | 'demo')."""
    p = Path(path) if path else (DEFAULT_CONFIG if DEFAULT_CONFIG.is_file() else EXAMPLE_CONFIG)
    raw = yaml.safe_load(p.read_text(encoding="utf-8")) or {}
    env = env or str(raw.get("env", "prod"))
    ep = (raw.get("endpoints") or {}).get(env)
    if not ep:
        raise ValueError(f"no endpoints for env {env!r} in {p}")
    auth = raw.get("auth") or {}
    key_id = os.environ.get(str(auth.get("key_id_env", "KALSHI_KEY_ID")), "") or str(auth.get("key_id") or "")
    key_path = os.environ.get(str(auth.get("private_key_path_env", "KALSHI_PRIVATE_KEY_PATH")), "") or str(
        auth.get("private_key_path") or ""
    )
    return KalshiConfig(
        env=env,
        rest_url=str(ep["rest"]),
        ws_url=str(ep["ws"]),
        series=list(raw.get("series") or ["KXBTCD", "KXBTC", "KXBTC15M"]),
        index_ids=list(raw.get("index_ids") or ["BRTI"]),
        key_id=key_id,
        private_key_path=key_path,
        rest=dict(raw.get("rest") or {}),
        ws=dict(raw.get("ws") or {}),
        rate_limits=dict(raw.get("rate_limits") or {}),
        fees=dict(raw.get("fees") or {}),
        history=dict(raw.get("history") or {}),
        source=str(p),
    )
