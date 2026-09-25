from __future__ import annotations

import importlib.util
from pathlib import Path
from typing import Any

import pytest

from dh.kalshi.config import EXAMPLE_CONFIG, load_config
from dh.kalshi.fees import FeeEngine

from . import samples as S

ROOT = Path(__file__).resolve().parents[2]


def load_script(name: str):
    spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / f"{name}.py")
    mod = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(mod)
    return mod


def test_example_config_loads_with_env_credentials(monkeypatch, tmp_path: Path, rsa_pem):
    monkeypatch.delenv("KALSHI_KEY_ID", raising=False)
    monkeypatch.delenv("KALSHI_PRIVATE_KEY_PATH", raising=False)
    cfg = load_config(EXAMPLE_CONFIG)
    assert cfg.env == "prod" and cfg.rest_url.endswith("/trade-api/v2") and cfg.ws_url.endswith("/trade-api/ws/v2")
    assert cfg.series == ["KXBTCD", "KXBTC", "KXBTC15M"] and cfg.index_ids == ["BRTI"]
    assert cfg.signer() is None  # placeholder path does not exist -> public only
    assert cfg.limiter().write.limit.refill_rate == 50
    assert cfg.fee_engine().rates.balance_precision_micros == 10_000
    assert cfg.rest_kwargs()["write_timeout_s"] == 5.0
    demo = load_config(EXAMPLE_CONFIG, env="demo")
    assert "demo" in demo.rest_url
    key = tmp_path / "k.pem"
    key.write_bytes(rsa_pem)
    monkeypatch.setenv("KALSHI_KEY_ID", "kid")
    monkeypatch.setenv("KALSHI_PRIVATE_KEY_PATH", str(key))
    assert load_config(EXAMPLE_CONFIG).signer().key_id == "kid"
    with pytest.raises(ValueError):
        load_config(EXAMPLE_CONFIG, env="staging")


def test_fee_engine_precision_override(tmp_path: Path):
    p = tmp_path / "k.yaml"
    p.write_text(EXAMPLE_CONFIG.read_text().replace("balance_precision_dollars: null", 'balance_precision_dollars: "0.0001"')
                 .replace("config: config/fees.yaml", f"config: {ROOT / 'config' / 'fees.yaml'}"))
    assert load_config(p).fee_engine().rates.balance_precision_micros == 100


class FakeRest:
    def __init__(self, fills: list[dict[str, Any]]) -> None:
        self.fills = fills

    async def iter_fills(self, **kw):
        for f in self.fills:
            yield f

    async def iter_historical_fills(self, **kw):
        for f in self.fills[:1]:  # overlap: must be de-duplicated
            yield f

    async def get_market(self, t):
        return {"market": S.MARKET_KXBTCD}

    async def get_event(self, e):
        return {"event": dict(S.EVENT_KXBTCD, fee_type_override="quadratic_with_maker_fees")}

    async def get_series(self, s):
        return {"series": S.SERIES_KXBTCD}

    async def get_series_fee_changes(self, s, show_historical=False):
        return {"series_fee_change_arr": []}


def fill(fid: str, fee: str, px: str = "0.5000", count: str = "1.00", taker: bool = False, t: str = "2025-08-05T20:40:00Z") -> dict:
    return dict(S.FILL_ROW, fill_id=fid, trade_id=fid, order_id="o-9", yes_price_dollars=px, no_price_dollars="0.5000",
                count_fp=count, is_taker=taker, fee_cost=fee, created_time=t, book_side="bid", outcome_side="yes")


async def test_verify_fee_schedule_logic(capsys):
    vf = load_script("verify_fee_schedule")
    eng = FeeEngine()
    # maker fee 0.0175 * 0.25 = 0.004375: net 0.01 at $0.01 precision, 0.0044 at $0.0001
    fills = [fill("a", "0.004400"), fill("b", "0.004400", t="2025-08-05T20:41:00Z")]
    bad = await vf.check_fills(FakeRest(fills), eng, days=10_000, prefix="KXBTC", show=5)
    out = capsys.readouterr().out
    assert bad == 0 and "best-matching balance precision: $0.0001" in out and "2 in the last" in out
    bad = await vf.check_fills(FakeRest([fill("c", "0.050000")]), eng, days=10_000, prefix="", show=5)
    assert bad == 1 and "MISMATCH" in capsys.readouterr().out
    assert await vf.check_series(FakeRest([]), eng, ["KXBTCD"]) == 0
