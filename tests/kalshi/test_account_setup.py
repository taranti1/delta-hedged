"""scripts/account_setup.py against a fake REST client (offline; nothing reaches Kalshi).

Covers: dry runs send nothing; --execute without the typed confirmation sends nothing; the
client_transfer_id is persisted before sending and reused on a retry; a running System 2 blocks
transfers out of subaccount 0; every request body matches its openapi 3.31.0 schema; the System 2
declare-transfer command; cross-shard polling and partial failure; restricted-key creation.
"""

from __future__ import annotations

import importlib.util
import json
import re
import stat
import sys
import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
import yaml

from dh.kalshi.rest import KalshiHTTPError, UnknownOutcome

ROOT = Path(__file__).resolve().parents[2]
SPEC = yaml.safe_load((ROOT / "docs" / "kalshi_specs" / "openapi.yaml").read_text(encoding="utf-8"))
ADMIN_KEY = "admin-key-000001"


def _load_script():
    spec = importlib.util.spec_from_file_location("account_setup", ROOT / "scripts" / "account_setup.py")
    mod = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    sys.modules[spec.name] = mod  # dataclasses resolve string annotations through sys.modules
    spec.loader.exec_module(mod)
    return mod


A = _load_script()

APPLY = object()  # write result: apply the effect and answer like Kalshi
APPLY_THEN_LOSE = object()  # apply the effect, then the response is lost (timeout)
APPLY_LATER = object()  # applied at Kalshi, response lost, the balances show it only after FakeRest.settle()


class FakeRest:
    """Spec-shaped answers for the endpoints the tool uses; writes change the fake account."""

    base_url = "https://fake.invalid/trade-api/v2"

    def __init__(self, *, tier: str = "advanced", balances: dict[tuple[int, int], str] | None = None,
                 netting: list[dict[str, Any]] | None = None, api_keys: list[dict[str, Any]] | None = None,
                 intra_statuses: list[str] | None = None, partial: bool = False, pem: str = "") -> None:
        self.tier = tier
        self.balances = {k: Decimal(v) for k, v in (balances or {(0, 0): "400", (0, 2): "30", (1, 2): "0"}).items()}
        self.netting = netting if netting is not None else [{"subaccount_number": 0, "enabled": True, "exchange_index": 0}]
        self.api_keys = api_keys if api_keys is not None else [{"api_key_id": ADMIN_KEY, "name": "admin", "scopes": ["read", "write"]}]
        self.intra_statuses = list(intra_statuses or ["pending", "complete"])
        self.partial = partial
        self.pem = pem
        self.results: list[Any] = []
        self.gets: list[tuple[str, list]] = []
        self.writes: list[tuple[str, str, Any, list]] = []
        self.on_write = None
        self.next_sub = 1 + max([s for s, _ in self.balances] + [0])
        self.sub_transfers: list[dict[str, Any]] = []  # GET /portfolio/subaccounts/transfers rows (applied ones)
        self.now_s: Any = lambda: 0  # the harness sets its clock (created_ts of listed transfers)
        self.later: list[tuple[str, str, Any]] = []  # APPLY_LATER effects, applied by settle()
        self.fail_gets: dict[str, int] = {}  # path -> number of GETs that raise (network down)
        self.intra_amount = Decimal("150")  # the last intra-account transfer's amount in dollars

    def settle(self) -> None:
        """Apply the transfers answered with APPLY_LATER (Kalshi applied them, the balances show it only now)."""
        for method, path, body in self.later:
            self._apply(method, path, body)
        self.later.clear()

    async def get(self, path: str, params=(), *, stream: str = "") -> dict[str, Any]:
        self.gets.append((path, list(params)))
        if self.fail_gets.get(path, 0) > 0:
            self.fail_gets[path] -= 1
            from dh.kalshi.rest import TransportError

            raise TransportError("network down")
        if path == "/portfolio/subaccounts/transfers":
            return {"transfers": list(reversed(self.sub_transfers))}
        if path == "/account/limits":
            return {"usage_tier": self.tier, "read": {"refill_rate": 200, "bucket_capacity": 600},
                    "write": {"refill_rate": 100, "bucket_capacity": 100}, "grants": []}
        if path == "/portfolio/subaccounts/balances":
            return {"subaccount_balances": [{"subaccount_number": s, "exchange_index": x, "balance": f"{b:.4f}", "updated_ts": 1}
                                            for (s, x), b in sorted(self.balances.items())]}
        if path == "/portfolio/balance":
            tot = sum((b for (s, _), b in self.balances.items() if s == 0), Decimal(0))
            return {"balance": int(tot * 100), "balance_dollars": f"{tot:.4f}", "portfolio_value": 0, "updated_ts": 1}
        if path == "/portfolio/subaccounts/netting":
            return {"netting_configs": list(self.netting)}
        if path.startswith("/series/"):
            return {"series": {"ticker": path.rsplit("/", 1)[1], "exchange_index": 2}}
        if path == "/api_keys":
            return {"api_keys": list(self.api_keys)}
        if path in ("/portfolio/subaccounts/transfers", "/portfolio/intra_exchange_instance_transfers"):
            return {"transfers": []}
        if path.startswith("/portfolio/intra_exchange_instance_transfers/"):
            status = self.intra_statuses.pop(0) if len(self.intra_statuses) > 1 else self.intra_statuses[0]
            return {"transfer": {"transfer_id": path.rsplit("/", 1)[1], "source": "event_contract",
                                 "destination": "event_contract", "source_exchange_shard": 0,
                                 "destination_exchange_shard": 2, "amount": f"{self.intra_amount:.4f}", "status": status,
                                 "created_ts": 1}}
        raise AssertionError(f"unexpected GET {path}")

    async def write(self, method: str, path: str, *, json_body: Any = None, params=(), stream: str = "", n_items: int = 1):
        self.writes.append((method, path, json_body, list(params)))
        if self.on_write is not None:
            self.on_write(method, path, json_body)
        r = self.results.pop(0) if self.results else APPLY
        if isinstance(r, Exception):
            raise r
        if r is APPLY_LATER:  # applied at Kalshi, the response is lost AND the balances lag behind
            self.later.append((method, path, json_body))
            return UnknownOutcome(method, path, json_body, "ResponseLostError: timeout")
        if r is APPLY or r is APPLY_THEN_LOSE:
            res = self._apply(method, path, json_body)
            return UnknownOutcome(method, path, json_body, "ResponseLostError: timeout") if r is APPLY_THEN_LOSE else res
        return r

    def _move(self, a: tuple[int, int], b: tuple[int, int], amt: Decimal) -> None:
        self.balances[a] = self.balances.get(a, Decimal(0)) - amt
        self.balances[b] = self.balances.get(b, Decimal(0)) + amt

    def _apply(self, method: str, path: str, body: Any) -> dict[str, Any]:
        if path == "/portfolio/subaccounts/transfer":
            x = body.get("exchange_index", 0)
            self._move((body["from_subaccount"], x), (body["to_subaccount"], x), Decimal(body["amount_cents"]) / 100)
            self.sub_transfers.append({"transfer_id": f"st-{len(self.sub_transfers) + 1}", "from_subaccount": body["from_subaccount"],
                                       "to_subaccount": body["to_subaccount"], "amount_cents": body["amount_cents"],
                                       "exchange_index": x, "created_ts": int(self.now_s())})
            return {}
        if path == "/portfolio/intra_exchange_instance_transfer":
            amt = Decimal(body["amount"]) / 10_000
            self.intra_amount = amt  # listed (FixedPointDollars) by GET .../intra_exchange_instance_transfers/{id}
            dst = (0, body["destination_exchange_shard"]) if self.partial else (body["destination_subaccount"], body["destination_exchange_shard"])
            self._move((body["source_subaccount"], body["source_exchange_shard"]), dst, amt)
            return {"transfer_id": "tr-0001"}
        if path == "/portfolio/subaccounts":
            n = self.next_sub
            self.next_sub += 1
            self.balances[(n, body.get("exchange_index", 0))] = Decimal(0)
            return {"subaccount_number": n}
        if path == "/account/api_usage_level/upgrade":
            self.tier = "advanced"
            return {}
        if path == "/portfolio/subaccounts/netting":
            self.netting = [r for r in self.netting if r["subaccount_number"] != body["subaccount_number"]]
            self.netting.append({"subaccount_number": body["subaccount_number"], "enabled": body["enabled"], "exchange_index": 2})
            return {}
        if path == "/api_keys/generate":
            kid = "new-key-" + uuid.uuid4().hex
            self.api_keys.append({"api_key_id": kid, "name": body["name"], "scopes": body["scopes"], "subaccount": body["subaccount"]})
            return {"api_key_id": kid, "key_type": "rsa", "private_key": self.pem}
        raise AssertionError(f"unexpected write {method} {path}")


class Harness:
    def __init__(self, tmp_path: Path, rest: FakeRest, *, ps_lines: list[str] | None = None) -> None:
        self.tmp = tmp_path
        self.rest = rest
        self.lines: list[str] = []
        self.prompts: list[str] = []
        self.answers: list[str] = []
        self.ps_lines = ps_lines or []
        self.t = datetime(2026, 9, 25, 20, 0, 0, tzinfo=UTC)
        self.log = tmp_path / "logs" / "account_setup.jsonl"
        self.state = tmp_path / "logs" / "account_setup_state.json"
        self.system2 = tmp_path / "system2"
        rest.now_s = lambda: int(self.t.timestamp())

    def now(self) -> datetime:
        self.t += timedelta(seconds=1)
        return self.t

    def ask(self, prompt: str) -> str:
        self.prompts.append(prompt)
        return self.answers.pop(0) if self.answers else ""

    async def sleep(self, s: float) -> None:
        return None

    async def __call__(self, *argv: str, answer: str | None = None) -> int:
        self.answers = [answer] if answer is not None else []  # one answer per invocation
        return await A.run(["--system2-root", str(self.system2), *argv], rest=self.rest, ask=self.ask,
                           ps=lambda: list(self.ps_lines), now=self.now, sleep=self.sleep, out=self.lines.append,
                           log_path=self.log, state_path=self.state, key_id=ADMIN_KEY)

    @property
    def text(self) -> str:
        return "\n".join(self.lines)

    def log_rows(self) -> list[dict[str, Any]]:
        return [json.loads(x) for x in self.log.read_text().splitlines()] if self.log.exists() else []

    def state_data(self) -> dict[str, Any]:
        return json.loads(self.state.read_text()) if self.state.exists() else {"pending": {}, "completed": []}


@pytest.fixture
def rsa_pem_text(rsa_pem: bytes) -> str:
    return rsa_pem.decode()


def _key_args(tmp: Path, name: str = "dh-sub1-runner") -> list[str]:
    return ["create-key", "--subaccount", "1", "--name", name, "--pem", str(tmp / "keys" / f"{name}.pem"),
            "--env-file", str(tmp / "keys" / "dh-sub1.env"), "--id-var", "KALSHI_KEY_ID", "--path-var", "KALSHI_PRIVATE_KEY_PATH"]


WRITE_CASES = {
    "upgrade-tier": (["upgrade-tier", "--to", "advanced"], "advanced", {"tier": "basic"}),
    "create-subaccount": (["create-subaccount", "--exchange-index", "2"], "CREATE 2", {"balances": {(0, 0): "400"}}),
    "transfer": (["transfer", "--from", "0", "--to", "1", "--amount-dollars", "150", "--exchange-index", "0"], "150", {}),
    "shard-transfer": (["shard-transfer", "--from-subaccount", "0", "--from-shard", "0", "--to-subaccount", "1",
                        "--to-shard", "2", "--probe-dollars", "1"], "1.00", {}),  # the mandatory first (probe) step
    "set-netting": (["set-netting", "--subaccount", "1", "--off"], "netting off 1", {}),
}


# ============================================================================ dry runs
@pytest.mark.parametrize("name", sorted(WRITE_CASES) + ["create-key"])
async def test_dry_run_sends_nothing(tmp_path: Path, name: str, rsa_pem_text: str):
    if name == "create-key":
        argv, kw = _key_args(tmp_path), {}
    else:
        argv, _, kw = WRITE_CASES[name]
    h = Harness(tmp_path, FakeRest(pem=rsa_pem_text, **kw))
    assert await h(*argv) == 0
    assert h.rest.writes == []  # nothing sent
    assert h.prompts == []  # a dry run never asks
    assert "DRY RUN: nothing was sent" in h.text and "Request (prod): " in h.text
    assert h.state_data()["pending"] == {}  # no idempotency key persisted for a dry run
    assert [r["event"] for r in h.log_rows()] == ["dry_run"]
    assert not (tmp_path / "keys").exists()  # create-key: no key file, no env file


def test_dry_run_and_status_build_a_read_only_client():
    assert not A.sends_writes(A.parse_args([]))  # default = status
    assert not A.sends_writes(A.parse_args(["transfer", "--from", "0", "--to", "1", "--amount-dollars", "1", "--exchange-index", "2"]))
    assert A.sends_writes(A.parse_args(["transfer", "--from", "0", "--to", "1", "--amount-dollars", "1", "--exchange-index", "2", "--execute"]))
    assert not A.sends_writes(A.parse_args(["shard-transfer", "--resume", "tr-1", "--execute"]))
    assert not A.sends_writes(A.parse_args(["forget-pending", "--id", "x"]))


# ============================================================================ typed confirmation
@pytest.mark.parametrize("name", sorted(WRITE_CASES) + ["create-key"])
@pytest.mark.parametrize("typed", ["", "yes", "y"])
async def test_execute_without_typed_confirmation_sends_nothing(tmp_path: Path, name: str, typed: str, rsa_pem_text: str):
    if name == "create-key":
        argv, kw = _key_args(tmp_path), {}
    else:
        argv, _, kw = WRITE_CASES[name]
    h = Harness(tmp_path, FakeRest(pem=rsa_pem_text, **kw))
    assert await h(*argv, "--execute", answer=typed) == 3
    assert h.rest.writes == [] and len(h.prompts) == 1
    assert "Confirmation did not match: nothing was sent." in h.text
    assert h.state_data()["pending"] == {}
    assert not (tmp_path / "keys" / "dh-sub1-runner.pem").exists()


async def test_transfer_confirmation_is_the_amount(tmp_path: Path):
    h = Harness(tmp_path, FakeRest())
    assert await h("transfer", "--from", "0", "--to", "1", "--amount-dollars", "150", "--exchange-index", "0",
                   "--execute", answer="15") == 3
    assert h.rest.writes == []
    assert await h("transfer", "--from", "0", "--to", "1", "--amount-dollars", "150", "--exchange-index", "0",
                   "--execute", answer="$150.00") == 0
    assert len(h.rest.writes) == 1


# ============================================================================ idempotency
TRANSFER = ("transfer", "--from", "0", "--to", "1", "--amount-dollars", "150", "--exchange-index", "0", "--execute")


async def test_client_transfer_id_persisted_before_send_and_reused_on_retry(tmp_path: Path):
    rest = FakeRest()
    h = Harness(tmp_path, rest)
    seen: list[str] = []

    def on_write(method, path, body):  # the id must already be on disk when the request leaves
        seen.append(body["client_transfer_id"])
        assert body["client_transfer_id"] in json.loads(h.state.read_text())["pending"]

    rest.on_write = on_write
    # the first attempt got a 5xx before Kalshi processed it: unknown outcome, nothing moved
    rest.results = [UnknownOutcome("POST", "/portfolio/subaccounts/transfer", None, "HTTP 502", 502)]
    assert await h(*TRANSFER, answer="150") == 1
    cid = seen[0]
    uuid.UUID(cid)
    pend = h.state_data()["pending"]
    assert list(pend) == [cid] and pend[cid]["attempts"] == 1
    assert pend[cid]["first_rows"] and pend[cid]["first_pre"]  # the bracket of the FIRST attempt is saved
    assert "STAYS saved" in h.text and "declare-transfer" not in h.text

    # a different transfer is refused while this one is unresolved
    assert await h("transfer", "--from", "0", "--to", "1", "--amount-dollars", "10", "--exchange-index", "0", "--execute",
                   answer="10") == 2
    assert len(rest.writes) == 1

    # the retry sends the SAME id and is applied now (200)
    h.lines.clear()
    assert await h(*TRANSFER, answer="150") == 0
    assert seen == [cid, cid]
    assert rest.balances[(1, 0)] == Decimal("150")  # moved once
    data = h.state_data()
    assert data["pending"] == {} and data["completed"][0]["id"] == cid
    assert data["completed"][0]["result"] == "applied_after_retry:ok"
    assert data["completed"][0]["declaration"]["after"] == pend[cid]["first_pre"]["at"]  # bracket from the FIRST attempt
    assert "RETRY of the unresolved transfer" in h.text and "declare-transfer" in h.text


async def test_rejected_transfer_is_not_left_pending(tmp_path: Path):
    rest = FakeRest()
    rest.results = [KalshiHTTPError("POST", "/portfolio/subaccounts/transfer", 400, {"error": {"code": "bad_request", "message": "x"}})]
    h = Harness(tmp_path, rest)
    assert await h(*TRANSFER, answer="150") == 1
    assert h.state_data()["pending"] == {}
    assert rest.balances.get((1, 0), Decimal(0)) == Decimal(0)


# ============================================================================ System 2 guard
SYSTEM2_PS = ["  101 /usr/bin/login", "20389 .venv/bin/python -m kalshi_m1.experiments.two_leg_launcher watch --auto-launch"]


@pytest.mark.parametrize("argv", [
    TRANSFER,
    ("shard-transfer", "--from-subaccount", "0", "--from-shard", "0", "--to-subaccount", "1", "--to-shard", "2",
     "--amount-dollars", "150", "--execute"),
])
async def test_running_system2_blocks_transfers_out_of_subaccount_0(tmp_path: Path, argv):
    h = Harness(tmp_path, FakeRest(), ps_lines=SYSTEM2_PS)
    assert await h(*argv, answer="150") == 2
    assert h.rest.writes == [] and h.prompts == []
    assert "REFUSED" in h.text and "two_leg_launcher" in h.text
    assert h.log_rows()[-1]["event"] == "refused"


async def test_i_stopped_system2_overrides_the_process_guard(tmp_path: Path):
    h = Harness(tmp_path, FakeRest(), ps_lines=["20352 .venv/bin/python scripts/short_duration_screener.py --profile live"])
    assert await h(*TRANSFER, "--i-stopped-system2", answer="150") == 0
    assert len(h.rest.writes) == 1


async def test_system2_starting_while_the_operator_types_aborts_the_send(tmp_path: Path):
    calls = {"n": 0}

    def ps() -> list[str]:  # nothing at the first check, a launched session at the last look
        calls["n"] += 1
        return [] if calls["n"] == 1 else SYSTEM2_PS

    h = Harness(tmp_path, FakeRest())
    assert await A.run(list(TRANSFER), rest=h.rest, ask=lambda _: "150", ps=ps, now=h.now, out=h.lines.append,
                       log_path=h.log, state_path=h.state, key_id=ADMIN_KEY) == 2
    assert h.rest.writes == [] and h.state_data()["pending"] == {}
    assert "REFUSED just before sending" in h.text and h.log_rows()[-1]["event"] == "aborted"


async def test_kalshi_read_error_is_reported_not_raised(tmp_path: Path):
    rest = FakeRest()

    async def broken(path, params=(), *, stream=""):
        raise KalshiHTTPError("GET", path, 503, {"error": {"code": "unavailable", "message": "down"}})

    rest.get = broken
    h = Harness(tmp_path, rest)
    assert await h("set-netting", "--subaccount", "1", "--off", "--execute", answer="netting off 1") == 1
    assert rest.writes == [] and "Kalshi request failed" in h.text


def test_system2_process_match_ignores_own_pid_and_other_processes():
    lines = ["  7 python scripts/account_setup.py transfer", " 8 /bin/zsh", "9 python -m kalshi_m1.experiments dry-run", "junk"]
    assert A.system2_processes(lines, own_pid=7) == ["pid 9: python -m kalshi_m1.experiments dry-run"]


async def test_failing_ps_fails_closed(tmp_path: Path):
    h = Harness(tmp_path, FakeRest())

    def boom() -> list[str]:
        raise OSError("no ps")

    assert await A.run(["transfer", "--from", "0", "--to", "1", "--amount-dollars", "150", "--exchange-index", "0", "--execute"],
                       rest=h.rest, ask=h.ask, ps=boom, now=h.now, out=h.lines.append, log_path=h.log,
                       state_path=h.state, key_id=ADMIN_KEY) == 2
    assert h.rest.writes == []


# ============================================================================ declaration for System 2
async def test_transfer_prints_exact_declare_transfer_command(tmp_path: Path):
    h = Harness(tmp_path, FakeRest())
    assert await h(*TRANSFER, answer="150") == 0
    decl = h.state_data()["completed"][0]["declaration"]
    assert decl["required"] and decl["amount"] == "-150.00" and decl["observed_delta"] == "-150.0000"
    after, before = datetime.fromisoformat(decl["after"]), datetime.fromisoformat(decl["before"])
    assert after < before and after.tzinfo is not None
    cmd = [ln.strip() for ln in h.lines if "declare-transfer" in ln and ln.strip().startswith("cd ")][0]
    assert cmd == (f"cd {h.system2} && .venv/bin/python -m kalshi_m1.experiments.two_leg_launcher declare-transfer "
                   f"--amount=-150.00 --after {decl['after']} --before {decl['before']} --note '{decl['note']}'")
    # status: not declared yet -> declared once System 2's ledger holds the row
    h.lines.clear()
    await h("status")
    assert "NOT declared to System 2" in h.text
    ledger = h.system2 / "data" / "two_leg_live" / "cash_transfers.json"
    ledger.parent.mkdir(parents=True)
    ledger.write_text(json.dumps({"schema": "cash_transfers/1", "transfers": [
        {"id": "xfer_1", "amount": "-150.00", "after": decl["after"], "before": decl["before"], "note": "n"}]}))
    h.lines.clear()
    await h("status")
    assert "declared to System 2" in h.text and "NOT declared" not in h.text


async def test_transfer_between_numbered_subaccounts_needs_no_declaration(tmp_path: Path):
    rest = FakeRest(balances={(0, 0): "400", (1, 2): "200", (2, 2): "0"})
    h = Harness(tmp_path, rest, ps_lines=SYSTEM2_PS)  # System 2 is irrelevant: subaccount 0 untouched
    assert await h("transfer", "--from", "1", "--to", "2", "--amount-dollars", "50", "--exchange-index", "2", "--execute",
                   answer="50") == 0
    assert "declaration" not in h.state_data()["completed"][0]
    assert ("/portfolio/balance", []) not in rest.gets


@pytest.mark.parametrize("argv", [
    ("transfer", "--from", "1", "--to", "0", "--amount-dollars", "5", "--exchange-index", "2"),
    ("shard-transfer", "--from-subaccount", "1", "--from-shard", "2", "--to-subaccount", "0", "--to-shard", "0", "--amount-dollars", "5"),
    ("set-netting", "--subaccount", "0", "--off"),
    ("create-key", "--subaccount", "0", "--name", "x", "--pem", "/tmp/x.pem", "--env-file", "/tmp/x.env",
     "--id-var", "A", "--path-var", "B"),
])
async def test_subaccount_0_is_never_a_destination(tmp_path: Path, argv):
    h = Harness(tmp_path, FakeRest())
    assert await h(*argv, "--execute", answer="5") == 2
    assert h.rest.writes == [] and "error:" in h.text


# ============================================================================ shard transfer
SHARD = ("shard-transfer", "--from-subaccount", "0", "--from-shard", "0", "--to-subaccount", "1", "--to-shard", "2",
         "--amount-dollars", "150", "--execute")


def seed_probe(h: Harness, route: str = "0:0->1:2") -> None:
    """A completed, exact probe transfer on ``route`` (the precondition of a full shard-transfer)."""
    h.state.parent.mkdir(parents=True, exist_ok=True)
    data = h.state_data() if h.state.exists() else {"version": 1, "pending": {}, "completed": []}
    data["completed"].append({"kind": "shard_transfer", "id": "shard-probe", "probe": True, "route": route,
                              "result": "complete", "params": {"amount_dollars": "1.00"}})
    h.state.write_text(json.dumps(data))


async def test_shard_transfer_polls_until_complete(tmp_path: Path):
    rest = FakeRest(intra_statuses=["pending", "pending", "complete"])
    h = Harness(tmp_path, rest)
    seed_probe(h)
    assert await h(*SHARD, answer="150") == 0
    polls = [p for p, _ in rest.gets if p == "/portfolio/intra_exchange_instance_transfers/tr-0001"]
    assert len(polls) == 3
    assert rest.balances[(1, 2)] == Decimal("150")
    assert "Complete: subaccount 1 on shard 2 received $150.00" in h.text
    assert h.state_data()["completed"][-1]["declaration"]["amount"] == "-150.00"


async def test_shard_transfer_partial_failure_is_reported(tmp_path: Path):
    rest = FakeRest(partial=True)  # money stops in the primary account on shard 2
    h = Harness(tmp_path, rest)
    seed_probe(h)
    assert await h(*SHARD, answer="150") == 1
    assert "PARTIAL" in h.text and "non-atomic" in h.text
    assert "transfer --from 0 --to 1 --amount-dollars 150.00 --exchange-index 2 --execute" in h.text
    decl = h.state_data()["completed"][-1]["declaration"]
    assert decl["required"] is False  # the primary aggregate did not change: nothing to declare yet


async def test_shard_transfer_timeout_stays_pending_and_resumes(tmp_path: Path):
    rest = FakeRest(intra_statuses=["pending"])
    h = Harness(tmp_path, rest)
    seed_probe(h)
    assert await h(*SHARD, "--timeout-s", "4", "--poll-s", "2", answer="150") == 1
    pend = h.state_data()["pending"]
    assert len(pend) == 1 and next(iter(pend.values()))["transfer_id"] == "tr-0001"
    # no second transfer while one is unresolved (no idempotency key in this API)
    assert await h(*SHARD, answer="150") == 2
    assert len(rest.writes) == 1
    rest.intra_statuses = ["complete"]
    assert await h("shard-transfer", "--resume", "tr-0001") == 0
    assert h.state_data()["pending"] == {} and len(rest.writes) == 1


async def test_shard_transfer_unknown_outcome_blocks_until_forgotten(tmp_path: Path):
    rest = FakeRest()
    rest.results = [UnknownOutcome("POST", "/portfolio/intra_exchange_instance_transfer", None, "ResponseLostError: timeout")]
    h = Harness(tmp_path, rest)
    seed_probe(h)
    assert await h(*SHARD, answer="150") == 1
    (local,) = h.state_data()["pending"]
    assert await h(*TRANSFER, answer="150") == 2  # every transfer refused meanwhile
    assert await h("forget-pending", "--id", local, answer="nope") == 3
    assert await h("forget-pending", "--id", local, answer=local) == 0
    assert h.state_data()["pending"] == {}
    assert await h(*TRANSFER, answer="150") == 0


# ============================================================================ other writes
async def test_create_subaccount_refuses_on_basic_and_when_one_exists(tmp_path: Path):
    h = Harness(tmp_path, FakeRest(tier="basic", balances={(0, 0): "400"}))
    assert await h("create-subaccount", "--exchange-index", "2", "--execute", answer="CREATE 2") == 2
    h2 = Harness(tmp_path / "b", FakeRest())  # subaccount 1 exists already
    assert await h2("create-subaccount", "--exchange-index", "2", "--execute", answer="CREATE 2") == 2
    assert h.rest.writes == [] and h2.rest.writes == []
    assert await h2("create-subaccount", "--exchange-index", "2", "--another", "--execute", answer="CREATE 2") == 0


async def test_upgrade_tier_skips_when_already_advanced(tmp_path: Path):
    h = Harness(tmp_path, FakeRest(tier="advanced"))
    assert await h("upgrade-tier", "--to", "advanced", "--execute", answer="advanced") == 0
    assert h.rest.writes == [] and h.prompts == []


async def test_restricted_admin_key_is_refused(tmp_path: Path):
    rest = FakeRest(api_keys=[{"api_key_id": ADMIN_KEY, "name": "runner", "scopes": ["read", "write"], "subaccount": 1}])
    h = Harness(tmp_path, rest)
    assert await h("set-netting", "--subaccount", "1", "--off", "--execute", answer="netting off 1") == 2
    assert rest.writes == [] and "UNRESTRICTED" in h.text


async def test_create_key_writes_pem_and_env_file_privately(tmp_path: Path, rsa_pem_text: str):
    rest = FakeRest(pem=rsa_pem_text)
    h = Harness(tmp_path, rest)
    assert await h(*_key_args(tmp_path), "--execute", answer="dh-sub1-runner") == 0
    pem = tmp_path / "keys" / "dh-sub1-runner.pem"
    env = tmp_path / "keys" / "dh-sub1.env"
    assert pem.read_text() == rsa_pem_text
    assert stat.S_IMODE(pem.stat().st_mode) == 0o600 and stat.S_IMODE(env.stat().st_mode) == 0o600
    kid = rest.api_keys[-1]["api_key_id"]
    assert env.read_text() == f"KALSHI_KEY_ID={kid}\nKALSHI_PRIVATE_KEY_PATH={pem}\n"
    # the watchdog key goes into the same env file under other names
    assert await h(*_key_args(tmp_path, "dh-sub1-watchdog")[:-4], "--id-var", "KALSHI_WATCHDOG_KEY_ID",
                   "--path-var", "KALSHI_WATCHDOG_PRIVATE_KEY_PATH", "--execute", answer="dh-sub1-watchdog") == 0
    assert env.read_text().count("\n") == 4
    # the same variable twice is refused
    assert await h(*_key_args(tmp_path, "dh-sub1-again"), "--execute", answer="dh-sub1-again") == 2
    everything = h.text + h.log.read_text()
    assert "PRIVATE KEY" not in everything and kid not in everything  # never printed or logged
    assert rest.writes[0][2] == {"name": "dh-sub1-runner", "key_type": "rsa", "scopes": ["read", "write"], "subaccount": 1}


async def test_create_key_refuses_paths_inside_the_repo(tmp_path: Path):
    h = Harness(tmp_path, FakeRest())
    argv = ["create-key", "--subaccount", "1", "--name", "k", "--pem", str(ROOT / "secrets" / "k.pem"),
            "--env-file", str(tmp_path / "k.env"), "--id-var", "A", "--path-var", "B", "--execute"]
    assert await h(*argv, answer="k") == 2 and h.rest.writes == []


# ============================================================================ openapi schema
def _resolve(schema: dict[str, Any]) -> dict[str, Any]:
    while "$ref" in schema:
        name = schema["$ref"].rsplit("/", 1)[1]
        schema = SPEC["components"]["schemas"][name]
    return schema


def _validate(schema: dict[str, Any], value: Any, where: str) -> None:
    schema = _resolve(schema)
    for sub in schema.get("allOf") or []:
        _validate(sub, value, where)
    t = schema.get("type")
    if t == "object" or "properties" in schema:
        assert isinstance(value, dict), where
        for r in schema.get("required") or []:
            assert r in value, f"{where}: missing required {r}"
        props = schema.get("properties") or {}
        for k, v in value.items():
            assert k in props, f"{where}: {k} is not in the schema"
            _validate(props[k], v, f"{where}.{k}")
    elif t == "integer":
        assert isinstance(value, int) and not isinstance(value, bool), where
        assert value >= schema.get("minimum", value) and value <= schema.get("maximum", value), where
    elif t == "string":
        assert isinstance(value, str), where
        if schema.get("format") == "uuid":
            uuid.UUID(value)
    elif t == "boolean":
        assert isinstance(value, bool), where
    elif t == "array":
        assert isinstance(value, list), where
        for i, x in enumerate(value):
            _validate(schema["items"], x, f"{where}[{i}]")
    if "enum" in schema:
        assert value in schema["enum"], f"{where}: {value!r} not in {schema['enum']}"


def _template(path: str) -> str:
    if path in SPEC["paths"]:
        return path
    for tpl in SPEC["paths"]:
        if re.fullmatch(re.sub(r"\\\{[^}]+\\\}", "[^/]+", re.escape(tpl)), path):
            return tpl
    raise AssertionError(f"{path} is not in openapi.yaml")


async def test_request_bodies_and_paths_match_openapi(tmp_path: Path, rsa_pem_text: str):
    runs = []
    for name, (argv, answer, kw) in WRITE_CASES.items():
        h = Harness(tmp_path / name, FakeRest(**kw))
        assert await h(*argv, "--execute", answer=answer) == 0, (name, h.text)
        runs.append(h)
    h = Harness(tmp_path / "key", FakeRest(pem=rsa_pem_text))
    assert await h(*_key_args(tmp_path / "key"), "--execute", answer="dh-sub1-runner") == 0
    runs.append(h)
    await runs[0]("status")
    sent = {}
    for h in runs:
        for method, path, body, params in h.rest.writes:
            op = SPEC["paths"][_template(path)][method.lower()]
            rb = op.get("requestBody")
            if rb is None:
                assert body is None, (method, path)
            else:
                _validate(rb["content"]["application/json"]["schema"], body, f"{method} {path}")
            assert params == []
            sent[(method, path)] = body
        for path, _ in h.rest.gets:
            assert "get" in SPEC["paths"][_template(path)], path
    assert set(sent) == {("POST", "/account/api_usage_level/upgrade"), ("POST", "/portfolio/subaccounts"),
                         ("POST", "/portfolio/subaccounts/transfer"), ("POST", "/portfolio/intra_exchange_instance_transfer"),
                         ("PUT", "/portfolio/subaccounts/netting"), ("POST", "/api_keys/generate")}
    # units: cents for subaccount transfers, centicents for intra-account transfers
    assert sent[("POST", "/portfolio/subaccounts/transfer")]["amount_cents"] == 15_000
    assert sent[("POST", "/portfolio/intra_exchange_instance_transfer")]["amount"] == 10_000  # the $1 probe
    assert sent[("POST", "/portfolio/subaccounts")] == {"exchange_index": 2}
    assert sent[("PUT", "/portfolio/subaccounts/netting")] == {"subaccount_number": 1, "enabled": False}


def test_amount_parsing():
    assert A.parse_dollars("150") == Decimal("150.00") and A.to_cents(Decimal("150")) == 15_000
    assert A.to_centicents(Decimal("0.01")) == 100
    for bad in ("0", "-5", "1.001", "abc", "NaN", "5000"):
        with pytest.raises(A.UsageError):
            A.parse_dollars(bad)


# ============================================================================ status
async def test_status_is_read_only_and_prints_the_checklist(tmp_path: Path):
    rest = FakeRest(tier="basic", balances={(0, 0): "400"}, api_keys=[{"api_key_id": ADMIN_KEY, "name": "s2", "scopes": ["read", "write"]}])
    h = Harness(tmp_path, rest, ps_lines=SYSTEM2_PS)
    assert await h() == 0  # no subcommand = status
    assert rest.writes == []
    t = h.text
    assert "usage_tier basic" in t and "KXBTCD: exchange_index 2" in t and "two_leg_launcher" in t
    assert "[ ] 1. API tier Advanced or above (now basic)" in t
    assert "[ ] 3. subaccount 1 exists on shard 2" in t
    assert "unscoped $400.0000; sum of subaccount-0 rows $400.0000: equal" in t
    assert "Next: python scripts/account_setup.py upgrade-tier --to advanced --execute" in t
    assert not h.log.exists()  # status logs nothing (no writes)


async def test_status_all_done(tmp_path: Path):
    keys = [{"api_key_id": ADMIN_KEY, "name": "s2", "scopes": ["read", "write"]},
            {"api_key_id": "k-runner-1", "name": "runner", "scopes": ["read", "write"], "subaccount": 1},
            {"api_key_id": "k-watchdog-1", "name": "watchdog", "scopes": ["read", "write"], "subaccount": 1}]
    netting = [{"subaccount_number": 0, "enabled": True, "exchange_index": 0},
               {"subaccount_number": 1, "enabled": False, "exchange_index": 2}]
    h = Harness(tmp_path, FakeRest(balances={(0, 0): "250", (1, 2): "150"}, api_keys=keys, netting=netting))
    assert await h("status") == 0
    assert "[ ]" not in h.text and "Next: nothing" in h.text
