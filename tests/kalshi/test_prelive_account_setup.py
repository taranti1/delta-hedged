"""Regression tests for the pre-live review of scripts/account_setup.py
(docs/research/prelive_review_2026-09-25: H1 variants A1, A2, A4, A5; M2; L5 state/--resume).

Offline, against the fake REST client of test_account_setup (spec-shaped answers; transfers are
listed in GET /portfolio/subaccounts/transfers once applied). Each test reproduces one review
probe and asserts the FIXED behaviour: a saved client_transfer_id is never dropped once a
request may have left, "applied / not applied" comes from the balances since the FIRST attempt
and the transfer list, a declaration is required exactly when money left subaccount 0, and a
shard-transfer must match the amount exactly after a mandatory probe.
"""

from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path

from dh.kalshi.rest import KalshiHTTPError, NotSentError, UnknownOutcome

from .test_account_setup import (
    APPLY,
    APPLY_LATER,
    SYSTEM2_PS,
    A,
    FakeRest,
    Harness,
    seed_probe,
)

TRANSFER = ("transfer", "--from", "0", "--to", "1", "--amount-dollars", "150", "--exchange-index", "2", "--execute")
BAL = {(0, 0): "400", (0, 2): "300", (1, 2): "0"}


def _one_client_id(rest: FakeRest) -> set[str]:
    return {w[2]["client_transfer_id"] for w in rest.writes if w[1] == "/portfolio/subaccounts/transfer"}


# ============================================================================ H1 A1: a 400 on a retry
async def test_retry_answered_400_keeps_the_id_and_resolves_by_evidence(tmp_path: Path):
    """Review A1: the first attempt was applied but the response was lost (and the balances lag);
    the retry's duplicate id is answered 400. The id must NOT be dropped: the evidence (balances
    since the FIRST attempt + the transfer list) shows the transfer applied, so it completes with
    a REQUIRED System 2 declaration; re-running the command never moves the money twice."""
    rest = FakeRest(balances=BAL)
    h = Harness(tmp_path, rest)
    rest.results = [APPLY_LATER]
    assert await h(*TRANSFER, answer="150") == 1
    (cid,) = h.state_data()["pending"]
    rest.settle()  # Kalshi had applied it: the balances show it now
    rest.results = [KalshiHTTPError("POST", "/portfolio/subaccounts/transfer", 400,
                                    {"error": {"code": "bad_request", "message": "duplicate client_transfer_id"}})]
    h.lines.clear()
    rc = await h(*TRANSFER, answer="150")
    # the retry found the evidence BEFORE sending anything (nothing is sent again)
    assert rc == 0 and len(rest.writes) == 1
    data = h.state_data()
    assert data["pending"] == {}
    rec = data["completed"][-1]
    assert rec["id"] == cid and rec["evidence"]["balance"] == "applied" and rec["evidence"]["listed"] is True
    assert rec["declaration"]["required"] is True and rec["declaration"]["amount"] == "-150.00"
    assert "declare-transfer" in h.text
    # re-running "the exact command" does not start over with a new id
    h.lines.clear()
    assert await h(*TRANSFER, answer="150") == 2 and "already completed" in h.text
    assert rest.balances[(1, 2)] == Decimal("150") and rest.balances[(0, 2)] == Decimal("150")
    assert _one_client_id(rest) == {cid}


async def test_400_on_a_retry_that_is_sent_never_drops_the_id(tmp_path: Path):
    """The retry is SENT (the balances still lag when it is decided) and answered 400: the id
    stays saved (no evidence either way yet), then a later run resolves it as applied."""
    rest = FakeRest(balances=BAL)
    h = Harness(tmp_path, rest)
    rest.results = [APPLY_LATER]
    assert await h(*TRANSFER, answer="150") == 1
    (cid,) = h.state_data()["pending"]
    rest.results = [KalshiHTTPError("POST", "/portfolio/subaccounts/transfer", 400,
                                    {"error": {"code": "bad_request", "message": "duplicate client_transfer_id"}})]
    assert await h(*TRANSFER, answer="150") == 1
    assert list(h.state_data()["pending"]) == [cid]  # NOT dropped
    assert "definitely not applied" not in h.text and "declare-transfer" not in h.text
    rest.settle()
    assert await h(*TRANSFER, answer="150") == 0
    assert h.state_data()["completed"][-1]["declaration"]["required"] is True
    assert rest.balances[(1, 2)] == Decimal("150") and _one_client_id(rest) == {cid}


# ============================================================================ H1 A2: 409 but nothing moved
async def test_409_on_retry_without_a_balance_move_declares_nothing(tmp_path: Path):
    """Review A2: the first attempt failed (5xx, not applied); the retry is answered 409. The
    balances since the first attempt did not move and the transfer list does not show it: the
    transfer is NOT recorded as applied and no declaration of money that did not move is asked
    for; the id stays saved (forget-pending after checking status)."""
    rest = FakeRest(balances=BAL)
    h = Harness(tmp_path, rest)
    rest.results = [UnknownOutcome("POST", "/portfolio/subaccounts/transfer", None, "HTTP 500", 500)]
    assert await h(*TRANSFER, answer="150") == 1
    (cid,) = h.state_data()["pending"]
    rest.results = [UnknownOutcome("POST", "/portfolio/subaccounts/transfer", None, "HTTP 409", 409)]
    h.lines.clear()
    rc = await h(*TRANSFER, answer="150")
    assert rest.balances[(1, 2)] == Decimal("0")
    assert rc == 1
    data = h.state_data()
    assert list(data["pending"]) == [cid] and data["completed"] == []
    assert data["pending"][cid]["evidence"]["balance"] == "none"
    assert "declare-transfer" not in h.text and "NOT applied so far" in h.text
    h.lines.clear()
    assert await h("status") == 0 and "(0 missing)" in h.text


# ============================================================================ H1 A4: 200 on a retry
async def test_retry_answered_200_still_requires_the_declaration(tmp_path: Path):
    """Review A4: the first attempt was applied (response lost), the retry is answered 200 as an
    idempotent replay. The declaration must be bracketed from the FIRST attempt's reading (not
    the retry's, which already includes the change) and be REQUIRED; status lists it missing."""
    rest = FakeRest(balances=BAL)
    h = Harness(tmp_path, rest)
    rest.results = [APPLY_LATER]
    assert await h(*TRANSFER, answer="150") == 1
    first = h.state_data()["pending"]
    (cid,) = first
    first_pre = first[cid]["first_pre"]
    rest.results = [{}]  # 200 {} without applying again
    h.lines.clear()
    # the balances still lag: sent, answered 200, but nothing is recorded or declared before
    # a reading WITH the change exists (the declaration's `before`)
    assert await h(*TRANSFER, answer="150") == 1
    assert list(h.state_data()["pending"]) == [cid]
    assert "HTTP 200" in h.text and "declare-transfer" not in h.text
    rest.settle()
    h.lines.clear()
    assert await h(*TRANSFER, answer="150") == 0  # finished from the evidence, nothing sent again
    assert len(rest.writes) == 2
    comp = h.state_data()["completed"][-1]
    assert comp["id"] == cid and comp["declaration"]["required"] is True
    assert comp["declaration"]["after"] == first_pre["at"]  # the bracket opens at the FIRST attempt
    assert comp["declaration"]["observed_delta"] == "-150.0000"
    h.lines.clear()
    assert await h("status") == 0
    assert "(1 missing)" in h.text


async def test_retry_answered_200_after_the_balances_moved_requires_the_declaration(tmp_path: Path):
    rest = FakeRest(balances=BAL)
    h = Harness(tmp_path, rest)
    rest.results = [APPLY_LATER]
    assert await h(*TRANSFER, answer="150") == 1
    (cid,) = h.state_data()["pending"]
    rest.settle()
    rest.fail_gets = {"/portfolio/subaccounts/transfers": 10}  # the list is unreadable: the balances decide
    rest.results = [{}]
    assert await h(*TRANSFER, answer="150") == 0
    comp = h.state_data()["completed"][-1]
    assert comp["declaration"]["required"] is True and comp["declaration"]["observed_delta"] == "-150.0000"
    assert rest.balances[(1, 2)] == Decimal("150")
    h.lines.clear()
    assert await h("status") == 0 and "(1 missing)" in h.text


# ============================================================================ H1 A5: the retry never connects
async def test_retry_not_sent_keeps_the_id_and_never_moves_twice(tmp_path: Path):
    """Review A5: the retry raises NotSentError (network down). The saved id must survive (an
    earlier attempt may have been applied); once the evidence shows the transfer, it completes,
    and no later run can apply it a second time."""
    rest = FakeRest(balances=BAL)
    h = Harness(tmp_path, rest)
    rest.results = [APPLY_LATER]
    assert await h(*TRANSFER, answer="150") == 1
    (cid,) = h.state_data()["pending"]
    rest.results = [NotSentError("ClientConnectorError: network down")]
    assert await h(*TRANSFER, answer="150") == 1
    assert list(h.state_data()["pending"]) == [cid]  # the saved client_transfer_id is kept
    rest.settle()
    rest.results = [NotSentError("ClientConnectorError: network down")]
    assert await h(*TRANSFER, answer="150") == 0  # resolved by evidence, nothing sent
    assert await h(*TRANSFER, answer="150") == 2  # a re-run is refused (already completed)
    rest.results = [APPLY]
    assert rest.balances[(1, 2)] == Decimal("150")  # moved ONCE
    assert _one_client_id(rest) == {cid}
    assert h.state_data()["completed"][-1]["declaration"]["required"] is True


async def test_first_attempt_rejected_or_not_sent_is_definite(tmp_path: Path):
    """The ONLY attempt refused with a 4xx, or never connected: definitely not applied, dropped."""
    for r in (KalshiHTTPError("POST", "/portfolio/subaccounts/transfer", 400, {"error": {"code": "bad_request"}}),
              NotSentError("ClientConnectorError: refused")):
        rest = FakeRest(balances=BAL)
        h = Harness(tmp_path / type(r).__name__, rest)
        rest.results = [r]
        assert await h(*TRANSFER, answer="150") == 1
        assert h.state_data()["pending"] == {}


async def test_unknown_first_attempt_that_was_applied_completes_at_once(tmp_path: Path):
    """Response lost, but the balances already show the move: completed with the declaration."""
    from .test_account_setup import APPLY_THEN_LOSE

    rest = FakeRest(balances=BAL)
    h = Harness(tmp_path, rest)
    rest.results = [APPLY_THEN_LOSE]
    assert await h(*TRANSFER, answer="150") == 0
    comp = h.state_data()["completed"][-1]
    assert comp["result"] == "applied_after_retry:unknown" and comp["declaration"]["required"] is True


# ============================================================================ H1: state location, --resume
async def test_state_is_per_user_and_a_second_checkout_cannot_start_over(tmp_path: Path, monkeypatch):
    """The saved id lives in ONE per-user file (not per checkout): a second checkout sees the
    unresolved transfer; an old per-checkout file with an unresolved transfer blocks until merged."""
    monkeypatch.delenv(A.STATE_ENV_VAR, raising=False)
    assert A.default_state_path("prod") == A.STATE_DIR / "state.prod.json"
    assert A.STATE_DIR.parts[-2:] == (".kalshi", "dh_account_setup")
    monkeypatch.setenv(A.STATE_ENV_VAR, str(tmp_path / "x.json"))
    assert A.default_state_path("prod") == tmp_path / "x.json"
    # two checkouts, one per-user state: the second checkout's run is a RETRY of the same id
    rest = FakeRest(balances=BAL)
    shared = tmp_path / "user" / "state.json"
    h1 = Harness(tmp_path / "checkout1", rest)
    h2 = Harness(tmp_path / "checkout2", rest)
    h1.state = h2.state = shared
    rest.results = [UnknownOutcome("POST", "/portfolio/subaccounts/transfer", None, "HTTP 500", 500)]
    assert await h1(*TRANSFER, answer="150") == 1
    (cid,) = json.loads(shared.read_text())["pending"]
    assert await h2(*TRANSFER, answer="150") == 0  # the same id (applied now)
    assert _one_client_id(rest) == {cid}
    # an old per-checkout state file is migrated once, and blocks when it conflicts
    legacy = tmp_path / "old" / "account_setup_state.json"
    legacy.parent.mkdir()
    legacy.write_text(json.dumps({"version": 1, "pending": {"old-id": {"kind": "subaccount_transfer", "key": "k",
                                                                        "params": {}}}, "completed": []}))
    fresh = tmp_path / "user2" / "state.json"
    out: list[str] = []
    assert await A.run(["status"], rest=rest, out=out.append, state_path=fresh, legacy_state_path=legacy,
                       ps=lambda: [], key_id="k") == 0
    assert "old-id" in json.loads(fresh.read_text())["pending"] and not legacy.exists()
    legacy.write_text(json.dumps({"version": 1, "pending": {"other": {"kind": "subaccount_transfer", "key": "k2",
                                                                       "params": {}}}, "completed": []}))
    out.clear()
    assert await A.run(list(TRANSFER), rest=rest, out=out.append, ask=lambda _: "150", state_path=fresh,
                       legacy_state_path=legacy, ps=lambda: [], key_id="k") == 2
    assert "old per-checkout state file" in "\n".join(out)


async def test_resume_rechecks_that_system2_is_stopped(tmp_path: Path):
    """Review L5: finishing a shard transfer out of subaccount 0 reads the unscoped balance that
    closes System 2's bracket: --resume refuses while a System 2 process runs."""
    rest = FakeRest(intra_statuses=["pending"])
    h = Harness(tmp_path, rest)
    seed_probe(h)
    assert await h("shard-transfer", "--from-subaccount", "0", "--from-shard", "0", "--to-subaccount", "1",
                   "--to-shard", "2", "--amount-dollars", "150", "--timeout-s", "0", "--execute", answer="150") == 1
    rest.intra_statuses = ["complete"]
    h.ps_lines = SYSTEM2_PS
    assert await h("shard-transfer", "--resume", "tr-0001") == 2
    assert h.state_data()["pending"]  # still unresolved, nothing finished
    assert await h("shard-transfer", "--resume", "tr-0001", "--i-stopped-system2") == 0


# ============================================================================ M2: exact deltas + probe
SHARD = ("shard-transfer", "--from-subaccount", "0", "--from-shard", "0", "--to-subaccount", "1", "--to-shard", "2")


class Over(FakeRest):
    """Kalshi reads IntraExchangeInstanceTransferRequest.amount as CENTS (a unit mistake): 100x."""

    def _apply(self, method, path, body):
        if path == "/portfolio/intra_exchange_instance_transfer":
            amt = Decimal(body["amount"]) / 100
            self.intra_amount = amt
            self._move((body["source_subaccount"], body["source_exchange_shard"]),
                       (body["destination_subaccount"], body["destination_exchange_shard"]), amt)
            return {"transfer_id": "tr-9"}
        return super()._apply(method, path, body)


async def test_shard_transfer_overmove_is_an_alarm_not_complete(tmp_path: Path):
    """Review M2: a transfer that moves 100x the amount must never be reported 'Complete'."""
    rest = Over(balances={(0, 0): "20000", (1, 2): "0"})
    h = Harness(tmp_path, rest)
    seed_probe(h)  # (even with a probe on record)
    rc = await h(*SHARD, "--amount-dollars", "150", "--execute", answer="150")
    assert rest.balances[(1, 2)] == Decimal("15000")
    assert rc == 1 and "Complete:" not in h.text and "ALARM" in h.text
    rec = h.state_data()["completed"][-1]
    assert rec["result"] == "mismatch"
    # System 2 must be told what REALLY left subaccount 0
    assert rec["declaration"]["required"] is True and rec["declaration"]["amount"] == "-15000.00"


async def test_the_probe_is_mandatory_and_catches_the_unit_mistake_for_1_dollar(tmp_path: Path):
    rest = Over(balances={(0, 0): "20000", (1, 2): "0"})
    h = Harness(tmp_path, rest)
    assert await h(*SHARD, "--amount-dollars", "150", "--execute", answer="150") == 2  # no probe yet
    assert rest.writes == [] and "--probe-dollars 1" in h.text
    assert await h(*SHARD, "--probe-dollars", "10", "--execute", answer="10") == 2  # the probe is small
    h.lines.clear()
    assert await h(*SHARD, "--probe-dollars", "1", "--execute", answer="1") == 1
    assert rest.writes[0][2]["amount"] == 10_000  # $1 in centicents
    assert "ALARM" in h.text and h.state_data()["completed"][-1]["result"] == "mismatch"
    assert await h(*SHARD, "--amount-dollars", "150", "--execute", answer="150") == 2  # a failed probe unlocks nothing
    assert rest.balances[(1, 2)] == Decimal("100")  # the mistake cost $100 of a probe, not $15,000


async def test_a_good_probe_unlocks_the_full_transfer_exactly_once(tmp_path: Path):
    rest = FakeRest(balances={(0, 0): "400", (1, 2): "0"})
    h = Harness(tmp_path, rest)
    assert await h(*SHARD, "--probe-dollars", "1", "--execute", answer="1") == 0
    probe = h.state_data()["completed"][-1]
    assert probe["probe"] and probe["result"] == "complete" and probe["declaration"]["amount"] == "-1.00"
    assert await h(*SHARD, "--amount-dollars", "150", "--execute", answer="150") == 0
    assert "Complete: subaccount 1 on shard 2 received $150.00 (exactly" in h.text
    assert rest.balances[(1, 2)] == Decimal("151")
    assert await h(*SHARD, "--amount-dollars", "150", "--execute", answer="150") == 2  # not twice without --again


async def test_listed_amount_must_match_the_request(tmp_path: Path):
    class Lists(FakeRest):
        async def get(self, path, params=(), *, stream=""):
            body = await super().get(path, params, stream=stream)
            if path.startswith("/portfolio/intra_exchange_instance_transfers/"):
                body["transfer"]["amount"] = "0.0100"  # listed in the wrong unit
            return body

    rest = Lists(balances={(0, 0): "400", (1, 2): "0"})
    h = Harness(tmp_path, rest)
    assert await h(*SHARD, "--probe-dollars", "1", "--execute", answer="1") == 1
    assert "ALARM: Kalshi lists transfer" in h.text and h.state_data()["completed"][-1]["result"] == "mismatch"


def test_request_units_match_the_openapi_spec():
    """amount_cents is cents; the intra-account request amount is CENTICENTS, its listing dollars."""
    from .test_account_setup import SPEC

    sch = SPEC["components"]["schemas"]
    assert "centicents" in sch["IntraExchangeInstanceTransferRequest"]["properties"]["amount"]["description"]
    assert sch["IntraExchangeInstanceTransfer"]["properties"]["amount"]["$ref"].endswith("/FixedPointDollars")
    assert "cents" in sch["ApplySubaccountTransferRequest"]["properties"]["amount_cents"]["description"]
    assert A.shard_transfer_body(Decimal("1"), 0, 2, 0, 1)["amount"] == 10_000
    assert A.subaccount_transfer_body("x", 0, 1, Decimal("1"), 2)["amount_cents"] == 100
    assert A.DELTA_TOL <= Decimal("0.01") and A.PROBE_MAX_DOLLARS <= Decimal("5")
