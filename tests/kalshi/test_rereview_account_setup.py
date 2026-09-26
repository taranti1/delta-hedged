"""Regression tests for the pre-live RE-review of scripts/account_setup.py
(docs/research/prelive_review_2026-09-25/PRELIVE_REREVIEW.md: NEW-4, NEW-6).

Offline, against the fake REST client of test_account_setup. NEW-4: a listed transfer is claimed
by exactly one local record (its transfer id is stored), an earlier identical transfer never
matches a later one, and "applied" needs the exact balance deltas on the route (a listed row plus
unrelated balance movement is never enough). NEW-6: a mismatched shard-transfer declares what
left subaccount 0 per the transfer record (never unrelated balance changes), and a state-file
override cannot bypass idempotency.
"""

from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path

from dh.kalshi.rest import UnknownOutcome

from .test_account_setup import A, FakeRest, Harness

TRANSFER = ("transfer", "--from", "0", "--to", "1", "--amount-dollars", "150", "--exchange-index", "2", "--execute")
BAL = {(0, 0): "400", (0, 2): "300", (1, 2): "0"}
SHARD = ("shard-transfer", "--from-subaccount", "0", "--from-shard", "0", "--to-subaccount", "1", "--to-shard", "2")


def _noise_after_write(rest: FakeRest, key: tuple[int, int], dollars: str) -> None:
    """A System 2 settlement lands on subaccount 0 right after the transfer request."""
    orig = rest.write

    async def write_and_settlement(*a, **k):
        r = await orig(*a, **k)
        rest.balances[key] = rest.balances.get(key, Decimal(0)) + Decimal(dollars)
        return r

    rest.write = write_and_settlement  # type: ignore[method-assign]


# ============================================================================ NEW-4
async def test_again_within_seconds_never_matches_the_earlier_identical_transfer(tmp_path: Path):
    """The review probe (test_N1_listed_false_positive): T1 completes; T2 (--again, seconds later)
    is NOT applied, but a System 2 settlement moves subaccount 0 ('mixed' balances) and T1's row
    is listed. T2 must stay UNRESOLVED: nothing completed, nothing declared to System 2."""
    rest = FakeRest(balances=BAL)
    h = Harness(tmp_path, rest)
    assert await h(*TRANSFER, answer="150") == 0  # T1 applied and listed
    t1 = h.state_data()["completed"][-1]
    assert t1["claimed_transfer_id"] == "st-1"
    rest.results = [UnknownOutcome("POST", "/portfolio/subaccounts/transfer", None, "HTTP 500", 500)]  # T2 NOT applied
    _noise_after_write(rest, (0, 2), "5")
    rc = await h(*TRANSFER, "--again", answer="150")
    data = h.state_data()
    assert rest.balances[(1, 2)] == Decimal("150")  # only T1 moved
    assert rc == 1 and len(data["completed"]) == 1 and len(data["pending"]) == 1  # T2 unresolved, not declared
    (t2,) = data["pending"].values()
    assert t2["evidence"]["balance"] == "mixed" and t2["evidence"]["listed"] is False
    assert "INCONCLUSIVE" in h.text


async def test_listed_row_plus_mixed_balances_is_not_applied(tmp_path: Path):
    """Even a real, unclaimed listed row is not enough while the balances moved by something else
    than exactly the amount on the route: unresolved (the operator checks), never declared."""
    rest = FakeRest(balances=BAL)
    h = Harness(tmp_path, rest)
    rest.results = [UnknownOutcome("POST", "/portfolio/subaccounts/transfer", None, "HTTP 500", 500)]
    orig_write = rest.write

    async def apply_then_noise(method, path, *, json_body=None, params=(), stream="", n_items=1):
        res = await orig_write(method, path, json_body=json_body, params=params, stream=stream, n_items=n_items)
        rest._apply(method, path, json_body)  # applied at Kalshi (listed), the response was a 500
        rest.balances[(0, 2)] += Decimal("5")  # plus a System 2 settlement
        return res

    rest.write = apply_then_noise  # type: ignore[method-assign]
    rc = await h(*TRANSFER, answer="150")
    data = h.state_data()
    assert rc == 1 and data["completed"] == [] and len(data["pending"]) == 1
    (e,) = data["pending"].values()
    assert e["evidence"]["balance"] == "mixed" and e["evidence"]["listed"] is True


async def test_two_records_never_claim_the_same_listed_transfer(tmp_path: Path):
    """T1 and T2 (--again) both applied: each record claims its OWN listed transfer id."""
    rest = FakeRest(balances={(0, 0): "400", (0, 2): "400", (1, 2): "0"})
    h = Harness(tmp_path, rest)
    assert await h(*TRANSFER, answer="150") == 0
    assert await h(*TRANSFER, "--again", answer="150") == 0
    comp = h.state_data()["completed"]
    assert [c["claimed_transfer_id"] for c in comp] == ["st-1", "st-2"]
    assert rest.balances[(1, 2)] == Decimal("300")


def test_evidence_verdict_needs_exact_route_deltas_and_one_unclaimed_row():
    E = A.Evidence
    assert E("applied", True, candidates=1, transfer_id="st-1").verdict == "applied"
    assert E("applied", None).verdict == "applied"  # list unreadable: exact deltas on the route decide
    assert E("applied", False).verdict == "unresolved"  # moved exactly, but no unclaimed listed row
    assert E("applied", True, candidates=2).verdict == "unresolved"  # ambiguous
    assert E("mixed", True, candidates=1).verdict == "unresolved"  # was 'applied' before the fix
    assert E("unreadable", True, candidates=1).verdict == "unresolved"
    assert E("none", False).verdict == "not_seen" and E("none", True, candidates=1).verdict == "conflict"
    assert A.TRANSFER_LIST_SKEW_S <= 10


# ============================================================================ NEW-6: mismatch declaration
async def test_mismatched_shard_transfer_declares_the_transfer_record_amount_not_the_balance_noise(tmp_path: Path):
    """A probe of $1 that Kalshi lists as $1, while a System 2 settlement takes $5 from subaccount 0
    on another shard in the same bracket: the result is a mismatch (something else moved), and the
    declaration is exactly the $1 that left per the transfer record, not the $6 subaccount-0 change.
    Both numbers are shown."""
    rest = FakeRest(balances={(0, 0): "400", (0, 3): "50", (1, 2): "0"})
    h = Harness(tmp_path, rest)
    _noise_after_write(rest, (0, 3), "-5")
    rc = await h(*SHARD, "--probe-dollars", "1", "--execute", answer="1")
    rec = h.state_data()["completed"][-1]
    assert rc == 1 and rec["result"] == "mismatch" and "ALARM" in h.text
    assert rec["declaration"]["amount"] == "-1.00"  # was -6.00 (all subaccount-0 changes)
    assert rec["declaration_basis"] == {"transfer_record": "1.0000", "observed_subaccount0_change": "-6.0000"}
    assert "per the transfer record" in h.text and "-6.0000" in h.text


# ============================================================================ NEW-6: --state-file
async def test_state_file_override_cannot_bypass_a_pending_transfer(tmp_path: Path):
    rest = FakeRest(balances=BAL)
    h = Harness(tmp_path, rest)
    rest.results = [UnknownOutcome("POST", "/portfolio/subaccounts/transfer", None, "HTTP 500", 500)]
    assert await h(*TRANSFER, answer="150") == 1  # unresolved, saved in the per-user file
    canonical = h.state
    assert json.loads(canonical.read_text())["pending"]
    empty = tmp_path / "elsewhere" / "state.json"
    n_writes = len(rest.writes)

    async def run(*argv: str, **kw) -> int:
        return await A.run(["--system2-root", str(h.system2), *argv], rest=rest, ask=h.ask, ps=lambda: [], now=h.now,
                           sleep=h.sleep, out=h.lines.append, log_path=h.log, canonical_state=canonical, **kw)

    h.answers = ["150"]
    assert await run("--state-file", str(empty), *TRANSFER) == 2
    assert "UNRESOLVED transfer(s)" in h.text and "--i-know-state-file" in h.text
    assert len(rest.writes) == n_writes  # nothing sent
    # a copy that holds the pending record is accepted (the retry reuses the saved id)
    empty.parent.mkdir(parents=True, exist_ok=True)
    empty.write_text(canonical.read_text())
    h.answers = ["150"]
    await run("--state-file", str(empty), *TRANSFER)
    assert len(rest.writes) == n_writes + 1
    assert {w[2]["client_transfer_id"] for w in rest.writes} == set(json.loads(canonical.read_text())["pending"])


async def test_state_file_override_is_accepted_with_the_explicit_flag(tmp_path: Path):
    rest = FakeRest(balances=BAL)
    h = Harness(tmp_path, rest)
    assert await h(*TRANSFER, answer="150") == 0  # completed in the per-user file
    other = tmp_path / "other.json"
    h.answers = ["150"]
    rc = await A.run(["--system2-root", str(h.system2), "--state-file", str(other), *TRANSFER], rest=rest, ask=h.ask,
                     ps=lambda: [], now=h.now, sleep=h.sleep, out=h.lines.append, log_path=h.log, canonical_state=h.state)
    assert rc == 2 and "1 completed transfer(s)" in h.text  # the completed one would be repeated without --again
    h.answers = ["150"]
    rc = await A.run(["--system2-root", str(h.system2), "--state-file", str(other), "--i-know-state-file", *TRANSFER],
                     rest=rest, ask=h.ask, ps=lambda: [], now=h.now, sleep=h.sleep, out=h.lines.append, log_path=h.log,
                     canonical_state=h.state)
    assert rc == 0 and rest.balances[(1, 2)] == Decimal("300")
