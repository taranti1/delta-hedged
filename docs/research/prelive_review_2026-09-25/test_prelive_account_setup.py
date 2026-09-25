"""Throwaway probes of scripts/account_setup.py against the repo's own FakeRest (offline)."""

from __future__ import annotations

import importlib.util
import sys
from decimal import Decimal
from pathlib import Path

REPO = Path("/Users/thomast/Desktop/delta-hedged")
sys.path.insert(0, str(REPO))

spec = importlib.util.spec_from_file_location("tas", REPO / "tests" / "kalshi" / "test_account_setup.py")
T = importlib.util.module_from_spec(spec)
sys.modules["tas"] = T
spec.loader.exec_module(T)  # type: ignore[union-attr]

from dh.kalshi.rest import KalshiHTTPError, UnknownOutcome  # noqa: E402

TRANSFER = ("transfer", "--from", "0", "--to", "1", "--amount-dollars", "150", "--exchange-index", "2", "--execute")
BAL = {(0, 0): "400", (0, 2): "300", (1, 2): "0"}


# A1: first attempt applied but its response lost; the retry's duplicate id is answered with a
# 4xx OTHER than 409 (e.g. 400 "duplicate client_transfer_id"): the tool says "definitely not
# applied", DROPS the pending id and prints no System 2 declaration. A re-run then moves money twice.
async def test_retry_duplicate_answered_400_loses_idempotency(tmp_path: Path):
    rest = T.FakeRest(balances=BAL)
    h = T.Harness(tmp_path, rest)
    rest.results = [T.APPLY_THEN_LOSE]
    assert await h(*TRANSFER, answer="150") == 1
    assert rest.balances[(1, 2)] == Decimal("150")  # applied
    rest.results = [KalshiHTTPError("POST", "/portfolio/subaccounts/transfer", 400,
                                    {"error": {"code": "bad_request", "message": "duplicate client_transfer_id"}})]
    assert await h(*TRANSFER, answer="150") == 1
    assert "definitely not applied" in h.text
    assert h.state_data()["pending"] == {}  # idempotency key forgotten
    assert "declare-transfer" not in h.text  # System 2 never told
    # operator re-runs "the exact command": NEW uuid -> applied a second time
    rest.results = [T.APPLY]
    assert await h(*TRANSFER, answer="150") == 0
    assert rest.balances[(1, 2)] == Decimal("300")
    assert rest.balances[(0, 2)] == Decimal("0")
    ids = {w[2]["client_transfer_id"] for w in rest.writes}
    assert len(ids) == 2


# A2: retry answered 409 although the earlier attempt was NOT applied (id recorded, transfer
# failed server-side): the tool reports "confirmed" and prints a REQUIRED declaration.
async def test_409_on_retry_trusted_without_balance_evidence(tmp_path: Path):
    rest = T.FakeRest(balances=BAL)
    h = T.Harness(tmp_path, rest)
    rest.results = [UnknownOutcome("POST", "/portfolio/subaccounts/transfer", None, "HTTP 500", 500)]
    assert await h(*TRANSFER, answer="150") == 1
    rest.results = [UnknownOutcome("POST", "/portfolio/subaccounts/transfer", None, "HTTP 409", 409)]
    rc = await h(*TRANSFER, answer="150")
    assert rest.balances[(1, 2)] == Decimal("0")  # nothing moved
    assert rc == 0
    comp = h.state_data()["completed"][-1]
    assert comp["result"] == "applied_earlier_409"
    assert comp["declaration"]["required"] is True
    print(comp["declaration"]["warning"])


# A3: shard transfer that moves 100x the amount (unit misread) is reported "Complete".
async def test_shard_transfer_overmove_reported_complete(tmp_path: Path):
    class Over(T.FakeRest):
        def _apply(self, method, path, body):
            if path == "/portfolio/intra_exchange_instance_transfer":
                amt = Decimal(body["amount"]) / 100  # server reads the field as CENTS
                self._move((body["source_subaccount"], body["source_exchange_shard"]),
                           (body["destination_subaccount"], body["destination_exchange_shard"]), amt)
                return {"transfer_id": "tr-9"}
            return super()._apply(method, path, body)

    rest = Over(balances={(0, 0): "20000", (1, 2): "0"})
    h = T.Harness(tmp_path, rest)
    rc = await h("shard-transfer", "--from-subaccount", "0", "--from-shard", "0", "--to-subaccount", "1",
                 "--to-shard", "2", "--amount-dollars", "150", "--execute", answer="150")
    assert rest.balances[(1, 2)] == Decimal("15000")
    assert rc == 0 and "Complete: subaccount 1 on shard 2 received $150.00" in h.text


# A4: first attempt applied (response lost); Kalshi answers the retry with 200 (idempotent
# replay) instead of 409: the bracket starts at the RETRY's reading (after the change), the
# observed delta is 0, the declaration is marked NOT required and `status` never lists it.
async def test_retry_answered_200_declaration_dropped(tmp_path: Path):
    rest = T.FakeRest(balances=BAL)
    h = T.Harness(tmp_path, rest)
    rest.results = [T.APPLY_THEN_LOSE]
    assert await h(*TRANSFER, answer="150") == 1
    rest.results = [{}]  # 200 {} without applying again
    assert await h(*TRANSFER, answer="150") == 0
    comp = h.state_data()["completed"][-1]
    assert comp["declaration"]["required"] is False
    assert rest.balances[(1, 2)] == Decimal("150")
    h.lines.clear()
    assert await h("status") == 0
    assert "(0 missing)" in h.text  # status: nothing to declare, although System 2 lost $150


# A5: same as A1 but the retry never connects (NotSentError): the pending id is dropped too.
async def test_retry_not_sent_drops_pending_then_double_move(tmp_path: Path):
    from dh.kalshi.rest import NotSentError

    rest = T.FakeRest(balances=BAL)
    h = T.Harness(tmp_path, rest)
    rest.results = [T.APPLY_THEN_LOSE]
    assert await h(*TRANSFER, answer="150") == 1  # applied, response lost
    rest.results = [NotSentError("ClientConnectorError: network down")]
    assert await h(*TRANSFER, answer="150") == 1
    assert h.state_data()["pending"] == {}  # the saved client_transfer_id is gone
    rest.results = [T.APPLY]
    assert await h(*TRANSFER, answer="150") == 0  # fresh uuid
    assert rest.balances[(1, 2)] == Decimal("300")  # moved twice
