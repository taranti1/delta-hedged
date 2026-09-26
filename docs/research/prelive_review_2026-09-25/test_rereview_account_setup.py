"""Re-review probes of scripts/account_setup.py (post-fix d83d737), offline, repo FakeRest."""

from __future__ import annotations

import importlib.util
import sys
from decimal import Decimal
from pathlib import Path

REPO = Path("/Users/thomast/Desktop/delta-hedged")
sys.path.insert(0, str(REPO))
spec = importlib.util.spec_from_file_location("tas2", REPO / "tests" / "kalshi" / "test_account_setup.py")
T = importlib.util.module_from_spec(spec)
sys.modules["tas2"] = T
spec.loader.exec_module(T)  # type: ignore[union-attr]

from dh.kalshi.rest import KalshiHTTPError, NotSentError, UnknownOutcome  # noqa: E402

TRANSFER = ("transfer", "--from", "0", "--to", "1", "--amount-dollars", "150", "--exchange-index", "2", "--execute")
BAL = {(0, 0): "400", (0, 2): "300", (1, 2): "0"}


def ids(rest):
    return {w[2]["client_transfer_id"] for w in rest.writes if w[1] == "/portfolio/subaccounts/transfer"}


# A5 (was: double move after NotSent on retry) -> must stay single
async def test_A5_not_sent_retry_keeps_id(tmp_path: Path):
    rest = T.FakeRest(balances=BAL)
    h = T.Harness(tmp_path, rest)
    rest.results = [T.APPLY_LATER]
    assert await h(*TRANSFER, answer="150") == 1
    rest.results = [NotSentError("down")]
    await h(*TRANSFER, answer="150")
    assert len(h.state_data()["pending"]) == 1
    rest.settle()
    rest.results = [T.APPLY]
    await h(*TRANSFER, answer="150")
    await h(*TRANSFER, answer="150")
    assert rest.balances[(1, 2)] == Decimal("150") and len(ids(rest)) == 1


# A1: 400 on the retry
async def test_A1_400_on_retry(tmp_path: Path):
    rest = T.FakeRest(balances=BAL)
    h = T.Harness(tmp_path, rest)
    rest.results = [T.APPLY_THEN_LOSE]
    assert await h(*TRANSFER, answer="150") == 0 or True
    rest.results = [KalshiHTTPError("POST", "/portfolio/subaccounts/transfer", 400, {"error": {"code": "dup"}})]
    await h(*TRANSFER, answer="150")
    rest.results = [T.APPLY]
    await h(*TRANSFER, answer="150")
    assert rest.balances[(1, 2)] == Decimal("150") and len(ids(rest)) == 1


# A4: 200 replay on retry -> declaration must be required
async def test_A4_200_replay(tmp_path: Path):
    rest = T.FakeRest(balances=BAL)
    h = T.Harness(tmp_path, rest)
    rest.results = [T.APPLY_LATER]
    await h(*TRANSFER, answer="150")
    rest.settle()
    rest.results = [{}]
    await h(*TRANSFER, answer="150")
    comp = h.state_data()["completed"][-1]
    assert comp["declaration"]["required"] is True and comp["declaration"]["amount"] == "-150.00"


# A2: 409 on retry while nothing moved -> no declaration, stays pending
async def test_A2_409_nothing_moved(tmp_path: Path):
    rest = T.FakeRest(balances=BAL)
    h = T.Harness(tmp_path, rest)
    rest.results = [UnknownOutcome("POST", "/portfolio/subaccounts/transfer", None, "HTTP 500", 500)]
    await h(*TRANSFER, answer="150")
    rest.results = [UnknownOutcome("POST", "/portfolio/subaccounts/transfer", None, "HTTP 409", 409)]
    rc = await h(*TRANSFER, answer="150")
    assert rc == 1 and h.state_data()["completed"] == [] and len(h.state_data()["pending"]) == 1


# A3/M2: 100x over-move needs a probe first; the probe itself alarms and a full transfer stays refused
async def test_A3_overmove_probe(tmp_path: Path):
    class Over(T.FakeRest):
        def _apply(self, method, path, body):
            if path == "/portfolio/intra_exchange_instance_transfer":
                amt = Decimal(body["amount"]) / 100
                self.intra_amount = amt
                self._move((body["source_subaccount"], body["source_exchange_shard"]),
                           (body["destination_subaccount"], body["destination_exchange_shard"]), amt)
                return {"transfer_id": "tr-9"}
            return super()._apply(method, path, body)

    rest = Over(balances={(0, 0): "20000", (1, 2): "0"})
    h = T.Harness(tmp_path, rest)
    full = ("shard-transfer", "--from-subaccount", "0", "--from-shard", "0", "--to-subaccount", "1", "--to-shard", "2")
    assert await h(*full, "--amount-dollars", "150", "--execute", answer="150") == 2  # no probe yet
    assert rest.writes == []
    rc = await h(*full, "--probe-dollars", "1", "--execute", answer="1")
    assert rc == 1 and rest.balances[(1, 2)] == Decimal("100")
    assert "ALARM" in h.text
    assert await h(*full, "--amount-dollars", "150", "--execute", answer="150") == 2  # still refused


# NEW N1: "listed" matched by an EARLIER identical transfer (--again within the 120 s skew) plus
# unrelated balance movement ("mixed") -> an UNAPPLIED transfer is completed and declared.
async def test_N1_listed_false_positive(tmp_path: Path):
    rest = T.FakeRest(balances=BAL)
    h = T.Harness(tmp_path, rest)
    rest.now_s = lambda: int(h.t.timestamp())
    assert await h(*TRANSFER, answer="150") == 0  # T1 applied and listed
    rest.results = [UnknownOutcome("POST", "/portfolio/subaccounts/transfer", None, "HTTP 500", 500)]  # T2 NOT applied
    orig = rest.write

    async def write_and_settlement(*a, **k):
        r = await orig(*a, **k)
        rest.balances[(0, 2)] += Decimal("5")  # a System 2 settlement lands on subaccount 0 shard 2
        return r

    rest.write = write_and_settlement
    rc = await h(*TRANSFER, "--again", answer="150")
    comp = h.state_data()["completed"]
    print(rc, [c.get("result") for c in comp], comp[-1].get("evidence"))
    assert rest.balances[(1, 2)] == Decimal("150")  # only T1 moved
    assert len(comp) == 2 and comp[-1]["declaration"]["required"] is True  # T2 declared although not applied
