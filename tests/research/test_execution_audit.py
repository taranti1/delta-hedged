import json

from dh.research.execution_audit import calibration_report, quote_outcomes


def test_only_causal_quote_predictions_are_graded(tmp_path):
    p = tmp_path / "paper.jsonl"
    records = [
        {"k": "log.quote", "t": 100, "ts_decision": 100, "ticker": "A", "coid": "q1", "side": "bid",
         "edge": .05, "adverse": .01, "fee": .001, "inv_charge": .02},
        {"k": "log.fill", "t": 300, "ts_exch": 200, "ticker": "A", "coid": "q1", "side": "bid", "qty": 100,
         "px": 5000, "fee": 1000, "trade_id": "f1"},
        {"k": "log.fill", "t": 400, "ticker": "A", "coid": "q1", "side": "bid", "qty": 100,
         "px": 5000, "F": .99, "trade_id": "f2"},
        {"k": "log.fill", "t": 400, "ts_exch": 90, "ticker": "A", "coid": "q1", "side": "bid", "qty": 100,
         "px": 5000, "trade_id": "f3"},
    ]
    p.write_text("\n".join(json.dumps(r) for r in records))
    df = quote_outcomes([p], {"A": 1}, hedging_enabled={str(p): False})
    assert df.causal_prediction.tolist() == [True, False, False]
    assert abs(df.iloc[0].predicted_net_c_per_contract - 3.9) < 1e-9
    report = calibration_report(df, {"A": 86_400_000_000_000}, n_boot=25)
    assert report["causally_linked_settled_fills"] == 1
    assert report["tradable"] is False
    assert report["bins"][0]["settlement_days"] == 1


def test_session_scoped_ids_unresolved_and_hedges(tmp_path):
    records = [{"k": "log.fill", "t": 100, "ticker": "A", "coid": "q", "side": "bid", "qty": 100,
                "px": 5000, "trade_id": "same-id"}]
    paths = [tmp_path / name for name in ("a.jsonl", "b.jsonl")]
    for p in paths:
        p.write_text("\n".join(json.dumps(r) for r in records * 2))
    df = quote_outcomes(paths, {}, hedging_enabled={str(paths[0]): False, str(paths[1]): True})
    assert len(df) == 2
    report = calibration_report(df, {})
    assert report["unresolved_fills"] == 2
    assert report["fills_without_complete_hedge_costs"] == 1


def test_missing_hedge_metadata_does_not_assert_complete_costs(tmp_path):
    p = tmp_path / "legacy.jsonl"
    p.write_text(json.dumps({"k": "log.fill", "t": 100, "ticker": "A", "side": "bid", "qty": 100, "px": 5000}))
    df = quote_outcomes([p], {"A": 1})
    assert not df.iloc[0].hedge_cost_complete
