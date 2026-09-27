"""Grade pre-trade quote predictions against actual filled settlement outcomes.

This diagnostic never tunes a policy or promotes one to live trading. In particular,
delivery-time fair value in legacy fill logs is not a pre-trade prediction. Missing
quote identifiers/times stay missing rather than being reconstructed using hindsight.
"""
from __future__ import annotations

import math
import argparse
import json
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd

from dh.live.replay import iter_log_records, log_fill_is_new
from dh.research.exp_common import ratio_ci

# Fixed beforehand: never choose bin boundaries to make this sample look profitable.
EDGE_BINS_C = (-math.inf, 0, .15, .2, .5, 1, 2, 5, math.inf)


def quote_outcomes(paths: Iterable[str | Path], outcomes: dict[str, float], *,
                   hedging_enabled: dict[str, bool] | None = None) -> pd.DataFrame:
    """One row per fill. ``outcomes`` contains known YES payouts in dollars (0 or 1).

    Identifiers are scoped to a session because simulator trade/order ids can repeat.
    Net P&L here excludes hedging: only sessions explicitly configured with hedging
    disabled are eligible. Unknown/enabled configurations stay visible but ungraded.
    """
    rows = []
    for path in paths:
        quotes, seen = {}, set()
        session_rows, has_hedge = [], False
        hedge_enabled = (hedging_enabled or {}).get(str(path))
        for r in iter_log_records(path):
            kind = r.get("k", "")
            if kind == "session_start" and isinstance(r.get("hedge_enabled"), bool):
                logged = r["hedge_enabled"]
                if hedge_enabled is not None and hedge_enabled != logged:
                    raise ValueError("conflicting hedge configuration metadata")
                hedge_enabled = logged
            if "hedge" in kind and "fill" in kind:
                has_hedge = True
            if kind == "log.quote" and r.get("coid"):
                quotes[str(r["coid"])] = r
            elif kind == "log.fill" and log_fill_is_new(r, seen):
                q = quotes.get(str(r.get("coid", "")))
                fill_ts = int(r.get("ts_exch") or r["t"])
                decision_ts = int(q.get("ts_decision", q["t"])) if q else None
                # A legacy fill's delivery timestamp cannot establish causal match order.
                linked = bool(q and r.get("ts_exch") and decision_ts <= fill_ts
                              and q.get("ticker") == r["ticker"] and q.get("side") == r["side"])
                contracts = int(r["qty"]) / 100
                px = int(r["px"]) / 10_000
                payout = outcomes.get(r["ticker"], math.nan)
                if math.isfinite(payout) and payout not in (0, 1):
                    raise ValueError("expected binary settlement payout")
                sign = 1 if r["side"] == "bid" else -1
                fee = int(r.get("fee", 0)) / 1_000_000
                net = contracts * sign * (payout - px) - fee
                # Inventory charge is a risk preference, not an exchange cash expense.
                prediction = (100 * (float(q["edge"]) - float(q.get("adverse", 0))
                                     - float(q.get("fee", 0)) - float(q.get("hedge_cost", 0)))) if linked else math.nan
                session_rows.append({"session": Path(path).name, "ticker": r["ticker"],
                                     "coid": r.get("coid", ""), "ts": fill_ts,
                                     "decision_ts": decision_ts, "causal_prediction": linked,
                                     "contracts": contracts, "net_usd": net,
                                     "net_c_per_contract": 100 * net / contracts if contracts else math.nan,
                                     "predicted_net_c_per_contract": prediction,
                                     "position": q.get("position", "unknown") if q else "unknown",
                                     "model_hash": q.get("fv_model_hash", "") if q else "",
                                     "settled": math.isfinite(payout)})
        for row in session_rows:
            row["hedge_cost_complete"] = hedge_enabled is False and not has_hedge
        rows.extend(session_rows)
    return pd.DataFrame(rows)


def calibration_report(fills: pd.DataFrame, expiration_of: dict[str, int], *, n_boot: int = 500) -> dict:
    """Fixed-bin, day-clustered prediction diagnostics; no claim of profitable edge."""
    out = {"tradable": False, "status": "diagnostic_only", "fills": len(fills),
           "limitations": ["simulated fills do not prove exchange execution",
                           "fill-conditioned calibration does not estimate missed fills or capacity",
                           "no untouched policy confirmation is performed by this diagnostic"], "bins": []}
    if fills.empty:
        out["causally_linked_settled_fills"] = 0
        return out
    valid = fills.causal_prediction & fills.settled & fills.hedge_cost_complete
    out["causally_linked_settled_fills"] = int(valid.sum())
    out["unresolved_fills"] = int((~fills.settled).sum())
    out["missing_pretrade_prediction_fills"] = int((~fills.causal_prediction).sum())
    out["fills_without_complete_hedge_costs"] = int((~fills.hedge_cost_complete).sum())
    df = fills.loc[valid].copy()
    if df.empty:
        return out
    df["prediction_bin_c"] = pd.cut(df.predicted_net_c_per_contract, EDGE_BINS_C, right=False)
    df["settlement_day"] = [expiration_of.get(t, 0) // 86_400_000_000_000 for t in df.ticker]
    out["missing_expiration_fills"] = int((df.settlement_day == 0).sum())
    for label, g in df.groupby("prediction_bin_c", observed=True):
        contract_count = float(g.contracts.sum())
        prediction = float(np.average(g.predicted_net_c_per_contract, weights=g.contracts))
        realized = 100 * float(g.net_usd.sum()) / contract_count
        days = g[g.settlement_day != 0].groupby("settlement_day").agg(net=("net_usd", "sum"), ct=("contracts", "sum"))
        ci = ratio_ci(100 * days.net.to_numpy(), days.ct.to_numpy(), n_boot=n_boot)
        out["bins"].append({"predicted_c_range": str(label), "fills": len(g), "contracts": contract_count,
                            "predicted_net_c": prediction, "realized_net_c": realized,
                            "overprediction_c": prediction - realized, "net_usd": float(g.net_usd.sum()),
                            "settlement_days": len(days), "realized_net_day_ci95": [ci.lo, ci.hi]})
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--logs-dir", required=True, type=Path)
    parser.add_argument("--data", required=True, type=Path, help="paper recording root")
    parser.add_argument("--outcomes-root", type=Path)
    parser.add_argument("--out", required=True, type=Path)
    args = parser.parse_args(argv)
    from dh.live.replay import add_downloaded_outcomes, ledger_from_logs, load_session, session_specs
    from dh.live.tools import _session_of

    paths = sorted(p for p in args.logs_dir.iterdir()
                   if p.name.startswith("paper-") and p.name.endswith((".jsonl", ".jsonl.zst", ".jsonl.gz")))
    if not paths or len({_session_of(str(p)) for p in paths}) != len(paths):
        raise ValueError("expected unique paper session logs (do not include restored and compressed copies together)")
    specs, hedging = {}, {}
    for p in paths:
        info = load_session(args.data, _session_of(str(p)))
        if info.mode != "paper":
            raise ValueError("this command audits paper sessions only")
        specs.update({s.ticker: s for s in session_specs(info)})
        enabled = info.start.get("strategy_config", {}).get("hedge", {}).get("enabled")
        if isinstance(enabled, bool):
            hedging[str(p)] = enabled
    ledger = ledger_from_logs(paths, list(specs.values()))
    if args.outcomes_root:
        add_downloaded_outcomes(ledger, args.outcomes_root)
    fills = quote_outcomes(paths, ledger.settle, hedging_enabled=hedging)
    report = calibration_report(fills, ledger.expiration_of)
    args.out.mkdir(parents=True, exist_ok=True)
    fills.to_csv(args.out / "quote_fill_outcomes.csv", index=False)
    (args.out / "execution_calibration.json").write_text(json.dumps(report, indent=2, default=str) + "\n")
    print(json.dumps(report, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
