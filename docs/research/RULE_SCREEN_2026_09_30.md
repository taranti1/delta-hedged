# Quick screen of three quote rules (2026-09-30)

**Question:** before live trading, should any of three rules be added to the strategy to raise total
net profit or risk-adjusted profit? The rules are a final-window extension, a pause after BTC
jumps, and a cap on disagreement with the Kalshi price.

**Method:** fixed-opportunity screen. Every recorded, settled fill is either kept or removed by a
rule. Freed risk budget, and the other quotes the strategy would have placed instead, are not
modeled. That needs a full replay.

**Data:** 1,843 fills (97–99% settled):
- paper trading, Sep 25 18:42 – Sep 30 06:02 ET: legacy code, corrected `m1`, and `m1_small`
- baseline replays under fill policies B and C: `m1` on Sep 26 and Sep 27 to 15:50 UTC; `m1_small` on Sep 26

Kalshi best bid/ask comes from the recorded `ticker` channel; the BRTI 5 Hz series comes from the
recorded `kalshi.ws` stream. 90–96% of fills have a two-sided Kalshi quote at quote time; the rest
fall in recording gaps and are never blocked.

**Caveats on independence:** replays B and C are the same days under two fill assumptions, not
independent samples, and legacy paper overlaps the replay days. The most independent samples are
corrected `m1` paper (Sep 27–29) and `m1_small` paper (Sep 29–30).

**Primary settings,** declared before any outcome was examined:

| Rule | Primary setting |
|---|---|
| Final window | block tau < 150 s with \|z\| < 2.5 |
| Jump pause | 4× the 1-second sigma, pause and pull quotes for 10 s |
| Disagreement cap | block when \|F − Kalshi mid\| > 10¢ at quote time |

Every other setting in the grids was chosen after seeing results, so treat them as context.

## Results (change in total net $ from applying the rule; 95% CI clustered by expiration)

| Rule | Paper all | Paper corrected `m1` | Paper `m1_small` | Replay B `m1` | Replay C `m1` | Replay B small | Replay C small |
|---|---|---|---|---|---|---|---|
| Final window 150 s | +6.25 (−4.7, 20.6) | −2.75 | +8.65 | +1.52 | −1.29 | −0.21 | +1.22 |
| Jump pause 4σ, 10 s | **−41.91 (−78.2, −9.1)** | −11.74 | −9.75 | **−33.07** | −17.44 | −6.43 | −8.72 |
| Disagreement cap 10¢ | +3.57 (−12.0, 20.5) | −6.58 | +8.45 | +2.28 | +6.23 | +2.25 | −2.45 |
| Disagreement cap 15¢ (post hoc) | +7.15 (−3.3, 23.4) | +0.65 | 0 (no fills blocked) | +3.36 | +6.78 | +2.25 | +0.50 |

Full grid: `rule_results.csv`. Per-fill data: `fills.parquet`. Script: `screen.py`.

## Findings

1. **Jump pause: reject.** It loses money at every setting (3–5σ, 5–30 s, block or pull) on every
   dataset. Fills quoted within 10 s after a 4σ BRTI jump earned more per contract than all other
   fills in 4 of 5 samples, and about the same in the fifth:

   | Sample | After a jump (¢/contract) | All other fills (¢/contract) |
   |---|---:|---:|
   | Replay B | +6.5 | −0.6 |
   | Replay C | +4.6 | −0.6 |
   | Legacy paper | +7.2 | +0.4 |
   | `m1_small` paper | +12.0 | −6.5 |
   | Corrected `m1` paper | +5.0 | +5.2 |

   Much of the strategy's edge appears to come from repricing faster than Kalshi quotes after BTC
   moves. **Corollary:** the existing 6σ / 120 s abnormal-move pause may also be costing money.
   This screen cannot measure that, since no fills occur during a pause; it needs a replay.
2. **Final window: inconclusive; do not adopt.** The two `m1_small` losses (selling YES at 12–15¢
   about 110 s before expiry) did not repeat. Near-expiry cheap-YES sales made +7.7 to +10¢ per
   contract in corrected `m1` paper and both replays. Blocking *all* fills with under 300 s left
   helps in 4 of 5 samples but loses in corrected `m1` paper, the most recent independent `m1`
   sample.
3. **Disagreement cap: the primary 10¢ is inconclusive** (it loses $6.58 in corrected `m1`).
   **15¢ helps in every sample where it fires,** blocks only 1–2% of contracts, and cuts the
   replay C maximum drawdown from $16.79 to $11.65. But it fires on only 3–9 fills per sample, and
   15¢ was picked after seeing the grid. It would not have stopped either `m1_small` loss
   (disagreement 13.4¢ and 10.5¢). Candidate for replay, not proven.

## Recommendation for the first live test

- Keep `m1_small` unchanged. Any strategy change alters its digest and restarts the 7-day paper
  clock (RUNBOOK 5.1), and nothing here clears the evidence bar.
- Narrowed replay (both policies, Sep 26–29), before Oct 6:
  - `m1_small` baseline
  - disagreement cap 15¢
  - a softer abnormal-move pause: higher threshold or shorter than 120 s
