# Neighbouring-strike rule: quick screen (2026-09-30)

**Rule:** `risk.block_opposite_neighbor_usd`, off by default. When on, no new order may open or
add a YES position whose sign is opposite to one we hold on a 'greater' strike of the same
settlement within G dollars. Reductions are never blocked.

**Motivation:** the first live hour (live-20261001T022326Z-tm7in2t7).
- We sold 5 YES on the 83,400 strike at 55¢ and bought 5 YES on 83,500 at 47¢.
- Together that is "lose if the 11 PM average lands between the strikes".
- It settled at 83,447.47, for −$4.60.

**Method:** fixed-opportunity, as in `RULE_SCREEN_2026_09_30.md`. Every recorded fill is kept or
removed (the rule-screen data plus the two live fills); freed risk budget is not modeled. Script:
`dh/research/neighbor_screen_20260930.py`. Results: `data/results/screen_20260930/neighbor_rule.csv`.

## Results

"Change" is the change in total net $ from blocking flagged fills: + means the rule helps. The
interval is a 95% CI clustered by expiration.

| Dataset | Fills flagged (G = $100) | Flagged fills, ¢/ct | Other fills, ¢/ct | Change, G = $100 (95% CI) | G = $200 | G = $300 |
|---|---|---|---|---|---|---|
| Paper, all | 117 of 682 | +2.1 | +3.6 | −$8.38 (−45.0, +24.3) | −$3.84 | −$15.51 |
| Paper, corrected m1 | 35 of 316 | +4.2 | +5.3 | −$5.60 (−28.6, +12.4) | +$1.07 | −$6.08 |
| Paper, m1_small | 4 of 56 | −54.0 | +4.2 | +$8.10 (0.0, +20.6) | +$8.25 | +$7.71 |
| Replay B, m1 | 126 of 441 | −2.0 | +2.9 | +$9.41 (−21.4, +41.6) | −$1.19 | −$10.29 |
| Replay C, m1 | 116 of 398 | −2.2 | +1.5 | +$8.45 (−21.9, +39.2) | −$2.63 | −$8.49 |
| Replay B, small | 43 of 156 | +1.2 | +1.9 | −$1.93 | −$11.88 | −$11.66 |
| Replay C, small | 39 of 122 | −0.6 | +3.7 | +$0.82 | −$11.73 | −$10.13 |
| Live (1 h) | 1 of 2 | −47 | −45 | +$2.35 | +$2.35 | +$2.35 |

## Conclusion: do not adopt the rule, and do not spend a full replay on it now

1. **The pattern is ordinary.** At G = $100 it covers 17–29% of all fills, because the strategy
   quotes both sides across a ladder of strikes $100 apart. Most such pairs win a little; this hour
   landed in the band.
2. **The effect is inconsistent and indistinguishable from zero.**
   - It helps on the Sep 26–27 replays and on m1_small paper. On m1_small paper that is the same
     two bad near-expiry trades the earlier screen flagged.
   - It hurts on all paper, on corrected m1 paper (the most recent independent m1 sample) and on
     the small-config replays.
   - Every interval spans roughly ±$20–45.
3. **Wider gaps are worse.** G = $200 or $300 removes 30–60% of fills and costs money in most
   samples.
4. **The worst hour is not reliably improved.** In paper-all it gets worse, −$5.03 → −$6.65.

Keep the rule available, off (`block_opposite_neighbor_usd: 0` in m1_live), and re-screen it
once live sessions supply real fills. The hour's −$4.60 was inside both the m1_live $10 and the
old m1_small $5 per-event cap, so no existing limit would have stopped it either. A loss of this
size in one hour is within what the strategy is configured to accept.
