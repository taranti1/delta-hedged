# M1.2 Settlement check: BRTI prints vs published `expiration_value`

Date: 2026-09-25/26. Read-only against Kalshi: public GETs unsigned; CF Benchmarks history
through the signed passthrough at 10 % of the account read budget. Reproduce with
`python -m dh.research.settlement_check live --root data` (recorder capture) and
`python -m dh.research.settlement_check history --history data/external/kalshi` (downloaded
BRTI history + every settled KXBTC15M quarter + downloaded KXBTCD/KXBTC markets). Code:
`dh/research/settlement_check.py`; convention: `dh/settlement/convention.py`,
`dh/core/market.py` (`SettlementSpec`, `MarketSpec.yes_wins` / `settle_thresholds`).

## 1. Verdict

**PASS [VERIFIED].** The published `expiration_value` of every KXBTCD, KXBTC and KXBTC15M
expiration checked equals, to the cent, the simple average of the 60 once-per-second BRTI prints
whose CF source timestamps are **T-60 s, T-59 s, ..., T-1 s**, with **T = `close_time`**, rounded
**half up** to 2 decimals:

| data | expirations checked | match `[T-60 s, T)` | pass rate | max \|avg - value\| |
|---|---|---|---|---|
| recorder capture (1 Hz `cfbenchmarks_value`), 2026-09-25 20:00-23:00Z | 13 (4 hourly + 9 quarter-hour) | 13 | **100 %** | $0.0048 |
| CF Benchmarks history (5 Hz passthrough), 2026-06-11 .. 2026-09-25: every KXBTC15M quarter (9,378) + every KXBTCD/KXBTC hour of the last 30 days (697) | 10,075 | 10,015 | **99.40 %** (100 % with the pre-2026-08-21 tie rule, s.2) | $0.0050 |

The M1.2 criterion (>= 99 % of events to $0.01) is met on both sources. Final-minute trading is
not blocked by the settlement convention (BUILD_PLAN K.1 last bullet does not trigger).

Two things we had wrong before this check, both now fixed in code:

1. **T is `close_time`, not `expected_expiration_time`** (= close + 5 min on every KXBTC* market).
   Averaging the 60 s before `expected_expiration_time` misses by $11-$112 (live) and essentially
   never matches (0 of 12 live, 2 of 10,072 history, by chance).
2. **The window is `[T-60 s, T)`, not `(T-60 s, T]`.** "The sixty seconds of BRTI *before* 4 PM"
   is the prints stamped 15:59:00 ... 15:59:59; the print stamped exactly 16:00:00 is not in it.
   The `(T-60 s, T]` window matched 2 of 13 live expirations (only where the two averages happen to
   round alike) and 191 of 10,075 (1.9 %) in the history. **Kalshi's own streamed
   `last_60s_windowed_average_15min` uses `(T-60 s, T]`** (its final value at `window_size` 60
   equals our `(T-60 s, T]` average to 1e-8 on all 13) and therefore is NOT the settlement value:
   on 2026-09-25 20:00Z it read 84028.0878 against a published 84028.57. Use it only as a
   feed-health cross-check.

## 2. Convention (as implemented)

* **T** = `close_time`. `rest_market_to_spec` takes `expiration_ts = close_time`, keeps
  `expected_expiration_time` only as `MarketSpec.expected_expiration_ts` (metadata), and refuses
  (UnsupportedMarket) a market whose `rules_primary` states a different time; the event-ticker
  time (New York time: `KXBTCD-26SEP2515` = 15:00 EDT, `KXBTC15M-26SEP251500`) is checked by
  `metadata.rules_flags` (blocking `settlement_time_mismatch` when the rules text has no parseable
  time). All 30 real sample markets in `docs/kalshi_specs/samples_2026-09-25/` verify
  (`tests/settlement/test_convention.py`).
* **Window**: `SettlementSpec()` default `include_close_tick=False`: observations stamped
  T-60 s .. T-1 s. The average is fully determined one second before the close.
* **Rounding**: the average is rounded half up to cents before the strike comparison
  (`SettlementSpec.round_decimals = 2` for every KXBTC* market). Exact half-cent ties (sum of the
  60 integer-cent prints = 30 mod 60; 157 in the history) pin down the rule, and it CHANGED:
  * since 2026-08-21 ~21:45Z: **exact half up**, 58 of 58 ties (40/40 in September; no live tie
    yet);
  * before (99 ties, 2026-06-11 .. 2026-08-21 20:45Z): `round(sum(dollar floats) / 60, 2)` on
    binary doubles, 99 of 99 (up 39, down 60, decided by float representation noise).
  All 60 history mismatches of the production rule are such pre-change ties, off by exactly $0.01
  (published one cent lower). The near-ties (29 or 31 mod 60, 298 events) match 100 %, so the
  prints carry no hidden sub-cent precision. Production uses exact half up (the current rule); the
  two rules differ by one cent on ~1 in 60 expirations, which matters only at the strike cent.
* **Strikes**: KXBTCD `greater` (strict "above"; strikes at X.99, so YES iff the rounded value
  >= X+1.00, i.e. the unrounded average >= X.995); KXBTC ranges `between` (inclusive) plus
  `greater`/`less` tails; KXBTC15M `greater_or_equal` against the previous quarter's
  `expiration_value` (the next quarter's `floor_strike` arrives in a `metadata_updated` lifecycle
  message ~0.5 s after the close, e.g. 21:00:00.54Z). The continuous pricing model uses the
  equivalent thresholds on the unrounded average (`MarketSpec.settle_thresholds`).
* **Same value across series**: every expiration shared by KXBTCD, KXBTC and KXBTC15M carried one
  identical `expiration_value` (0 disagreements live and in history; the one apparent disagreement,
  2026-08-28 12:00Z, is KXBTC15M publishing "79,604.96" with a thousands separator, as also on
  2026-09-01 20:30Z "77,362.10": parse `expiration_value` defensively), so a settlement event is
  one expiration time (the research clustering is right).
* **Missing data resolves No** (contract terms). A missing print inside the window is counted
  (`WindowState.n_missing`) and the strategy stops quoting every market of that expiration whose
  YES band exceeds 2c (`brti_gap_in_window`, `quoting.window_gap_max_yes_p`).

## 3. Data and method

* **Prints.** Recorder: `cfbenchmarks_value` (1 Hz) frames carry the CF frame
  `{"time": <ms>, "value": "83854.69"}`; every one of 11,309 1 Hz ticks was stamped on a whole
  second and had 2 decimals, and the 5 Hz `cfbenchmarks_value_5hz` tick stamped on the same second
  had the identical value (11,309 / 11,309). History: `GET /cfbenchmarks/history/values?id=BRTI&
  timespan=HOUR&timestamp=<hour START>` returns the 18,000 5 Hz ticks of `[timestamp, timestamp +
  1 h)` (verified: timestamp 20:00:00.000Z -> 20:00:00.000 .. 20:59:59.800, exact 200 ms steps;
  the timestamp is the window START). The on-the-second tick is the 1 Hz print.
* **Published values.** `GET /markets?series_ticker=S&status=settled` and
  `GET /historical/markets?series_ticker=KXBTC15M` (26,961 quarters, 2025-12-10 .. 2026-09-25,
  unsigned), plus the downloaded KXBTCD / KXBTC markets.
* **Arithmetic.** Integer cents: sum of 60 prints S, rounded value (2S + 60) // 120, compared with
  the published value in cents. A window with any missing second is "not observable", never a
  mismatch.
* **Alternatives tested** (`dh/research/settlement_check.WINDOWS`): `(T-60,T]`, `[T-60,T]` (61
  prints), `(T-59,T+1]`, `[T-61,T-1)`, the 60 s before `expected_expiration_time`, and the mean of
  all 5 Hz ticks in `[T-60,T)`.

## 4. Results by window

Live (recorder, 13 expirations 2026-09-25 20:00-23:00Z):

| window | matches | pass rate | median \|diff\| | max \|diff\| |
|---|---|---|---|---|
| **`[T-60,T)` (production)** | 13 | **100 %** | $0.0030 | $0.0048 |
| `(T-60,T]` (Kalshi's `last_60s_windowed_average_15min`) | 2 | 15 % | $0.034 | $0.48 |
| `[T-60,T]` (61 prints) | 2 | 15 % | $0.12 | $0.35 |
| `(T-59,T+1]` | 1 | 8 % | $0.055 | $0.96 |
| `[T-61,T-1)` | 2 | 15 % | $0.075 | $0.53 |
| 60 s before `expected_expiration_time` | 0 / 12 | 0 % | $42 | $112 |
| mean of all 5 Hz ticks in `[T-60,T)` | 2 | 15 % | $0.11 | $0.39 |

History (CF passthrough; 10,075 expirations 2026-06-11 .. 2026-09-25; 9 more not observable):

| window | matches | pass rate | median \|diff\| | max \|diff\| |
|---|---|---|---|---|
| **`[T-60,T)` (production)** | 10,015 | **99.40 %** | $0.0025 | $0.0050 |
| `(T-60,T]` | 191 | 1.9 % | $0.19 | $7.02 |
| `[T-60,T]` (61 prints) | 420 | 4.2 % | $0.10 | $4.50 |
| `(T-59,T+1]` | 102 | 1.0 % | $0.39 | $13.14 |
| `[T-61,T-1)` | 208 | 2.1 % | $0.19 | $7.57 |
| 60 s before `expected_expiration_time` | 2 | 0.02 % | $33.66 | $1,708 |
| mean of all 5 Hz ticks in `[T-60,T)` | 282 | 2.8 % | $0.10 | $2.61 |

Mismatches: only the 60 pre-2026-08-21 exact ties (s.2), each off by exactly one cent. Counting the
9 unobservable windows as failures too, the pass rate is still 99.32 %.

**Not observable (9 of 10,084, 0.09 %)**: in 8 windows the CF history itself has 4-29 missing
seconds (no 5 Hz tick at all; e.g. 2026-07-02 20:15Z, 2026-09-01 20:15Z; mostly around 16:15 EDT)
and the ninth is the "77,362.10" formatting case (it matches once parsed). Kalshi nevertheless
published normal values and results for the 8 gap windows, and those values match neither a
carry-forward of the last tick nor the average of the prints present (e.g. 2026-09-01 20:15Z:
published 77362.28, carry-forward 77357.61, average of the 31 present prints 77374.33). So these
are holes in the CF *history* API, not benchmark outages resolved to No: the history store is not
a complete record of the settlement prints, and the live WS capture stays the primary source.

## 5. Between close and settlement: how Kalshi presents a market (answers RUNBOOK / riskstate question)

Observed on the recorder's `kalshi.ws` capture (every KXBTC* close 20:00-23:00Z, `data/results/
settlement_check/close_to_settlement_ws.csv`) and on a 1-2 s poll of `GET /markets/{ticker}`
around the 23:00Z close (KXBTC15M-26SEP251900-00 and KXBTCD-26SEP2519-T84099.99):

| time after close_time | WebSocket | REST `GET /markets/{t}` |
|---|---|---|
| +0 .. +0.01 s | a few `orderbook_delta`s stamped at the close (in-flight) ; **no trade after the close, ever** | `status: closed` from the first poll (+0.44 s) |
| +0.02 .. +3.2 s | `orderbook_snapshot` with an **empty book** (the exchange cancels every resting order); then `ticker` with `yes_bid 0.0000 / yes_ask 1.0000`, `price_dollars` = last trade, volume and OI unchanged | quote fields STALE for several seconds: the 15M market kept the pre-close 0.991/0.996 until >= +8.8 s; the KXBTCD market switched to 0/1.00 between +2.6 and +4.7 s |
| KXBTC15M: +0.75 .. +1.34 s (median ~1.1 s) | `market_lifecycle_v2 determined` (`result`, `settlement_value`, `determination_ts`) | `determined` never seen: `closed` until >= +8.8 s, then `finalized` |
| KXBTC15M: +6.5 .. +8.2 s | `settled` | `finalized` from +10.9 s with `result`, `expiration_value`, `settlement_value_dollars`, `settlement_ts` (+6.5 s) |
| KXBTCD / KXBTC: +82 .. +96 s (20:00Z: +283 s) | `determined` for every market of the event within 1 ms | `determined` first seen +106.8 s (WS +91.6 s) with `result` and `expiration_value` |
| KXBTCD / KXBTC: +148 .. +158 s (20:00Z: +348 s) | `settled` (several duplicate `settled` messages per market: 8 per market at 21:00Z) | `finalized` first seen +167.6 s (`settlement_ts` +156.5 s) |

Historical settlement lags from `settlement_ts - close_time`: KXBTC15M (26,961 quarters since
2025-12-10) median 11.6 s, p90 147 s, p99 32 min, max 24 h; 33 % above 60 s. KXBTCD (last 45
downloaded events) median 158 s, max 348 s. So the lag is usually seconds (15M) or ~2.5 min
(hourly) but has a long tail: design for "determination may take a day" (contract terms Rule 7.1).

**Implemented** (2026-09-26): `dh/settlement/closemark.py` (the rules below), used by
`dh/live/riskstate.py` (`mark_px`, `open_marks`, `backfill_window_prints`: start-up), `dh/strategy/mm.py`
(`close_marks`: the strategy's own positions) and `dh/live/runner.py` (`_remark_excluded`); RUNBOOK section 8.

**What `dh/live/riskstate.py` `mark_px` should do** (not edited here; `dh/live` is another
agent's area). Today it marks a held position by: `result` present -> payout; else the YES bid
(long) / ask (short); else, when not `active`, the last trade. After this check:

1. `result` present (`determined` / `finalized`, WS `determined` or REST) -> payout, as now. Prefer
   the WS `determined` message: REST lags it by ~10-15 s (hourly) and may never show
   `determined` for a 15-minute market.
2. **Closed, no result yet: the outcome is already known.** The window `[T-60 s, T)` is complete
   one second BEFORE the close, so from the recorded BRTI prints (`SettlementTracker.
   settlement_value(spec, T, rounded=True)` + `MarketSpec.yes_wins`) the mark is the exact payout
   (0 or 1), unless (a) a print of the window is missing (`n_missing > 0`: contract says "No" if
   data is incomplete, so mark YES longs at 0 / keep the position flagged), or (b) the rounded
   average is within $0.01 of the strike (then use 0.5 +- the model, or the last trade, flagged).
3. Never use REST `yes_bid` / `yes_ask` of a market whose `close_time` has passed: they are stale
   pre-close quotes for up to ~9 s, then the empty book 0 / 1 (a long would be "unmarkable" and
   fall to the last trade, which can be far from the determined outcome when the average moved in
   the final minute). The last trade is only a fallback when no BRTI prints are available.
4. The day-P&L restart path ("restarting in the minutes between a held market's close and its
   determination") therefore needs the BRTI prints of `[T-60 s, T)`: back-fill them with one CF
   passthrough call (`timespan=HOUR`, `timestamp=<hour start>`) when the recorder did not see them.

## 6. BRTI data quality

Recorder, current session (20:17Z onward): no missing 1 Hz second. Earlier gaps (19:55, 20:01,
20:06, 20:07, 20:17; 8-15 s each) were recorder restarts, not feed gaps. Every 1 Hz tick had a
whole-second CF stamp and 2 decimals, and the 5 Hz tick stamped on the same second always had the
same value, so the 5 Hz stream is a valid fallback for a missing 1 Hz print. History: 18,000 ticks
per full hour in the hours sampled, with occasional multi-second holes (8 settlement windows, above).

## 7. Files

* `dh/settlement/convention.py` (new), `dh/core/market.py`, `dh/settlement/window.py`,
  `dh/kalshi/normalize.py` (`default_settlement`, `rest_market_to_spec`), `dh/kalshi/metadata.py`,
  `dh/models/fairvalue.py`, `dh/strategy/{mm,scenario,config}.py`, `dh/research/kalshi_data.py`,
  `dh/research/settlement_check.py` (new), `scripts/download_kalshi_history.py`.
* Tests: `tests/settlement/test_convention.py` (real sample markets and the 13 recorded
  expirations as fixtures, `tests/settlement/fixtures/brti_live_2026-09-25.json`),
  `tests/research/test_settlement_check.py`, `tests/strategy/test_audit2_regressions.py`
  (`brti_gap_in_window`).
* Results: `data/results/settlement_check/settlement_check_{live,history}.csv`,
  `close_to_settlement_ws.csv` (local, gitignored).
