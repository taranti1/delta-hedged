# delta-hedged

Deterministic research and trading system for passive market making on Kalshi BTC threshold
markets (`KXBTCD` and related), with portfolio-level BTC delta hedging only when it pays.

Start here:
1. `docs/00_PREMISE_CHALLENGE.md`: what we are building, why the naive version fails, and the
   hypotheses that data must confirm or kill.
2. `docs/ENVIRONMENT.md`: data access, reference provenance (official Kalshi specs vendored in
   `docs/kalshi_specs/`).
3. `docs/INTERFACES.md`: module contracts.

Status: under active construction. See the docs for current results; every Kalshi-specific
performance number is labeled as an estimate until measured on captured data.

```sh
uv venv .venv && . .venv/bin/activate && uv pip install -e '.[dev]'
pytest -q
```
