# Kalshi reference files: provenance

All files fetched 2026-09-25 (19:03-19:12 UTC) with plain unauthenticated HTTP GETs. Index:
https://docs.kalshi.com/llms.txt (section "OpenAPI Specs" / "AsyncAPI Specs").

| File | Source URL | Version (`info.version`) | sha256 |
|---|---|---|---|
| `openapi.yaml` (Predictions REST) | https://docs.kalshi.com/openapi.yaml | 3.31.0 | `7a870e939ec61793ff31d04e89c40a85246a82d5a60e0f3803a45806444baa97` |
| `asyncapi.yaml` (Predictions WebSocket) | https://docs.kalshi.com/asyncapi.yaml | 2.0.0 (content newer than the pinned 2.0.0 copy) | `1fe32a4b7c63fe6b98b09cdb8a2510ae14cc09764c0679feb7d2fe772a023537` |
| `perps_openapi.yaml` (Perps/margin REST) | https://docs.kalshi.com/perps_openapi.yaml | 0.0.1 | `ee12ad15285f70c6a1d456f0eccf5e7796aef264bd97b58c5bb349a866180df5` |
| `perps_asyncapi.yaml` (Perps/margin WebSocket) | https://docs.kalshi.com/perps_asyncapi.yaml | 2.0.0 | `309a119d47095efeddbdc8d8da5ec544fcc3b14893befbde7dfd2f0b8f1a8b53` |
| `perps_scm_openapi.yaml` (Klear clearing-member API) | https://docs.kalshi.com/perps_scm_openapi.yaml | 0.0.1 | `f93ec9f138ea087451351272a6de64b90a7b2df280f3e4ef69779e1fe7593bc6` |
| `llms.txt` (doc index) | https://docs.kalshi.com/llms.txt | - | `4cfc2a82e2ecd5fcdbb30fb5891088c4e0f668cd7cdd989100f0cb3913507b95` |
| `contract_terms/BTC.pdf` (KXBTCD, KXBTC `contract_terms_url`) | https://assets.kalshi.com/contract_terms/BTC.pdf | - | `e7d857369971e75e9db14c5e2d91c29b94eb9a06e83e2acd9777991c4f2a0e2f` |
| `contract_terms/CRYPTO.pdf` (KXBTC15M `contract_terms_url`) | https://assets.kalshi.com/contract_terms/CRYPTO.pdf | - | `fde90b9c0825df277b0b2b2be6239af221eafd01a624c1b2d3a9eaff2d6fe75c` |
| `samples_2026-09-25/series_*.json`, `markets_*.json`, `historical_cutoff.json` | public GETs on https://api.elections.kalshi.com/trade-api/v2 (`/series/{t}`, `/markets?series_ticker=..&status=open|settled&limit=..`, `/historical/cutoff`) | - | see `shasum -a 256 samples_2026-09-25/*` |

Replaced pinned copies (still in git history, commit d879f16):
`openapi.yaml` 3.30.0 sha256 `75098a17c5363226b0419120f233ce7bd1533fed897bc98496f7608bcf9e3199` and
`asyncapi.yaml` 2.0.0 sha256 `fdeb90edfa18d0798ab1c97ac55d2153d0a59f69d4c7e4513612c0f51cc945ba`
(both copied 2026-09-24 from the sibling repo). The diff and its impact on `dh` are in
`docs/research/KALSHI_DOCS_RECONCILIATION.md`.

Not obtained: the fee schedule (https://kalshi.com/docs/kalshi-fee-schedule.pdf and
https://kalshi.com/regulatory/fee-schedule) answers HTTP 429 with a "Vercel Security Checkpoint"
bot-detection page; it was not bypassed. Download it manually in a browser.
