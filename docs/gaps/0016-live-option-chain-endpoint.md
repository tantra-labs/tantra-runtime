# 0016 — Dedicated `GET /api/v2/options/chain` Endpoint (Multi-Expiry Chain, OI/ΔOI, IV & Greeks)

**Status:** Open (2026-11-22)  
**Owner Repo:** `tantra-runtime` (`tantra-runtime-api-rest`, `tantra-market-finance`)  
**Consumers:** `tantra-ui-web` (`OptionChainPanel.tsx`, `src/lib/option-chain.ts`)  
**Related Plans:** `docs/plans/0017-options-training-and-strategy-builder.md`, `docs/plans/0018-quantsapp-tool-models-and-ai-analysis-page.md`

---

## Gap Description

`tantra-runtime-api-rest` already queries `InstrumentChainReader::options_chain(...)` internally inside `POST /studies/experimental-analysis/options-overlay`, `POST /studies/strategy-loop/*`, and `tantra-agent` chat tools (`options_chain`), **but does not yet expose a dedicated read endpoint `GET /api/v2/options/chain`** returning full multi-expiry Call/Put strike rows for interactive option-chain panels.

Until this endpoint lands, `tantra-ui-web` uses `POST /studies/experimental-analysis/options-overlay` (`strike_lab`) + `GET /quotes` and computes missing OI% / IV / Greeks fields deterministically via `src/lib/option-chain.ts` (marked `Theoretical BS Model`).

## Target Contract

`GET /api/v2/options/chain?symbol=NSE:NIFTY50-INDEX&expiry=YYYY-MM-DD`

Response (`Decimal` strings throughout; zero floats):
- `instrument_id`, `spot_price`, `spot_change`, `spot_change_pct`, `lot_size`
- `expiries`: `Array<{ expiry: string; label: string; is_weekly: boolean; dte: number }>`
- `active_expiry`: `string`
- `pcr`, `max_pain_strike`, `atm_strike`, `atm_iv_pct`, `iv_percentile`, `iv_percentile_regime`
- `source`: `"live_chain" | "theoretical_bs"`
- `rows`: `Array<{ strike, is_atm, call: OptionLegSnapshot, put: OptionLegSnapshot }>` where `OptionLegSnapshot` carries `contract_symbol`, `ltp`, `ltp_change_pct`, `bid`, `ask`, `oi`, `oi_change_pct`, `volume`, `iv_pct`, `delta`, `gamma`, `theta`, `vega`.

## Acceptance Criteria

1. Register `GET /api/v2/options/chain` in `route_table` / `v2.rs` and `OpenApiDocument::v2_json()`.
2. Populate from `InstrumentChainReader::options_chain` without fabricating bid/ask liquidity when the feed is offline.
3. Add REST conformance + handler tests under `tantra-runtime-api-rest/tests/` and update `tantra-ui-web/src/lib/api-schema.d.ts` + `src/lib/option-chain.ts`.
