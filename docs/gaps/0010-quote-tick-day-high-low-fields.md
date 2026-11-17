# 0010 — `QuoteTick` (`GET /quotes`) missing session `day_high`, `day_low`, `prev_close`, and `volume` fields

**Status**: open

## What's stubbed or hardcoded

`tantra_runtime_api::wire::QuoteTick` (`crates/tantra-runtime-api/src/wire.rs:525`, returned by `GET /quotes`) only carries top-of-book fields:

- `instrument`
- `bid`, `ask`, `bid_qty`, `ask_qty`
- `exchange_ts`, `receive_ts`

It does not expose intraday session summary fields (`day_high`, `day_low`, `prev_close`, `last_price`, `volume`). Downstream consumers in `tantra-ui-web` (`src/lib/vyapaar-runtime-client.ts`, `/watchlists/[id]` Day range column) currently have to cross-reference `GET /bars/{id}` for daily `high`/`low` or leave `dayHigh`/`dayLow` empty when live quotes are used without daily bar enrichment.

## Why

Top-of-book `QuoteTick` was kept minimal to mirror `tantra_core::Tick`'s quote arm cleanly without snapshot aggregation in `MarketDataReader` (related to `docs/gaps/0007` for chat market-reader approximations of `lastPrice`/`volume`).

## How to close it

1. Extend `QuoteTick` (or add a dedicated `/quotes/summary` response schema in `tantra-runtime-api::wire` / `tantra-runtime-api-rest`) with optional `last_price`, `day_high`, `day_low`, `prev_close`, and `volume` fields populated from the feed / session aggregator.
2. Regenerate the OpenAPI schema (`docs/gaps/0005`) and map `dayHigh`/`dayLow` directly in `tantra-ui-web/src/lib/vyapaar-runtime-client.ts`.
