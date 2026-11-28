# 0039 — Live risk gate has no price-band / freeze-limit loader; REST and backtest engines run `Permissive`

**Status**: open
**Filed**: 2026-11-27 — closing `tantra-risk` gap 0006 (fail-closed reference data) · **Kind**: stub · **Severity**: high

## What's stubbed or hardcoded

- `tantra-risk` 0006 made `RiskLimits::strictness` default to `FailClosed`: market orders under a collar or
  notional limit need a `PriceBand`, F&O orders (NFO/MCX/CDS) need a `freeze_limits` entry, bands older than
  `max_reference_age_ms` (or with no `PriceBand::as_of`) are refused.
- Nothing in `tantra-runtime` loads exchange price bands or freeze limits into a `RiskEngine`
  (`set_price_band` has no production caller; `freeze_limits` only comes from config TOML).
- So the paper REST gate (`tantra-runtime-api-rest/src/state.rs`, `AppState::new`) and the backtest/sweep engine
  (`tantra-runtime-api-rest/src/executor.rs`) are built with `RiskStrictness::Permissive` explicitly, citing
  this gap, to keep today's behaviour.

## Why

A live deployment wired the same way would approve F&O and market orders without band/freeze protection, or —
once switched to `FailClosed` — refuse every such order. Either is wrong for live.

## How to close it

1. A reference-data port (bands with `as_of`, freeze quantities per instrument) fed from the instrument master /
   exchange circuit files (`tantra-market` / `tantra-data` provider), refreshed each trading day.
2. A runtime actor that applies it to the shared `RiskEngine` (see `tantra-risk` 0010 for shared state).
3. Build the REST/live gate with `FailClosed` and `max_reference_age_ms`; keep `Permissive` only for backtest/research.

**Verification**: an integration test where a REST F&O order is refused without loaded data and approved after the
loader runs; `make check`, targeted `cargo test -p tantra-runtime-api-rest --test orders_risk --profile prod`.

## Related

- `tantra-risk` gap 0006 (closed with this filing), 0010 (shared risk state).
- Root gap 0013 (single shared fail-closed risk gate), 0019 (instrument master / calendar sources).
