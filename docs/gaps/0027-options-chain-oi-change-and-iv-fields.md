# 0027 — Options-chain fields encode unavailable data as zero or model values, with no per-field provenance

**Status**: open
**Filed**: 2026-11-23 — workspace design review (root plan 0021) · **Kind**: design-flaw · **Severity**: high
**Plan**: [0012](../plans/0012-api-design-review-2026-11.md)

## What's stubbed or hardcoded

`GET /api/v2/options/chain` does return `oi`, `oi_change_pct`, `ltp_change_pct` and `iv_pct` per leg
(`crates/tantra-runtime-api/src/options_chain.rs:76-90`), but every one of them is a non-optional `Decimal`, and the
handler fills missing data with zeros or model output that is indistinguishable from market data:

- `crates/tantra-runtime-api-rest/src/options_chain.rs:321` — `let mut ltp_change_pct = Decimal::ZERO;` is only
  overwritten when a prior quote exists (`:335-339`); with no previous price the client receives `0.00` (a flat day).
- `crates/tantra-runtime-api-rest/src/options_chain.rs:282` — `resolve_oi_with_change` returns
  `(direct_oi.unwrap_or(Decimal::ZERO), Decimal::ZERO)`; further zero fallbacks at `:291` and `:297`. An empty OI store
  therefore yields OI `0` and OI-change `0`, which then feed PCR / max pain.
- `crates/tantra-runtime-api-rest/src/options_chain.rs:257-272` — `skew_iv_pct` synthesises a per-strike IV from the ATM
  IV plus `|offset| × 0.22` (+ `|offset| × 0.18` for puts), floored at 6 %; the result is returned as `iv_pct` and drives
  the Black-Scholes greeks. The same constants are duplicated in the web fallback (`tantra-ui-web/src/lib/option-chain.ts:451-452`).
- Provenance exists only at response level — `OptionChainSource { LiveChain, TheoreticalBs }`
  (`crates/tantra-runtime-api/src/options_chain.rs:48-53`) — so a `LiveChain` response can still mix quoted values with
  zero-filled OI and modelled IV, with no way for a consumer to tell which is which.

## Why

A trader reading PCR, max pain, OI build-up or IV skew cannot distinguish real market data from zeros and model output.
`.agents/rules/interface-first-and-modularity.md` §1.3 requires an explicit `unavailable` marker instead of fake
defaults; `.agents/rules/gap-tracking.md` requires stand-ins to be filed. The consumer
(`tantra-ui-web` gap 0079) cannot render honestly until the wire contract can say "unknown".

## How to close it

1. Make the leg fields optional or tagged in `OptionLegSnapshotDto` — e.g. `oi: Option<Decimal>`,
   `oi_change_pct: Option<Decimal>`, `ltp_change_pct: Option<Decimal>` — plus a per-field (or per-leg) provenance enum
   `Quoted | Derived | Modelled | Unavailable` for `iv_pct` and the greeks.
2. In the handler, return `None`/`Unavailable` instead of `Decimal::ZERO` when there is no prior quote or OI data; tag
   `skew_iv_pct` output as `Modelled`.
3. Compute PCR and max pain only from legs with real OI; return them as unavailable otherwise.
4. Bump the response schema version, regenerate `schema/openapi.v2.json` and the TS types; the consumer cleanup is
   `tantra-ui-web` gap 0079.

**Verification**: tests in `crates/tantra-runtime-api-rest/tests/` covering an empty OI store and a missing prior quote
(fields come back `null`/`Unavailable`, never `0`), and a modelled-IV case tagged `Modelled`; `openapi_conformance`
green. Gates: `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, `make check`,
`cargo test -p tantra-runtime-api-rest --test openapi_conformance --profile prod`, `make prod`.

## Related

- Root gap 0012 (fabricated or synthetic data presented as real); `tantra-ui-web` gap 0079 (consumer side).
- Root gap 0015 (single source for pricing/IV math — the skew model is duplicated in TS).
- Follows closed gap 0016 (the dedicated options-chain endpoint).
