# 0027 — Options-chain endpoint lacks OI-change %, LTP-change % and market IV/skew, so the web client fabricates them

**Status**: open
**Filed**: 2026-11-23 — workspace design review (root plan 0021) · **Kind**: design-flaw · **Severity**: high
**Plan**: [0012](../plans/0012-api-design-review-2026-11.md)

## What's stubbed or hardcoded

- `GET /api/v2/options/chain` (`OptionsChainResponse` / `OptionChainRowDto` / `OptionLegSnapshotDto` in `tantra-runtime-api`, handler in `v2_canonical.rs`) carries no OI-change %, LTP-change % or market IV/skew fields, and returns null OI when the OI store is empty.
- Consequence in the consumer: `tantra-ui-web/src/lib/option-chain.ts:461-470` falls back to a synthetic OI profile when `open_interest` is falsy and always computes `ltpChangePct` from hard-coded formulas (landed 2026-11-23 in tantra-ui-web commit `59f02d1`). Related fabrication in the same file: `deterministicOiProfile` (`:418-435`, a gaussian bell with hardcoded bias constants), `oiChangePct` always synthetic (`:475,490`), IV skew hardcoded at `0.22`/`0.18` per step (`:451-452`); max pain and PCR (`:383-415`) are computed from the synthetic OI.

## Why

A trader sees a PCR, max pain and OI buildup that are invented. `.agents/rules/interface-first-and-modularity.md` §1.3 requires an explicit `PortError::unavailable`, not fake defaults, and `.agents/rules/gap-tracking.md` requires every stub to be filed with the provider.

## How to close it

1. Add `oi_change` (and `oi_change_pct`), `ltp_change_pct` and market `iv` (per strike/leg, enough for skew) to `OptionChainRowDto`/`OptionLegSnapshotDto` in `tantra-runtime-api`, derived from the OI store, prior close and the quote store.
2. When a value is unavailable (empty OI store, no prior close, no IV quote), return an explicit `unavailable` marker (or typed null with a reason), never a default.
3. Compute PCR and max pain on the server only from rows with live OI, and mark them unavailable otherwise.
4. Regenerate `schema/openapi.v2.json` and the TS client types; consumer cleanup is `tantra-ui-web` gap 0079.

**Verification**: tests in `tantra-runtime-api` (`options_chain` analytics with and without OI) and `tantra-runtime-api-rest` (empty OI store yields `unavailable`, not zeros); `openapi_conformance` green. Gates: `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, `make check`, targeted `cargo test -p tantra-runtime-api-rest --test openapi_conformance --profile prod`.

## Related

- Root gap 0012 (fabricated or synthetic data presented as real); `tantra-ui-web` gap 0079 (consumer side). Pricing inputs: `tantra-market`.
- Follows closed gap 0016 (the dedicated options-chain endpoint).
