# 0024 — Float conversions of Decimal money on the wire and in feed/exec glue

**Status**: open
**Filed**: 2026-11-23 — workspace design review (root plan 0021) · **Kind**: design-flaw · **Severity**: medium
**Plan**: [0012](../plans/0012-api-design-review-2026-11.md)

## What's stubbed or hardcoded

- `crates/tantra-runtime-api-rest/src/approval.rs:17-20` — `json_number`: `Decimal -> to_f64().unwrap_or(0.0) -> Number::from_f64` for queued-order price/qty on `GET /approvals`; a failed conversion silently becomes `0`.
- `crates/tantra-runtime-api-rest/src/bar_pump.rs:36-40` — OHLCV `Decimal -> f64 .unwrap_or_default()` into the feed `Bar` (the store wire type is f64; provider repo `tantra-market-store`).
- `crates/tantra-runtime-api-rest/src/portfolio_store.rs:118` — `leverage: f64` on the stored `Account` (margin-adjacent).
- `crates/tantra-runtime-api-rest/src/ticks_file.rs:45-56,241-247` and `paper_serve.rs:415-416` — tape ticks, `lot_size`/`tick_size` and `equity_curve(starting: f64, ..)` are all f64.
- `crates/tantra-runtime-api-rest/src/executor.rs:320` — optimizer fitness from `net_pnl.to_f64().unwrap_or(0.0)`; a lossy fallback ranks failures as zero P&L.

## Why

Breaks the zero-floats rule (`.agents/rules/interface-first-and-modularity.md` §2.3, `.agents/rules/coding-and-ddd.md` Rust). `0.1`-style prices and large quantities round-trip lossy, `lot_size`/`tick_size` as f64 can mis-round tick validation, and `unwrap_or(0.0)` hides conversion failure as a valid price. `approval.rs` also forms a second decimal-as-number wire convention next to `tantra-core` decimal-string serde.

## How to close it

1. Serialize approval price/qty as `Price`/`Quantity` decimal strings, matching the rest of v2.
2. Make `Account.leverage` a `Decimal`; parse tape `lot_size`/`tick_size` as `Decimal` from strings; use `Decimal` for the equity-curve start.
3. Rank optimizer fitness by `Decimal` ordering (no `unwrap_or(0.0)`).
4. `bar_pump`: file a gap in the provider repo so `tantra-market-store`'s wire `Bar` carries `Decimal`/`Price`; keep the f64 conversion cited to that gap until it lands.

**Verification**: proptest decimal round-trip for approvals and tape parsing; golden approval JSON; a conversion failure surfaces as an error, never `0`. Gates: `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, `make check`, targeted `cargo test -p tantra-runtime-api-rest --profile prod`.

## Related

- Provider: `tantra-market` (wire `Bar` f64; see its price/exact-decimal ingestion gap 0011).
- Root gap 0015 (pricing and quant math single source) and root gap 0014 (Position/account model) for `Account` shape.
