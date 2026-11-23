# 0037 — Backtest silently truncates quantities and maps all non-India venues to NSE fees

**Status**: open
**Filed**: 2026-11-23 — workspace design review (root plan 0021) · **Kind**: design-flaw · **Severity**: medium
**Plan**: [0013](../plans/0013-engine-design-review-2026-11.md)

## What's stubbed or hardcoded

- `crates/tantra-runtime-backtest/src/engine.rs:72` `decimal_qty_to_u64` truncates fractions and saturates negative or oversize values to `0` with no error.
- `crates/tantra-runtime-backtest/src/engine.rs:103`: `CountryCode::Us | Uk | Jp => Exchange::Nse`, so statutory fees for foreign instruments are computed with Indian NSE rates.
- Legacy `kokoro` naming at `crates/tantra-runtime-backtest/src/engine.rs:90,94`.
- `crates/tantra-runtime-backtest/src/engine.rs:105` re-implements add-to-position logic (a workaround for the ledger `apply_fill` side bug).

## Why

Fractional lots (F&O lot sizes, fractional-share markets) are dropped, producing silent zero-quantity fills. Foreign-venue backtests report Indian taxes as if they were true results, which misleads promotion decisions. `kokoro` identifiers break `.agents/rules/coding-and-ddd.md` §3.

## How to close it

1. Return a `QuantityError` from the conversion instead of saturating to zero; propagate it as a run failure.
2. Fail with `unsupported_jurisdiction` (`PortError::unavailable`, paired with a gap in `tantra-ledger`/`tantra-market`) instead of defaulting to NSE for US/UK/JP.
3. Remove `kokoro` identifiers.
4. Delete the local add-to-position logic once `tantra-ledger` gap 0015 lands and call `apply_fill`.

**Verification**: proptest on quantity round-trip and rejection of negative/oversize/fractional values in `crates/tantra-runtime-backtest/tests/`; test asserting a US instrument errors instead of producing NSE fees; `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, `make check`, `cargo test -p tantra-runtime-backtest --profile prod`, `make prod`.

## Related

- `tantra-ledger` gap 0015 (`apply_fill` ignores side; consumer fix at `crates/tantra-runtime-backtest/src/engine.rs:105`).
- Root gap 0012 (fabricated data presented as real), root gap 0021 (simulation/fill-model unification).
