# 0036 — `PortfolioActor` keeps an unbounded `fills_history` of whole-account clones

**Status**: open
**Filed**: 2026-11-23 — workspace design review (root plan 0021) · **Kind**: design-flaw · **Severity**: medium
**Plan**: [0013](../plans/0013-engine-design-review-2026-11.md)

## What's stubbed or hardcoded

- `crates/tantra-runtime-actors/src/portfolio.rs:30,170`: `fills_history.push((account.clone(), ...))` per fill, iterated again at `:297` for the tax summary.
- `crates/tantra-runtime-actors/src/portfolio.rs:130`: `fill.quantity.0.try_into().unwrap_or(0)` turns a fractional or overflowing quantity into a zero-quantity fill silently.
- `crates/tantra-runtime-actors/src/portfolio.rs:156`: `.get_mut("default").unwrap()`, a production `unwrap`.

## Why

Memory grows O(fills x positions) per day and the history is not persisted, so portfolio state is lost on any actor restart. Fractional quantities (fractional-share markets, crypto) are silently ignored, so the portfolio disagrees with the broker. `unwrap` on a request path breaks `.agents/rules/coding-and-ddd.md` §2 (no panics in production `src/`).

## How to close it

1. Store only fill ids and tax lots (or read from `SqlxPositionStore`) instead of cloning whole accounts per fill; bound the history with a ring or make it persistence-backed.
2. Introduce `PortfolioError::InvalidQuantity` and reject fractional/overflowing quantities explicitly (or support them where the instrument allows) instead of `unwrap_or(0)`.
3. Remove the `unwrap` at `:156`.
4. Does not re-file the known `is_delivery` or `"default"` account items.

**Verification**: proptest in `crates/tantra-runtime-actors/tests/` on fill sequences asserting bounded memory and quantity round-trip; test for the invalid-quantity error; `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, `make check`, `cargo test -p tantra-runtime-actors --profile prod`, `make prod`.

## Related

- Closed `tantra-runtime` gap 0017 (per-caller paper accounts); [0021](0021-durable-trading-state.md) (persistence).
- `tantra-ledger` gap 0022 (bounded state and snapshots), gap 0015 (`apply_fill` ignores side).
