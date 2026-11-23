# 0021 — Trading-critical state is process-memory only and grows without bound (orders, exec maps, approvals, halts, jobs)

**Status**: open
**Filed**: 2026-11-23 — workspace design review (root plan 0021) · **Kind**: design-flaw · **Severity**: high
**Plan**: [0012](../plans/0012-api-design-review-2026-11.md)

## What's stubbed or hardcoded

- `crates/tantra-runtime-api-rest/src/orders.rs:387-395` — `OrderLedger` is three `Arc<Mutex<BTreeMap<String, ..>>>` (orders, refusals, owners); inserts at 500, 522, 559-560; no eviction, cap or TTL (`rg "evict|MAX_ORDERS"` empty). `state.rs:191-193` documents "a restart empties it".
- `crates/tantra-runtime-api-rest/src/exec.rs:65-77` — `ledger_to_broker`, `broker_to_ledger`, `cum_qty` `HashMap`s plus `last_seen_fills`, all unbounded and unpersisted; the whole `PaperExchange` sits in one `Mutex`.
- `crates/tantra-runtime-api-rest/src/approval.rs:27,31-39` — `ApprovalQueue` is an in-memory `VecDeque` with `Instant` timestamps (not restart-safe, not wall time).
- `crates/tantra-runtime-api-rest/src/state.rs:222` `chat_sessions` in-memory fallback; `:149` `strategy_loops`; `:167-168` operator trading state "never restored" from its journal.
- `crates/tantra-runtime-api-rest/src/trading_state.rs` — halt handlers have no store; `RiskActor` halts live only in `RiskEngine` memory (`crates/tantra-runtime-actors/src/risk.rs:246,256`).
- Migrations hold positions, position_lots, traders, agent_chat_session/message, fills, decisions, orders, sebi_audit_ledger — no `approvals`, `trading_state`/halts or `jobs` table.

## Why

A restart loses idempotency keys, so a client retrying `POST /orders` after a crash re-places the order; the operator `Halted` state resets to `Active` (a halt is silently lifted); pending approvals vanish; a long-running instance leaks per-order memory; with N replicas a halt on replica A does not stop replica B. Violates `.agents/rules/scalability-and-concurrency.md` §4 and blocks multi-replica deployment (root plan 0022).

## How to close it

1. Define `OrderRepository`, `ApprovalRepository` and `TradingStateStore` ports (typed `PortError`) in `tantra-runtime-api`.
2. Add migrations for `trading_state`, `approval_queue` and `jobs`; implement the ports on `tantra-runtime-persistence::Database` (SQLite WAL / Postgres).
3. Restore trading state in `prepare_server`; fail closed (halted) if the load fails.
4. Add retention for terminal order rows and bound the exec maps.
5. Cross-replica halt via Postgres `LISTEN/NOTIFY` or polling.

**Verification**: restart round-trip tests — build state, place an order, drop, rebuild from the same DB; idempotent replay returns the same order; halt and pending approvals persist; load failure yields halted. Gates: `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, `make check`, targeted `cargo test -p tantra-runtime-persistence --profile prod`, `make prod`.

## Related

- Extends root plan 0019 P2.7 (file stores / persistence wiring for portfolio, profile, run stores); this gap is the order/approval/halt path it does not list. See also root plan 0022.
- Gap 0017 (per-caller paper accounts); gap 0036 (portfolio actor bounded history).
