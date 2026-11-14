# Shared Ledger — the runtime's ledger-repository handle (docs/gaps/0006 design decision)

**Decision date**: 2026-11-14 (T7 remaining batch)
**Status**: Implemented — the decision below is what closes the portfolio-backed half of
`docs/gaps/0006-chat-agent-session-persistence-and-portfolio-tools.md` (gap doc removed; the
session-persistence half closed earlier in `933c53a`).

## Problem

The chat agent's portfolio-backed tools (`LedgerPortfolioReader`, in
`tantra-runtime-api::chat_adapters`) need the four `tantra-ledger` repository trait objects
`AccountRepository + PositionRepository + OrderRepository + LedgerRepository`. `AppState` held no
ledger-repository handle of any kind, so `chat.rs` answered every portfolio tool with
`PortError::unavailable` — deliberately, so an LLM could not mistake "not wired up" for "this
account has zero positions".

## Decision

`AppState` holds one **`SharedLedger`** (`tantra-runtime-api::shared_ledger`): a cloneable handle
over the four in-memory `tantra-ledger` repository implementations
([`InMemoryAccountRepository`], [`InMemoryPositionRepository`], [`InMemoryOrderRepository`],
[`InMemoryLedgerRepository`]) plus a **matched-lot history** (`ClosedLotEntry`), each behind its
own `Mutex`.

- **Reader side**: `SharedLedger` implements all four repository traits, so the same
  `Arc<SharedLedger>` coerces to each of the four `Arc<dyn Trait>` handles
  `LedgerPortfolioReader::new` needs — one handle, four trait objects, always mutually
  consistent. The trait `&mut self` receivers are satisfied by internal locking (never used
  through the shared handle; the writer side uses the `record_*` methods below).
- **Writer side**: the order/fill flow populates it through `&self` `record_*` methods:
  `record_account`, `record_order`, `record_fill` (a buy opens a FIFO `TaxLot` via
  `save_lot`; a sell matches via `match_lots`, accumulates the realized `MatchedLot`s into the
  closed-lot history, and adjusts the account cash by the fill notional), and
  `update_order_fill` (keeps the order row's fill state in lockstep with the REST ledger).
- **Honesty contract**: the handle starts empty and is only ever populated from the real order
  flow (`orders.rs` opens the default `rest-api` account and records every approved placement;
  `exec.rs` records every simulated fill), so an empty `accounts()`/`positions()` answer
  genuinely means "no trading activity in this process" — never fabricated data. This is what
  allowed the explicit `PortError::unavailable` stub to be removed.

## Wire contract

- **Default account**: `AccountId::from("rest-api")` = the REST write-ledger algo id
  (`orders::REST_ALGO_ID`). Every approved `POST /orders` placement opens it (cash `0`) and
  records its order row; every simulated fill moves cash and FIFO lots.
- **Orders** are keyed by the same idempotency key the REST ledger uses
  (`client_order_id`), so `LedgerPortfolioReader::orders` mirrors `GET /orders`.
- **FIFO semantics** are owned by `tantra-ledger-portfolio`'s `AccountState` (buys open lots,
  sells match oldest-lot-first, realized P&L `(exit − entry) × qty`), as in the ledger's own
  `repository_and_sizing_tests`.
- **Tax/statements**: the closed-lot history is keyed by account and carries the instrument,
  entry/exit prices, realized P&L, holding period, long-term flag, and match timestamp —
  `FeedTaxReportReader` (docs/gaps/0007 item 19) filters it per account/date range.

## Files

- `tantra-runtime-api/src/shared_ledger.rs` — `SharedLedger` + `ClosedLotEntry`.
- `tantra-runtime-api-rest/src/state.rs` — `AppState::shared_ledger`.
- `tantra-runtime-api-rest/src/orders.rs` — `record_into_shared` (account + order on approval).
- `tantra-runtime-api-rest/src/exec.rs` — `record_fill` + `update_order_fill` on every sim fill.
- `tantra-runtime-api-rest/src/chat.rs` — `LedgerPortfolioReader` built from the shared ledger.
- Tests: `tantra-runtime-api/tests/shared_ledger_tests.rs` (6), plus the order-flow→portfolio/tax
  integration in `tantra-runtime-api-rest/tests/chat_remaining_t7_tests.rs`.

## Follow-up

- The in-memory repositories are process-scoped; a restart empties them. Persisting them (or
  switching to a SQL-backed repository implementation) is a larger, separate piece — tracked by
  the runtime's follow-on work, not by a gap (nothing is stubbed or hardcoded).
