# 0013 — Trade journal and `/ws/live` paper account are not partitioned per caller

**Status**: open

## What's stubbed or hardcoded
Design 0006 R0 (gap 0012) made `/trade-journal/*` and `GET /ws/live` scoped and `Caller`-gated,
so anonymous requests are rejected when auth is on. Both still serve **one shared instance** to
every authenticated caller:
- `/trade-journal/*` reads and writes the single `AppState.journal: Option<Arc<Mutex<JournalStore>>>`
  (`src/state.rs`). `JournalStore` (`tantra-ledger-performance`) has no owner field, so two
  tokens see and edit the same accounts, annotations and notes.
- `GET /ws/live` fans out the one paper-exchange account to every client and accepts
  `place_order` / `cancel_order` against it.

The handlers carry a citation comment pointing here.

## Why
Partitioning needs a per-caller store (or an owner key on journal records) and a per-caller
paper account, which is a state-model change, not an auth fix. R0 was kept to closing the
fail-open routes.

## How to close it
1. Key the journal by `(caller_id, workspace)`: either `DashMap<CallerId, Arc<Mutex<JournalStore>>>`
   in `AppState` (configured store becomes the `DEFAULT_USER_ID` partition) or an owner column once
   the journal is persisted through `tantra-runtime-persistence`.
2. Give each caller its own paper account on `/ws/live` (or fold it into the design 0006 `/stream`
   socket with per-caller `orders`/`fills`/`portfolio` channels).
3. Negative tests: caller A cannot read or modify caller B's journal entries or paper orders.
