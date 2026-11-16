# Gap 0008 — Dual Execution-Port Boundary (`BrokerAdapter` vs `tantra_exec_core::BrokerExecutor`)

**Repo**: `tantra-runtime` · **Scope**: `tantra-runtime-strategy::dispatch`
**Status**: closed — option 2 landed (2026-11-16): the sync `SimExecutionPort` port now lives in `tantra-exec-core` (T14.4, root plan 0007); runtime `BrokerAdapter` is a thin re-export; `tantra-exec-paper::PaperExchangeExecutor` implements it. · **Source of truth**: `tantra-exec-core::sim`.

## Problem

`tantra-runtime` owns a **second** broker abstraction alongside
`tantra_exec_core::BrokerExecutor`:

- `BrokerAdapter` (`dispatch.rs`) — **synchronous**, temporally deterministic
  (`submit_order(&order, now: DateTime<Utc>)`), returns `Result<String, String>`.
- `tantra_exec_core::BrokerExecutor` — **asynchronous**, canonical exec port.

`BrokerAdapter` is the *intentional* dual-execution-target port (AGENTS §4.3.3):
the sync/`now`-stamped surface lets the backtest and the paper broker replay
byte-identical fills, while real wire transports delegate to `tantra-exec`
(Zerodha Kite via `tantra_exec_zerodha`, closed gap 0003). The cost is a second
"execution" vocabulary in the runtime and duplicated broker semantics
(`DispatchError` vs `BrokerError` string-mapped in `map_broker_error`).

## Options

1. **Keep `BrokerAdapter` as the runtime's deterministic sim port** (status
   quo, documented here): `BrokerAdapter` = simulation/backtest boundary;
   `BrokerExecutor` = live wire boundary; the `ExecutionActor` composes exec's
   `RiskGatedExecutor`/`RateLimitedBroker` wrappers.
2. **Lift `BrokerAdapter` into `tantra-exec-core`** so both execution ports
   live in the Hand bounded context; the runtime only orchestrates. Larger
   refactor — exec gains the sync port; `dispatch.rs` becomes a thin re-export.
3. **Merge onto `BrokerExecutor`**: lose the deterministic sync surface —
   rejected (breaks research-live parity).

## Acceptance Criteria

- The boundary is documented in `docs/design/code-strategy.md` / this gap so
  new contributors don't fork a third abstraction.
- The runtime's `ExecutionActor` composes exec's wrappers (done — see
  `execution.rs::with_exec_stack`) rather than re-implementing risk/rate logic.
- This gap is removed and recorded as Closed when option 2 lands.

## Decision (2026-11-14, T7.8)

**Option 1 chosen — keep `BrokerAdapter` as the runtime's deterministic sim port.**
Recorded in `docs/design/code-strategy.md` §8.2 and this gap: `BrokerAdapter` is the
sync/`now`-stamped simulation + paper-broker boundary; `tantra_exec_core::BrokerExecutor`
is the live wire port (with `ZerodhaKiteAdapter` delegating to `tantra-exec-zerodha`); the
`ExecutionActor` composes exec's `RiskGatedExecutor`/`RateLimitedBroker` wrappers via
`with_exec_stack`. Option 2 (lift into `tantra-exec-core`) remains tracked as the close
event for this gap; option 3 is rejected (breaks research-live parity).

## Resolution (2026-11-16, T14.4)

**Option 2 landed.** The sync execution port moved into the Hand bounded context:

- `tantra-exec-core::sim::SimExecutionPort` is the new canonical sync
  `now`-stamped execution port (`submit_order(&order, now)` /
  `cancel_order(&id, now)` / `broker_name`, `Send + Sync`, with the `Arc<T>`
  blanket impl) — the exact runtime `BrokerAdapter` shape, mirrored
  verbatim.
- `tantra-runtime-strategy::dispatch::BrokerAdapter` is now a thin re-export:
  `pub use tantra_exec_core::sim::SimExecutionPort as BrokerAdapter;`. The
  `PaperAdapter` / `ZerodhaKiteAdapter` / `AngelOneSmartApiAdapter` impls and
  `Dispatcher` compile unchanged (name and shape preserved).
- `tantra-exec-paper::PaperExchangeExecutor` implements `SimExecutionPort` by
  delegating to its existing deterministic `submit_order_sync` /
  `cancel_order_sync` (zero-I/O, zero wall-clock, explicit `now`), so the
  paper broker replays byte-identically.
- Acceptance criteria met: the boundary is now documented at its home
  (`tantra-exec-core::sim`), the runtime delegates rather than owns a third
  abstraction, and no third execution vocabulary exists.
- Verification green: `cargo check --workspace --profile prod`,
  `cargo test -p tantra-exec-core -p tantra-exec-paper --profile prod`,
  `cargo test -p tantra-runtime-strategy --profile prod`, clippy `-D warnings`,
  and `make prod` in both repos.
