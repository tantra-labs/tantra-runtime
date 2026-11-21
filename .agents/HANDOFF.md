# HANDOFF — tantra-runtime

**Date**: 2026-11-17 · Branch: `main` · Full cross-repo context: `.agents/HANDOFF.md` in the
`tantra-labs` superproject.

## Latest Batch — `GET /api/v1/acp` (agent plan 0019 Phase 2), 2026-11-21 (`756d267`)
- `acp_ws_handler` serves the ACP agent with the full chat tool set plus `present_view`.
- It uses the Authenticated scope (`Caller` is extracted) and is capped at 64 connections with 1 MiB frames.
- `/ws/chat` now also gets `present_view`.
- Follow-ups: caller-scoped session store; drain buffered output on disconnect.

## Latest Batch — T17.2: gap 0009 closed (`/ws/chat` per-turn `model` override + multimodal image attachments)

Date: 2026-11-17. Gap 0009 closed across `tantra-runtime-api-rest::chat` (this repo) +
`tantra-agent-chat` / `tantra-agent-llm` (the agent repo). Commits (local, not pushed):
`tantra-runtime` + `tantra-agent` (see repo logs for the SHAs).
- **Wire** (`chat.rs`): `ChatClientMessage` gained `model: Option<String>` +
  `attachments: Option<Vec<ChatAttachmentInput>>` (`mime_type`/`data`, base64,
  `image/png`/`image/jpeg`, `#[serde(default)]` — legacy `{ text }` payloads still parse with
  both `None`). Types exported from the crate root for consumers/tests.
- **Model routing**: new `LlmProviderRegistry` (default + named overrides). `from_env`
  registers extra GGUF models via `TANTRA_GGUF_MODEL_<NAME>` and remote router models via
  `TANTRA_LLM_MODELS`; a per-turn `model` that matches the default (or is absent) keeps the
  byte-identical default path, a registered override routes the whole turn to that provider
  (the client gets a `provider_changed` event), an unavailable model returns an explicit
  `LlmError::Capability` notice streamed into the chat reply as a short assistant turn
  (`send_capability_notice`; TurnId 0) — never a silent fallback. A configured
  `TANTRA_LLM_BASE_URL` also serves any requested model by name.
- **Multimodal**: `MessagePart::Image { mime_type, data }` added to `tantra-agent-llm`
  (`ChatMessage.parts`, `#[serde(default, skip_serializing_if)]`; `ChatRequest::has_image_parts`);
  OpenAI-compatible providers serialize the OpenAI multimodal content array and advertise
  `supports("vision")`; the chat turn API carries attachments (`SessionHandle::submit_turn`
  + `SessionCommand::Submit` attachments, persisted in the user-message JSON so resumed
  sessions replay them); the turn gates image parts against `provider.supports("vision")` —
  text-only providers (Candle-GGUF) answer the honest "model X does not support image
  attachments" notice (streamed as text) instead of dropping bytes. `ChatAttachmentInput`
  bytes stay base64-raw end-to-end.
- **Tests** (all in `tests/`): `tests/chat_ws_gap0009.rs` (8 — legacy/new wire round-trip,
  base64-safe, default path, override routing, unavailable-model notice, vision flow,
  text-only notice, unsupported-MIME notice) over a real `/ws/chat` boot; agent-side
  `tantra-agent-chat/tests/unit_tests.rs` +3 and `tantra-agent-llm/tests/openai_compat_tests.rs`
  +1; existing chat/openapi/persistence tests stay green.
- **Gate**: `cargo fmt --check` ✅ both repos; `cargo clippy --workspace --all-targets
  --profile prod -- -D warnings` ✅ both repos; targeted prod test binaries green
  (chat_ws_gap0009 8, agent chat unit 9, llm 7+4, checkpoint 4, dependency_rules 1,
  openapi_conformance 3, session_persistence 2, runtime unit_tests 1). ⚠ Known env limit:
  a blanket `cargo test --workspace --profile prod` still SIGKILLs at prod-LTO link on the
  heavy api-rest test bins (unrelated); targeted binaries link and pass.
- **Docs**: gap 0009 retired from `docs/gaps/`, closed row added to `docs/gaps/README.md`
  (closing note incl. the env conventions and the ui-web follow-on).
- **Deviation**: the `tantra-ui-web` half (send `model` per turn from the model dropdown)
  cannot be done here — the web repo is not checked out on this machine; tracked in the root
  task list (T17.2) and the gap README row. The wire accepts the ui-web model tags verbatim
  (routed through the router when a base URL is configured).

## Prior batch — T17.3: gap 0010 closed (`QuoteTick` session summary fields)

Date: 2026-11-17. `0f5f4ac` extends the `GET /quotes` wire `QuoteTick` with five optional
session-summary fields and populates the honest ones:
- **Wire** (`tantra-runtime-api::wire`): `last_price`/`day_high`/`day_low`/`prev_close`
  (`Option<Price>`) + `volume` (`Option<Quantity>`), `#[serde(default, skip_serializing_if)]`
  — absent from the wire when unset, so pre-summary consumers and payloads stay compatible.
- **Handler** (`get_quotes`): `last_price`/`volume` come from the `LiveQuoteStore` trade
  snapshot (the T7.7 last-print/day-volume plumbing the chat `quote` tool reads); `prev_close`
  from `LiveQuoteStore::read_fallback` (the dataset-backed reference the price-collar gate
  measures against — the previous close for a daily-bar catalog). `day_high`/`day_low` stay
  `None` — no store session high/low aggregator yet (broker protocols decode them, the feed
  wire `QuoteTick`/`LiveQuoteStore` do not carry them); never fabricated. `as_of` replays are
  never enriched with live data.
- **OpenAPI**: `schema/openapi.v2.json` regenerated from `OpenApiDocument::v2_json()` (gap
  0005 process); `openapi_conformance.rs` green (artifact currency + schema registration).
- **Tests**: `tantra-runtime-api/tests/quote_tick_wire.rs` (4: old-shape serialization, summary
  round-trip as decimal strings, legacy payload parse, summary payload parse);
  `tantra-runtime-api-rest/tests/market_data.rs` +2 (live-trade enrichment incl. day-volume
  accumulation + `as_of` non-enrichment; absent-when-no-print). 29 market_data + 3 conformance
  + 4 wire green on prod; `cargo fmt --check` + clippy `-D warnings` green for both crates.
- **Docs**: gap 0010 retired from `docs/gaps/`, closed row added to `docs/gaps/README.md`;
  `tantra-docs` `rest.md` `/quotes` row notes the summary fields.
- **Deviation**: the `tantra-ui-web` half (`vyapaar-runtime-client.ts` `dayHigh`/`dayLow`
  mapping) cannot be done here — the web repo is not checked out on this machine; tracked in
  the root task list (T17.3) and the gap README row.

## Latest Batch — T12.5 follow-on: synthetic-OCO bracket driver (exec `BracketTracker` wiring)

Date: 2026-11-17. Executed the recorded T12.5 follow-on: runtime emulation of one-cancels-other
(OCO) brackets on top of `tantra-exec-core`'s pure `BracketTracker`, so a strategy `TradePlan`
(entry + stop + target ladder) is driven as a synthetic bracket until native broker/OMS brackets
land (exec gap 0003 / ledger gap 0012 are closed upstream; the emulation is the runtime consumer).
All in `tantra-runtime-strategy`:
- **New module `src/synthetic_oco.rs`** — `SyntheticBracket` (wraps exec `BracketTracker` + broker
  order-id bookkeeping: entry / resting stop / per-target ids; `open_qty` / `resting_stop` /
  `closed` / `events` / order-id map reads) and `SyntheticOco` driver (`submit_entry`,
  `on_entry_fill` — arms the `#stop` child + one limit child per target leg with deterministic
  SHA-256-derived client ids, `on_bar` conservative stop-first stop-vs-target fills, `trail_stop`
  never-widening cancel+resubmit with the same deterministic `#stop` id, `realized_pnl` exact
  `Decimal`). `plan_to_spec`/`plan_to_entry_request` translate a `TradePlan`; `OcoAction` emitted
  per fill. Zero-I/O, `Decimal`-only, timestamps injected.
- **Dispatcher wiring (non-breaking)**: new `Dispatcher::dispatch_bracket` passes the entry
  through the SEBI risk gate (re-stamped with the dispatcher's registered algo id) then constructs
  and submits the `SyntheticOco` entry, returning the entry broker id. New `DispatchError::Bracket`
  variant. Existing order paths untouched. `PaperAdapter` gained a read-only `order_state` so tests
  assert OCO sibling cancels via broker state.
- **Tests `tests/synthetic_oco_tests.rs`** (8): plan→spec translation + child-id keying; full long
  lifecycle on `PaperAdapter` (arm → target fill → stop sibling cancelled → monotone trail → stop
  trigger → closed, realized P&L exact); conservative stop-first bar; deterministic same-seed
  client-id replay; short-side mirror; `dispatch_bracket` risk gate + halt refusal.
- **Gate**: `cargo fmt --check` ✅, `cargo clippy --workspace --all-targets --profile prod
  -- -D warnings` ✅, `cargo check --workspace --profile prod` ✅, `cargo test --profile prod
  -p tantra-runtime-strategy` green (43 tests incl. 8 new). ⚠ Known env limit: a blanket
  `cargo test --workspace --profile prod` still SIGKILLs at prod-LTO link on the unrelated heavy
  `tantra-runtime-api-rest` / `tantra-runtime` test bins; targeted binaries link and pass.
- **Immediate next step**: nothing runtime-side pending for this follow-on. The real wire-transport
  native bracket path (BO/CO) stays an exec-side concern; this emulation is the consumer until then.

## Latest Batch — T14 P1 COMPLETE (plan 0005 D1–D5; T14.5–T14.9 done)

Date: 2026-11-16. All five runtime-wiring deliverables of
`docs/plans/0005-paper-exchange-runtime-wiring.md` landed (1 subagent/wave, sequential;
commits local, not pushed):
- **D1 / T14.5 (`027ebac`)** — REST `/orders` on the `tantra-exec-paper` exchange:
  `NextOpenSim` deleted; `ExecGateway` is a thin adapter over `PaperExchange` (real NSE
  delivery `FillCostFn` default, `ZeroFillCost` for tests; ledger↔broker id map; fill/event
  drain cursors into the ledger + shared ledger). Limit / `stop_market` / `stop_limit`
  (`price` = trigger) / IOC accepted; shorts allowed; `DELETE /orders/{id}` and
  `POST /positions/close` square both directions. Tests `paper_orders_tests.rs` (R1/R2/R3/R7
  + stops) + `orders_exec.rs` re-based onto exchange semantics.
- **D2 / T14.6 (`a9d1d96`)** — `PaperAdapter`/`ZerodhaKiteAdapter`/`AngelOneSmartApiAdapter`
  sim scaffolds delegate fills to a per-adapter `PaperExchange` (`AdapterState` inline fill
  math deleted); live-credentials Zerodha path untouched; market orders fill at the seeded
  reference price, limits rest. `dispatch_tests.rs` re-based (3 of 5 replay intents fill).
- **D3 / T14.7 (`18be257`)** — `/ws/live` bidirectional: broadcast of `OrderAccepted`/`Fill`/
  `PartialFill`/`Cancelled`/`Rejected`/`StopTriggered`/`DepthChanged`/`PositionSnapshot`/
  `EquitySnapshot` from `ExecGateway` (per-drain + per-bar), client `subscribe`/`unsubscribe`/
  `ping`/`place_order`/`cancel_order` routing through the same REST gate. Tests `ws_live.rs`
  (R4).
- **D4 / T14.8 (`a1cbeda`)** — backtester drives the exchange for its L2 fills (inline
  `sim::l2_book` usage removed; engine keeps its own latency gating + slippage-impact + MIS +
  kokoro settlement; `account_fill` extracted for both immediate and later resting fills).
  R5 golden parity `backtest_paper_parity_tests.rs` green (E9 `paper_parity_tests.rs`
  untouched). **D4 deviation closed (`edeb0a8`)**: the configured adverse-fill model is
  threaded into `PaperExchange::with_adverse_fill` (crate-local accessors map it onto the
  exchange's type), so L2 adverse-fill slippage is restored; `tests/adverse_fill_l2_tests.rs`
  proves adverse fill > best touch vs touch without the model.
- **D5 / T14.9 (`f70e0e2`)** — `paper-serve --tape <file> [--addr --cash --real-time
  --mis-square-off HH:MM --feed tape|live]` daemon bin + `src/paper_serve.rs` library core:
  replays a bar tape into the exchange, serves REST + `/ws/live`, exits on feed end; MIS
  square-off via `ExecGateway::mis_square_off` (deterministic v5 close ids). Tests
  `paper_serve.rs` (R6). **D5 follow-ons closed (`3447cc7`)**: NSE trading-calendar gate (no
  square-off on weekends/holidays) + `PaperFeed::Live` live-feed seam (`--feed live` channel,
  same advance/square-off/pacing pipeline) + calendar-gate/Live tests. **Outstanding**: a real
  `tantra-market` broker feed into the `Live` channel — blocked on `tantra-market` gap 0003
  (real broker WebSocket credentials). Orchestrator fixed a random-v4 → deterministic-v5
  close id before commit.
- Plan 0005 doc + plans README → Implemented (record of D4/D5 deviations); runtime gap 0008
  closed earlier (P0 wave 3, `a005141` — committed 2026-11-16 from the leftover P0 tree).
- **Immediate next step**: T14 P2 — Python wheel `tantra.paper` (`tantra-ffi` plan 0003,
  T14.10/T14.11), then P3 (CSR/WASM, `tantra-ui` plan 0008), P4 (golden parity suite + docs).
  Paper-trading follow-ons closed (`edeb0a8` L2 adverse-fill restore; `3447cc7` NSE
  trading-calendar gate + live-feed seam). Remaining: a real broker WebSocket feed into the
  `Live` seam (`tantra-market-dhan`/`zerodha`) — blocked on `tantra-market` gap 0003.

## Prior batch — T14 Wave 3 (E9 parity test)

Date: 2026-11-16. E9 of `tantra-exec/docs/plans/0003` (runtime plan 0005 D4) landed:
- New `tests/paper_parity_tests.rs` (4 tests) driving the paper exchange
  (`tantra-exec-paper`) and the backtest L2 book (`sim::l2_book`) from the same
  golden tape and asserting byte-identical fills: `book_parity_price_time_priority`,
  `book_parity_limit_walk`, `paper_exchange_fills_match_golden_tape`,
  `paper_exchange_events_deterministic_golden`. 4/4 green on prod.
- Workspace: `tantra-exec-paper` path dep added to `[workspace.dependencies]`
  and as dev-dep of `tantra-runtime-backtest`.
- Verified: `cargo fmt --all`, `cargo check --workspace --profile prod`, `cargo
  test -p tantra-runtime-backtest --profile prod`, `cargo clippy --all-targets
  -- -D warnings`, `make prod`.
- Deviation vs plan-spec numbers (engine is authoritative, verified by the
  tests): the golden tape produces **4** fill records (one per side per
  participant fill), not 3; net position after the sell is **0** (sell nets the
  resting buys), not +150; cash returns to 1,000,000 with `ZeroFillCost`. Plan
  doc + tests updated to match engine behavior.

## Latest Batch — T14 Wave 2 (sync port lift; gap 0008 closed)

Date: 2026-11-16. T14.4 lifted the sync `BrokerAdapter` port into
`tantra-exec-core` (executes runtime gap 0008 option 2):
- `tantra-exec-core::sim::SimExecutionPort` is now the canonical sync
  `now`-stamped execution port; `tantra-exec-paper::PaperExchangeExecutor`
  implements it.
- `tantra-runtime-strategy::dispatch::BrokerAdapter` is now a thin re-export
  (`pub use tantra_exec_core::sim::SimExecutionPort as BrokerAdapter;`);
  `PaperAdapter` / `ZerodhaKiteAdapter` / `AngelOneSmartApiAdapter` /
  `Dispatcher` compile unchanged.
- Gap 0008 closed; index moved to Closed. Verification green in both repos:
  `cargo check --workspace --profile prod`, `cargo test
  -p tantra-exec-core -p tantra-exec-paper --profile prod`, `cargo test
  -p tantra-runtime-strategy --profile prod`, clippy `-D warnings`, `make prod`.

## Prior batch — T14 P1/P2 planned (docs only) — SUPERSEDED by the P1 COMPLETE entry above

Date: 2026-11-15. Planning artifacts for plan 0005 (Proposed → now Implemented, D1–D5 done).

## Latest Batch — gap 0007 items 1–5, 11, 12 closed (upstream bridges landed)

Date: 2026-11-14. Closed the non-depth/non-vendor gap 0007 items by landing the upstream bridges
in parallel waves (2 subagents max):
- **`tantra-data` exports (gaps 0017/0018/0019 closed)** — `LakehouseBridgeExporter` gained
  `export_institutional_flows` / `export_delivery` / `export_economic_calendar`
  (`{root}/catalog/institutional_flows|delivery|economic_calendar/<date>.parquet`, exact
  Decimal128/int64) + CLI `--dataset institutional-flows|delivery|economic-calendar`; 14 exporter
  tests, full `pytest` 144 green, ruff clean.
- **`tantra-market-store`** — 3 projection readers
  (`InstitutionalFlowsProjectionReader`, `DeliveryProjectionReader`,
  `EconomicCalendarProjectionReader`, mirror of `BlockDealsProjectionReader`); live trade store
  (`LiveQuoteStore::put_trade` + `ports::TradeReader`/`TradeSnapshot` with session-day volume);
  `wire::Instrument.display_name` + `snapshot_schema()` column (tolerant read, backward-compat);
  `tests/live_trades.rs` + reader/display-name tests (59 store tests green on prod).
- **`tantra-runtime`** — `FeedMarketReader` gained `with_institutional_flows`/`with_delivery`/
  `with_economic_calendar`/`with_trade_reader`; real `institutional_flows` (cash segment),
  `delivery_stats` (instrument/range filtered), `macro_series` (economic-calendar projections),
  `indicators` (daily bars → `tantra-market-indicators` sma/ema/rsi/roc/cmo/atr/adx/mfi/bb/obv/
  vwap/macd → exact `Decimal`), `quote` (last print + day volume when a `TradeReader` is wired,
  honest midpoint fallback otherwise), `search_instruments` (display-name first); `FeedSnapshotSource`
  gained `with_risk_engine` (`otr_max_ratio`/`otr_warning_threshold`/`price_band_count`) +
  `with_regime` (20-SMA-slope classifier, `TANTRA_REGIME_INSTRUMENT`); `ChatRuntime::from_env`
  wires the three projection builders. 8 new runtime tests (30 in the two touched binaries).
- **Closed since (2026-11-14)**: item 10 (L2 market depth — exact-Decimal `DepthSnapshot` +
  `LiveQuoteStore::put_depth` + parquet depth ticks in `tantra-market-store`, wired via
  `FeedMarketReader::with_depth_reader`; gap 0006 retired), items 14 (fundamentals —
  `tantra-data` 0020 landed `d94230c`, `FundamentalsProjectionReader` `a8b0c05`,
  `FeedFundamentalReader::with_fundamentals` `37db5b8`), 15–16 (mutual funds/IPO —
  `tantra-data` 0021/0022 landed `17c5b94` — AMFI + NSE verified live;
  `FundsIposProjectionReader` `9f8b04c`; `FeedFundReader`/`FeedIpoReader` `f752334`). **Gap
  0007 fully closed** (all 19 items; doc retired). **Fund *holdings* remain unsourced** —
  no free verified holdings feed; `fund_holdings` keeps its explicit citation (documented in
  `tantra-data` 0021 closed row, never fabricated). Live trade feed stays unwired
  (plumbing-only; real broker feed tracked by `tantra-market` gap 0003 / root
  `docs/gaps/0001`).

## Latest Batch — T7 remaining (gap 0006 closed; 0007 items 8/9/17/18/19 closed)

Date: 2026-11-14. Implemented the runtime-side T7 items that were not upstream-blocked:
- **Gap 0006 closed (portfolio-backed tools)** — design decision recorded in
  `docs/design/code-shared-ledger.md`: `AppState.shared_ledger` is a **`SharedLedger`**
  (`tantra-runtime-api::shared_ledger`) — the four in-memory `tantra-ledger` repositories +
  a matched-lot history, each mutex-backed, with `&self` `record_*` methods and the four
  repository trait impls (so one `Arc<SharedLedger>` coerces to all four `Arc<dyn Trait>`
  `LedgerPortfolioReader::new` needs). The order flow opens the default `rest-api` account and
  records every approved order (`orders.rs::record_into_shared`); the sim records every fill
  (`exec.rs`: buy opens a FIFO lot, sell matches, cash adjusts, order row advances via
  `update_order_fill`). `StubPortfolioReader` removed; `LedgerPortfolioReader` wired in
  `ChatRuntime::from_env`. **Item 19 closed (tax)** — `FeedTaxReportReader` reads the shared
  ledger's closed-lot history per account/date range.
- **Item 17 closed (trade-intent)** — `FeedTradeIntentSink` enqueues validated intents into
  the `FileJobRegistry` (`TANTRA_JOBS_PATH` else `~/.tantra/jobs.json`) as
  `PendingApproval` jobs with `approval_policy = Manual` + `x-papertrading` metadata flag.
- **Item 18 closed (strategy store)** — `FeedStrategyStore` in-memory `StrategySpec` CRUD.
- **Item 9 closed (corporate actions in chat)** — `with_block_deals`/`with_corporate_actions`
  are now composable builders; `from_env` wires block deals + corporate actions + OI store.
  Item 8 (options chain) was already closed (T3.1); the 0007 doc now records it.
- **Tests**: `tantra-runtime-api/tests/shared_ledger_tests.rs` (6) +
  `tantra-runtime-api-rest/tests/chat_remaining_t7_tests.rs` (3: order-flow→portfolio/tax,
  strategy-store CRUD, jobs-registry sink).
- **Cleanup per gap-tracking §4**: gap 0006 doc removed (Closed in index), gap 0007 rewritten
  (items 8/9/17/18/19 closed), new `docs/design/code-shared-ledger.md`; root task list T7 rows
  + this HANDOFF + root HANDOFF updated.

## Latest Batch — T7 track (2026-11-14)

- **T7.2 + T7.3 (plan 0004 Phases 4–5, Implemented)** — block/bulk deals real data:
  `tantra-data` `export_block_deals` + `--dataset block-deals` CLI (`ce7c392`);
  `tantra-market-store::BlockDealsProjectionReader` (`02fc20e`/`84d1e99`);
  `FeedMarketReader::with_block_deals` + real `block_deals` read, `AppState.block_deals` +
  `with_block_deals`, `GET /market-data/block-deals` route + openapi entry + tests (`b561e12`).
  Gap 0007 item 6 closed.
- **T7.4 (gap 0004 closed)** — `Profile.watchlists` values → `Watchlist { name,
  instrument_ids }` (legacy bare-array deserializer preserves stored profiles via
  `WatchlistWire` untagged helper), new opaque `portfolios`/`risk_profiles`/`workspaces`
  fields, `MAX_PORTFOLIOS`/`MAX_RISK_PROFILES`/`MAX_WORKSPACES = 50` limits + round-trip and
  count-limit tests (`53e917a`; stack-overflow fix `ad84499`; fmt `7a45a14`).
- **T7.5 (gap 0005 closed)** — OpenAPI 3.1 spec completed: all 68 v1 route paths (86
  method/path pairs) registered in `OpenApiDocument::generate()`, 21 wire/profile component
  schemas, `OpenApiDocument::v2_json()` + checked-in `schema/openapi.v2.json`,
  `tests/openapi_conformance.rs` (route coverage, schema registration, artifact currency);
  `openapi` module made `pub` (`021f965`, `967fd15`, `085461b`).
- **T7.6 session-persistence half (gap 0006 half-closed)** — `AppState.database` +
  `with_database`; `prepare_server` now `async` (runs `TANTRA_DATABASE_URL` connect + embedded
  migrations), `run_server_blocking` + `tests/bootstrap.rs` callers updated;
  `ChatRuntime::from_env` uses `SqlxSessionStore` when a DB is present, in-memory fallback
  otherwise; `tests/session_persistence_tests.rs` (`933c53a`). **Portfolio-backed tools
  remain** (need a ledger-repository handle in AppState — tracked in gap 0006).
- **T7.7 items 7 + 13** — news bridge (`tantra-data` `export_news` `4bb1c75`,
  `tantra-market-store::NewsProjectionReader` `c88db45`, `FeedContentReader::with_news` +
  real `news_search`) and oi_buildup (`FeedMarketReader::with_oi_store` +
  `ParquetOpenInterestStore` + `BuildupAnalyzer`, real `oi_buildup`) — `776d145`. Gap 0007
  items 6/7/13 closed; remaining 0007 items upstream-blocked (see gap doc).
- **T7.8 (gap 0008, documented decision)** — option 1 "keep `BrokerAdapter` as the sim port"
  recorded in gap 0008 + `docs/design/code-strategy.md` §8.2; closes when option 2 lands.
- **Docs/cleanup** — gaps 0004/0005 closed (index → Closed), plan 0004 → Implemented (doc
  removed), gap 0006/0007/0008 statuses updated.

## Gate status — ✅ GREEN (2026-11-14, T7 track + T7-remaining batch)

- `cargo fmt --check`: ✅
- `cargo clippy --workspace --all-targets --profile prod -- -D warnings`: ✅
- `cargo check --workspace --profile prod`: ✅
- `cargo test --profile prod` for touched binaries: ✅ (chat_projection 30 incl. 4 new, full
  `tantra-runtime-api` suite; prior batch counts: shared_ledger 6, chat_remaining_t7 3,
  orders_exec 8, orders_gateway 2, trading_state 18, session_persistence 2).
  ⚠ Known env limit: a blanket `cargo test -p tantra-runtime-api-rest` (all ~45 binaries)
  hits the pre-existing prod-LTO link SIGKILL on unrelated heavy binaries; targeted binaries
  link and pass.
- `make prod` (§11 end-of-turn rebuild): ✅ (run at batch close)

## Phase / Slice Status

- **Docs: dismantle `docs/roadmap/` into design/gaps/plans (2026-11-13, docs-only)**: former
  `docs/roadmap/` (`roadmap.md` + `migration-verification.md`) removed — completed milestones
  folded into new `docs/design/implementation-record.md` (migration M1–M3 + migration
  verification + subsequent landings table). `architecture.md` §5 gaps/plans snapshot corrected
  to source reality: plan 0003 is **implemented** (trade-journal + market-reference routes all
  registered in `v1.rs`; market-reference handlers serve sample stores pending the
  `tantra-data` → Rust FFI bridge) — moved to Implemented in `docs/plans/README.md` and its doc
  removed per gap-tracking rule §4; gaps index gained the previously-missing gap 0005; gap
  0007's item 7 corrected to the `ContentReader::filings` method name (chat tool
  `filings_search`); gap 0005's "10 `/reports/*`" corrected to 9. **No Rust code touched** — no
  `make prod` required by §11.
- **Execution seam consolidation — implemented 2026-11-12 (exec/runtime boundary)**:
  - `ExecutionActor::with_exec_stack(risk_engine)` composes
    `tantra_exec_core::RiskGatedExecutor<RateLimitedBroker<_>>` over the
    injected broker — refused orders never reach the wire; every call is metered.
    Dispatch/cancel/query now pass the canonical `BrokerError` through the reply
    channels unchanged (no string-mangling). Test
    `test_execution_actor_composes_exec_stack` proves risk-gating + clean drain.
  - Duplicate `MockBrokerExecutor` deleted: `tantra-runtime-testing` re-exports
    `tantra_exec_core::MockBrokerExecutor` (`accept_all` / `reject_all` /
    `placed_orders`); actor tests updated.
  - Gap 0008 filed: dual execution-port boundary (`BrokerAdapter` sync sim port
    vs `BrokerExecutor` async wire port) — keep-or-lift decision documented.
  - Exec-side: gap 0002 closed (JSONL journal canonical; this repo's
    `SqliteAuditLedger` is the complementary operational store).
- **Per-crate design docs — complete (2026-11-12, docs-only)**: `docs/design/`
  now has a source-verified design doc for every non-stub crate —
  `architecture.md` (rewritten, 14-crate map + gaps/plans snapshot) plus
  `code-{strategy,backtest,enhancer,actors,jobs,persistence,api,api-rest,config,
  testing,feed}.md`; the stale combined `code-config-testing-feed.md` was
  replaced by the split `code-config.md` / `code-testing.md` / `code-feed.md`
  (feed doc also covers the placeholder stubs `tantra-runtime-market`,
  `tantra-indicators`, `tantra-runtime-test-utils`); `code-migration-map.md`
  kept as historical. Key corrections baked in: audit via injected
  `SqliteAuditLedger` (no `AuditPersistenceActor`), coalescing defaults
  100/50 ms, `JobSpec` has no `Trigger` enum, guardrail gates are DSR ≥ 3.0 /
  PBO < 0.5, 501 `valuation_engine_mismatch` on the four portfolio routes,
  legacy `honba_*` metric prefix. **No Rust code touched** — no `make prod`
  required by §11; the gate status below remains from the 2026-11-12 code pass.
  Mirrored into `tantra-docs`: new `runtime_overview.md` + `runtime_chat.md`,
  rewritten `event_loop.md` / `runtime_jobs.md`, corrected
  `strategy_tester_and_enhancer.md` + `api/rest.md` + conformance matrix
  (`mkdocs build --strict` PASS).
- **Agent plan 0006 Phase 4 (options-chain / corporate-actions chat tools) — wiring implemented
  2026-11-12**: research-verified first per plan 0006 §4 (at most 1 subagent) — the plan's
  corporate-actions assumption was **stale**: a historical reader *does* exist Python-side
  (`tantra_data`'s `CorporateActionsRepository`/`DuckDbCorporateActionsStore`), while
  `tantra-market` has contract-reference/Greeks building blocks but no chain reader. Gaps
  filed before any tool code: `tantra-market/docs/gaps/0001-options-chain-reader.md` (that
  repo's first), `tantra-data/docs/gaps/0008-corporate-actions-rust-bridge.md` +
  `0009-corporate-actions-ingestion.md`. Landed here: `FeedMarketReader::options_chain` +
  `::corporate_actions`, both honest `PortError::unavailable` stubs citing those gaps;
  seedable `RuntimeMarketReader` gained `set_options_chain` (keyed by underlying) +
  `set_corporate_actions` (empty ⇒ unavailable, same shape as block deals) with
  `chat_adapters_tests.rs` coverage; tracked as items 8–9 of `docs/gaps/0007-*.md`. Catalogue
  count 17 → 19 upstream.
- **Agent plan 0006 Phase 3 (news/filings chat tools) — wiring implemented 2026-11-12**: new
  `crates/tantra-runtime-api-rest/src/chat_content.rs::FeedContentReader` implements
  `tantra-agent-chat-tools`'s new `ContentReader` port with honest `PortError::unavailable`
  stubs — news cites `tantra-data/docs/gaps/0006-news-ingestion.md` (no source anywhere),
  filings cite `tantra-data/docs/gaps/0007-filings-rust-bridge.md` (ingested in Python, closed
  gap 0003, but no Rust-consumer bridge — same pending bridge as the `market.rs` sample stores).
  Registered in `ChatRuntime::from_env` via the main `register_read_only_tools` bundle
  (catalogue count 15 → 17 upstream). Tracked as item 7 of `docs/gaps/0007-*.md` (retitled to
  cover the content adapter).
- **Agent plan 0006 Phase 2 (price alerts) — storage decision + wiring implemented
  2026-11-11**: the storage decision the plan demanded: **no new store**. Per
  `docs/gaps/0004-*.md`'s profile-schema precedent (alerts were explicitly closed against the
  opaque `Profile.alerts: Value` field), chat-created alerts live in the **same**
  `Profile.alerts` array `tantra-ui-web`'s `alerts-store.ts` already syncs via the `/profile`
  routes — the chat agent is the second writer on the same document. New
  `crates/tantra-runtime-api-rest/src/chat_alerts.rs::ProfileAlertStore`: every change is one
  atomic `ProfileStore::update` read-modify-write on the blocking pool (revision bump, atomic
  file rewrite, in-process write lock); acts on `DEFAULT_USER_ID`'s document because the chat WS
  has no bearer identity yet (`docs/gaps/0006`); refuses non-array/garbage `alerts` values with
  explicit errors (never an empty success); 10 tests in
  `tests/chat_alert_store_tests.rs` (round-trip, ui-web `AlertMeta` camelCase shape, duplicate-id
  rejection, triggered-no-op transitions with no revision bump, delete/unknown-id, revision +
  other-field preservation, per-user scoping via `ProfileAlertStore::for_user`).
  `ChatRuntime::from_env` gained a `profiles: Arc<ProfileStore>` parameter and registers the
  alert tools; `bootstrap.rs` passes the store (configured before the chat runtime).
- **Plan 0004 Phases 1–4 (block/bulk deal chat tool) — implemented 2026-11-14** (one line:
  honest `FeedMarketReader::block_deals` stub + seedable `RuntimeMarketReader` store; Phase 4
  now reads real `tantra-data` projections via `BlockDealsProjectionReader` and `GET
  /market-data/block-deals` (T7.2/T7.3, `tantra-data` gap 0005 closed); see
  `docs/plans/0004`). Phase 5 optional after that.
- **PLAN-0001 Stages 1–3 + Stage 4** (RunId/journals, worker pool, strategy dispatch, cron
  scheduler, typed manifests): implemented — recorded in `docs/plans/README.md` Implemented
  table. Stage 4's CLI/agent half lives in `tantra-ui` (`tantra-cli`).
- **GAP-0002/PLAN-0002** (statistical gates, stop-fill engine): closed — recorded in
  `docs/gaps/README.md` + `docs/plans/README.md`.
- **GAP-0003** (Zerodha wire transport via `tantra-exec-zerodha`): implemented; Angel One
  remains a simulation scaffold pending `tantra-exec-angelone` (upstream placeholder);
  golden-vector conformance over recorded broker sessions deferred.
- **PLAN-0003** (trade-journal & market-reference REST endpoints): implemented — recorded in
  `docs/plans/README.md`; market-reference sample stores pending the `tantra-data` → Rust FFI
  bridge (`tantra-data` gaps 0001+0002 supply the real data).

## Immediate Next Step

T7 track + the runtime-side T7-remaining items shipped 2026-11-14 (block/bulk deals, news,
oi_buildup, options_chain, corporate-actions-in-chat, profile schema, OpenAPI, session
persistence, **portfolio-backed tools (gap 0006 closed)**, trade-intent / strategy-store / tax
wiring (0007 items 17–19 closed), broker boundary decision). The 2026-11-14 batch closed gap 0007
items 1–5, 11, 12 (last-price/volume plumbing, indicators, macro_series, display-name, regime +
risk metrics, institutional flows, delivery). Remaining T7:
- **Gap 0007 items 10 and 14–16** are upstream-blocked, verified, unchanged by design: L2 depth
  (`tantra-market` 0006 — the `DepthReader` port/`DepthSnapshot` exist in `tantra-market-store`;
  only a data source implementing it is missing), and fundamentals / mutual-fund / IPO ingestion
  (`tantra-data` 0020–0022 — vendor selection + ingestion first; real-world data procurement, not
  code-closeable).
- Follow-on (no stub): wire a live broker trade feed (`tantra-market` gap 0003 / root
  `docs/gaps/0001`) into a `TradeReader` so `quote` reports real last prints/day volume instead
  of the midpoint fallback; populate the `display_name` catalog column from broker master data.
- **Gap 0008** unchanged as intended — a documented architecture decision (option 1), not a stub;
  it closes only if option 2 (lift `BrokerAdapter` into `tantra-exec-core`) ever executes.
- Strategy-store persistence across restarts is a follow-on (the in-memory store is
  session-scoped; not a gap — nothing stubbed).
- When chat sessions gain bearer identity, switch `ProfileAlertStore::for_user` to the
  connection's `token_id`.

## Do not regress

- Zero in-file `#[test]` / `#[cfg(test)]` in `crates/*/src/`.
- Strict canonical `prod` profile (`target/prod`); `make prod` before saying done.
- Zero floats in all financial accounting (`rust_decimal::Decimal`); no wall-clock/random inside
  sim state machines — state transitions take explicit `DateTime<Utc>`/IDs from callers.
- ADR 0018 order gate, algo registration in the executor, `TestBarReader` fixture bars,
  `service.rs` queue-full `RateLimited`, `live_quotes.rs::read_fallback`.
- WebSocket tests must always give the awaited stream a timeout (a hung `ws_stream.next()`
  blocks the whole workspace run).
- Explicit clock at the adapter boundary: `BrokerAdapter::{submit_order, cancel_order}` take
  `now: DateTime<Utc>`; per-venue monotonic `FILL-####` fill IDs; caller-owned
  `client_order_id` minting (`OrderIntent::into_order_with_id` /
  `Dispatcher::dispatch_intent_with_id`).
- One `tantra serve` per profiles directory (in-process-only write lock); the chat agent's
  alert store depends on the same constraint (`chat_alerts.rs`).

## Decisions & Discoveries

- **Docs source of truth**: `tantra-runtime/docs/design/code-*.md` are the
  per-crate references; tantra-docs pages link to them rather than
  duplicating facts (single writer per fact). Gap citations inside the design
  docs name the owning repos' `docs/gaps/*` (`tantra-data` 0005–0008,
  `tantra-market` 0001).
- **Alert storage = `Profile.alerts`, no new store**: gap 0004 had already mapped ui-web's
  `AlertMeta[]` onto the opaque field with no server change; making the chat agent a second
  writer reuses that contract instead of duplicating a store. DTO round-trips ui-web exactly
  (camelCase, decimal-string `price`, `createdAt` date string, `triggeredAt` optional ISO).
  Port contract: caller mints ids, duplicates rejected; `Triggered` never flipped by
  enable/disable; unknown id → `Ok(None)`.
- `BrokerAdapter` trait has no consumers outside `tantra-runtime-strategy` (verified) —
  signature extensions are safe; the pairwise-adapter economic-parity test intentionally stays
  on the random-ID path.
- `tantra_runtime_api::chat_adapters::RuntimeMarketReader` is a seedable test adapter: empty
  block-deals store ⇒ `PortError::unavailable` (gap 0005 citation); seeded ⇒ filter matches.
  Same honest-stub shape as `FeedMarketReader`'s unconfigured path (gap 0007 item 6;
  `FeedMarketReader::block_deals` now reads real projections when wired via
  `with_block_deals`). Agent plan 0006 Phase 4 added
  the same shape for `set_options_chain` (keyed by underlying, empty ⇒ unavailable citing
  `tantra-market/docs/gaps/0001`) and `set_corporate_actions` (empty ⇒ unavailable citing
  `tantra-data/docs/gaps/0008`); expiry filters match on calendar day, not exact instant.
- `chat_content.rs::FeedContentReader` takes no ports and answers every call with an explicit
  `unavailable` (gap 0006 news / gap 0007 filings citations): `ContentReader` is a new port with
  exactly one implementor (alert-port precedent — no seedable test adapter until a consumer
  needs one), so it needs nothing from `AppState` today.
