# HANDOFF — tantra-runtime

**Date**: 2026-11-12 · Branch: `main` · Full cross-repo context: `.agents/HANDOFF.md` in the
`tantra-labs` superproject.

## Gate status — ✅ GREEN (2026-11-14, T7.5 openapi conformance batch)

- `cargo fmt --check`: ✅
- `cargo clippy --workspace --all-targets --profile prod -- -D warnings`: ✅
- `cargo test --profile prod -p tantra-runtime-api-rest --test openapi_conformance`: ✅
  3/3 (route-vs-spec, schema registration, static artifact).
  Also ran `--test capabilities` 5/5 green — the api-rest test binaries linked
  and ran without the OOM noted in earlier batches.
- `make prod` (§11 end-of-turn rebuild): ✅ fresh `target/prod`

## Phase / Slice Status

- **T7.5 — OpenAPI 3.1 route/schema completeness (2026-11-14)**: `docs/gaps/0005`
  closed in `021f965` + `967fd15` (index/cleanup). All 68 v1 route paths (86
  method/path pairs) registered in `openapi.rs::generate()` with the `operation`
  helper; 21 wire/profile component schemas added (`ResponseEnvelope<Value>`,
  `ErrorCode`, `ErrorDetail`, `QuoteTick`, `Trade`, `Position`,
  `PortfolioAccount/Position/Holding`, `Instrument`, `InstrumentKind`,
  `BarAggregation`, `PriceType`, `TradingState`, `Profile`, `Module`, `Source`,
  `LayoutDocument`, `LayoutsResponse`, `ModulesResponse`, `ProfileRevision`);
  `OpenApiDocument::v2_json()` export + checked-in `schema/openapi.v2.json`;
  `tests/openapi_conformance.rs` (route conformance, schema registration,
  artifact currency). `openapi` module made `pub` so the integration test can
  read the document; `ApiResponse`/`BarSpecification` deliberately NOT
  registered (no `JsonSchema` derive, per orchestrator decision).

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

Agent plan 0006 is fully implemented (Phases 1–4). Remaining chat-tool data blockers are all
upstream gaps, tracked in `docs/gaps/0007-*.md`: `tantra-market/docs/gaps/0001` (options-chain
reader) for `options_chain`; `tantra-data/docs/gaps/0008`+`0009` (Rust bridge + ingestion) for
`corporate_actions` (its runtime bridge lives in the same projection reader the block-deal
tools use; only the `with_corporate_actions` wiring is pending); `tantra-data/docs/gaps/0006`+`0007`
for the content tools. When chat sessions gain bearer identity (`docs/gaps/0006`), switch
`ProfileAlertStore::for_user` to the connection's `token_id`.

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
