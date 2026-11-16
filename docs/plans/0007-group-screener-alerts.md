# 0007 — Alpha-Style Group & Event Alerts Engine

**Status**: Proposed (updated 2026-11-16 after architectural review & Alpha-Alerts expansion)

## 0. Executive Summary & Objective

Replace price-only alerts with a full **Trendlyne Alpha-Alerts-grade** monitoring and notification
engine owned at L3 (`tantra-runtime` + `tantra-agent-chat-tools` ports, **not** `tantra-core` L0):

1. **Multi-universe group rules**: bind one rule to `1..N` universes simultaneously (portfolios,
   watchlists, indices, sectors/industries, cap buckets, or explicit symbol lists), deduplicating
   overlapping instruments while attributing which universe(s) matched.
2. **Two evaluation paradigms** under a unified `AlertRule`:
   - **`ScreenEdge`** (periodic bar-close / catalog refresh at `15m | 30m | 1h | daily`): evaluates a
     saved catalog preset ID (optionally constrained to the user's portfolios/watchlists) or an
     inline converged `ScreenerFilterGroup`, diffing consecutive match sets for `Enter | Exit | Both | Level`.
   - **`EventMatch`** (stream/feed-driven): fires immediately on ingested corporate/market events
     (declared results & YoY/QoQ surprises, upcoming dividends/bonuses/splits/board meetings,
     exchange announcements, earnings-call transcripts, analyst reports, bulk/block deals,
     insider/SAST >1% deals, and QVM state transitions) without cramming event streams into
     bar-metric predicates.
3. **Delivery & Digest Engine**: separates rule evaluation from `DeliveryPolicy` (`Live` vs
   `HourlyDigest` / `DailyDigest` / `EodSummary`) across pluggable sinks (WebSocket push, webhook,
   email, WhatsApp), with quiet hours, per-rule cooldowns, snooze/mute, dedupe, and bounded
   backpressure channels.
4. **One-click Preset Notification Packs**: "Manage Notifications"-style curated rule bundles
   (Price Extremes, MA Crossovers, Volume/Delivery Surges, Pivot Breakouts, Results & Filings
   Digests, Corporate Actions, QVM Transitions, Big Deals) subscribable over any portfolio/watchlist.
5. **MCP port wiring**: compose async `ScreenerPort`, `UniversePort`, `AlertRulePort`,
   `AlertSubscriptionPort`, and `WatchlistPort` implementations here for
   [tantra-agent 0013](../../../tantra-agent/docs/plans/0013-screener-mcp-tools.md) (Agent Rule R4).

References: research [`trendlyne-screener-alerts-mcp.md`](../../../docs/research/trendlyne-screener-alerts-mcp.md) (F7 engine, F8 wiring);
root roadmap [`0010`](../../../docs/plans/0010-screener-taxonomy-and-alerts.md).
Depends on: [tantra-core 0004](../../../tantra-core/docs/plans/0004-classification-and-alert-wire-types.md) (converged `ScreenerFilterGroup` & classification VOs),
[tantra-data 0007](../../../tantra-data/docs/plans/0007-instrument-master-index-membership.md) (Parquet universe/classification projections),
[tantra-data 0008](../../../tantra-data/docs/plans/0008-screen-dsl-catalog-qvm.md) (screen catalog & `ScreenerEvaluatorPort`).

## 1. Context & Gaps Covered

`tantra-runtime-api/src/screener.rs` has a standalone bar-only `evaluate_group(group, bars)`
(capped at 1,000 instruments) that diverges from `tantra_core::screener`. `chat_alerts.rs` keeps
single-stock `PriceAlert`s inside per-user `ProfileStore` JSON files (under a global `std::sync::Mutex`
and sync `std::fs`, flagged in `docs/design/0003-architecture-survey.md`), with no group rules, no
enter/exit edge detection, no event alerts, and no digests.

## 2. Component Specifications & Phases

### Phase A — Wire DTOs (`tantra-runtime-api`) & SQLx Persistence (`tantra-runtime-persistence`)
- **DTOs (L3, `schemars::JsonSchema`, `#[serde(deny_unknown_fields)]`, `Decimal` only)**:
  - `UniverseTarget`: `Portfolio(id) | Watchlist(id) | Index(IndexId) | Classification(ClassificationFilter) | Explicit(Vec<InstrumentId>)`.
  - `AlertCondition`:
    - `ScreenEdge { screen: ScreenSource (PresetId | Inline(ScreenerFilterGroup)), trigger: EdgeTrigger (Enter | Exit | Both | Level) }`
    - `EventMatch { event_kinds: Vec<AlertEventKind>, filters: EventFilterSpec }` (`ResultsDeclared`, `UpcomingResults`, `CorporateAction`, `CorporateAnnouncement`, `EarningsCall`, `ResearchReport`, `BlockBulkDeal`, `InsiderSastDeal`, `QvmTransition`)
  - `DeliveryPolicy`: `{ mode: Live | HourlyDigest | DailyDigest | EodSummary, channels: Vec<NotificationChannel>, quiet_hours?: SessionPolicy, priority: AlertPriority }`.
  - `AlertRule`: `{ id, owner, name, universes: Vec<UniverseTarget>, condition: AlertCondition, frequency: AlertFrequency, delivery: DeliveryPolicy, cooldown_secs, enabled, snoozed_until?, created_at, updated_at }`.
  - `AlertEvent`: `{ id, rule_id, owner, instrument, matched_universes: Vec<UniverseTarget>, trigger_kind, summary, payload, metrics: BTreeMap<String, Decimal>, observed_at, delivered_at? }`.
- **SQLx tables (`tantra-runtime-persistence`)**: `alert_rules`, `alert_rule_state` (last-matched instrument set snapshot per rule for restart-safe edge diffing), `alert_events`, `alert_digest_queue`, and `alert_notification_prefs` — all keyed by `(owner_id, id)`.
- **Migration of `ProfileStore` `PriceAlert`s**: idempotent migration into `alert_rules` as `Level` rules. Provide a read-through compatibility view on `/profile` `alerts` (removing `ProfileStore`'s sync write contention) until `tantra-ui-web` (`alerts-store.ts`) migrates to `/v1/alerts/*`.

### Phase B — Hybrid Evaluator & Scheduler (`ScreenEdge` + `EventMatch`)
- **L3 ↔ L6 evaluation split (no duplicate query semantics)**:
  - Bar/technical/price/MA/breakout `ScreenEdge` rules run natively in Rust over `tantra-market-store` + `tantra-data` Parquet projections (`catalog/{instruments,classification,index_membership}`).
  - Fundamental, peer-relative (`RelativeTo`), QVM, and catalog-preset `ScreenEdge` rules evaluate via an injected `ScreenerEvaluatorPort` backed by `tantra-data` (`0008` Phase B) or a unified Parquet/DataFusion reader — one authoritative semantics, no silent drift.
- **Scheduler**: per-frequency bar-close triggers (`15m`, `30m`, `1h`, `daily`), honoring `check_scan_budget`, resolving all `UniverseTarget`s as of the evaluation timestamp, deduplicating instruments across overlapping universes while preserving `matched_universes` attribution.
- **Edge detection & dedupe**: diff current vs persisted `alert_rule_state` set -> emit `Enter` / `Exit` (`Both` emits both; `Level` fires while true subject to `cooldown_secs`). Dedupe key: `(rule_id, instrument, trigger_kind, bar_ts_or_event_id)`. Skip rules where `enabled == false` or `now < snoozed_until`.
- **Event stream evaluator (`EventMatch`)**: subscribes to ingested event feeds (corporate actions, filings/announcements, block/bulk deals, results, QVM transitions, and — once `tantra-data` gap 0024 closes — insider/SAST & analyst reports), matching against active `EventMatch` rules scoped to the user's `UniverseTarget`s.
- **Clock purity**: inject runtime clock port (`now: DateTime<Utc>`); zero `Instant::now()` / `Utc::now()` in domain logic.

### Phase C — Notification Fan-Out & Digest Batcher
- `NotificationSink` port with implementations: WS push (live), webhook, email (live or digest), and extensible messaging adapter (e.g. WhatsApp).
- **Digest Batcher**: `DigestScheduler` flushes `alert_digest_queue` on `HourlyDigest` / `DailyDigest` / `EodSummary` boundaries, grouping events by `(owner, universe, event_category)` into structured digest payloads rather than flooding users with per-stock notifications.
- Bounded `tokio::sync::mpsc` channels with explicit `BackpressurePolicy` (`DropOldest` / overflow log); slow sinks never stall rule evaluation.

### Phase D — REST Surface (`/v1/alerts/*`, Caller-scoped, versioned)
- `/v1/alerts/rules` (`POST`, `GET`, `PATCH`, `DELETE`, `POST /{id}/snooze`)
- `/v1/alerts/presets` (`GET` catalog of Alpha-Alert packs, `POST /subscribe` one-click bind to portfolio/watchlist)
- `/v1/alerts/events` (`GET` paged, filter by rule/universe/unread/category) & `/v1/alerts/digests` (`GET`, `POST /preferences`)
- Strictly keyed by `(caller_id, rule_id)`, bounded body sizes, OpenAPI 3.1 schemas generated from `tantra-runtime-api`.

### Phase E — Built-In Alpha-Alert Rule & Event Packs
Ship parameterized preset templates matching the full Alpha-Alerts taxonomy:
1. **Price extremes**: daily move `±5% / ±10%`, week / 52-wk / 10-yr high/low cross & proximity (`within X%`).
2. **Moving averages**: Golden/Death cross; price crossing above/below SMA (`5,10,20,30,50,100,150,200`) & EMA (`5,10,12,20,26,50,100,200`).
3. **Volume & delivery**: volume shockers, high-volume top gainers/losers, rising/high delivery %.
4. **Breakouts**: LTP > pivot `R1/R2/R3`, LTP < pivot `S1/S2/S3`.
5. **QVM transitions**: high durability crossover, strong performer getting expensive, momentum trap, falling comet, rising/high momentum, weak.
6. **Screener enter/exit**: attach any `tantra-data` catalog preset or custom DSL screen over any portfolio/watchlist/index.
7. **Results & corporate actions (`EventMatch`)**: declared results (YoY/QoQ delta), post-result outperformance, upcoming results with rising price/delivery, upcoming dividend/bonus/split/board meetings.
8. **Filings & research digests (`EventMatch` + `Digest`)**: exchange announcements, conference/earnings calls (hourly/daily digest), analyst research reports (daily digest, gated on gap 0024).
9. **Big deals (`EventMatch`)**: bulk/block deals (available now via `tantra-data` closed gap 0005); insider/SAST >1% deals and superstar deals (gated on `tantra-data` gap 0024).

### Phase F — MCP & Chat Tool Port Wiring (Agent Rule R4)
- Compose and inject concrete implementations of `ScreenerPort`, `UniversePort`, `AlertRulePort`, `AlertSubscriptionPort`, and `WatchlistPort` into `tantra-agent-mcp` and `tantra-agent-chat-tools`.

## 3. Test Plan (TDD)

In `crates/*/tests/`:
- Multi-universe deduplication and `matched_universes` attribution tests.
- Edge-detection golden sequences (`Enter`, `Exit`, `Both`, `Level`), restart recovery of `alert_rule_state`, cooldown/snooze/dedupe key tests.
- `EventMatch` matching tests (corporate actions, block deals, results) + digest batching/grouping tests (`HourlyDigest` / `DailyDigest`).
- Budget rejection (`check_scan_budget`), Caller tenant-isolation negatives, backpressure on slow `NotificationSink`.
- `ProfileStore` `PriceAlert` migration + `/profile` read-through parity tests.
- Preset template validity (every `ScreenEdge` preset parses and passes converged DSL validation).

## 4. Dependencies & Tier Notes

L3; may use L0–L2 only (plus L3 `tantra-agent-*` ports). No `std::sync::Mutex` in handlers, no unbounded channels, no sync I/O on Tokio threads, `Decimal` only.

## 5. Open Questions

- Email/WhatsApp delivery transport priority: ship WS push + webhook + in-app/email digest payload generator first, external SMTP/WhatsApp gateway second?
- Per-user quotas: max active `ScreenEdge` rules (e.g., daily vs 15m frequency tiers) and max universes per rule (e.g., 5 universes per rule as in Alpha Alerts, or configurable).
- Transport for `ScreenerEvaluatorPort` across L3 ↔ L6 (`tantra-data` HTTP/IPC vs Rust Parquet evaluator).
