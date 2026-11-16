# 0005 — Group Screener Alerts Engine

**Status**: Proposed

## 0. Executive Summary & Objective

Replace price-only alerts with one engine: an alert is a saved screen with edge detection. A scheduler
evaluates `AlertRule`s per frequency on bar close using the existing `evaluate_group`, diffs
consecutive result sets to emit Enter/Exit events, and fans them out to notification sinks. The same
composition wires the MCP ports for [tantra-agent 0013](../../../tantra-agent/docs/plans/0013-screener-mcp-tools.md) (agent rule R4).

References: research [`trendlyne-screener-alerts-mcp.md`](../../../docs/research/trendlyne-screener-alerts-mcp.md) (F7 engine, F8 wiring);
root roadmap [`0007`](../../../docs/plans/0007-screener-taxonomy-and-alerts.md).
Depends on: [tantra-core 0004](../../../tantra-core/docs/plans/0004-classification-and-alert-wire-types.md),
[tantra-data 0007](../../../tantra-data/docs/plans/0007-instrument-master-index-membership.md).

## 1. Context & Gaps Covered

`tantra-runtime-api/src/screener.rs` has `validate_group`, `evaluate_group(group, bars)`,
`latest_metrics`, `check_scan_budget`; `tantra-runtime-api-rest` serves `GET /v1/screener/scan`.
`chat_alerts.rs` keeps price alerts in per-user `ProfileStore` with no group rules and no edge semantics.

## 2. Component Specifications & Phases

### Phase A — Persistence
- `AlertRule` and `AlertEvent` tables in `tantra-runtime-persistence` (SQLx), per-owner scoped,
  plus last-result-set snapshot per rule for edge detection. Restart-safe.
- Migrate existing `ProfileStore` price alerts as `Level` rules (idempotent migration, old route kept as a view).

### Phase B — Evaluator & scheduler
- Scheduler per frequency (15m/30m/1H/Daily) triggered on bar close; resolves universe, runs
  `evaluate_group`, honours `check_scan_budget`.
- Edge detection: diff against previous set -> `Enter`/`Exit`; `Level` fires while true subject to
  cooldown. Dedupe key `(rule, instrument, trigger, bar_ts)`; per-rule cooldown.
- Clock injected via the existing runtime clock port; no `Instant::now()` in domain logic.

### Phase C — Notification fan-out
- `NotificationSink` port; implementations: WS push, webhook, email digest. Bounded channels with an
  explicit `BackpressurePolicy`; slow sinks cannot stall evaluation.

### Phase D — REST (Caller-scoped, versioned)
- `/v1/alerts/rules` (POST/GET/PATCH/DELETE) and `/v1/alerts/events` (GET, paged); rules keyed by
  `(caller_id, rule_id)`; bounded body sizes; OpenAPI entries from core schemas.

### Phase E — Rule templates (one engine for every alert family)
Price (daily change +/-5%/10%, week/52-wk/10-yr high-low cross, near 52-wk extremes); moving averages
(golden/death cross, price vs SMA/EMA periods); volume and delivery (volume shockers, rising/high
delivery %); breakouts (LTP vs pivot R1-R3/S1-S3); results (declared today, YoY/QoQ revenue/profit,
post-result outperformance, upcoming results); corporate actions (dividend, bonus, split);
big deals (>1% insider/SAST, blocked on tantra-data gap 0024); QVM transitions (momentum trap, falling
comet, rising momentum, strong performer getting expensive); screener enter/exit; email digests.

### Phase F — MCP port wiring (R4)
- Compose `ScreenerPort`, `UniversePort`, `AlertRulePort`, `WatchlistPort` implementations here.

## 3. Test Plan (TDD)

In `crates/*/tests/`: edge-detection golden sequences, dedupe/cooldown, restart recovery, budget
rejection, Caller isolation negatives (other tenant's rule invisible), backpressure on slow sink,
price-alert migration, template validity (every template passes `validate_group`).

## 4. Dependencies & Tier Notes

L3; may use L0-L2 only. No `std::sync::Mutex` in handlers, no unbounded channels, no sync I/O.

## 5. Open Questions

- Email/WhatsApp delivery infrastructure: ship webhook + WS first, email as digest later?
- Per-user rule and universe-size quotas.
