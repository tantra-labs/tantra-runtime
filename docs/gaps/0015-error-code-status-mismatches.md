# 0015 — Error codes disagree with HTTP status on 34 v2 routes

**Status**: open

## What's stubbed or hardcoded
Design 0006 R1c fixed one HTTP status per `ErrorCode` (`ErrorCode::http_status()`), and
`tests/envelope_enforced.rs` checks every v2 GET error against it. The test allowlists 34 routes in
`KNOWN_STATUS_DEVIATIONS` (comment cites this gap):
- **33 routes answer `503` with code `not_implemented`** (the mapping says `501`). The cause is a
  missing backing store or reader: the handler means "unavailable", so the code should be
  `service_unavailable`. Emitters: `portfolio.rs:47-49,119`, `reports.rs:106-108`,
  `runs.rs:75-77,191-193`, `streams.rs:158-160`, `trade_journal.rs:332-334`, `profile.rs:38-40`
  (store-missing helpers). Routes: `/portfolio/{positions,holdings,trades,chart,concentration,wac}`,
  `/ledger`, `/tax/report`, `/fees`, `/positions`, `/positions/audit`, `/trades`,
  `/reports/{ledger,tax,snapshots,audit,concentration,wac,fees,equity-curve,fills/:order_id}`,
  `/backtests/:id/events`, `/orders/:id/fills`,
  `/trade-journal/{accounts,overview,trades,notes,compare}`.
- **`GET /trade-journal/calendar` answers `400` with `validation_invalid_request`** (mapping: `422`).

Related inconsistency, not yet in the allowlist because it isn't a GET: `runs.rs::status_of` maps
`NotImplemented`/`Unsupported` → 422 and `Timeout` → 429; `lib.rs` `unsupported_method` answers 405
with `not_implemented`.

## Why
R1c pinned the mapping and enforced the envelope; migrating every handler was kept out of that slice
because it changes wire codes that clients and ~8 test files assert (`not_implemented`).

## How to close it
1. Switch the store-missing helpers to `ErrorCode::ServiceUnavailable` and build responses with
   `market::error(detail)` (status from `http_status()`), citing the gap of the missing port where one
   exists (`PortError::unavailable` rule).
2. Make the calendar validation answer 422; replace `runs.rs::status_of` with `http_status()`.
3. Update the tests asserting `not_implemented` for these cases; empty `KNOWN_STATUS_DEVIATIONS`.
4. Note the wire-code change for web/TUI clients (R3 regenerates their types).
