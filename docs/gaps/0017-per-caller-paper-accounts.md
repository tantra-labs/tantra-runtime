# 0017 — Per-caller paper accounts (cash and positions)

**Status**: open

## What's stubbed or hardcoded
Order ownership is per caller (gap 0013): callers only list, fetch, cancel and stream their own
orders, and `client_order_id` cannot collide across callers. The paper exchange behind them is
still one shared instance, so every caller shares one cash balance and one set of positions:
`PositionSnapshot` / `EquitySnapshot` on the `/stream` `portfolio` channel, `POST /positions/close`,
and `OrderLedger::positions` are account-wide.

## Why
Separate per-caller accounts need their own exchange state; the shared `ExecGateway` and
`OrderLedger` were built as a single-tenant simulator.

## How to close it
A per-caller `ExecGateway` + `OrderLedger` (lazily created, capped like `CallerJournals`) with
bar/tick fan-out to every caller's exchange; route portfolio snapshots, close-positions and
position queries through the caller's instance; then drop the owner map from the shared ledger.
