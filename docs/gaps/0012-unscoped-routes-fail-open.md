# 0012 — Unscoped routes are public even with auth enabled (`/ws/live`, `/trade-journal/*`, `/market-data/*`)

**Status**: open

## What's stubbed or hardcoded
`tantra-runtime-api-rest` treats any route without a row in `ROUTE_SCOPES` / `V2_ONLY_SCOPES`
as **public** (`src/auth.rs:256`). The drift guard `unscoped_routes` (`src/auth.rs:582`) only
checks `tantra_runtime_api::ENDPOINTS`, not the real router. These routes have no row, so they
are reachable without a token even when `config.auth.is_enabled()`:
- `GET /ws/live` (`src/v1.rs:313`, `src/ws_live.rs:28`). It accepts `place_order` and
  `cancel_order` commands, so paper orders can be placed without authentication. No `Caller`.
- `/trade-journal/{accounts,overview,trades,trades/:id,calendar,notes,compare}`
  (`src/v1.rs:299-310`). This includes the writes `POST accounts`, `PATCH trades/:id` and
  `POST notes`. None of them is keyed by `Caller`.
- `GET /market-data/{symbol-profile/:id,calendar,block-deals}` (`src/v1.rs:184-189`).

Related:
- `/ws/chat` sessions are keyed by `?workspace=` and not by `Caller` (`src/chat.rs:512-521`).
- `src/trade_journal.rs:515,790` return bodies without the response envelope.

## Why
Routes were added after the scope table and drift guard. The guard's source of truth
(`ENDPOINTS`) isn't the router, and the default for missing rows is allow. That breaks the
fail-closed rule in `.agents/rules/security-and-secrets.md` §1 and the tenant-isolation
rule in §2.

## How to close it
Root design `docs/design/0006-runtime-api-surface.md` §5 phase R0:
1. Add scope rows: `/ws/live` → `trade` plus a `Caller` extractor; trade-journal reads →
   `read`, writes → `trade` (or a `journal` scope), keyed by `Caller`; `/market-data/*` → `read`.
2. Flip the default for unscoped routes to deny. Make the drift guard walk the actual router
   (or the route registry from design 0006 §4).
3. Key `/ws/chat` sessions by `(caller_id, workspace)`.
4. Envelope the two bare trade-journal bodies.
5. Add negative tests for each route: 401 without a token, 403 with the wrong scope.
