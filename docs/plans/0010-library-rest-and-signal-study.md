# 0010 — Library MCP Client & REST Proxy (wiring plan)

**Status**: Proposed — not wired (2026-11-20). Parent: root plan 0016 (Track T22.6). The library itself is
implemented in `tantra-strategies` (plan 0005, package `tantra_library`, stdio server `tantra-library-mcp`).

## 0. Objective

Expose the 4 library MCP tools over runtime REST by supervising `tantra-library-mcp` as a subprocess. No Rust
crate imports Python; the tier DAG is preserved (process boundary).

## 1. `LibraryMcpClient` (new module in the runtime composition layer)

- Spawns `TANTRA_LIBRARY_MCP_CMD` (default `tantra-library-mcp`) with env `TANTRA_LIBRARY_DATA_ROOT`
  (configured data root); stdio, newline JSON-RPC 2.0 (`initialize`, `tools/call`, `ping`).
- Bounded `mpsc` channels (explicit `BackpressurePolicy`: structured 429/503 when full), per-request timeout,
  single reader task correlating ids, max line 8 MiB.
- Supervision: restart with exponential backoff + jitter; health via `ping`; reported in readiness.
  Unavailable library returns `PortError::unavailable`-style 503, never fake data.
- No `Runtime::block_on`; async `tokio::process`.

## 2. REST proxy (1:1 with tools)

| Endpoint | Tool |
|---|---|
| `GET /api/v2/library/entries?query&kind&family&limit` | `tantra_library_search` |
| `GET /api/v2/library/entries/{id}?format=markdown\|json` | `tantra_library_describe` |
| `POST /api/v2/indicators/compute` | `tantra_indicator_compute` |
| `POST /api/v2/experimental-analysis/signal-study` | `tantra_signal_study` |

- Extract `Caller` on all routes; auth middleware per `config.auth.is_enabled()`; request body capped
  (`axum::body::to_bytes` with explicit cap, <= 4 MiB; inline bars <= 5000 per tool contract).
- Register request/response schemas in `openapi.rs`; versioned `/v2` only.
- Where `bars_file` is accepted, the runtime passes it through unchanged: path confinement is the server's
  (`TANTRA_LIBRARY_DATA_ROOT`); the REST layer must not accept absolute paths from untrusted callers
  without allow-listing.

## 3. v1/v2 reconciliation

Existing experimental-analysis routes are mounted in `tantra-runtime-api-rest/src/v1.rs` with handlers in
`src/experimental_analysis.rs` (also `auth.rs`, `openapi.rs`) under `/api/v1`, while plans say `/api/v2`.
New library routes go under `/api/v2`; decide whether to mount existing studies under `/api/v2` too (alias
with deprecation header on v1) and record the outcome here. Unrelated: `PythonStrategyActor`
(`tantra-runtime-actors/src/python.rs`) is for strategies and is not reused.

## 4. Lakehouse source

Today the server reads CSV from the data root. When `tantra-data` gap 0026 lands, only the MCP server config
changes (lakehouse `BarSource`); the REST contract is unchanged, and `ref` (data commit) is passed in
requests.

## 5. Tests

Fake MCP server (small script or test binary under `tests/`) covering: happy path per endpoint, timeout,
child crash + restart/backoff, oversize payload 413, unauthenticated 401, tenant `Caller` propagation,
backpressure 429, OpenAPI snapshot. Gates: `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`,
`make check`, targeted test binary, `make prod`.
