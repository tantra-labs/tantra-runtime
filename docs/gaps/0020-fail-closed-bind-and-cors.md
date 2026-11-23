# 0020 — Unauthenticated non-loopback bind only warns; panic on bad token file; CORS never allows Authorization

**Status**: open
**Filed**: 2026-11-23 — workspace design review (root plan 0021) · **Kind**: design-flaw · **Severity**: high
**Plan**: [0012](../plans/0012-api-design-review-2026-11.md)

## What's stubbed or hardcoded

- `crates/tantra-runtime-api-rest/src/bootstrap.rs:597-598` (and `:540`) — `non_loopback_warning` is an `eprintln!`; startup proceeds on `0.0.0.0` with no auth. The rule requires startup to fail unless an explicit insecure flag is passed.
- `crates/tantra-runtime-api-rest/src/lib.rs:250` — `FileTokenStore::new(..).expect("failed to load token file")` inside `api_router_with_config` (panic at router build, in library code).
- `crates/tantra-runtime-api-rest/src/lib.rs:366` — `.allow_headers([axum::http::header::CONTENT_TYPE])` only; no `AUTHORIZATION`, so browser callers of the authenticated API cannot send a bearer token cross-origin.
- `crates/tantra-runtime-api-rest/src/lib.rs:353-356` — empty `cors_origins` yields a bare `CorsLayer::new()`.

## Why

A container deployment binding `0.0.0.0:8000` without a token file silently exposes paper-order placement and all stores; CORS preflight for bearer-token SPAs on another origin fails. Breaks `.agents/rules/security-and-secrets.md` §1.2 and §1.3.

## How to close it

1. Add `ServeConfig.allow_insecure_bind: bool` (plus `TANTRA_ALLOW_INSECURE_BIND`); return `Err` from `prepare_server`/`serve*` when `!auth_enabled && !addr.ip().is_loopback()`.
2. Make `api_router_with_config` return `Result` and propagate token-file errors instead of `expect`.
3. Extend `allow_headers` with `AUTHORIZATION` when `config.auth.is_enabled()`; define the empty-origins behaviour explicitly.
4. Replace `non_loopback_warning` (public re-export at `crates/tantra-runtime-api-rest/src/lib.rs:94`) with the typed startup error.

**Verification**: negative bootstrap test (non-loopback, no auth, no override fails); bad token file returns `Err`, never panics; CORS preflight test asserting `access-control-allow-headers` contains `authorization`. Gates: `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, `make check`, targeted `cargo test -p tantra-runtime-api-rest --test bootstrap --profile prod`.

## Related

- Root plan 0019 P0.7 covers only the JWT issuer name.
- Gap 0026 (single composition root) hosts the shared startup path this check belongs in.
- Root plan 0022 (container deployment binds `0.0.0.0`).
