# 0023 — Three parallel route inventories (`ENDPOINTS`, `RouteTable`, hand-built OpenAPI) and stale capability manifest

**Status**: open
**Filed**: 2026-11-23 — workspace design review (root plan 0021) · **Kind**: duplication · **Severity**: medium
**Plan**: [0012](../plans/0012-api-design-review-2026-11.md)

## What's stubbed or hardcoded

- `crates/tantra-runtime-api/src/wire.rs:458` — `pub static ENDPOINTS: &[(&str, &str)]`, a hand-kept list.
- `crates/tantra-runtime-api-rest/src/route_table.rs` — the real registry (scope + body cap).
- `crates/tantra-runtime-api-rest/src/openapi.rs` (2648 LOC) — `OpenApiDocument::generate()` registers paths by hand plus `register_schema::<T>` at `openapi.rs:215`.
- Agreement is enforced only by tests (`tests/capabilities.rs:46-55,118`, `tests/auth_scope.rs:8`): drift is caught after the fact, not by construction.
- `crates/tantra-runtime-api-rest/src/lib.rs:402-405` — `capability_manifest()` still reports `crates: ["tantra-api","tantra-api-rest","tantra-messages","tantra-entities"]` and `market_packs`, crate names that no longer exist.
- `crates/tantra-runtime-api-rest/src/lib.rs:392` — `NOT_IMPLEMENTED_ENDPOINTS` is an always-empty const.
- Client layers keyed off the generated spec: `crates/tantra-runtime-client/src/lib.rs:25-35` `ROUTES` (8 routes, 444 LOC, checked only by `tests/contract.rs`), Python `tantra-ffi/python/tantra/client/_models.py` (generated models, hand-written call layer), TS `tantra-ui-web/src/lib/api-schema.d.ts`; each reimplements envelope/error parsing and idempotency-header logic.

## Why

Adding a route needs edits in three places, and any drift propagates to three clients. Error-envelope and idempotency-header handling exists three times.

## How to close it

1. Make `RouteTable` entries carry request/response schema registrations (`.get(path, handler, scope).returns::<T>()`); generate OpenAPI paths and `ENDPOINTS` from `RouteTable::entries()`; delete the static list and the empty const.
2. Fix manifest contents from `env!("CARGO_PKG_NAME")`.
3. Generate client operation stubs (ids, paths, request/response types) from `schema/openapi.v2.json` for the Rust and Python clients, as TS already does; keep only envelope/transport hand-written.
4. Add a CI `schema-drift` check across the three clients.

**Verification**: golden `schema/openapi.v2.json` stays byte-identical after the generation switch; `tests/openapi_conformance.rs`, `tests/capabilities.rs`, `tests/route_registry.rs` stay green; drift check fails on a deliberately unregistered route. Gates: `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, `make check`, targeted tests above with `--profile prod`.

## Related

- Root gap 0022 (one canonical OpenAPI document and generated clients); partial overlap with root plan 0019 P3.1 (splitting `openapi.rs`) and design 0006 (consolidation, R3 clients landed; call-layer generation not covered).
- Provider repos for client codegen: `tantra-ffi` (Python), `tantra-ui-web` (TS).
