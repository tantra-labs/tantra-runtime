# 0026 — Two composition roots and a partial `tantra serve` front-end; duplicated state wiring and scattered env config

**Status**: open
**Filed**: 2026-11-23 — workspace design review (root plan 0021) · **Kind**: design-flaw · **Severity**: high
**Plan**: [0012](../plans/0012-api-design-review-2026-11.md)

## What's stubbed or hardcoded

- `crates/tantra-runtime-api-rest/src/bootstrap.rs:535-560` (`serve_with_shutdown`) and `:594-603` (`serve`) duplicate the bind/warn/pump/serve/stop sequence; `crates/tantra-runtime-api-rest/src/paper_serve.rs:234` spawns a third serve path (`tokio::spawn(serve_with_config(..))`), plus `src/bin/paper_serve.rs:1-60` (hand-rolled arg parsing, 174 lines) over `paper_serve.rs:185` `run_tape_serve`, versus `bootstrap.rs:369` `prepare_server` / `:574` `run_server_blocking`.
- `tantra-ui/crates/tantra-cli/src/args.rs:218-239` `ServeArgs` exposes only data_dir, addr, cors_origin, profiles/portfolios/journals dirs and tokens_file; `tantra-ui/crates/tantra-cli/src/runner.rs:655-676` maps only those onto `ServeConfig`.
- `crates/tantra-runtime-api-rest/src/bootstrap.rs:163-279` — `ServeAuthConfig` (jwt_secret, jwt_rsa_key, issuer, audience, audit_log_file), `ServeIngestConfig`, `ServeRiskConfig.risk_config`, `ServeFillConfig`, `ServeRunsConfig.max_*`, `trading_state` and `operator_journal` are unreachable from the CLI; only the `TANTRA_DATABASE_URL` env path exists.
- `tantra-ui` messages tell users to run `tantra paper serve` (`crates/tantra-cli/src/args.rs:61`, `crates/tantra-cli/src/paper.rs:10,92,107`) but no such subcommand exists in `Command`.
- `crates/tantra-runtime-api-rest/src/bootstrap.rs` prints with `println!`/`eprintln!` (the `non_loopback_warning` free helper/re-export was removed in closed gap `0020`).
- Env config is read at scattered sites instead of `tantra-runtime-config`: `TANTRA_LIBRARY_MCP_CMD` (`bootstrap.rs:433`, `state.rs:235`), `TANTRA_DATABASE_URL` (`bootstrap.rs:459-478,580`), `TANTRA_JOBS_PATH` (`chat.rs:176`), `TANTRA_REGIME_INSTRUMENT` (`chat.rs:406`).

## Why

JWT-only auth (the fail-closed rule in `.agents/rules/security-and-secrets.md` §1) is impossible via `tantra serve`; operators must run an unshipped binary, and the L5 binary owning the server entry makes the UI repo a runtime release gate. Auth gating, warnings and shutdown order diverge between daemons, and config cannot be validated or dumped in one place.

## How to close it

1. Single `ServerRuntime::run(config, shutdown)` used by `serve`, `serve_with_shutdown` and `run_tape_serve`.
2. Move env parsing into `tantra-runtime-config::EnvConfig` with one `from_env()`; load the full `ServeConfig` from the `tantra.jsonc` `server` block + `TANTRA_*` env + flags (precedence flag > env > file).
3. Ship a `tantra-runtime` (or `tantra-server`) bin in `tantra-runtime-api-rest` using it; replace `bin/paper_serve.rs` hand-rolled parsing.
4. `tantra serve` becomes optional (`serve` cargo feature) and delegates; fix or remove the `tantra paper serve` text, or add `paper serve` delegating to `run_tape_serve`.

**Verification**: table-driven config precedence test (flag > env > file) in `tests/`; both binaries produce identical auth gating; spawn-binary test for startup failure on insecure bind (closed gap `0020`). Gates: `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, `make check`, targeted `cargo test -p tantra-runtime-api-rest --profile prod`, `make prod`.

## Related

- Root plan 0022 (deployment); `tantra-ui` plan 0011 R1 (UI side: demote `serve` to a delegating subcommand; this gap is its provider side). Root plan 0019 P3.5 covers runtimes only.
- Closed gap `0020` (fail-closed bind), gap 0022 (readiness/SIGTERM/logging), root gap 0028 (configuration contract).
