# व्यापार・运行时 vyapaar-runtime

> *Central Runtime & Automation: Tokio actor pipeline, event-driven backtester, strategy enhancer (GA/WFO), Axum REST/WS API, cron jobs, and SQLite snapshots.*

Core orchestration engine uniting data feeds, risk gates, strategies, and execution.

## Subcrates

- `vyapaar-runtime` — Unified runtime runner
- `vyapaar-runtime-actors` — Tokio actor engine and message router
- `vyapaar-runtime-backtest` — Event-driven historical backtester
- `vyapaar-runtime-strategy` — Strategy lifecycle manager and signal pipeline
- `vyapaar-runtime-enhancer` — Genetic algorithm & walk-forward optimizer
- `vyapaar-runtime-persistence` — SQLite state snapshots & git2 versioned state
- `vyapaar-runtime-api` — Core RPC/API definitions
- `vyapaar-runtime-api-rest` — Axum REST & WebSocket server
- `vyapaar-runtime-jobs` — Cron scheduler & background task worker
- `vyapaar-runtime-config` — Layered environment & TOML configuration
- `vyapaar-runtime-testing` — Mock brokers, feeds, and harness fixtures

## Build & Test

```bash
make prod
make check
make test
```

License: AGPL-3.0.
