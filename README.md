# व्यापार・运行时 tantra-runtime

> *Central Runtime & Automation: Tokio actor pipeline, event-driven backtester, strategy enhancer (GA/WFO), Axum REST/WS API, cron jobs, and SQLite snapshots.*

Core orchestration engine uniting data feeds, risk gates, strategies, and execution.

## Subcrates

- `tantra-runtime` — Unified runtime runner
- `tantra-runtime-actors` — Tokio actor engine and message router
- `tantra-runtime-backtest` — Event-driven historical backtester
- `tantra-runtime-strategy` — Strategy lifecycle manager and signal pipeline
- `tantra-runtime-enhancer` — Genetic algorithm & walk-forward optimizer
- `tantra-runtime-persistence` — SQLite state snapshots & git2 versioned state
- `tantra-runtime-api` — Core RPC/API definitions
- `tantra-runtime-api-rest` — Axum REST & WebSocket server
- `tantra-runtime-jobs` — Cron scheduler & background task worker
- `tantra-runtime-config` — Layered environment & TOML configuration
- `tantra-runtime-testing` — Mock brokers, feeds, and harness fixtures

## Build & Test

```bash
make prod
make check
make test
```

License: AGPL-3.0.
