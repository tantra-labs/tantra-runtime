# `vyapaar-runtime` — DDD Architecture & Central Runtime Bounded Context

## 1. Bounded Context & Responsibility
`vyapaar-runtime` (Tier 3B, ported from `honba-ya`) is the **Central Application & Orchestration Runtime** wiring together the pure domain heart (`vyapaar-ledger`), pre-trade risk gate (`vyapaar-risk`), market feeds (`vyapaar-market`), and execution gateways (`vyapaar-exec`) via supervised Tokio actors, deterministic backtesting, and an Axum OpenAPI 3.1 REST/WebSocket gateway.

## 2. Sub-Crate Topology (10 Crates)

| Sub-Crate | Layer | Responsibility |
| :--- | :--- | :--- |
| `vyapaar-runtime-config` | Config | Typed TOML/env configuration profiles (`LiveConfig`, `PaperConfig`, `BacktestConfig`). |
| `vyapaar-runtime-testing` | Test Harness | Deterministic `MockBroker`, `ScriptedGateway`, synthetic feeds, and contract assertions. |
| `vyapaar-runtime-strategy` | Pure Domain | `Strategy` trait, `StrategyContext`, `OrderIntent`, `StrategyManifest`, `StepPipeline` (`step.toml` IR), and zero-I/O `StrategyRunner`. |
| `vyapaar-runtime-persistence`| Storage Infra | SQLite & PostgreSQL (`sqlx`) implementations of `vyapaar-ledger` repository traits (`OrderRepository`, `AccountRepository`, `PositionRepository`, `LedgerRepository`). |
| `vyapaar-runtime-backtest` | Sim Engine | Event-driven research-live parity backtester (`MarketEvent` → `SignalEvent` → `OrderEvent` → `FillEvent`), Arrow `CatalogLoader`, L2 depth fill simulator, T+1 Demat settlement, and Indian statutory costs. |
| `vyapaar-runtime-enhancer` | Optimizer | Automatic Strategy Enhancer (Genetic Algorithm, Bayesian TPE, Walk-Forward Optimization, Deflated Sharpe Ratio, Monte Carlo stress). |
| `vyapaar-runtime-actors` | Actor System | Supervised Tokio actors (`MarketDataActor`, `StrategyActor`, `PositionSizingActor`, `RiskActor`, `ExecutionActor`) with OpenTelemetry trace propagation and mailbox metrics. |
| `vyapaar-runtime-jobs` **[NEW]**| Scheduler | Cron and background task scheduler (08:45 IST TOTP auth refresh, EOD bhavcopy sync, T+1 settlement sweep, state snapshotter). |
| `vyapaar-runtime-api` | API Contracts | High-level application service traits and DTOs for the control plane. |
| `vyapaar-runtime-api-rest` | HTTP/WS Server| Axum OpenAPI 3.1 REST endpoints (`/health`, `/orders`, `/positions`, `/portfolio`, `/risk`, `/screener/scan`) and zero-polling `/ws/live` broadcast gateway. |
