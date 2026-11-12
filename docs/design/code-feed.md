# `tantra-runtime-feed` — Market-Data Storage Shim Reference

Crate `crates/tantra-runtime-feed/` (1 src file, 30 lines).

## 1. Purpose

A pure re-export shim. The Parquet tick store, the market-data catalog, and the
local wire types live in `tantra-market-store` (the `tantra-market` repo,
Tier 2A). This crate keeps the historical `tantra_runtime_feed` import path
working for `tantra-runtime-api-rest` and other runtime consumers, so runtime
crates don't have to be renamed in one sweep.

## 2. Public API (all re-exports of `tantra_market_store`)

### Root re-exports

| Symbol | Role |
|---|---|
| `ParquetTickStore` | columnar tick/bar storage on Parquet files |
| `Lakehouse`, `LakehouseCompression`, `LakehouseError` | dataset catalog over stored data |
| `TickRecorder`, `RecorderConfig`, `RecorderStats` | live tick capture into the store |
| `CatalogInstrumentMaster`, `CatalogError` | instrument master backed by the catalog |
| `LiveQuoteStore` | latest-quote read model |
| `Dataset`, `DatasetFeed`, `DatasetReader`, `DatasetChunk`, `DatasetBuildError`, `StreamedDatasetFeed` | run-time bar/tick iteration |
| `InMemoryTickReader` | in-memory reader for tests |
| `ParquetBarSource`, `ParquetError`, `ReaderError`, `TickStoreError` | source/error types |
| `ColumnarSlice`, `ColumnarSliceBuilder` | zero-copy column access |
| `ReportWriter`, `CsvReportWriter`, `JsonReportWriter`, `MarkdownReportWriter`, `ExportError` | report writers |
| `TICK_DIR`, `tick_schema`, `snapshot_schema`, `write_tick_file` | layout + flat tick files (`VERSION = 1`) |

### Sub-modules

- `ports` — `tantra_market_store::ports::*` (local port traits and data types).
- `import` — Parquet import helpers (historical `tantra_runtime_feed::import` path).
- `wire` — local market-data wire types.

## 3. Data Flow

`DataSourceConfig::Local { root }` ([`code-config.md`](code-config.md)) →
`Lakehouse`/`DatasetReader`/`ParquetBarSource` → bars/ticks into
`tantra-runtime-backtest`; `TickRecorder` + `write_tick_file` persist live
captures; `CatalogInstrumentMaster` answers `/instruments`.

## 4. Dependencies

`tantra-market-store` only — this crate has no logic of its own.

## 5. Tests

None in-crate; coverage lives with `tantra-market-store` (owner of the real
implementations) and the api-rest integration tests that read Parquet through
this path (`tests/rest_backtest_l2_parquet.rs`).

## 6. Placeholder Crates in This Workspace

The runtime workspace still carries three stub crates whose real content lives
elsewhere; documented here so nobody re-implements them in the wrong repo:

| Stub crate | Current content | Real home |
|---|---|---|
| `tantra-runtime-market` | `// Stub crate: tantra-me-market` | `tantra-market` repo (tick coalescing, L2/L3 depth) |
| `tantra-indicators` | `// Stub crate: tantra-indicators` | `tantra-market` repo (indicator library) |
| `tantra-runtime-test-utils` | `// Stub crate: tantra-testing` | superseded by `tantra-runtime-testing` |

## 7. Invariants & Notables

- **No logic**: the shim exists only so the import path survives; adding logic
  here would duplicate `tantra-market-store`.
- `tantra-runtime-feed` must not be confused with the *live feed* subsystem:
  broker WebSockets are `tantra-market`; this is the *storage + read* layer.

## Related

- `code-config.md` — `DataSourceConfig` that selects the local root
- `code-api-rest.md` — the HTTP consumer of the catalog and tick files
- `code-backtest.md` — the engine that iterates `DatasetFeed`s
- `tantra-market` repo — owner of the re-exported implementations
