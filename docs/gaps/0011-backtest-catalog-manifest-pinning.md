# 0011 — Backtest Catalog Manifest Pinning & Symbology Alias Resolution

**Status**: open

## What's stubbed or hardcoded

`crates/tantra-runtime-backtest/src/catalog.rs` (`CatalogLoader`) loads historical bars exclusively by walking unversioned filesystem directories (`{root}/catalog/{TF}/{EXCHANGE}/{SYMBOL}/*.parquet` and Lakehouse v2 Hive `bars/timeframe=.../`):

1. Backtests cannot pin a specific catalog snapshot (`commit_sha` or `refs/tags/<tag>` per root ADR [`docs/design/0004-data-catalog-lakehouse-layout.md`](../../../docs/design/0004-data-catalog-lakehouse-layout.md)), nor do backtest run journals record which `data_commit_sha` produced a result.
2. `CatalogLoader` indexes instruments strictly by directory/ticker string (`{EXCHANGE}/{SYMBOL}`), without resolving historical renames or delisted symbols through `reference/aliases` (`instrument_uid`), and assumes one file per symbol rather than row-group-pruned multi-instrument `(instrument_uid, ts)` v3 partitions.

## Why

- Without `commit_sha` pinning, corporate-action restatements, backfill corrections, or compaction runs silently alter historical backtest outputs, breaking statistical gate reproducibility (DSR/PBO/walk-forward sweeps).
- Scanning 2,000+ per-symbol yearly directories slows multi-instrument universe sweeps (`load_market_events`), whereas pruning `(instrument_uid, ts)`-sorted v3 partitions via `tantra-market` gap `0008` cuts file opens by 100×+.

## How to close it

Depends on [`tantra-market` gap 0008](../../../tantra-market/docs/gaps/0008-lakehouse-v3-manifest-and-snapshot-reader.md) and [`tantra-data` plan 0009](../../../tantra-data/docs/plans/0009-unified-lakehouse-v3-and-cas-manifest.md).

1. Extend `CatalogLoader` (and backtest runner config / REST run request) with an optional `data_ref: Option<String>` (defaulting to `refs/heads/main`), resolving partition objects and `reference/aliases` through `tantra_market_store::ManifestResolver`.
2. Add Arrow `RowGroup` predicate pushdown / min-max statistics pruning for `(instrument_uid, ts)` multi-instrument CAS Parquet objects while retaining legacy `{TF}/{EX}/{SYM}` read fallback during migration.
3. Persist the resolved `data_commit_sha` (and `as_of_ist`) into backtest run artifacts and `CatalogVerificationReport`.
4. Add integration tests verifying bit-identical backtest journals across a post-run catalog mutation/compaction when pinned to the original tag/commit.
