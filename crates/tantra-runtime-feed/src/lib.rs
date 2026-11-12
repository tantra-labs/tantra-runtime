//! Re-export shim for the runtime API layer.
//!
//! The Parquet tick store, market-data catalog, and wire types live in
//! `tantra-market-store` (Tier 2A). This crate keeps the historical
//! `tantra_runtime_feed` import path working for `tantra-runtime-api-rest`
//! and other runtime consumers.

pub use tantra_market_store::{
    CatalogError, CatalogInstrumentMaster, ColumnarSlice, ColumnarSliceBuilder, CsvReportWriter,
    Dataset, DatasetBuildError, DatasetChunk, DatasetFeed, DatasetReader, ExportError,
    InMemoryTickReader, JsonReportWriter, Lakehouse, LakehouseCompression, LakehouseError,
    LiveQuoteStore, MarkdownReportWriter, ParquetBarSource, ParquetError, ParquetTickStore,
    ReaderError, RecorderConfig, RecorderStats, ReportWriter, StreamedDatasetFeed, TICK_DIR,
    TickRecorder, TickStoreError, snapshot_schema, tick_schema, write_tick_file,
};

/// Local port traits and data types.
pub mod ports {
    pub use tantra_market_store::ports::*;
}

/// Parquet import helpers (historical `tantra_runtime_feed::import` path).
pub mod import {
    pub use tantra_market_store::import::*;
}

/// Local market-data wire types.
pub mod wire {
    pub use tantra_market_store::wire::*;
}
