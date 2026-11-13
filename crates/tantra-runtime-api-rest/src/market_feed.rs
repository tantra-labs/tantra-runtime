//! Market-data feed/read ports re-exported from `tantra-market-store`.
//!
//! The Parquet tick store, market-data catalog, and wire types live in
//! `tantra-market-store` (Tier 2A). The historical `tantra_runtime_feed`
//! shim crate was absorbed into this module during crate-topology
//! consolidation (root plan 0003, gap 0004) so consumers keep a single
//! import path instead of reaching into the store crate directly.

pub use tantra_market_store::{
    CatalogError, CatalogInstrumentMaster, ColumnarSlice, ColumnarSliceBuilder, CsvReportWriter,
    Dataset, DatasetBuildError, DatasetChunk, DatasetFeed, DatasetReader, ExportError,
    InMemoryTickReader, JsonReportWriter, Lakehouse, LakehouseCompression, LakehouseError,
    LiveQuoteStore, MarkdownReportWriter, ParquetBarSource, ParquetError, ParquetTickStore,
    ReaderError, RecorderConfig, RecorderStats, ReportWriter, StreamedDatasetFeed, TICK_DIR,
    TickRecorder, TickStoreError, snapshot_schema, tick_schema, write_tick_file,
};

/// Local port traits and data types (historical `tantra_runtime_feed::ports` path).
pub mod ports {
    pub use tantra_market_store::ports::*;
}

/// Parquet import helpers (historical `tantra_runtime_feed::import` path).
pub mod import {
    pub use tantra_market_store::import::*;
}

/// Local market-data wire types (historical `tantra_runtime_feed::wire` path).
pub mod wire {
    pub use tantra_market_store::wire::*;
}
