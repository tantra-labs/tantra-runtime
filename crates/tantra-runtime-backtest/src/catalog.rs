//! Zero-copy Arrow/Parquet Lakehouse catalog loader for `tantra-runtime-backtest`.
//!
//! Reads historical bars from:
//! 1. **Lakehouse v3 CAS manifests** (`{root}/refs/{heads,tags}/*`,
//!    `{root}/commits/<sha>.json`, `{root}/trees/<tree_hash>.json`,
//!    `{root}/objects/sha256/<ab>/<sha>.parquet`, `{root}/views/`, plus
//!    `reference/aliases` symbology rename continuity) with Parquet RowGroup
//!    min/max statistics pruning on `(instrument_uid, ts)` (`gap 0011`),
//! 2. Legacy `tantra-data/data/catalog` (`{root}/catalog/{TF}/{EXCHANGE}/{SYMBOL}/*.parquet`),
//! 3. Lakehouse v2 Hive-partitioned directories (`{root}/bars/timeframe={tf}/exchange={EXCHANGE}/...`).
//!
//! Streams Arrow [`RecordBatch`]es directly into canonical [`tantra_core::Bar`]
//! and [`MarketEvent`] sequences with exact `rust_decimal::Decimal` conversion.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::str::FromStr;

use arrow::array::{
    Array, Decimal128Array, Float64Array, Int64Array, StringArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
};
use arrow::datatypes::{DataType, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Duration, NaiveDate, Utc};
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::statistics::Statistics;
use rust_decimal::Decimal;
use tantra_core::{
    Bar, BarReader, BarRequest, Exchange, InstrumentId, PortError, PortResult, Price, Quantity,
    Timeframe,
};
use tantra_market_store::manifest::{InstrumentAlias, arrow_utf8_array};
use tantra_market_store::{ManifestCommit, ManifestEntry, ManifestError, ManifestResolver};
use thiserror::Error;

use crate::events::MarketEvent;

/// Errors raised while loading historical bars from the Parquet catalog.
#[derive(Debug, Error)]
pub enum CatalogError {
    /// Filesystem I/O failure.
    #[error("catalog io error at {path}: {source}")]
    Io {
        /// Path being accessed.
        path: PathBuf,
        /// Underlying OS error.
        source: std::io::Error,
    },
    /// Lakehouse v3 manifest or alias resolution failure.
    #[error("manifest error: {0}")]
    Manifest(#[from] ManifestError),
    /// Parquet decoding failure.
    #[error("parquet error in {path}: {source}")]
    Parquet {
        /// Parquet file path.
        path: PathBuf,
        /// Underlying Parquet error.
        source: Box<parquet::errors::ParquetError>,
    },
    /// Arrow record-batch decoding failure.
    #[error("arrow error in {path}: {source}")]
    Arrow {
        /// Parquet file path.
        path: PathBuf,
        /// Underlying Arrow error.
        source: Box<arrow::error::ArrowError>,
    },
    /// Missing required column in Parquet file.
    #[error("missing required column `{column}` in {path}")]
    MissingColumn {
        /// Parquet file path.
        path: PathBuf,
        /// Column name.
        column: &'static str,
    },
    /// Unsupported column data type in Parquet file.
    #[error("unsupported data type {data_type:?} for column `{column}` in {path}")]
    UnsupportedColumnType {
        /// Parquet file path.
        path: PathBuf,
        /// Column name.
        column: &'static str,
        /// Actual Arrow data type.
        data_type: Box<DataType>,
    },
    /// Invalid or non-finite numeric value in row.
    #[error("invalid value in {path} row {row} column `{column}`: {reason}")]
    InvalidRow {
        /// Parquet file path.
        path: Box<Path>,
        /// Row index within batch.
        row: usize,
        /// Column name.
        column: &'static str,
        /// Explanation.
        reason: Box<str>,
    },
    /// Requested timeframe or instrument partition does not exist.
    #[error(
        "partition not found for instrument {instrument} at timeframe {timeframe} under {root}"
    )]
    NotFound {
        /// Catalog root searched.
        root: PathBuf,
        /// Instrument requested.
        instrument: Box<InstrumentId>,
        /// Timeframe requested.
        timeframe: Box<str>,
    },
}

impl From<CatalogError> for PortError {
    fn from(err: CatalogError) -> Self {
        match err {
            CatalogError::NotFound { .. } => PortError::NotFound(err.to_string()),
            CatalogError::Manifest(ref me)
                if matches!(
                    me,
                    ManifestError::RefNotFound(_)
                        | ManifestError::CommitNotFound(_)
                        | ManifestError::MissingTree { .. }
                        | ManifestError::MissingObject { .. }
                ) =>
            {
                PortError::NotFound(err.to_string())
            }
            other => PortError::ConnectionFailed(other.to_string()),
        }
    }
}

/// Parquet RowGroup statistics pruning counters from a catalog load.
/// Grouped Lakehouse v3 bars: lowercase timeframe -> canonical instrument -> sorted bars.
pub type GroupedV3Bars = BTreeMap<String, BTreeMap<InstrumentId, Vec<Bar>>>;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LoadStats {
    /// Total Parquet RowGroups across inspected partition files.
    pub row_groups_total: usize,
    /// Number of Parquet RowGroups decoded after min/max statistics pruning.
    pub row_groups_read: usize,
}

/// Summary of one instrument partition inside a timeframe during catalog inspection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogInstrumentSummary {
    /// Timeframe directory name (e.g. `"1d"` or `"1m"`).
    pub timeframe: String,
    /// Canonical instrument identifier.
    pub instrument: InstrumentId,
    /// Total loaded bars for this partition.
    pub bar_count: usize,
    /// First bar timestamp when non-empty.
    pub first_timestamp: Option<DateTime<Utc>>,
    /// Last bar timestamp when non-empty.
    pub last_timestamp: Option<DateTime<Utc>>,
    /// First bar close price when non-empty.
    pub first_close: Option<Decimal>,
    /// Last bar close price when non-empty.
    pub last_close: Option<Decimal>,
}

/// Integrity verification summary across a market-data catalog.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogVerificationReport {
    /// Number of timeframe directories checked.
    pub timeframes_checked: usize,
    /// Total instrument partitions checked.
    pub total_instruments: usize,
    /// Total bars validated across all checked partitions.
    pub total_bars: usize,
    /// Detected OHLC, timestamp monotonicity, or empty-partition anomalies.
    pub anomalies: Vec<String>,
    /// Resolved Lakehouse v3 commit SHA when verified against a v3 manifest (`None` for legacy-only).
    pub data_commit_sha: Option<String>,
    /// Resolved Lakehouse v3 IST trading date (`None` for legacy-only).
    pub data_as_of_ist: Option<NaiveDate>,
}

impl CatalogVerificationReport {
    /// Returns `true` when zero anomalies were found across at least one timeframe.
    pub fn is_clean(&self) -> bool {
        self.timeframes_checked > 0 && self.anomalies.is_empty()
    }
}

/// Zero-copy Arrow/Parquet reader over Lakehouse v3 (`refs/`/`commits/`/`trees/`/`objects/`/`views/`),
/// legacy `catalog/`, and v2 `bars/`.
#[derive(Debug, Clone)]
pub struct CatalogLoader {
    root: PathBuf,
    data_ref: Option<String>,
    legacy_fallback: bool,
}

impl CatalogLoader {
    /// Create a catalog loader rooted at `root`.
    ///
    /// `root` may point either to the Lakehouse/data directory (e.g. `.../tantra-data/data`)
    /// or directly to a legacy `catalog/` directory.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            data_ref: None,
            legacy_fallback: true,
        }
    }

    /// Pin this loader to a git-style Lakehouse v3 ref (`"refs/heads/main"`,
    /// `"refs/tags/<tag>"`, or raw 64-hex `commit_sha`).
    ///
    /// Pinning an explicit ref disables legacy v1/v2 filesystem fallback unless
    /// resolved strictly from the v3 manifest.
    #[must_use]
    pub fn with_ref(mut self, reference: impl Into<String>) -> Self {
        self.data_ref = Some(reference.into());
        self
    }

    /// Configure whether legacy `{root}/catalog/` and v2 `{root}/bars/` fallbacks
    /// are consulted when no v3 manifest covers the requested dataset.
    #[must_use]
    pub fn with_legacy_fallback(mut self, enabled: bool) -> Self {
        self.legacy_fallback = enabled;
        self
    }

    /// Configured Lakehouse v3 ref (or `None` for default `refs/heads/main`).
    pub fn data_ref(&self) -> Option<&str> {
        self.data_ref.as_deref()
    }

    /// Whether legacy v1/v2 filesystem fallback is enabled.
    pub fn legacy_fallback(&self) -> bool {
        self.legacy_fallback
    }

    /// Resolve the effective `catalog` directory path.
    pub fn catalog_dir(&self) -> PathBuf {
        let candidate = self.root.join("catalog");
        if candidate.is_dir() {
            candidate
        } else {
            self.root.clone()
        }
    }

    fn resolver(&self) -> ManifestResolver {
        ManifestResolver::new(&self.root).with_legacy_fallback(self.legacy_fallback)
    }

    /// Resolve the active Lakehouse v3 commit metadata (at `data_ref` or `refs/heads/main`),
    /// returning `Ok(None)` when `root` has no `refs/`/`commits/` directory and no explicit
    /// `data_ref` was requested.
    pub fn resolved_commit(&self) -> Result<Option<ManifestCommit>, CatalogError> {
        let has_refs_or_commits =
            self.root.join("refs").is_dir() || self.root.join("commits").is_dir();
        if let Some(ref reference) = self.data_ref {
            let commit = self.resolver().resolve_ref(Some(reference))?;
            return Ok(Some(commit));
        }
        if has_refs_or_commits {
            match self.resolver().resolve_ref(None) {
                Ok(commit) => return Ok(Some(commit)),
                Err(ManifestError::RefNotFound(_)) => return Ok(None),
                Err(e) => return Err(e.into()),
            }
        }
        Ok(None)
    }

    /// List all timeframe names present in the catalog, sorted alphabetically.
    pub fn list_timeframes(&self) -> Result<Vec<String>, CatalogError> {
        let resolver = self.resolver();
        if resolver.has_v3_layout() || self.data_ref.is_some() {
            let (_commit, pairs) = resolver.resolve_partition_entries(
                "market",
                "bars",
                None,
                None,
                None,
                None,
                self.data_ref.as_deref(),
            )?;
            if !pairs.is_empty() || self.data_ref.is_some() || !self.legacy_fallback {
                let tfs: BTreeSet<String> = pairs
                    .into_iter()
                    .filter_map(|(entry, _)| entry.timeframe)
                    .collect();
                return Ok(tfs.into_iter().collect());
            }
        }

        if !self.legacy_fallback {
            return Ok(Vec::new());
        }

        let cat = self.catalog_dir();
        let mut out = Vec::new();
        for dir in sorted_subdirs(&cat)? {
            if let Some(name) = dir.file_name().map(|n| n.to_string_lossy().into_owned()) {
                out.push(name);
            }
        }
        Ok(out)
    }

    /// List all instruments available under a given timeframe (case-insensitive, e.g. `"1D"` or `"5m"`).
    pub fn list_instruments(&self, timeframe: &str) -> Result<Vec<InstrumentId>, CatalogError> {
        let resolver = self.resolver();
        if resolver.has_v3_layout() || self.data_ref.is_some() {
            let (_commit, pairs) = resolver.resolve_partition_entries(
                "market",
                "bars",
                None,
                None,
                Some(timeframe),
                None,
                self.data_ref.as_deref(),
            )?;
            if !pairs.is_empty() || self.data_ref.is_some() || !self.legacy_fallback {
                let aliases = resolver.load_aliases(self.data_ref.as_deref())?;
                let uid_to_id = build_uid_to_canonical_map(&aliases);
                let mut instruments = BTreeSet::new();
                for (entry, path) in pairs {
                    collect_v3_file_instruments(&path, &entry, &uid_to_id, &mut instruments)?;
                }
                return Ok(instruments.into_iter().collect());
            }
        }

        if !self.legacy_fallback {
            return Ok(Vec::new());
        }

        let Some(tf_dir) = self.find_timeframe_dir(timeframe)? else {
            return Ok(Vec::new());
        };
        let mut instruments = Vec::new();
        for exch_dir in sorted_subdirs(&tf_dir)? {
            let exch_name = file_name(&exch_dir);
            let Ok(exchange) = exch_name.parse::<Exchange>() else {
                continue;
            };
            for sym_dir in sorted_subdirs(&exch_dir)? {
                let symbol = file_name(&sym_dir);
                if !symbol.is_empty() {
                    instruments.push(InstrumentId::new(exchange, symbol));
                }
            }
        }
        Ok(instruments)
    }

    /// Load chronological [`Bar`]s for `instrument` at `timeframe` (e.g. `"1D"`, `"5M"`),
    /// optionally bounded by inclusive `from` and `to` UTC timestamps.
    pub fn load_bars(
        &self,
        instrument: &InstrumentId,
        timeframe: &str,
        from: Option<DateTime<Utc>>,
        to: Option<DateTime<Utc>>,
    ) -> Result<Vec<Bar>, CatalogError> {
        let (bars, _stats) = self.load_bars_with_stats(instrument, timeframe, from, to)?;
        Ok(bars)
    }

    /// Like [`Self::load_bars`], also returning Parquet RowGroup pruning statistics
    /// ([`LoadStats`]).
    pub fn load_bars_with_stats(
        &self,
        instrument: &InstrumentId,
        timeframe: &str,
        from: Option<DateTime<Utc>>,
        to: Option<DateTime<Utc>>,
    ) -> Result<(Vec<Bar>, LoadStats), CatalogError> {
        let resolver = self.resolver();
        if resolver.has_v3_layout() || self.data_ref.is_some() {
            if let Some(ref reference) = self.data_ref
                && !(self.root.join("refs").is_dir() || self.root.join("commits").is_dir())
            {
                return Err(ManifestError::RefNotFound(reference.clone()).into());
            }

            let (_commit, mut pairs) = resolver.resolve_partition_entries(
                "market",
                "bars",
                Some(instrument.exchange.as_str()),
                None,
                Some(timeframe),
                None,
                self.data_ref.as_deref(),
            )?;

            // Check whether v3 actually holds bars for this timeframe/exchange (or overall when pinned/strict).
            let v3_active = !pairs.is_empty()
                || self.data_ref.is_some()
                || !self.legacy_fallback
                || !resolver
                    .resolve_partition(
                        "market",
                        "bars",
                        None,
                        None,
                        None,
                        None,
                        self.data_ref.as_deref(),
                    )?
                    .is_empty();

            if v3_active {
                if pairs.is_empty() {
                    return Err(CatalogError::NotFound {
                        root: self.root.clone(),
                        instrument: Box::new(instrument.clone()),
                        timeframe: timeframe.into(),
                    });
                }

                pairs.sort_by(|a, b| {
                    a.0.min_ts
                        .cmp(&b.0.min_ts)
                        .then_with(|| a.0.max_ts.cmp(&b.0.max_ts))
                        .then_with(|| a.1.cmp(&b.1))
                });

                let aliases = resolver.load_aliases(self.data_ref.as_deref())?;
                let matcher = InstrumentMatcher::build(instrument, &aliases);

                let mut bars = Vec::new();
                let mut stats = LoadStats::default();
                let mut last_ts: Option<DateTime<Utc>> = None;

                for (entry, path) in pairs {
                    // Entry-level timestamp pruning (with a generous 1-day cushion so `views/`
                    // filename-bucketed bounds never falsely prune IST/UTC boundary bars; exact
                    // Parquet RowGroup min/max stats still prune precisely inside the file).
                    let cushion = Duration::days(1);
                    if let Some(f) = from
                        && entry.max_ts + cushion < f
                    {
                        continue;
                    }
                    if let Some(t) = to
                        && entry.min_ts - cushion > t
                    {
                        continue;
                    }

                    read_v3_cas_bars_into(
                        &path,
                        instrument,
                        &matcher,
                        from,
                        to,
                        &mut last_ts,
                        &mut bars,
                        &mut stats,
                    )?;
                }

                if bars.is_empty() && from.is_none() && to.is_none() {
                    return Err(CatalogError::NotFound {
                        root: self.root.clone(),
                        instrument: Box::new(instrument.clone()),
                        timeframe: timeframe.into(),
                    });
                }

                return Ok((bars, stats));
            }
        }

        if !self.legacy_fallback || self.data_ref.is_some() {
            return Err(CatalogError::NotFound {
                root: self.root.clone(),
                instrument: Box::new(instrument.clone()),
                timeframe: timeframe.into(),
            });
        }

        let files = self.locate_partition_files(instrument, timeframe)?;
        if files.is_empty() {
            return Err(CatalogError::NotFound {
                root: self.catalog_dir(),
                instrument: Box::new(instrument.clone()),
                timeframe: timeframe.into(),
            });
        }

        let mut bars = Vec::new();
        let mut stats = LoadStats::default();
        let mut last_ts: Option<DateTime<Utc>> = None;

        for path in files {
            read_parquet_bars_into(
                &path,
                instrument,
                from,
                to,
                &mut last_ts,
                &mut bars,
                &mut stats,
            )?;
        }

        Ok((bars, stats))
    }

    /// Load all bars from a Lakehouse v3 snapshot (`data_ref` or `refs/heads/main`) in a single
    /// streaming pass, grouped by lowercase timeframe (e.g. `"1d"`, `"5m"`) and canonical
    /// [`InstrumentId`] resolved via `reference/aliases`.
    ///
    /// Intended for server/cache warm-up paths that need the full universe without re-opening
    /// multi-instrument Parquet objects once per symbol.
    pub fn load_v3_bars_grouped(
        &self,
    ) -> Result<(Option<ManifestCommit>, GroupedV3Bars), CatalogError> {
        let resolver = self.resolver();
        if !resolver.has_v3_layout() && self.data_ref.is_none() {
            return Ok((None, BTreeMap::new()));
        }
        if let Some(ref reference) = self.data_ref
            && !(self.root.join("refs").is_dir() || self.root.join("commits").is_dir())
        {
            return Err(ManifestError::RefNotFound(reference.clone()).into());
        }

        let commit = self.resolved_commit()?;
        let (_c, mut pairs) = resolver.resolve_partition_entries(
            "market",
            "bars",
            None,
            None,
            None,
            None,
            self.data_ref.as_deref(),
        )?;
        let aliases = resolver.load_aliases(self.data_ref.as_deref())?;
        let uid_to_id = build_uid_to_canonical_map(&aliases);

        pairs.sort_by(|a, b| {
            a.0.timeframe
                .cmp(&b.0.timeframe)
                .then_with(|| a.0.min_ts.cmp(&b.0.min_ts))
                .then_with(|| a.0.max_ts.cmp(&b.0.max_ts))
                .then_with(|| a.1.cmp(&b.1))
        });

        let mut grouped: BTreeMap<String, BTreeMap<InstrumentId, Vec<Bar>>> = BTreeMap::new();
        for (entry, path) in pairs {
            let tf_key = entry
                .timeframe
                .as_deref()
                .unwrap_or("1d")
                .to_ascii_lowercase();
            let tf_map = grouped.entry(tf_key).or_default();
            read_v3_cas_all_bars_into(&path, &entry, &uid_to_id, tf_map)?;
        }

        for inst_map in grouped.values_mut() {
            for bars in inst_map.values_mut() {
                if !bars.windows(2).all(|w| w[0].timestamp < w[1].timestamp) {
                    bars.sort_by_key(|b| b.timestamp);
                    bars.dedup_by_key(|b| b.timestamp);
                }
            }
        }

        Ok((commit, grouped))
    }

    /// Load and chronologically interleave [`MarketEvent::Bar`] events across multiple
    /// instruments for direct feeding into [`crate::BacktestEngine::run_events`].
    pub fn load_market_events(
        &self,
        instruments: &[InstrumentId],
        timeframe: &str,
        from: Option<DateTime<Utc>>,
        to: Option<DateTime<Utc>>,
    ) -> Result<Vec<MarketEvent>, CatalogError> {
        let mut all_bars = Vec::new();
        for inst in instruments {
            let inst_bars = self.load_bars(inst, timeframe, from, to)?;
            all_bars.extend(inst_bars);
        }
        // Deterministic sort by (timestamp, exchange, symbol)
        all_bars.sort_by(|a, b| {
            a.timestamp
                .cmp(&b.timestamp)
                .then_with(|| {
                    a.instrument
                        .exchange
                        .as_str()
                        .cmp(b.instrument.exchange.as_str())
                })
                .then_with(|| a.instrument.symbol.cmp(&b.instrument.symbol))
        });
        Ok(all_bars.into_iter().map(MarketEvent::Bar).collect())
    }

    /// Inspect catalog partitions under `timeframe` (or all timeframes when `None`) and return
    /// per-instrument bar counts, date bounds, and boundary close prices.
    pub fn inspect_summary(
        &self,
        timeframe: Option<&str>,
    ) -> Result<Vec<CatalogInstrumentSummary>, CatalogError> {
        let timeframes = self.list_timeframes()?;
        let target_tfs: Vec<String> = match timeframe {
            Some(tf) => vec![tf.to_string()],
            None => timeframes,
        };
        let mut summaries = Vec::new();
        for tf in target_tfs {
            for inst in self.list_instruments(&tf)? {
                let bars = self.load_bars(&inst, &tf, None, None)?;
                summaries.push(CatalogInstrumentSummary {
                    timeframe: tf.clone(),
                    instrument: inst,
                    bar_count: bars.len(),
                    first_timestamp: bars.first().map(|b| b.timestamp),
                    last_timestamp: bars.last().map(|b| b.timestamp),
                    first_close: bars.first().map(|b| b.close.0),
                    last_close: bars.last().map(|b| b.close.0),
                });
            }
        }
        Ok(summaries)
    }

    /// Verify catalog integrity: non-empty partitions, strict timestamp monotonicity,
    /// OHLC bounds (`low <= open <= high`, `low <= close <= high`), and non-negative volume.
    pub fn verify_integrity(
        &self,
        timeframe: Option<&str>,
    ) -> Result<CatalogVerificationReport, CatalogError> {
        let commit = self.resolved_commit()?;
        let timeframes = self.list_timeframes()?;
        let target_tfs: Vec<String> = match timeframe {
            Some(tf) => vec![tf.to_string()],
            None => timeframes,
        };

        let mut total_bars = 0usize;
        let mut total_instruments = 0usize;
        let mut anomalies = Vec::new();

        for tf in &target_tfs {
            for inst in self.list_instruments(tf)? {
                total_instruments += 1;
                let bars = self.load_bars(&inst, tf, None, None)?;
                if bars.is_empty() {
                    anomalies.push(format!("{inst} ({tf}): empty partition"));
                    continue;
                }
                total_bars += bars.len();
                let mut prev_ts = None;
                for (idx, b) in bars.iter().enumerate() {
                    if let Some(prev) = prev_ts
                        && b.timestamp <= prev
                    {
                        anomalies.push(format!(
                            "{inst} ({tf}) row {idx}: retrograde timestamp {} <= prev {}",
                            b.timestamp, prev
                        ));
                    }
                    prev_ts = Some(b.timestamp);

                    if b.low.0 > b.high.0 {
                        anomalies.push(format!(
                            "{inst} ({tf}) row {idx}: low {} > high {}",
                            b.low.0, b.high.0
                        ));
                    }
                    if b.open.0 < b.low.0 || b.open.0 > b.high.0 {
                        anomalies.push(format!(
                            "{inst} ({tf}) row {idx}: open {} out of bounds [{}, {}]",
                            b.open.0, b.low.0, b.high.0
                        ));
                    }
                    if b.close.0 < b.low.0 || b.close.0 > b.high.0 {
                        anomalies.push(format!(
                            "{inst} ({tf}) row {idx}: close {} out of bounds [{}, {}]",
                            b.close.0, b.low.0, b.high.0
                        ));
                    }
                    if b.volume.0 < Decimal::ZERO {
                        anomalies.push(format!(
                            "{inst} ({tf}) row {idx}: negative volume {}",
                            b.volume.0
                        ));
                    }
                }
            }
        }

        Ok(CatalogVerificationReport {
            timeframes_checked: target_tfs.len(),
            total_instruments,
            total_bars,
            anomalies,
            data_commit_sha: commit.as_ref().map(|c| c.commit_sha.clone()),
            data_as_of_ist: commit.map(|c| c.as_of_ist),
        })
    }

    fn find_timeframe_dir(&self, timeframe: &str) -> Result<Option<PathBuf>, CatalogError> {
        let cat = self.catalog_dir();
        if !cat.is_dir() {
            return Ok(None);
        }
        for dir in sorted_subdirs(&cat)? {
            let name = file_name(&dir);
            if name.eq_ignore_ascii_case(timeframe) {
                return Ok(Some(dir));
            }
        }
        Ok(None)
    }

    fn locate_partition_files(
        &self,
        instrument: &InstrumentId,
        timeframe: &str,
    ) -> Result<Vec<PathBuf>, CatalogError> {
        // 1. Check standard tantra-data/data/catalog/{TF}/{EXCHANGE}/{SYMBOL}/*.parquet
        if let Some(tf_dir) = self.find_timeframe_dir(timeframe)? {
            for exch_dir in sorted_subdirs(&tf_dir)? {
                if exch_dir.file_name().is_some_and(|n| {
                    n.to_string_lossy()
                        .eq_ignore_ascii_case(instrument.exchange.as_str())
                }) {
                    for sym_dir in sorted_subdirs(&exch_dir)? {
                        if sym_dir.file_name().is_some_and(|n| {
                            n.to_string_lossy().eq_ignore_ascii_case(&instrument.symbol)
                        }) {
                            return sorted_parquet_files(&sym_dir);
                        }
                    }
                }
            }
        }

        // 2. Fallback: Check Lakehouse v2 layout:
        //    {root}/bars/timeframe={tf}/exchange={EXCHANGE}/year=*/month=*/{SYMBOL}*.parquet
        let lh_tf_dir = self
            .root
            .join("bars")
            .join(format!("timeframe={}", timeframe.to_ascii_lowercase()))
            .join(format!("exchange={}", instrument.exchange.as_str()));
        if lh_tf_dir.is_dir() {
            let mut files = Vec::new();
            for year_dir in sorted_subdirs(&lh_tf_dir)? {
                for month_dir in sorted_subdirs(&year_dir)? {
                    for file in sorted_parquet_files(&month_dir)? {
                        let fname = file_name(&file);
                        let base = format!("{}.parquet", instrument.symbol);
                        let prefix = format!("{}.part-", instrument.symbol);
                        if fname == base || fname.starts_with(&prefix) {
                            files.push(file);
                        }
                    }
                }
            }
            return Ok(files);
        }

        Ok(Vec::new())
    }
}

impl BarReader for CatalogLoader {
    fn read_bars(&self, req: &BarRequest) -> PortResult<Vec<Bar>> {
        let tf_str = timeframe_to_catalog_str(req.timeframe);
        let bars = self.load_bars(&req.instrument, tf_str, Some(req.from), Some(req.to))?;
        Ok(bars)
    }
}

fn timeframe_to_catalog_str(tf: Timeframe) -> &'static str {
    match tf {
        Timeframe::M1 => "1M",
        Timeframe::M5 => "5M",
        Timeframe::M15 => "15M",
        Timeframe::M30 => "30M",
        Timeframe::H1 => "1H",
        Timeframe::H2 => "2H",
        Timeframe::H4 => "4H",
        Timeframe::D1 => "1D",
        Timeframe::W1 => "1W",
        Timeframe::Month1 => "1MO",
    }
}

struct InstrumentMatcher {
    exchange: String,
    alias_uids: Vec<String>,
    target_uids: HashSet<String>,
    target_symbols: HashSet<String>,
}

impl InstrumentMatcher {
    fn build(instrument: &InstrumentId, aliases: &[InstrumentAlias]) -> Self {
        let exchange = instrument.exchange.as_str().to_string();
        let mut alias_uids = Vec::new();
        let mut target_uids = HashSet::new();
        let mut target_symbols = HashSet::new();

        for a in aliases {
            if (a.exchange.eq_ignore_ascii_case(&exchange)
                && a.symbol.eq_ignore_ascii_case(&instrument.symbol))
                || a.instrument_uid == instrument.symbol
            {
                if !alias_uids.contains(&a.instrument_uid) {
                    alias_uids.push(a.instrument_uid.clone());
                }
                target_uids.insert(a.instrument_uid.clone());
            }
        }

        for uid in &alias_uids {
            for a in ManifestResolver::aliases_for_uid(aliases, uid) {
                if a.exchange.eq_ignore_ascii_case(&exchange) {
                    target_symbols.insert(a.symbol.to_ascii_uppercase());
                }
            }
        }

        target_symbols.insert(instrument.symbol.to_ascii_uppercase());
        target_uids.insert(format!("{exchange}:{}", instrument.symbol));
        target_uids.insert(instrument.symbol.clone());

        Self {
            exchange,
            alias_uids,
            target_uids,
            target_symbols,
        }
    }
}

fn build_uid_to_canonical_map(aliases: &[InstrumentAlias]) -> HashMap<String, InstrumentId> {
    type AliasRank = (bool, bool, Option<NaiveDate>, Option<NaiveDate>);
    let mut best: HashMap<&str, (AliasRank, InstrumentId)> = HashMap::with_capacity(aliases.len());
    for a in aliases {
        let Ok(ex) = a.exchange.parse::<Exchange>() else {
            continue;
        };
        let is_eq = a
            .series
            .as_deref()
            .is_some_and(|s| s.eq_ignore_ascii_case("EQ"));
        let is_open = a.valid_to.is_none();
        let rank: AliasRank = (is_eq, is_open, a.valid_to, a.valid_from);
        match best.entry(a.instrument_uid.as_str()) {
            std::collections::hash_map::Entry::Vacant(slot) => {
                slot.insert((rank, InstrumentId::new(ex, &a.symbol)));
            }
            std::collections::hash_map::Entry::Occupied(mut slot) => {
                if rank > slot.get().0 {
                    *slot.get_mut() = (rank, InstrumentId::new(ex, &a.symbol));
                }
            }
        }
    }
    best.into_iter()
        .map(|(uid, (_, id))| (uid.to_string(), id))
        .collect()
}

fn collect_v3_file_instruments(
    path: &Path,
    entry: &ManifestEntry,
    uid_to_id: &HashMap<String, InstrumentId>,
    out: &mut BTreeSet<InstrumentId>,
) -> Result<(), CatalogError> {
    let file = File::open(path).map_err(|source| CatalogError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    let builder =
        ParquetRecordBatchReaderBuilder::try_new(file).map_err(|source| CatalogError::Parquet {
            path: path.to_path_buf(),
            source: Box::new(source),
        })?;
    let schema = builder.schema().clone();
    let uid_pos = schema.index_of("instrument_uid").ok();
    let sym_pos = schema.index_of("symbol").ok();
    let ex_pos = schema.index_of("exchange").ok();

    let mut indices = Vec::new();
    if let Some(i) = uid_pos {
        indices.push(i);
    }
    if let Some(i) = sym_pos {
        indices.push(i);
    }
    if let Some(i) = ex_pos {
        indices.push(i);
    }
    if indices.is_empty() {
        return Ok(());
    }

    let mask = ProjectionMask::roots(builder.parquet_schema(), indices);
    let reader = builder
        .with_projection(mask)
        .build()
        .map_err(|source| CatalogError::Parquet {
            path: path.to_path_buf(),
            source: Box::new(source),
        })?;

    let default_exchange = entry
        .exchange
        .as_deref()
        .and_then(|ex| ex.parse::<Exchange>().ok());
    let mut last_uid: Option<String> = None;

    for batch_res in reader {
        let batch = batch_res.map_err(|source| CatalogError::Arrow {
            path: path.to_path_buf(),
            source: Box::new(source),
        })?;
        let batch_schema = batch.schema();
        let uids = match batch_schema.index_of("instrument_uid") {
            Ok(i) => Some(
                arrow_utf8_array(batch.column(i).as_ref()).map_err(|source| {
                    CatalogError::Arrow {
                        path: path.to_path_buf(),
                        source: Box::new(source),
                    }
                })?,
            ),
            Err(_) => None,
        };
        let syms = match batch_schema.index_of("symbol") {
            Ok(i) => Some(
                arrow_utf8_array(batch.column(i).as_ref()).map_err(|source| {
                    CatalogError::Arrow {
                        path: path.to_path_buf(),
                        source: Box::new(source),
                    }
                })?,
            ),
            Err(_) => None,
        };
        let exs = match batch_schema.index_of("exchange") {
            Ok(i) => Some(
                arrow_utf8_array(batch.column(i).as_ref()).map_err(|source| {
                    CatalogError::Arrow {
                        path: path.to_path_buf(),
                        source: Box::new(source),
                    }
                })?,
            ),
            Err(_) => None,
        };

        for row in 0..batch.num_rows() {
            if let Some(uids) = &uids
                && !uids.is_null(row)
            {
                let uid = uids.value(row);
                if last_uid.as_deref() == Some(uid) {
                    continue;
                }
                if let Some(id) = uid_to_id.get(uid) {
                    out.insert(id.clone());
                    last_uid = Some(uid.to_owned());
                    continue;
                }
                if let Some((ex_str, sym)) = uid.split_once(':')
                    && let Ok(ex) = ex_str.parse::<Exchange>()
                    && !sym.is_empty()
                {
                    out.insert(InstrumentId::new(ex, sym));
                    last_uid = Some(uid.to_owned());
                    continue;
                }
            }

            let row_ex = exs
                .as_ref()
                .filter(|a| !a.is_null(row))
                .and_then(|a| a.value(row).parse::<Exchange>().ok())
                .or(default_exchange);

            if let Some(syms) = &syms
                && !syms.is_null(row)
                && let Some(ex) = row_ex
            {
                let sym = syms.value(row);
                if !sym.is_empty() {
                    out.insert(InstrumentId::new(ex, sym));
                }
            }
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn read_v3_cas_bars_into(
    path: &Path,
    instrument: &InstrumentId,
    matcher: &InstrumentMatcher,
    from: Option<DateTime<Utc>>,
    to: Option<DateTime<Utc>>,
    last_ts: &mut Option<DateTime<Utc>>,
    out: &mut Vec<Bar>,
    stats: &mut LoadStats,
) -> Result<(), CatalogError> {
    let file = File::open(path).map_err(|source| CatalogError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    let builder =
        ParquetRecordBatchReaderBuilder::try_new(file).map_err(|source| CatalogError::Parquet {
            path: path.to_path_buf(),
            source: Box::new(source),
        })?;

    let arrow_schema = builder.schema().clone();
    let metadata = builder.metadata().clone();
    let num_rgs = metadata.num_row_groups();
    stats.row_groups_total += num_rgs;

    let selected_rgs = select_matching_row_groups(&metadata, &arrow_schema, matcher, from, to);
    stats.row_groups_read += selected_rgs.len();
    if selected_rgs.is_empty() {
        return Ok(());
    }

    let reader = builder
        .with_row_groups(selected_rgs)
        .build()
        .map_err(|source| CatalogError::Parquet {
            path: path.to_path_buf(),
            source: Box::new(source),
        })?;

    for batch_res in reader {
        let batch = batch_res.map_err(|source| CatalogError::Arrow {
            path: path.to_path_buf(),
            source: Box::new(source),
        })?;
        append_batch_bars(
            path,
            &batch,
            instrument,
            Some(matcher),
            from,
            to,
            last_ts,
            out,
        )?;
    }

    Ok(())
}

fn read_v3_cas_all_bars_into(
    path: &Path,
    entry: &ManifestEntry,
    uid_to_id: &HashMap<String, InstrumentId>,
    out_map: &mut BTreeMap<InstrumentId, Vec<Bar>>,
) -> Result<(), CatalogError> {
    let file = File::open(path).map_err(|source| CatalogError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|source| CatalogError::Parquet {
            path: path.to_path_buf(),
            source: Box::new(source),
        })?
        .build()
        .map_err(|source| CatalogError::Parquet {
            path: path.to_path_buf(),
            source: Box::new(source),
        })?;

    let default_exchange = entry
        .exchange
        .as_deref()
        .and_then(|ex| ex.parse::<Exchange>().ok());

    let mut cached_key: Option<(String, Option<String>)> = None;
    let mut cached_id: Option<InstrumentId> = None;

    for batch_res in reader {
        let batch = batch_res.map_err(|source| CatalogError::Arrow {
            path: path.to_path_buf(),
            source: Box::new(source),
        })?;
        let num_rows = batch.num_rows();
        if num_rows == 0 {
            continue;
        }
        let schema = batch.schema();
        let ts_idx = schema
            .index_of("ts")
            .or_else(|_| schema.index_of("timestamp"))
            .map_err(|_| CatalogError::MissingColumn {
                path: path.to_path_buf(),
                column: "ts",
            })?;
        let col_idx = |name: &'static str| -> Result<usize, CatalogError> {
            schema
                .index_of(name)
                .map_err(|_| CatalogError::MissingColumn {
                    path: path.to_path_buf(),
                    column: name,
                })
        };
        let opt_str_col = |name: &str| -> Result<Option<StringArray>, CatalogError> {
            match schema.index_of(name) {
                Ok(i) => arrow_utf8_array(batch.column(i).as_ref())
                    .map(Some)
                    .map_err(|source| CatalogError::Arrow {
                        path: path.to_path_buf(),
                        source: Box::new(source),
                    }),
                Err(_) => Ok(None),
            }
        };

        let uid_col = opt_str_col("instrument_uid")?;
        let sym_col = opt_str_col("symbol")?;
        let ex_col = opt_str_col("exchange")?;
        let series_col = opt_str_col("series")?;

        let open_col = batch.column(col_idx("open")?).as_ref();
        let high_col = batch.column(col_idx("high")?).as_ref();
        let low_col = batch.column(col_idx("low")?).as_ref();
        let close_col = batch.column(col_idx("close")?).as_ref();
        let vol_col = batch.column(col_idx("volume")?).as_ref();
        let ts_col = batch.column(ts_idx).as_ref();

        for row in 0..num_rows {
            let uid_val = uid_col
                .as_ref()
                .filter(|c| !c.is_null(row))
                .map(|c| c.value(row));
            let sym_val = sym_col
                .as_ref()
                .filter(|c| !c.is_null(row))
                .map(|c| c.value(row));

            let id = if let (Some((c_uid, c_sym)), Some(c_id)) = (&cached_key, &cached_id)
                && uid_val == Some(c_uid.as_str())
                && sym_val == c_sym.as_deref()
            {
                c_id.clone()
            } else {
                let row_ex = ex_col
                    .as_ref()
                    .filter(|c| !c.is_null(row))
                    .and_then(|c| c.value(row).parse::<Exchange>().ok())
                    .or(default_exchange);
                let resolved = if let Some(uid) = uid_val {
                    if let Some(canon) = uid_to_id.get(uid) {
                        Some(canon.clone())
                    } else if let Some((ex_str, sym)) = uid.split_once(':')
                        && let Ok(ex) = ex_str.parse::<Exchange>()
                        && !sym.is_empty()
                    {
                        Some(InstrumentId::new(ex, sym))
                    } else if let (Some(sym), Some(ex)) =
                        (sym_val.filter(|s| !s.is_empty()), row_ex)
                    {
                        Some(InstrumentId::new(ex, sym))
                    } else {
                        None
                    }
                } else if let (Some(sym), Some(ex)) = (sym_val.filter(|s| !s.is_empty()), row_ex) {
                    Some(InstrumentId::new(ex, sym))
                } else {
                    None
                };

                let Some(resolved_id) = resolved else {
                    continue;
                };
                if let Some(uid) = uid_val {
                    cached_key = Some((uid.to_string(), sym_val.map(ToOwned::to_owned)));
                    cached_id = Some(resolved_id.clone());
                } else {
                    cached_key = None;
                    cached_id = None;
                }
                resolved_id
            };

            let Some(timestamp) = extract_timestamp(path, ts_col, row)? else {
                continue;
            };
            let open = Price(extract_decimal(path, open_col, "open", row)?);
            let high = Price(extract_decimal(path, high_col, "high", row)?);
            let low = Price(extract_decimal(path, low_col, "low", row)?);
            let close = Price(extract_decimal(path, close_col, "close", row)?);
            let volume = if vol_col.is_null(row) {
                Quantity(Decimal::ZERO)
            } else {
                Quantity(extract_decimal(path, vol_col, "volume", row)?.max(Decimal::ZERO))
            };
            let is_eq = series_col
                .as_ref()
                .filter(|c| !c.is_null(row))
                .is_some_and(|c| c.value(row).eq_ignore_ascii_case("EQ"));

            let bar = Bar {
                instrument: id.clone(),
                open,
                high,
                low,
                close,
                volume,
                timestamp,
            };
            if let Some(vec) = out_map.get_mut(&id) {
                if let Some(last) = vec.last_mut()
                    && last.timestamp == timestamp
                {
                    if is_eq {
                        *last = bar;
                    }
                } else {
                    vec.push(bar);
                }
            } else {
                out_map.insert(id, vec![bar]);
            }
        }
    }

    Ok(())
}

fn select_matching_row_groups(
    metadata: &parquet::file::metadata::ParquetMetaData,
    arrow_schema: &Schema,
    matcher: &InstrumentMatcher,
    from: Option<DateTime<Utc>>,
    to: Option<DateTime<Utc>>,
) -> Vec<usize> {
    let cols = metadata.file_metadata().schema_descr().columns();
    let find_rg_col = |name: &str| cols.iter().position(|c| c.name() == name);
    let uid_col = find_rg_col("instrument_uid");
    let sym_col = find_rg_col("symbol");
    let ts_col = find_rg_col("ts").or_else(|| find_rg_col("timestamp"));
    let ts_type = ts_col
        .and_then(|_| {
            arrow_schema
                .field_with_name("ts")
                .or_else(|_| arrow_schema.field_with_name("timestamp"))
                .ok()
        })
        .map(|f| f.data_type().clone());

    let mut selected = Vec::with_capacity(metadata.num_row_groups());
    for (rg_idx, rg) in metadata.row_groups().iter().enumerate() {
        // 1. Instrument UID / symbol min/max statistics pruning
        if !matcher.alias_uids.is_empty() {
            if let Some(col_idx) = uid_col
                && let Some(Statistics::ByteArray(stat)) = rg.column(col_idx).statistics()
                && !byte_array_stats_may_contain(
                    stat,
                    matcher.alias_uids.iter().map(String::as_str),
                )
            {
                continue;
            }
        } else if let Some(col_idx) = sym_col {
            if let Some(Statistics::ByteArray(stat)) = rg.column(col_idx).statistics()
                && !byte_array_stats_may_contain(
                    stat,
                    matcher.target_symbols.iter().map(String::as_str),
                )
            {
                continue;
            }
        } else if let Some(col_idx) = uid_col
            && let Some(Statistics::ByteArray(stat)) = rg.column(col_idx).statistics()
            && !byte_array_stats_may_contain(stat, matcher.target_uids.iter().map(String::as_str))
        {
            continue;
        }

        // 2. Timestamp min/max statistics pruning
        if (from.is_some() || to.is_some())
            && let (Some(col_idx), Some(dt_type)) = (ts_col, ts_type.as_ref())
            && let Some(Statistics::Int64(stat)) = rg.column(col_idx).statistics()
            && let (Some(&min_raw), Some(&max_raw)) = (stat.min_opt(), stat.max_opt())
            && let (Some(rg_min), Some(rg_max)) = (
                raw_ts_to_utc(min_raw, dt_type),
                raw_ts_to_utc(max_raw, dt_type),
            )
        {
            if let Some(f) = from
                && rg_max < f
            {
                continue;
            }
            if let Some(t) = to
                && rg_min > t
            {
                continue;
            }
        }

        selected.push(rg_idx);
    }
    selected
}

fn byte_array_stats_may_contain<'a>(
    stat: &parquet::file::statistics::ValueStatistics<parquet::data_type::ByteArray>,
    mut candidates: impl Iterator<Item = &'a str>,
) -> bool {
    let (Some(min_ba), Some(max_ba)) = (stat.min_opt(), stat.max_opt()) else {
        return true;
    };
    let (Ok(min_s), Ok(max_s)) = (
        std::str::from_utf8(min_ba.data()),
        std::str::from_utf8(max_ba.data()),
    ) else {
        return true;
    };
    // Guard against Parquet 64-byte truncated statistics.
    if min_s.len() >= 64 || max_s.len() >= 64 {
        return true;
    }
    candidates.any(|c| c >= min_s && c <= max_s)
}

fn raw_ts_to_utc(raw: i64, dt_type: &DataType) -> Option<DateTime<Utc>> {
    match dt_type {
        DataType::Int64 | DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            Some(DateTime::<Utc>::from_timestamp_nanos(raw))
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            DateTime::<Utc>::from_timestamp_micros(raw)
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            DateTime::<Utc>::from_timestamp_millis(raw)
        }
        DataType::Timestamp(TimeUnit::Second, _) => DateTime::<Utc>::from_timestamp(raw, 0),
        _ => None,
    }
}

fn read_parquet_bars_into(
    path: &Path,
    instrument: &InstrumentId,
    from: Option<DateTime<Utc>>,
    to: Option<DateTime<Utc>>,
    last_ts: &mut Option<DateTime<Utc>>,
    out: &mut Vec<Bar>,
    stats: &mut LoadStats,
) -> Result<(), CatalogError> {
    let file = File::open(path).map_err(|source| CatalogError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    let builder =
        ParquetRecordBatchReaderBuilder::try_new(file).map_err(|source| CatalogError::Parquet {
            path: path.to_path_buf(),
            source: Box::new(source),
        })?;
    let num_rgs = builder.metadata().num_row_groups();
    stats.row_groups_total += num_rgs;
    stats.row_groups_read += num_rgs;

    let reader = builder.build().map_err(|source| CatalogError::Parquet {
        path: path.to_path_buf(),
        source: Box::new(source),
    })?;

    for batch_res in reader {
        let batch = batch_res.map_err(|source| CatalogError::Arrow {
            path: path.to_path_buf(),
            source: Box::new(source),
        })?;
        append_batch_bars(path, &batch, instrument, None, from, to, last_ts, out)?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn append_batch_bars(
    path: &Path,
    batch: &RecordBatch,
    instrument: &InstrumentId,
    matcher: Option<&InstrumentMatcher>,
    from: Option<DateTime<Utc>>,
    to: Option<DateTime<Utc>>,
    last_ts: &mut Option<DateTime<Utc>>,
    out: &mut Vec<Bar>,
) -> Result<(), CatalogError> {
    let num_rows = batch.num_rows();
    if num_rows == 0 {
        return Ok(());
    }

    let schema = batch.schema();
    let ts_idx = schema
        .index_of("ts")
        .or_else(|_| schema.index_of("timestamp"))
        .map_err(|_| CatalogError::MissingColumn {
            path: path.to_path_buf(),
            column: "ts",
        })?;

    let col_idx = |name: &'static str| -> Result<usize, CatalogError> {
        schema
            .index_of(name)
            .map_err(|_| CatalogError::MissingColumn {
                path: path.to_path_buf(),
                column: name,
            })
    };

    let opt_str_col = |name: &str| -> Result<Option<StringArray>, CatalogError> {
        match schema.index_of(name) {
            Ok(i) => arrow_utf8_array(batch.column(i).as_ref())
                .map(Some)
                .map_err(|source| CatalogError::Arrow {
                    path: path.to_path_buf(),
                    source: Box::new(source),
                }),
            Err(_) => Ok(None),
        }
    };

    let uid_col = if matcher.is_some() {
        opt_str_col("instrument_uid")?
    } else {
        None
    };
    let sym_col = if matcher.is_some() {
        opt_str_col("symbol")?
    } else {
        None
    };
    let ex_col = if matcher.is_some() {
        opt_str_col("exchange")?
    } else {
        None
    };

    let open_col = batch.column(col_idx("open")?).as_ref();
    let high_col = batch.column(col_idx("high")?).as_ref();
    let low_col = batch.column(col_idx("low")?).as_ref();
    let close_col = batch.column(col_idx("close")?).as_ref();
    let vol_col = batch.column(col_idx("volume")?).as_ref();
    let ts_col = batch.column(ts_idx).as_ref();

    for row in 0..num_rows {
        if let Some(m) = matcher {
            let ex_matches = ex_col
                .as_ref()
                .is_none_or(|c| c.is_null(row) || c.value(row).eq_ignore_ascii_case(&m.exchange));
            if !ex_matches {
                continue;
            }
            let uid_match = uid_col
                .as_ref()
                .is_some_and(|c| !c.is_null(row) && m.target_uids.contains(c.value(row)));
            let sym_match = m.alias_uids.is_empty()
                && sym_col.as_ref().is_some_and(|c| {
                    !c.is_null(row)
                        && m.target_symbols
                            .contains(&c.value(row).to_ascii_uppercase())
                });
            if (uid_col.is_some() || sym_col.is_some()) && !uid_match && !sym_match {
                continue;
            }
        }

        let Some(timestamp) = extract_timestamp(path, ts_col, row)? else {
            continue;
        };

        // Deduplicate boundary rows or non-increasing timestamps across partitions
        if let Some(prev) = *last_ts
            && timestamp <= prev
        {
            continue;
        }
        *last_ts = Some(timestamp);

        if let Some(f) = from
            && timestamp < f
        {
            continue;
        }
        if let Some(t) = to
            && timestamp > t
        {
            continue;
        }

        let open = Price(extract_decimal(path, open_col, "open", row)?);
        let high = Price(extract_decimal(path, high_col, "high", row)?);
        let low = Price(extract_decimal(path, low_col, "low", row)?);
        let close = Price(extract_decimal(path, close_col, "close", row)?);
        let volume = if vol_col.is_null(row) {
            Quantity(Decimal::ZERO)
        } else {
            Quantity(extract_decimal(path, vol_col, "volume", row)?.max(Decimal::ZERO))
        };

        out.push(Bar {
            instrument: instrument.clone(),
            open,
            high,
            low,
            close,
            volume,
            timestamp,
        });
    }

    Ok(())
}

fn extract_timestamp(
    path: &Path,
    col: &dyn Array,
    row: usize,
) -> Result<Option<DateTime<Utc>>, CatalogError> {
    if col.is_null(row) {
        return Ok(None);
    }
    match col.data_type() {
        DataType::Int64 => {
            let arr = col.as_any().downcast_ref::<Int64Array>().unwrap();
            Ok(Some(DateTime::<Utc>::from_timestamp_nanos(arr.value(row))))
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let arr = col
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap();
            Ok(Some(DateTime::<Utc>::from_timestamp_nanos(arr.value(row))))
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let arr = col
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            Ok(DateTime::<Utc>::from_timestamp_micros(arr.value(row)))
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let arr = col
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap();
            Ok(DateTime::<Utc>::from_timestamp_millis(arr.value(row)))
        }
        DataType::Timestamp(TimeUnit::Second, _) => {
            let arr = col.as_any().downcast_ref::<TimestampSecondArray>().unwrap();
            Ok(DateTime::<Utc>::from_timestamp(arr.value(row), 0))
        }
        other => Err(CatalogError::UnsupportedColumnType {
            path: path.to_path_buf(),
            column: "ts",
            data_type: Box::new(other.clone()),
        }),
    }
}

fn extract_decimal(
    path: &Path,
    col: &dyn Array,
    column: &'static str,
    row: usize,
) -> Result<Decimal, CatalogError> {
    if col.is_null(row) {
        return Err(CatalogError::InvalidRow {
            path: path.into(),
            row,
            column,
            reason: "null numeric value".into(),
        });
    }
    match col.data_type() {
        DataType::Decimal128(_, scale) => {
            let arr = col.as_any().downcast_ref::<Decimal128Array>().unwrap();
            Ok(Decimal::from_i128_with_scale(arr.value(row), *scale as u32))
        }
        DataType::Float64 => {
            let arr = col.as_any().downcast_ref::<Float64Array>().unwrap();
            let val = arr.value(row);
            decimal_from_f64_lossless(val).ok_or_else(|| CatalogError::InvalidRow {
                path: path.into(),
                row,
                column,
                reason: format!("non-finite float value {val}").into(),
            })
        }
        DataType::Int64 => {
            let arr = col.as_any().downcast_ref::<Int64Array>().unwrap();
            Ok(Decimal::from(arr.value(row)))
        }
        other => Err(CatalogError::UnsupportedColumnType {
            path: path.to_path_buf(),
            column,
            data_type: Box::new(other.clone()),
        }),
    }
}

/// Convert a finite `f64` from legacy Parquet columns into an exact `Decimal`
/// rounded to 4 decimal places (sub-paise tick precision), avoiding binary
/// float artifacts such as `1001.7000122070312` -> `1001.7000`.
pub fn decimal_from_f64_lossless(v: f64) -> Option<Decimal> {
    if !v.is_finite() {
        return None;
    }
    let s = format!("{v:.4}");
    Decimal::from_str(&s)
        .or_else(|_| Decimal::from_f64_retain(v).ok_or(()))
        .ok()
        .map(|d| d.round_dp(4).normalize())
}

fn sorted_subdirs(dir: &Path) -> Result<Vec<PathBuf>, CatalogError> {
    if !dir.is_dir() {
        return Ok(Vec::new());
    }
    let mut dirs = Vec::new();
    for entry in fs::read_dir(dir).map_err(|source| CatalogError::Io {
        path: dir.to_path_buf(),
        source,
    })? {
        let path = entry
            .map_err(|source| CatalogError::Io {
                path: dir.to_path_buf(),
                source,
            })?
            .path();
        if path.is_dir() && !file_name(&path).starts_with('.') {
            dirs.push(path);
        }
    }
    dirs.sort();
    Ok(dirs)
}

fn sorted_parquet_files(dir: &Path) -> Result<Vec<PathBuf>, CatalogError> {
    if !dir.is_dir() {
        return Ok(Vec::new());
    }
    let mut files = Vec::new();
    for entry in fs::read_dir(dir).map_err(|source| CatalogError::Io {
        path: dir.to_path_buf(),
        source,
    })? {
        let path = entry
            .map_err(|source| CatalogError::Io {
                path: dir.to_path_buf(),
                source,
            })?
            .path();
        if path.is_file() && path.extension().is_some_and(|ext| ext == "parquet") {
            files.push(path);
        }
    }
    files.sort();
    Ok(files)
}

fn file_name(path: &Path) -> String {
    path.file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default()
}
