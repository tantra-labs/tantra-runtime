//! National Stock Exchange of India (NSE) trading calendar and session rules.
//!
//! Models:
//! - Regular market hours (09:15 to 15:30 IST)
//! - Weekend exclusion (Saturdays & Sundays)
//! - Declared exchange holidays (Republic Day, Diwali, Independence Day, etc.)
//! - Special trading sessions such as annual Diwali **Muhurat trading** (18:15 to 19:15 IST)

use chrono::{DateTime, Datelike, Duration, FixedOffset, NaiveDate, NaiveTime, Utc, Weekday};
use std::collections::HashSet;
use std::fs::File;
use std::path::{Path, PathBuf};

use arrow::array::{Array, Date32Array, StringArray};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use tantra_core::TradingCalendar;
use thiserror::Error;

/// Column schema of the `catalog/trading_holidays/<year>.parquet` bridge
/// projection that `tantra-data` exports (gap 0015). The projection also
/// carries a `description` column that this consumer does not need.
const HOLIDAY_DATE_COLUMN: &str = "holiday_date";
const HOLIDAY_KIND_COLUMN: &str = "kind";

/// Session window on a trading date.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionWindow {
    /// Session opening time (IST)
    pub open: NaiveTime,
    /// Session closing time (IST)
    pub close: NaiveTime,
}

impl SessionWindow {
    /// Standard NSE cash equity continuous trading session: 09:15 to 15:30 IST.
    pub fn regular() -> Self {
        Self {
            open: NaiveTime::from_hms_opt(9, 15, 0).unwrap(),
            close: NaiveTime::from_hms_opt(15, 30, 0).unwrap(),
        }
    }

    /// Special Diwali Muhurat trading session (1 hour in the evening): 18:15 to 19:15 IST.
    pub fn muhurat() -> Self {
        Self {
            open: NaiveTime::from_hms_opt(18, 15, 0).unwrap(),
            close: NaiveTime::from_hms_opt(19, 15, 0).unwrap(),
        }
    }

    /// Check if an IST time falls within this session window.
    pub fn contains(&self, time: NaiveTime) -> bool {
        time >= self.open && time <= self.close
    }
}

/// Errors raised while loading the NSE trading-holiday projection from the
/// `tantra-data` Parquet bridge (gap 0015).
#[derive(Debug, Error)]
pub enum TradingHolidayError {
    /// No `*.parquet` holiday files were found in the searched directory.
    #[error("trading-holiday data not found under {dir}")]
    NotFound { dir: PathBuf },
    /// Filesystem I/O failure.
    #[error("trading-holiday io error at {path}: {source}")]
    Io {
        /// Path being accessed.
        path: PathBuf,
        /// Underlying OS error.
        source: std::io::Error,
    },
    /// Parquet decoding failure.
    #[error("trading-holiday parquet error in {path}: {source}")]
    Parquet {
        /// Parquet file path.
        path: PathBuf,
        /// Underlying Parquet error.
        source: Box<parquet::errors::ParquetError>,
    },
    /// Arrow record-batch decoding failure.
    #[error("trading-holiday arrow error in {path}: {source}")]
    Arrow {
        /// Parquet file path.
        path: PathBuf,
        /// Underlying Arrow error.
        source: Box<arrow::error::ArrowError>,
    },
    /// Missing required column in a holiday Parquet file.
    #[error("missing required column `{column}` in {path}")]
    MissingColumn {
        /// Parquet file path.
        path: PathBuf,
        /// Column name.
        column: &'static str,
    },
    /// Unsupported column data type in a holiday Parquet file.
    #[error("unsupported data type {data_type:?} for column `{column}` in {path}")]
    UnsupportedColumnType {
        /// Parquet file path.
        path: PathBuf,
        /// Column name.
        column: &'static str,
        /// Actual Arrow data type.
        data_type: Box<DataType>,
    },
    /// Invalid or unrepresentable value in a holiday row.
    #[error("invalid value in {path} row {row} column `{column}`: {reason}")]
    InvalidRow {
        /// Parquet file path.
        path: Box<Path>,
        /// Row index within the batch.
        row: usize,
        /// Column name.
        column: &'static str,
        /// Explanation.
        reason: Box<str>,
    },
}

/// Load the trading-holiday dates from every `*.parquet` file under `dir`.
///
/// Returns `(holidays, muhurat_dates)`. Every row of the bridge projection
/// (columns `holiday_date`, `description`, `kind`) marks a declared closure;
/// rows whose `kind` is `"muhurat"` (Diwali Laxmi Pujan) additionally mark a
/// Muhurat trading session. This mirrors the historic seed semantics: a
/// Muhurat date is both a clearing holiday (the regular session is closed) and
/// a trading day (`is_trading_day` checks the Muhurat session first).
fn load_trading_holidays_from_dir(
    dir: &Path,
) -> Result<(Vec<NaiveDate>, Vec<NaiveDate>), TradingHolidayError> {
    let files = sorted_parquet_files(dir)?;
    if files.is_empty() {
        return Err(TradingHolidayError::NotFound {
            dir: dir.to_path_buf(),
        });
    }
    let mut holidays: Vec<NaiveDate> = Vec::new();
    let mut muhurat: Vec<NaiveDate> = Vec::new();
    for path in files {
        let file = File::open(&path).map_err(|source| TradingHolidayError::Io {
            path: path.clone(),
            source,
        })?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|source| TradingHolidayError::Parquet {
                path: path.clone(),
                source: Box::new(source),
            })?
            .build()
            .map_err(|source| TradingHolidayError::Parquet {
                path: path.clone(),
                source: Box::new(source),
            })?;
        for batch_result in reader {
            let batch = batch_result.map_err(|source| TradingHolidayError::Arrow {
                path: path.clone(),
                source: Box::new(source),
            })?;
            append_holiday_batch(&path, &batch, &mut holidays, &mut muhurat)?;
        }
    }
    Ok((holidays, muhurat))
}

fn append_holiday_batch(
    path: &Path,
    batch: &RecordBatch,
    holidays: &mut Vec<NaiveDate>,
    muhurat: &mut Vec<NaiveDate>,
) -> Result<(), TradingHolidayError> {
    let num_rows = batch.num_rows();
    if num_rows == 0 {
        return Ok(());
    }
    let schema = batch.schema();
    let date_idx =
        schema
            .index_of(HOLIDAY_DATE_COLUMN)
            .map_err(|_| TradingHolidayError::MissingColumn {
                path: path.to_path_buf(),
                column: HOLIDAY_DATE_COLUMN,
            })?;
    let kind_idx =
        schema
            .index_of(HOLIDAY_KIND_COLUMN)
            .map_err(|_| TradingHolidayError::MissingColumn {
                path: path.to_path_buf(),
                column: HOLIDAY_KIND_COLUMN,
            })?;
    let date_col = batch.column(date_idx);
    let date_arr = date_col
        .as_any()
        .downcast_ref::<Date32Array>()
        .ok_or_else(|| TradingHolidayError::UnsupportedColumnType {
            path: path.to_path_buf(),
            column: HOLIDAY_DATE_COLUMN,
            data_type: Box::new(date_col.data_type().clone()),
        })?;
    let kind_col = batch.column(kind_idx);
    let kind_arr = kind_col
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| TradingHolidayError::UnsupportedColumnType {
            path: path.to_path_buf(),
            column: HOLIDAY_KIND_COLUMN,
            data_type: Box::new(kind_col.data_type().clone()),
        })?;

    for row in 0..num_rows {
        if date_arr.is_null(row) {
            return Err(TradingHolidayError::InvalidRow {
                path: path.into(),
                row,
                column: HOLIDAY_DATE_COLUMN,
                reason: "null holiday date".into(),
            });
        }
        let Some(holiday_date) = date32_to_naive(date_arr.value(row)) else {
            return Err(TradingHolidayError::InvalidRow {
                path: path.into(),
                row,
                column: HOLIDAY_DATE_COLUMN,
                reason: format!("date out of range: {}", date_arr.value(row)).into(),
            });
        };
        if kind_arr.is_null(row) {
            return Err(TradingHolidayError::InvalidRow {
                path: path.into(),
                row,
                column: HOLIDAY_KIND_COLUMN,
                reason: "null holiday kind".into(),
            });
        }
        if kind_arr.value(row) == "muhurat" {
            muhurat.push(holiday_date);
        }
        holidays.push(holiday_date);
    }
    Ok(())
}

/// Convert an Arrow `date32` days-since-epoch value into a `NaiveDate`.
fn date32_to_naive(days: i32) -> Option<NaiveDate> {
    NaiveDate::from_ymd_opt(1970, 1, 1)?.checked_add_signed(Duration::days(i64::from(days)))
}

fn sorted_parquet_files(dir: &Path) -> Result<Vec<PathBuf>, TradingHolidayError> {
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
            return Err(TradingHolidayError::NotFound {
                dir: dir.to_path_buf(),
            });
        }
        Err(source) => {
            return Err(TradingHolidayError::Io {
                path: dir.to_path_buf(),
                source,
            });
        }
    };
    let mut files = Vec::new();
    for entry in entries {
        let path = entry
            .map_err(|source| TradingHolidayError::Io {
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

/// NSE trading calendar managing holidays, trading days, and special sessions.
#[derive(Debug, Clone)]
pub struct NseTradingCalendar {
    holidays: HashSet<NaiveDate>,
    muhurat_dates: HashSet<NaiveDate>,
    ist_offset: FixedOffset,
}

impl Default for NseTradingCalendar {
    fn default() -> Self {
        Self::new()
    }
}

impl NseTradingCalendar {
    /// Create the deterministic offline NSE calendar seeded with the bundled
    /// standard national holidays and Muhurat dates.
    ///
    /// This is the **offline fallback** (gap 0015, `tantra-ledger` plan 0005):
    /// `tantra-data` owns the trading-holiday reference data, so callers that
    /// have the data deployed should use [`Self::from_data_root`] /
    /// [`Self::from_data_root_env`] instead. `new()` is kept as the seed-table
    /// default purely so backtests stay byte-for-byte deterministic when no
    /// holiday data is available; the hardcoded table can silently go stale,
    /// which is exactly the staleness gap 0015 closes.
    pub fn new() -> Self {
        let mut cal = Self {
            holidays: HashSet::new(),
            muhurat_dates: HashSet::new(),
            ist_offset: FixedOffset::east_opt(5 * 3600 + 30 * 60).unwrap(), // UTC+05:30
        };
        cal.seed_standard_holidays();
        cal
    }

    /// Load the calendar strictly from the `tantra-data` trading-holiday
    /// Parquet bridge at `dir` (gap 0015).
    ///
    /// Every `*.parquet` file under `dir` is read; each row needs the
    /// `holiday_date` (date32), `kind` and `description` columns of the
    /// `catalog/trading_holidays/<year>.parquet` projection. Rows with
    /// `kind == "muhurat"` register a Muhurat session date (which is also a
    /// clearing holiday, matching the historic seed semantics). Returns
    /// [`TradingHolidayError::NotFound`] when `dir` holds no holiday files.
    pub fn from_parquet_dir(dir: impl AsRef<Path>) -> Result<Self, TradingHolidayError> {
        let (holidays, muhurat) = load_trading_holidays_from_dir(dir.as_ref())?;
        let mut cal = Self {
            holidays: HashSet::new(),
            muhurat_dates: HashSet::new(),
            ist_offset: FixedOffset::east_opt(5 * 3600 + 30 * 60).unwrap(), // UTC+05:30
        };
        for holiday in holidays {
            cal.holidays.insert(holiday);
        }
        for session in muhurat {
            cal.muhurat_dates.insert(session);
        }
        Ok(cal)
    }

    /// Build the calendar from the `tantra-data` data root, preferring the
    /// trading-holiday bridge projection and falling back to the offline seed
    /// table only when no holiday data is available (gap 0015).
    ///
    /// `root` may point to the data directory (whose `catalog/` child holds
    /// the `trading_holidays/` projection) or directly to the directory that
    /// contains `trading_holidays/`. When neither location has holiday data,
    /// or the projection is unreadable, this falls back to [`Self::new`]
    /// (logging a warning) so callers always get a usable, deterministic
    /// calendar.
    pub fn from_data_root(root: impl AsRef<Path>) -> Self {
        let root = root.as_ref();
        for candidate in [
            root.join("catalog").join("trading_holidays"),
            root.join("trading_holidays"),
        ] {
            if !candidate.is_dir() {
                continue;
            }
            match Self::from_parquet_dir(&candidate) {
                Ok(calendar) => return calendar,
                Err(err) => {
                    tracing::warn!(
                        "ignoring unreadable trading-holiday data at {}: {err}",
                        candidate.display()
                    );
                }
            }
        }
        Self::new()
    }

    /// Build the calendar from the `TANTRA_DATA_DIR` environment variable.
    ///
    /// When `TANTRA_DATA_DIR` is set to a data root that carries the
    /// `catalog/trading_holidays/` projection, the calendar is data-driven;
    /// otherwise (unset, empty, or no holiday data) it returns the
    /// deterministic offline [`Self::new`] fallback. This is the runtime's
    /// opt-in seam for gap 0015.
    pub fn from_data_root_env() -> Self {
        match std::env::var("TANTRA_DATA_DIR") {
            Ok(dir) if !dir.trim().is_empty() => Self::from_data_root(PathBuf::from(dir)),
            _ => Self::new(),
        }
    }

    /// Add a market holiday.
    pub fn add_holiday(&mut self, date: NaiveDate) {
        self.holidays.insert(date);
    }

    /// Add a special Muhurat trading session date.
    pub fn add_muhurat_session(&mut self, date: NaiveDate) {
        self.muhurat_dates.insert(date);
    }

    /// Check if a date is a declared market holiday.
    pub fn is_holiday(&self, date: NaiveDate) -> bool {
        self.holidays.contains(&date)
    }

    /// Check if a date hosts an annual Diwali Muhurat trading session.
    pub fn is_muhurat_session(&self, date: NaiveDate) -> bool {
        self.muhurat_dates.contains(&date)
    }

    /// Check if a date is a recognized NSE trading day.
    ///
    /// Regular weekdays that are not declared holidays, or special Muhurat session dates.
    pub fn is_trading_day(&self, date: NaiveDate) -> bool {
        if self.is_muhurat_session(date) {
            return true;
        }
        let weekday = date.weekday();
        if weekday == Weekday::Sat || weekday == Weekday::Sun {
            return false;
        }
        !self.is_holiday(date)
    }

    /// Get the applicable session window for a date.
    pub fn session_for_date(&self, date: NaiveDate) -> Option<SessionWindow> {
        if self.is_muhurat_session(date) {
            Some(SessionWindow::muhurat())
        } else if self.is_trading_day(date) {
            Some(SessionWindow::regular())
        } else {
            None
        }
    }

    /// Find the next trading day strictly after `date`.
    pub fn next_trading_day(&self, mut date: NaiveDate) -> NaiveDate {
        loop {
            date += Duration::days(1);
            if self.is_trading_day(date) {
                return date;
            }
        }
    }

    /// IST offset (UTC+05:30) used to convert UTC timestamps for session checks.
    pub fn ist_offset(&self) -> FixedOffset {
        self.ist_offset
    }

    /// Every declared holiday date, as a flat list. This is the real NSE
    /// holiday *data* this calendar owns; it gets injected into
    /// `tantra_ledger_portfolio::ClearingCalendar::with_holidays` so that crate
    /// never duplicates or hardcodes its own (potentially diverging) copy
    /// of the same table.
    ///
    /// Muhurat session dates (Diwali Laxmi Pujan) are included: the regular
    /// session is closed on those days, so they are clearing holidays too —
    /// even though `is_trading_day` returns `true` for them because of the
    /// special Muhurat session.
    pub fn holidays(&self) -> Vec<NaiveDate> {
        self.holidays.iter().copied().collect()
    }

    /// Check if a UTC timestamp falls within open NSE market hours.
    pub fn is_market_open(&self, utc_time: DateTime<Utc>) -> bool {
        let ist_time = utc_time.with_timezone(&self.ist_offset);
        let date = ist_time.date_naive();
        if let Some(session) = self.session_for_date(date) {
            session.contains(ist_time.time())
        } else {
            false
        }
    }

    fn seed_standard_holidays(&mut self) {
        // Offline fallback (gap 0015 / tantra-ledger plan 0005): the canonical
        // source of this table is now `tantra-data`'s trading-holiday provider;
        // this seed exists only so `new()` stays deterministic when no bridge
        // data is deployed. Prefer `from_data_root` / `from_data_root_env`.
        // Standard declared NSE holidays across recent years
        let holidays = [
            // 2025
            NaiveDate::from_ymd_opt(2025, 1, 26).unwrap(), // Republic Day
            NaiveDate::from_ymd_opt(2025, 2, 26).unwrap(), // Mahashivratri
            NaiveDate::from_ymd_opt(2025, 3, 14).unwrap(), // Holi
            NaiveDate::from_ymd_opt(2025, 3, 31).unwrap(), // Id-Ul-Fitr
            NaiveDate::from_ymd_opt(2025, 4, 10).unwrap(), // Mahavir Jayanti
            NaiveDate::from_ymd_opt(2025, 4, 14).unwrap(), // Dr. Ambedkar Jayanti
            NaiveDate::from_ymd_opt(2025, 4, 18).unwrap(), // Good Friday
            NaiveDate::from_ymd_opt(2025, 5, 1).unwrap(),  // Maharashtra Day
            NaiveDate::from_ymd_opt(2025, 8, 15).unwrap(), // Independence Day
            NaiveDate::from_ymd_opt(2025, 8, 27).unwrap(), // Ganesh Chaturthi
            NaiveDate::from_ymd_opt(2025, 10, 2).unwrap(), // Mahatma Gandhi Jayanti
            NaiveDate::from_ymd_opt(2025, 10, 21).unwrap(), // Diwali Laxmi Pujan (Muhurat session evening)
            NaiveDate::from_ymd_opt(2025, 10, 22).unwrap(), // Diwali Balipratipada
            NaiveDate::from_ymd_opt(2025, 11, 5).unwrap(),  // Prakash Gurpurb
            NaiveDate::from_ymd_opt(2025, 12, 25).unwrap(), // Christmas
            // 2026
            NaiveDate::from_ymd_opt(2026, 1, 26).unwrap(), // Republic Day
            NaiveDate::from_ymd_opt(2026, 3, 3).unwrap(),  // Holi
            NaiveDate::from_ymd_opt(2026, 4, 3).unwrap(),  // Good Friday
            NaiveDate::from_ymd_opt(2026, 4, 14).unwrap(), // Ambedkar Jayanti
            NaiveDate::from_ymd_opt(2026, 5, 1).unwrap(),  // Maharashtra Day
            NaiveDate::from_ymd_opt(2026, 8, 15).unwrap(), // Independence Day
            NaiveDate::from_ymd_opt(2026, 10, 2).unwrap(), // Gandhi Jayanti
            NaiveDate::from_ymd_opt(2026, 11, 8).unwrap(), // Diwali Laxmi Pujan (Muhurat)
            NaiveDate::from_ymd_opt(2026, 12, 25).unwrap(), // Christmas
        ];

        for h in holidays {
            self.holidays.insert(h);
        }

        // Register special Muhurat sessions
        self.muhurat_dates
            .insert(NaiveDate::from_ymd_opt(2025, 10, 21).unwrap());
        self.muhurat_dates
            .insert(NaiveDate::from_ymd_opt(2026, 11, 8).unwrap());
    }
}

/// `NseTradingCalendar` implements the canonical object-safe
/// [`tantra_core::TradingCalendar`] port, allowing the backtest engine and
/// enhancer optimizers to be decoupled from the concrete NSE venue.
impl TradingCalendar for NseTradingCalendar {
    fn is_trading_day(&self, date: NaiveDate) -> bool {
        self.is_trading_day(date)
    }

    fn is_holiday(&self, date: NaiveDate) -> bool {
        self.is_holiday(date)
    }

    fn session_for_date(&self, date: NaiveDate) -> Option<tantra_core::SessionWindow> {
        self.session_for_date(date)
            .map(|sw| tantra_core::SessionWindow {
                open: sw.open,
                close: sw.close,
            })
    }

    fn holidays(&self) -> Vec<NaiveDate> {
        self.holidays()
    }

    fn timezone_offset(&self) -> FixedOffset {
        self.ist_offset
    }
}
