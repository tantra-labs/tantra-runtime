//! Versioned DTOs for `Experimental-Analysis` multi-asset statistical studies.
//!
//! All numeric financial/statistical fields use exact [`Decimal`] (serialized as
//! decimal strings on the wire under the Zero Floats Policy). Dates use ISO
//! `YYYY-MM-DD` strings so UI charts and tables never suffer local-timezone shifts.

use rust_decimal::Decimal;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::wire::InstrumentKind;

/// Generalized asset class supported by the `Experimental-Analysis` workbench.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum StudyAssetClass {
    /// Benchmark cash & index benchmarks (`NIFTY 50`, `BANK NIFTY`, `SENSEX`, `INDIA VIX`).
    Index,
    /// F&O eligible stocks and continuous derivative underlyings.
    FnoStock,
    /// Exchange-traded funds.
    Etf,
    /// Cash equities.
    Equity,
    /// Foreign exchange pairs and currency futures.
    Forex,
    /// Commodities (`GOLD`, `SILVER`, `CRUDEOIL`, `NATURALGAS`, `COPPER`, MCX/global).
    Commodity,
}

/// Period slicing convention mapped across asset classes and calendars.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum PeriodScheduleKind {
    /// NSE/BSE weekly derivative expiry cycle (pre/post Sep 3, 2025 cutover aware).
    WeeklyExpiry,
    /// Overlapping `Week + 1` carry window (`cycle_k.open` → `cycle_{k+1}.close`).
    WeekPlusOne,
    /// Monthly derivative expiry cycle.
    MonthlyExpiry,
    /// Standard Monday-to-Friday calendar week.
    CalendarWeekly,
    /// Standard calendar month.
    CalendarMonthly,
    /// Front-month futures rollover cycle.
    FrontMonthCycle,
    /// MCX commodity expiry cycle.
    McxExpiryCycle,
    /// Single daily trading session.
    Daily,
}

/// Sub-cycle horizon modifier (`Current` vs `Period + 1` carry).
#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum SubCycleKind {
    /// Single current period.
    #[default]
    Current,
    /// Carry from current period open to following period close (`+1`).
    PlusOne,
}

/// Return calculation convention across each sliced period.
#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum ReturnCalcKind {
    /// Open-to-Close return: `(exit_close - entry_open) / entry_open * 100`.
    #[default]
    OpenToClose,
    /// High-to-Low range %: `(period_high - period_low) / entry_open * 100`.
    HighToLow,
    /// Close-to-Close return (gap-inclusive): `(exit_close - prev_close) / prev_close * 100`.
    CloseToClose,
}

/// Directional polarity of a streak (`0%` neutral periods are excluded).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum StreakDirection {
    /// Positive return run (`r > 0`).
    Positive,
    /// Negative return run (`r < 0`).
    Negative,
}

/// Six discrete Markov states encoding streak polarity × duration bucket.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum MarkovState {
    /// 1–2 consecutive positive periods.
    ShortPositive,
    /// 3–4 consecutive positive periods.
    MediumPositive,
    /// 5+ consecutive positive periods.
    LongPositive,
    /// 1–2 consecutive negative periods.
    ShortNegative,
    /// 3–4 consecutive negative periods.
    MediumNegative,
    /// 5+ consecutive negative periods.
    LongNegative,
}

/// Survivability outlier classification against symmetric empirical quantiles.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum OutlierStatus {
    /// Inside empirical coverage boundaries.
    WithinBoundary,
    /// Strictly above upper percentile bound.
    UpsideOutlier,
    /// Strictly below lower percentile bound.
    DownsideOutlier,
}

/// Query parameters for `GET /experimental-analysis/catalog`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ExperimentalCatalogQuery {
    /// Optional asset class filter.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub asset_class: Option<StudyAssetClass>,
}

/// Period schedule metadata advertised for an asset class.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ScheduleOption {
    /// Machine schedule key.
    pub kind: PeriodScheduleKind,
    /// Human display label.
    pub label: String,
    /// Short explanation of cycle boundaries.
    pub description: String,
    /// Whether this schedule is the default for its asset class.
    pub is_default: bool,
}

/// Single instrument entry returned in the `Experimental-Analysis` catalog.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct CatalogInstrumentEntry {
    /// Canonical `EXCHANGE:SYMBOL` instrument ID.
    pub instrument_id: String,
    /// Ticker symbol.
    pub symbol: String,
    /// Exchange code (`NSE`, `BSE`, `NFO`, `MCX`, `CDS`).
    pub exchange: String,
    /// Classified study asset class.
    pub asset_class: StudyAssetClass,
    /// Underlying wire instrument kind.
    pub instrument_kind: InstrumentKind,
    /// Settlement currency (`INR`, etc.).
    pub currency: String,
    /// Tradable lot size.
    pub lot_size: Decimal,
    /// Tick increment.
    pub tick_size: Decimal,
}

/// Grouped instruments and supported schedules for one asset class.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct AssetClassCatalogGroup {
    /// Asset class key.
    pub asset_class: StudyAssetClass,
    /// Display heading (`Indices`, `F&O Stocks`, etc.).
    pub label: String,
    /// Default schedule applied when selecting this asset class.
    pub default_schedule: PeriodScheduleKind,
    /// Supported period schedules for this asset class.
    pub schedules: Vec<ScheduleOption>,
    /// Instruments available in the runtime store for this asset class.
    pub instruments: Vec<CatalogInstrumentEntry>,
}

/// Market data & feed connectivity summary for the left control rail.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct DataSourceStatus {
    /// Whether historical Lakehouse v3 root is attached.
    pub lakehouse_connected: bool,
    /// Active Lakehouse commit hash when pinned.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lakehouse_commit_id: Option<String>,
    /// Whether a live quote store/broadcast feed is wired.
    pub live_feed_connected: bool,
    /// Human-readable source label.
    pub adapter_label: String,
    /// Total indexed instruments in the runtime master.
    pub indexed_instruments: usize,
    /// Count of indexed F&O derivative contracts/symbols.
    pub fno_contract_count: usize,
}

/// Response payload for `GET /experimental-analysis/catalog`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ExperimentalCatalogResponse {
    /// Per-asset-class instrument groups and schedules.
    pub groups: Vec<AssetClassCatalogGroup>,
    /// Feed and Lakehouse status card metadata.
    pub source_status: DataSourceStatus,
}

/// Request payload for `POST /experimental-analysis/returns`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ExperimentalReturnsRequest {
    /// Canonical `EXCHANGE:SYMBOL` instrument ID (or bare symbol resolved cleanly).
    pub instrument_id: String,
    /// Asset class used for schedule adaptation.
    pub asset_class: StudyAssetClass,
    /// Period slicing schedule.
    pub period: PeriodScheduleKind,
    /// Current period vs `+1` carry horizon.
    #[serde(default)]
    pub sub_cycle: SubCycleKind,
    /// Open-to-Close, High-to-Low, or Close-to-Close.
    #[serde(default)]
    pub calc_type: ReturnCalcKind,
    /// Inclusive start date (`YYYY-MM-DD`).
    pub start_date: String,
    /// Inclusive end date (`YYYY-MM-DD`).
    pub end_date: String,
    /// Moving average window in periods (`2..=120`, default `20`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ma_window: Option<usize>,
    /// Outlier survivability boundary coverage `%` (`50..=99`, default `90`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outlier_coverage_pct: Option<Decimal>,
    /// Large-move threshold `%` for conditional forward accordions (default `2.0`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tail_threshold_pct: Option<Decimal>,
    /// 1-based page number for `daily_ohlc_rows` pagination (default `1`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ohlc_page: Option<usize>,
    /// Rows per page for `daily_ohlc_rows` (`10..=500`, default `50`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ohlc_page_size: Option<usize>,
}

/// 3×3 KPI strip for Tab 1 (`Returns Analysis`).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ReturnKpis {
    /// Total sliced periods.
    pub total_periods: usize,
    /// Mean return (%).
    pub avg_return_pct: Decimal,
    /// Maximum single-period gain (%).
    pub max_gain_pct: Decimal,
    /// Maximum single-period loss (%).
    pub max_loss_pct: Decimal,
    /// Sample standard deviation of period returns (%).
    pub std_dev_pct: Decimal,
    /// Number of strictly positive periods (`r > 0`).
    pub positive_count: usize,
    /// Percentage of positive periods.
    pub positive_pct: Decimal,
    /// Number of strictly negative periods (`r < 0`).
    pub negative_count: usize,
    /// Percentage of negative periods.
    pub negative_pct: Decimal,
    /// Count of exact zero-return periods.
    pub neutral_count: usize,
    /// Non-annualized per-period Sharpe ratio (`mu / sigma`).
    pub sharpe_ratio: Decimal,
    /// Per-period Sortino ratio (`mu / downside_dev`).
    pub sortino_ratio: Decimal,
    /// Profit factor (`sum(r > 0) / abs(sum(r < 0))`).
    pub profit_factor: Decimal,
}

/// Chronological period point rendered in Lollipop/Bar/SMA charts and Period table.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct PeriodPoint {
    /// 1-based ordinal index.
    pub index: usize,
    /// Period start date (`YYYY-MM-DD`).
    pub start_date: String,
    /// Period end date (`YYYY-MM-DD`).
    pub end_date: String,
    /// Period opening price.
    pub entry_price: Decimal,
    /// Period high price.
    pub high_price: Decimal,
    /// Period low price.
    pub low_price: Decimal,
    /// Period closing price.
    pub exit_price: Decimal,
    /// Computed return (%).
    pub return_pct: Decimal,
    /// Rolling SMA across returns when window is warm.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ma_pct: Option<Decimal>,
    /// Regime/schedule label for tooltips & annotations.
    pub regime_label: String,
}

/// 7-metric KPI bar above the Daily OHLC chart & table.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct DailyOhlcSummary {
    /// Most recent session close price.
    pub latest_close: Decimal,
    /// Maximum high across the date window.
    pub period_high: Decimal,
    /// Minimum low across the date window.
    pub period_low: Decimal,
    /// Total daily trading bars in range.
    pub total_trading_days: usize,
    /// Mean close-to-close daily return (%).
    pub avg_daily_return_pct: Decimal,
    /// Sample standard deviation of daily returns (%).
    pub daily_return_std_pct: Decimal,
    /// Mean daily traded volume.
    pub avg_volume: Decimal,
}

/// Single session row inside the paginated `Daily OHLC Data` table.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct DailyOhlcRow {
    /// Session date (`YYYY-MM-DD`).
    pub date: String,
    /// Opening price.
    pub open: Decimal,
    /// High price.
    pub high: Decimal,
    /// Low price.
    pub low: Decimal,
    /// Close price.
    pub close: Decimal,
    /// Volume.
    pub volume: Decimal,
    /// Dividend on ex-date.
    pub dividend: Decimal,
    /// Split factor on ex-date.
    pub split_factor: Decimal,
    /// Session close-to-close return (%).
    pub daily_return_pct: Decimal,
}

/// One contiguous positive or negative run in `Streak Details`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct StreakDetail {
    /// 1-based chronological streak index.
    pub index: usize,
    /// Streak direction.
    pub streak_type: StreakDirection,
    /// Number of non-neutral periods in run.
    pub length: usize,
    /// Streak start date (`YYYY-MM-DD`).
    pub start_date: String,
    /// Streak end date (`YYYY-MM-DD`).
    pub end_date: String,
    /// Opening price of first period in streak.
    pub start_price: Decimal,
    /// Closing price of final period in streak.
    pub end_price: Decimal,
    /// Compounded total return over the streak (%).
    pub total_return_pct: Decimal,
    /// Average per-period return inside streak (%).
    pub avg_period_return_pct: Decimal,
}

/// Aggregate KPI metrics across all completed streaks.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct StreakSummary {
    /// Active streak direction.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_streak_type: Option<StreakDirection>,
    /// Length of active streak.
    pub current_streak_length: usize,
    /// Compounded return of active streak (%).
    pub current_streak_return_pct: Decimal,
    /// Markov state of active streak.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_markov_state: Option<MarkovState>,
    /// Total positive streaks.
    pub positive_streak_count: usize,
    /// Total negative streaks.
    pub negative_streak_count: usize,
    /// Longest positive streak length.
    pub longest_positive: usize,
    /// Longest negative streak length.
    pub longest_negative: usize,
    /// Best positive streak total return (%).
    pub best_positive_return_pct: Decimal,
    /// Worst negative streak total return (%).
    pub worst_negative_return_pct: Decimal,
    /// Average positive streak length.
    pub avg_positive_length: Decimal,
    /// Average negative streak length.
    pub avg_negative_length: Decimal,
    /// Exact zero-return periods excluded before streak grouping.
    pub excluded_neutral_count: usize,
}

/// Histogram bar comparing positive vs negative streak counts by duration.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct StreakLengthBucket {
    /// Run length (`1, 2, 3, ...`).
    pub length: usize,
    /// Positive streak count.
    pub positive_count: usize,
    /// Negative streak count.
    pub negative_count: usize,
}

/// Descriptive distribution stats for one streak polarity side.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SideStreakStats {
    /// Modal streak length.
    pub most_common_length: usize,
    /// Observation count at the mode.
    pub most_common_count: usize,
    /// Median streak length.
    pub median_length: Decimal,
    /// Sample standard deviation of streak lengths.
    pub std_dev_length: Decimal,
}

/// Complete Streak Analysis bundle.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct StreakAnalysis {
    /// Active & summary streak KPIs.
    pub summary: StreakSummary,
    /// Chronological list of streaks.
    pub details: Vec<StreakDetail>,
    /// Streak length histogram.
    pub length_distribution: Vec<StreakLengthBucket>,
    /// Positive streak statistics.
    pub positive_stats: SideStreakStats,
    /// Negative streak statistics.
    pub negative_stats: SideStreakStats,
}

/// Probability weight (`0..100`) associated with one Markov state.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct MarkovStateProbability {
    /// Target state.
    pub state: MarkovState,
    /// Display label.
    pub label: String,
    /// Empirical probability percentage.
    pub probability_pct: Decimal,
}

/// One row of the 6×6 Markov transition matrix.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct MarkovTransitionRow {
    /// Source Markov state.
    pub from_state: MarkovState,
    /// Display label.
    pub from_label: String,
    /// Historical outgoing transition sample size.
    pub observation_count: usize,
    /// Row probabilities matching `[ShortPos, MedPos, LongPos, ShortNeg, MedNeg, LongNeg]` (%).
    pub probs_pct: Vec<Decimal>,
}

/// Conditional next-streak prediction anchored to active streak state.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct MarkovCurrentPrediction {
    /// Active streak Markov state.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_state: Option<MarkovState>,
    /// Active streak display label.
    pub current_label: String,
    /// Total probability (%) of next streak being Positive.
    pub positive_next_pct: Decimal,
    /// Total probability (%) of next streak being Negative.
    pub negative_next_pct: Decimal,
    /// Most likely next Markov state.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub strongest_next_state: Option<MarkovState>,
    /// Label of strongest next state.
    pub strongest_next_label: String,
    /// Probability (%) of strongest next state.
    pub strongest_probability_pct: Decimal,
    /// Ranked breakdown across all 6 target states.
    pub state_breakdown: Vec<MarkovStateProbability>,
}

/// Complete 6-State Markov Chain analysis bundle.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct MarkovAnalysis {
    /// 6×6 transition matrix rows.
    pub transition_matrix: Vec<MarkovTransitionRow>,
    /// Conditional prediction from active state.
    pub current_prediction: MarkovCurrentPrediction,
    /// Long-run stationary distribution (%).
    pub steady_state: Vec<MarkovStateProbability>,
}

/// Seven empirical quantile landmarks (`P99..P1`).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ReturnPercentiles {
    /// 99th percentile.
    pub p99: Decimal,
    /// 95th percentile.
    pub p95: Decimal,
    /// 75th percentile.
    pub p75: Decimal,
    /// 50th percentile (median).
    pub p50: Decimal,
    /// 25th percentile.
    pub p25: Decimal,
    /// 5th percentile.
    pub p5: Decimal,
    /// 1st percentile.
    pub p1: Decimal,
}

/// One row of the 16-bucket or 10-bps return distribution table.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ReturnBucketRow {
    /// Range label.
    pub label: String,
    /// Lower bound (%).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_pct: Option<Decimal>,
    /// Upper bound (%).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_pct: Option<Decimal>,
    /// Period count.
    pub count: usize,
    /// Share of total periods (%).
    pub pct_of_total: Decimal,
    /// Cumulative quantile range display label.
    pub percentile_range: String,
    /// Cumulative probability display label.
    pub probability_label: String,
    /// Projected lower spot price anchored to reference price.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub price_range_low: Option<Decimal>,
    /// Projected upper spot price anchored to reference price.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub price_range_high: Option<Decimal>,
}

/// Exceedance row for `Probability-Based Levels (Easy Reference)`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ProbabilityLevelRow {
    /// Probability (%) of spot finishing above `implied_price`.
    pub prob_above_pct: usize,
    /// Return cut (%).
    pub return_pct: Decimal,
    /// Implied spot price anchored to reference price.
    pub implied_price: Decimal,
    /// Percentage change from reference price.
    pub change_pct: Decimal,
    /// Directional scenario tag.
    pub scenario: String,
}

/// Descriptive stats for Green (`r > 0`) or Red (`r < 0`) periods.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct PolarityStats {
    /// Period count on this side.
    pub count: usize,
    /// Percentage share across all periods.
    pub share_pct: Decimal,
    /// Mean return (%).
    pub mean_pct: Decimal,
    /// Median return (%).
    pub median_pct: Decimal,
    /// Sample standard deviation (%).
    pub std_dev_pct: Decimal,
    /// Most extreme return on this side (%).
    pub extreme_pct: Decimal,
}

/// Magnitude PDF comparison bin (`Green %` vs `Red %`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct MagnitudePdfBucket {
    /// Magnitude label (`"0% - 1%"`, etc.).
    pub label: String,
    /// Green count.
    pub green_count: usize,
    /// Green share (%).
    pub green_pct: Decimal,
    /// Red count.
    pub red_count: usize,
    /// Red share (%).
    pub red_pct: Decimal,
}

/// Green vs Red split comparison bundle.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct GreenVsRedComparison {
    /// Green side stats.
    pub green: PolarityStats,
    /// Red side stats.
    pub red: PolarityStats,
    /// Side-by-side empirical PDF magnitude bins.
    pub magnitude_buckets: Vec<MagnitudePdfBucket>,
}

/// Single next-period observation following a large trigger move.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ForwardConditionEvent {
    /// Trigger period end date (`YYYY-MM-DD`).
    pub trigger_end_date: String,
    /// Trigger period return (%).
    pub trigger_return_pct: Decimal,
    /// Following period end date (`YYYY-MM-DD`).
    pub next_end_date: String,
    /// Following period return (%).
    pub next_return_pct: Decimal,
}

/// Aggregate forward expectancy following large positive or negative moves.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ForwardConditionalGroup {
    /// Trigger magnitude threshold (%).
    pub threshold_pct: Decimal,
    /// Mean forward return (%) after large positive trigger.
    pub after_large_pos_mean_pct: Decimal,
    /// Median forward return (%) after large positive trigger.
    pub after_large_pos_median_pct: Decimal,
    /// Positive win rate (%) after large positive trigger.
    pub after_large_pos_win_pct: Decimal,
    /// Event rows after large positive trigger.
    pub after_large_pos_events: Vec<ForwardConditionEvent>,
    /// Mean forward return (%) after large negative trigger.
    pub after_large_neg_mean_pct: Decimal,
    /// Median forward return (%) after large negative trigger.
    pub after_large_neg_median_pct: Decimal,
    /// Positive win rate (%) after large negative trigger.
    pub after_large_neg_win_pct: Decimal,
    /// Event rows after large negative trigger.
    pub after_large_neg_events: Vec<ForwardConditionEvent>,
}

/// Single period point classified by empirical survivability bounds.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct OutlierPeriodPoint {
    /// Cycle index.
    pub index: usize,
    /// Start date (`YYYY-MM-DD`).
    pub start_date: String,
    /// End date (`YYYY-MM-DD`).
    pub end_date: String,
    /// Return (%).
    pub return_pct: Decimal,
    /// Survivability status.
    pub status: OutlierStatus,
}

/// 4-KPI outlier summary strip + computed bounds.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct OutlierKpis {
    /// Coverage slider value (`50..=99`).
    pub coverage_pct: Decimal,
    /// Upper threshold (%).
    pub upper_bound_pct: Decimal,
    /// Lower threshold (%).
    pub lower_bound_pct: Decimal,
    /// Total periods.
    pub total_periods: usize,
    /// Total outliers.
    pub total_outliers: usize,
    /// Outlier percentage.
    pub outlier_pct: Decimal,
    /// Upside outliers count.
    pub upside_outliers: usize,
    /// Downside outliers count.
    pub downside_outliers: usize,
}

/// Survivability Outlier bundle for Tab 2.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct OutlierAnalysis {
    /// Summary KPIs & empirical thresholds.
    pub kpis: OutlierKpis,
    /// Chronological scatter points.
    pub scatter_points: Vec<OutlierPeriodPoint>,
    /// Filtered outlier rows sorted by `|return_pct|` descending.
    pub outlier_table: Vec<OutlierPeriodPoint>,
}

/// Full single-shot statistical report returned by `POST /experimental-analysis/returns`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ExperimentalReturnsResponse {
    /// Analyzed instrument ID (`EXCHANGE:SYMBOL`).
    pub instrument_id: String,
    /// Instrument symbol.
    pub symbol: String,
    /// Exchange code.
    pub exchange: String,
    /// Currency code (`INR`, etc.).
    pub currency: String,
    /// Asset class evaluated.
    pub asset_class: StudyAssetClass,
    /// Period schedule applied.
    pub period: PeriodScheduleKind,
    /// Sub-cycle modifier (`current` or `plus_one`).
    pub sub_cycle: SubCycleKind,
    /// Return formula (`open_to_close`, `high_to_low`, `close_to_close`).
    pub calc_type: ReturnCalcKind,
    /// Effective first period start date (`YYYY-MM-DD`).
    pub start_date: String,
    /// Effective last period end date (`YYYY-MM-DD`).
    pub end_date: String,
    /// Optional NSE Sep 2025 or exchange regime transition banner.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub regime_note: Option<String>,
    /// Historical source description (`Lakehouse v3` or `Store BarReader`).
    pub source_label: String,
    /// Total sliced periods analyzed.
    pub period_count: usize,
    /// 3×3 Return KPIs.
    pub kpis: ReturnKpis,
    /// Chronological period return series with rolling SMA.
    pub series: Vec<PeriodPoint>,
    /// Streak KPIs, timeline, details, and histogram.
    pub streaks: StreakAnalysis,
    /// 6-state streak Markov chain analysis.
    pub markov: MarkovAnalysis,
    /// Empirical return percentiles (`P99..P1`).
    pub percentiles: ReturnPercentiles,
    /// Current live or latest close price.
    pub current_price: Decimal,
    /// Current cycle opening reference price.
    pub current_reference_price: Decimal,
    /// Percentage delta of current price vs current cycle reference price.
    pub current_reference_change_pct: Decimal,
    /// 16 coarse return buckets.
    pub coarse_buckets: Vec<ReturnBucketRow>,
    /// Non-empty 10-bps (`0.1%`) granular return buckets.
    pub granular_10bps: Vec<ReturnBucketRow>,
    /// Exceedance price levels (`10%..99%`).
    pub probability_levels: Vec<ProbabilityLevelRow>,
    /// Green vs Red comparative statistics and PDF bins.
    pub green_vs_red: GreenVsRedComparison,
    /// Conditional forward moves after large period swings.
    pub forward_conditional: ForwardConditionalGroup,
    /// Optional conditional forward moves after large `Period+1` swings.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub forward_conditional_plus_one: Option<ForwardConditionalGroup>,
    /// Survivability outlier scatter & ranked table.
    pub outliers: OutlierAnalysis,
    /// 7-KPI daily OHLC summary strip.
    pub daily_ohlc_summary: DailyOhlcSummary,
    /// Total daily OHLC rows in range (for pagination).
    pub daily_ohlc_total_rows: usize,
    /// Active 1-based page number.
    pub daily_ohlc_page: usize,
    /// Page size applied.
    pub daily_ohlc_page_size: usize,
    /// Current page of daily OHLC rows (newest-first).
    pub daily_ohlc_rows: Vec<DailyOhlcRow>,
}
