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

/// F&O return adjustment regime applied over sliced periods.
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
pub enum RollBasisMode {
    /// Unadjusted spot / bar returns.
    #[default]
    GrossSpot,
    /// Applies futures rollover/carry basis (`futures_roll_basis_bps`) to spot periods.
    IncludeRollBasis,
    /// Applies rollover basis AND deducts bid-ask spread slippage + statutory exchange/STT costs.
    NetAfterSpreadAndCosts,
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

/// Query parameters for `GET /studies/experimental-analysis/catalog`.
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

/// Response payload for `GET /studies/experimental-analysis/catalog`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ExperimentalCatalogResponse {
    /// Per-asset-class instrument groups and schedules.
    pub groups: Vec<AssetClassCatalogGroup>,
    /// Feed and Lakehouse status card metadata.
    pub source_status: DataSourceStatus,
}

/// Request payload for `POST /studies/experimental-analysis/returns`.
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
    /// F&O return adjustment mode (default `gross_spot`).
    #[serde(default)]
    pub roll_basis_mode: RollBasisMode,
    /// Round-trip bid-ask spread slippage in basis points (`0..=500`, default `0`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spread_slippage_bps: Option<Decimal>,
    /// Futures rollover / carry basis adjustment per period in bps (`-500..=500`, default `0`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub futures_roll_basis_bps: Option<Decimal>,
    /// Deduct Indian statutory F&O/cash turnover charges when in `net_after_spread_and_costs`.
    #[serde(default)]
    pub include_statutory_costs: bool,
}

/// Gross vs Net comparison summary quantifying F&O spread, basis, and statutory fee drag.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct FrictionSummary {
    /// Active roll/friction adjustment mode.
    pub mode: RollBasisMode,
    /// Gross arithmetic average period return (%) before friction.
    pub gross_avg_return_pct: Decimal,
    /// Net arithmetic average period return (%) after spread, basis, and statutory costs.
    pub net_avg_return_pct: Decimal,
    /// Gross period Sharpe ratio.
    pub gross_sharpe_ratio: Decimal,
    /// Net period Sharpe ratio after friction.
    pub net_sharpe_ratio: Decimal,
    /// Gross profit factor.
    pub gross_profit_factor: Decimal,
    /// Net profit factor after friction.
    pub net_profit_factor: Decimal,
    /// Configured round-trip bid-ask spread slippage per period (bps).
    pub per_period_spread_bps: Decimal,
    /// Round-trip statutory derivative turnover charge per period (bps).
    pub per_period_statutory_bps: Decimal,
    /// Configured futures roll/carry basis adjustment per period (bps).
    pub per_period_basis_bps: Decimal,
    /// Cumulative percentage-point impact across all evaluated periods (`sum(net - gross)`).
    pub cumulative_net_delta_pct: Decimal,
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

/// Full single-shot statistical report returned by `POST /studies/experimental-analysis/returns`.
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
    /// Optional F&O spread, roll-basis & statutory cost reconciliation summary.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub friction_summary: Option<FrictionSummary>,
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

/// Statistics for one time-of-day intraday bucket (`15m` or `60m`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct IntradaySlotStat {
    /// Human-readable clock slot (`HH:MM`, e.g. `"09:15"`).
    pub slot_label: String,
    /// Minute offset from midnight local exchange time (`0..1440`).
    pub minute_of_day: u16,
    /// Number of intraday bars observed in this clock slot.
    pub bar_count: usize,
    /// Mean intra-bar range % `(high - low) / open * 100`.
    pub avg_range_pct: Decimal,
    /// Maximum observed range % in this slot.
    pub max_range_pct: Decimal,
    /// Mean absolute close-to-open move % `|close - open| / open * 100`.
    pub avg_abs_move_pct: Decimal,
    /// Average bar volume.
    pub avg_volume: Decimal,
    /// Number of sessions whose High-of-Day occurred inside this slot.
    pub hod_count: usize,
    /// Number of sessions whose Low-of-Day occurred inside this slot.
    pub lod_count: usize,
}

/// Tab 3: Intraday movement, session range & HOD/LOD clock profile.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct IntradayMovementAnalysis {
    /// Whether `5m`/intraday bars were available for this instrument.
    pub available: bool,
    /// Number of distinct trading sessions with intraday bars.
    pub total_sessions: usize,
    /// Average session high-low range (%).
    pub avg_session_range_pct: Decimal,
    /// 15-minute slot with peak average volatility.
    pub peak_volatility_slot: String,
    /// Most frequent High-of-Day clock slot.
    pub most_common_hod_slot: String,
    /// Most frequent Low-of-Day clock slot.
    pub most_common_lod_slot: String,
    /// Breakdown across 15-minute buckets.
    pub slots_15m: Vec<IntradaySlotStat>,
    /// Breakdown across 60-minute hourly buckets.
    pub slots_hourly: Vec<IntradaySlotStat>,
}

/// One bucket row in the Overnight Gap distribution table.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct GapBucketRow {
    /// Display bucket label (e.g. `"+0.5% to +1.0%"`).
    pub label: String,
    /// Observed count.
    pub count: usize,
    /// Share of all gap sessions (%).
    pub share_pct: Decimal,
    /// Gap-fill rate (%) inside this bucket.
    pub fill_rate_pct: Decimal,
    /// Mean intraday open-to-close move (%) following gaps in this bucket.
    pub avg_intraday_follow_pct: Decimal,
}

/// Individual notable overnight gap record.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct GapEventRow {
    /// Session date (`YYYY-MM-DD`).
    pub date: String,
    /// Prior close.
    pub prev_close: Decimal,
    /// Current open.
    pub open: Decimal,
    /// Current close.
    pub close: Decimal,
    /// Overnight gap %.
    pub gap_pct: Decimal,
    /// Intraday open-to-close return %.
    pub intraday_return_pct: Decimal,
    /// Whether the session touched `prev_close` (gap filled).
    pub filled: bool,
}

/// Tab 4: Overnight gap distribution, fill probability & continuation expectancy.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct GapAnalysis {
    /// Total sessions evaluated (`daily_bars.len() - 1`).
    pub total_sessions: usize,
    /// Count of gap-up sessions (`gap_pct > 0`).
    pub gap_up_count: usize,
    /// Count of gap-down sessions (`gap_pct < 0`).
    pub gap_down_count: usize,
    /// Average positive gap magnitude (%).
    pub avg_gap_up_pct: Decimal,
    /// Average negative gap magnitude (%).
    pub avg_gap_down_pct: Decimal,
    /// Overall gap-up fill probability (%).
    pub gap_up_fill_rate_pct: Decimal,
    /// Overall gap-down fill probability (%).
    pub gap_down_fill_rate_pct: Decimal,
    /// Average open-to-close fade/continuation after a gap up (%).
    pub gap_up_follow_pct: Decimal,
    /// Average open-to-close fade/continuation after a gap down (%).
    pub gap_down_follow_pct: Decimal,
    /// Bucketed distribution + conditional fill & follow-through rates.
    pub buckets: Vec<GapBucketRow>,
    /// Most recent gap events (up to 40 rows).
    pub recent_events: Vec<GapEventRow>,
}

/// One point in the Rolling 5-Day Move trajectory.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct RollingFiveDayPoint {
    /// Session end date (`YYYY-MM-DD`).
    pub date: String,
    /// Session close.
    pub close: Decimal,
    /// Trailing 5-trading-day return (%).
    pub return_5d_pct: Decimal,
}

/// Tab 5: Rolling 5-trading-day move distribution & exhaustion profile.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct RollingFiveDayAnalysis {
    /// Total 5-day windows evaluated.
    pub sample_count: usize,
    /// Mean 5-day return (%).
    pub avg_return_pct: Decimal,
    /// Sample standard deviation of 5-day returns (%).
    pub std_dev_pct: Decimal,
    /// Share of positive 5-day moves (%).
    pub positive_pct: Decimal,
    /// 99th percentile 5-day move (%).
    pub p99_pct: Decimal,
    /// 95th percentile 5-day move (%).
    pub p95_pct: Decimal,
    /// Median 5-day move (%).
    pub p50_pct: Decimal,
    /// 5th percentile 5-day move (%).
    pub p5_pct: Decimal,
    /// 1st percentile 5-day move (%).
    pub p1_pct: Decimal,
    /// Average forward 5-day return after `5d >= +tail_threshold_pct` (%).
    pub after_surge_next_5d_avg_pct: Decimal,
    /// Average forward 5-day return after `5d <= -tail_threshold_pct` (%).
    pub after_drop_next_5d_avg_pct: Decimal,
    /// Time series points for chart rendering.
    pub series: Vec<RollingFiveDayPoint>,
}

/// One normalized progress decile (`0%, 10%, ..., 100%`) along a period cycle.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct CycleProgressDecile {
    /// Normalized cycle completion percentage (`0, 10, 20, ..., 100`).
    pub progress_pct: u16,
    /// Number of periods contributing to this decile.
    pub sample_count: usize,
    /// Mean cumulative drift from period entry (%).
    pub mean_pct: Decimal,
    /// 10th percentile drift (%).
    pub p10_pct: Decimal,
    /// 25th percentile drift (%).
    pub p25_pct: Decimal,
    /// Median cumulative drift (%).
    pub p50_pct: Decimal,
    /// 75th percentile drift (%).
    pub p75_pct: Decimal,
    /// 90th percentile drift (%).
    pub p90_pct: Decimal,
}

/// Tab 6: Intra-cycle progress (0% → 100%) normalized drift cone.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct CycleProgressAnalysis {
    /// Number of multi-bar cycles contributing to the cone.
    pub cycle_count: usize,
    /// 11 normalized progress deciles (`0%` through `100%`).
    pub deciles: Vec<CycleProgressDecile>,
}

/// One simulated intraday ORB trade leg.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct OrbTradeRow {
    /// Session date (`YYYY-MM-DD`).
    pub date: String,
    /// `"long"` or `"short"`.
    pub direction: String,
    /// Flip sequence inside session (`0` = initial breakout, `1` = first flip, etc.).
    pub flip_index: usize,
    /// Opening range high for the session.
    pub or_high: Decimal,
    /// Opening range low for the session.
    pub or_low: Decimal,
    /// Entry price.
    pub entry_price: Decimal,
    /// Exit price.
    pub exit_price: Decimal,
    /// Exit reason (`"target"`, `"stop_flip"`, `"stop_max_flips"`, `"eod"`).
    pub exit_reason: String,
    /// Gross trade return % before spread & fees.
    pub gross_return_pct: Decimal,
    /// Deducted spread + statutory cost friction (bps).
    pub friction_bps: Decimal,
    /// Net trade return % after spread & fees.
    pub net_return_pct: Decimal,
}

/// Equity curve sample point for ORB backtest visualization.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct OrbEquityPoint {
    /// Session date (`YYYY-MM-DD`).
    pub date: String,
    /// Cumulative gross return (%).
    pub cumulative_gross_pct: Decimal,
    /// Cumulative net return (%).
    pub cumulative_net_pct: Decimal,
}

/// Tab 10: Complete 15m Opening Range Breakout + Flip simulation result.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct OrbBacktestResult {
    /// Whether `5m` intraday bars were available to run the simulation.
    pub available: bool,
    /// Total simulated trades across all sessions.
    pub total_trades: usize,
    /// Count of winning trades (`net_return_pct > 0`).
    pub winning_trades: usize,
    /// Win rate (%).
    pub win_rate_pct: Decimal,
    /// Cumulative sum of gross trade returns (%).
    pub gross_return_pct: Decimal,
    /// Total spread + statutory friction deducted (% points).
    pub total_friction_pct: Decimal,
    /// Cumulative sum of net trade returns (%).
    pub net_return_pct: Decimal,
    /// Empirical net profit factor.
    pub profit_factor: Decimal,
    /// Maximum peak-to-trough net return drawdown (%).
    pub max_drawdown_pct: Decimal,
    /// Mean net return per trade (%).
    pub avg_trade_net_pct: Decimal,
    /// Cumulative gross vs net equity curve by session date.
    pub equity_curve: Vec<OrbEquityPoint>,
    /// Most recent simulated trade legs (up to 100).
    pub trades: Vec<OrbTradeRow>,
}

/// Request payload for `POST /studies/experimental-analysis/intraday`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ExperimentalIntradayRequest {
    /// Canonical `EXCHANGE:SYMBOL` instrument ID (or `SYMBOL.EXCHANGE`).
    pub instrument_id: String,
    /// Asset class used for cycle-progress slicing and statutory fee mapping.
    pub asset_class: StudyAssetClass,
    /// Period schedule for Cycle Progress (0% → 100%) cone.
    pub period: PeriodScheduleKind,
    /// Inclusive start date (`YYYY-MM-DD`).
    pub start_date: String,
    /// Inclusive end date (`YYYY-MM-DD`).
    pub end_date: String,
    /// Large-move threshold % for 5-day exhaustion calculation (default `2.0`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tail_threshold_pct: Option<Decimal>,
    /// Opening range width in minutes (`5..=60`, default `15`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub opening_range_minutes: Option<u16>,
    /// Maximum flips per day in ORB simulation (`0..=10`, default `2`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_flips_per_day: Option<usize>,
    /// Fixed percentage stop-loss if tighter than opposite OR boundary (`0` disables).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stop_loss_pct: Option<Decimal>,
    /// Fixed percentage take-profit target (`0` holds to EOD/flip).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_pct: Option<Decimal>,
    /// Round-trip bid-ask spread slippage per ORB trade in bps (`0..=500`, default `0`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spread_slippage_bps: Option<Decimal>,
    /// Whether to deduct statutory exchange/STT fees per ORB trade.
    #[serde(default)]
    pub include_statutory_costs: bool,
}

/// Combined Phase 2 Intraday / Gap / 5-Day / Cycle Progress / 15m ORB response.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ExperimentalIntradayResponse {
    /// Analyzed canonical instrument ID.
    pub instrument_id: String,
    /// Symbol.
    pub symbol: String,
    /// Exchange code.
    pub exchange: String,
    /// Asset class evaluated.
    pub asset_class: StudyAssetClass,
    /// Cycle progress schedule applied.
    pub period: PeriodScheduleKind,
    /// Start date (`YYYY-MM-DD`).
    pub start_date: String,
    /// End date (`YYYY-MM-DD`).
    pub end_date: String,
    /// Tab 3: Intraday movement & HOD/LOD clock profile (honest `available: false` if no 5m bars).
    pub intraday: IntradayMovementAnalysis,
    /// Tab 4: Overnight gap distribution & fill probabilities.
    pub gaps: GapAnalysis,
    /// Tab 5: Rolling 5-day move quantiles & exhaustion response.
    pub five_day: RollingFiveDayAnalysis,
    /// Tab 6: Intra-cycle 0% → 100% progress drift cone.
    pub cycle_progress: CycleProgressAnalysis,
    /// Tab 10: 15m Opening Range Breakout + Flip simulation (honest `available: false` if no 5m bars).
    pub orb_backtest: OrbBacktestResult,
}

/// One rolling correlation & spread sample between primary and comparison benchmark.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct RollingPairPoint {
    /// Trading date (`YYYY-MM-DD`).
    pub date: String,
    /// Trailing `rolling_window` Pearson correlation (`-1..=1`).
    pub rolling_corr: Decimal,
    /// Relative normalized spread `(Primary_idx - Benchmark_idx)` in percentage points.
    pub relative_spread_pct: Decimal,
    /// Raw price spread `(Primary_close - Benchmark_close)`.
    pub raw_spread: Decimal,
}

/// Request payload for `POST /studies/experimental-analysis/correlation`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ExperimentalCorrelationRequest {
    /// Primary instrument ID (`EXCHANGE:SYMBOL` or `SYMBOL.EXCHANGE`).
    pub primary_id: String,
    /// Benchmark instrument ID for rolling beta & spread curves.
    pub benchmark_id: String,
    /// List of instrument IDs to include in the `N × N` correlation matrix (`2..=12`).
    pub symbols: Vec<String>,
    /// Inclusive start date (`YYYY-MM-DD`).
    pub start_date: String,
    /// Inclusive end date (`YYYY-MM-DD`).
    pub end_date: String,
    /// Rolling window in overlapping sessions (`5..=250`, default `20`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rolling_window: Option<usize>,
}

/// Response payload for `POST /studies/experimental-analysis/correlation`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ExperimentalCorrelationResponse {
    /// Labels corresponding to matrix rows & columns.
    pub labels: Vec<String>,
    /// Primary instrument ID.
    pub primary_id: String,
    /// Comparison benchmark ID.
    pub benchmark_id: String,
    /// Rolling window length used.
    pub rolling_window: usize,
    /// Overlapping sessions between primary and benchmark.
    pub aligned_sessions: usize,
    /// `N × N` Pearson correlation matrix.
    pub pearson_matrix: Vec<Vec<Decimal>>,
    /// `N × N` Spearman rank correlation matrix.
    pub spearman_matrix: Vec<Vec<Decimal>>,
    /// Empirical OLS Beta of primary relative to benchmark.
    pub pair_beta: Decimal,
    /// Full-sample Pearson correlation between primary and benchmark.
    pub pair_pearson: Decimal,
    /// Full-sample Spearman correlation between primary and benchmark.
    pub pair_spearman: Decimal,
    /// Latest raw close spread (`primary - benchmark`).
    pub latest_raw_spread: Decimal,
    /// Latest normalized spread (% outperformance since start of aligned window).
    pub latest_relative_spread_pct: Decimal,
    /// Rolling correlation and spread curve.
    pub rolling_series: Vec<RollingPairPoint>,
}

// ─────────────────────────────────────────────────────────────────────────────
// Phase 3 DTOs: Options Overlay, Forecasting, Event Studies & Outlier News
// ─────────────────────────────────────────────────────────────────────────────

/// Option overlay strategy family evaluated in Tab 8 & Tab 9.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum OptionOverlayStrategy {
    /// Long 1× underlying + Short 1× OTM Call at empirical target probability strike.
    #[default]
    CoveredCall,
    /// Defined-risk Bull Call vertical spread (Long ATM CE + Short OTM CE).
    BullCallSpread,
    /// Defined-risk Bear Put vertical spread (Long ATM PE + Short OTM PE).
    BearPutSpread,
    /// Four-leg range-bound Iron Condor (Short OTM CE/PE + Long protective wings).
    IronCondor,
    /// Calendar time-spread roll proxy (Short near-cycle ATM CE + Long next-cycle ATM CE).
    CalendarSpread,
}

/// Single option or underlying leg inside a multi-leg structure.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct OptionLegSpec {
    /// Strategy leg label (`"Long Underlying"`, `"Short OTM Call"`, etc.).
    pub role: String,
    /// `"BUY"` or `"SELL"`.
    pub side: String,
    /// `"SPOT"`, `"CE"`, or `"PE"`.
    pub option_type: String,
    /// Selected strike price (or spot reference).
    pub strike: Decimal,
    /// Empirical cycle fair value (% of spot).
    pub fair_value_pct: Decimal,
    /// Quoted live bid price when option chain is loaded.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bid: Option<Decimal>,
    /// Quoted live ask price when option chain is loaded.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ask: Option<Decimal>,
    /// Quoted bid-ask spread in price units (`ask - bid`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spread_abs: Option<Decimal>,
    /// Quoted bid-ask spread in basis points relative to mid price.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spread_bps: Option<Decimal>,
    /// Effective execution price after crossing bid/ask or configured slippage.
    pub execution_price: Decimal,
}

/// Strike liquidity & empirical exceedance row for Tab 9 (`Expiry Strike Lab`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct StrikeLabRow {
    /// `"CE"` or `"PE"`.
    pub option_type: String,
    /// Strike price.
    pub strike: Decimal,
    /// Option expiry date (`YYYY-MM-DD`) when sourced from live chain.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expiry: Option<String>,
    /// Best bid price when quoted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bid: Option<Decimal>,
    /// Best ask price when quoted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ask: Option<Decimal>,
    /// Midpoint `(bid + ask) / 2`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mid: Option<Decimal>,
    /// Quoted bid-ask spread (`ask - bid`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spread_abs: Option<Decimal>,
    /// Quoted bid-ask spread in bps.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spread_bps: Option<Decimal>,
    /// Liquidity tier: `"tight"`, `"moderate"`, `"wide"`, or `"unquoted"`.
    pub liquidity_tier: String,
    /// Latest open interest when available.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub open_interest: Option<Decimal>,
    /// Empirical probability (%) spot closes beyond this strike.
    pub exceedance_prob_pct: Decimal,
    /// Strike moneyness relative to spot (%).
    pub moneyness_pct: Decimal,
}

/// Period-by-period equity curve sample comparing Buy-and-Hold vs Options Overlay.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct OverlayEquityPoint {
    /// Period end date (`YYYY-MM-DD`).
    pub date: String,
    /// Buy-and-Hold cumulative return (%).
    pub buy_hold_cum_pct: Decimal,
    /// Gross overlay cumulative return (%) before spread & statutory costs.
    pub overlay_gross_cum_pct: Decimal,
    /// Net overlay cumulative return (%) after spread & statutory costs.
    pub overlay_net_cum_pct: Decimal,
    /// Single-period net strategy payoff (%).
    pub period_payoff_pct: Decimal,
}

/// Request payload for `POST /studies/experimental-analysis/options-overlay`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ExperimentalOptionsOverlayRequest {
    /// Canonical `EXCHANGE:SYMBOL` instrument ID (or `SYMBOL.EXCHANGE`).
    pub instrument_id: String,
    /// Asset class context for schedule slicing and exchange fee tier.
    pub asset_class: StudyAssetClass,
    /// Period slicing schedule.
    pub period: PeriodScheduleKind,
    /// Inclusive start date (`YYYY-MM-DD`).
    pub start_date: String,
    /// Inclusive end date (`YYYY-MM-DD`).
    pub end_date: String,
    /// Option overlay strategy family (default `covered_call`).
    #[serde(default)]
    pub strategy: OptionOverlayStrategy,
    /// Target non-exceedance probability (`55..=98`, default `80`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_short_prob_pct: Option<Decimal>,
    /// Protective wing width percentage (`0.5..=15.0`, default `3.0`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wing_width_pct: Option<Decimal>,
    /// Fallback round-trip spread slippage in bps (`0..=500`, default `15`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spread_slippage_bps: Option<Decimal>,
    /// Whether to deduct statutory Indian derivative turnover charges.
    #[serde(default)]
    pub include_statutory_costs: bool,
}

/// Response payload for `POST /studies/experimental-analysis/options-overlay`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ExperimentalOptionsOverlayResponse {
    /// Analyzed canonical instrument ID.
    pub instrument_id: String,
    /// Symbol.
    pub symbol: String,
    /// Exchange code.
    pub exchange: String,
    /// Asset class evaluated.
    pub asset_class: StudyAssetClass,
    /// Period schedule applied.
    pub period: PeriodScheduleKind,
    /// Effective start date (`YYYY-MM-DD`).
    pub start_date: String,
    /// Effective end date (`YYYY-MM-DD`).
    pub end_date: String,
    /// Evaluated option strategy.
    pub strategy: OptionOverlayStrategy,
    /// Reference underlying spot price.
    pub spot_price: Decimal,
    /// Empirical annualized realized volatility (%).
    pub empirical_rv_pct: Decimal,
    /// Implied volatility proxy (%) from live ATM option quotes, when live chain exists.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quoted_iv_proxy_pct: Option<Decimal>,
    /// Volatility risk premium (`IV - RV`) in % points when live chain exists.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub iv_minus_rv_pct: Option<Decimal>,
    /// True when live option quotes were matched from the runtime quote store.
    pub live_chain_available: bool,
    /// Count of live option contracts inspected.
    pub live_contract_count: usize,
    /// Nearest live option expiry date (`YYYY-MM-DD`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub nearest_expiry: Option<String>,
    /// Configured strategy legs with quoted bid/ask & execution pricing.
    pub legs: Vec<OptionLegSpec>,
    /// Net option credit/debit per cycle (% of spot) before friction.
    pub net_premium_pct: Decimal,
    /// Bid-ask spread drag (% of spot) per cycle.
    pub total_spread_drag_pct: Decimal,
    /// Statutory fee drag (% of spot) per cycle.
    pub statutory_cost_pct: Decimal,
    /// Net option credit/debit (% of spot) after spread & fees.
    pub net_after_friction_pct: Decimal,
    /// Capped max profit per cycle (% of spot), if bounded.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_profit_pct: Option<Decimal>,
    /// Capped max loss per cycle (% of spot, <= 0), if bounded.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_loss_pct: Option<Decimal>,
    /// Empirical probability of profit (%) after friction.
    pub probability_of_profit_pct: Decimal,
    /// Final buy-and-hold cumulative return (%).
    pub buy_hold_cum_pct: Decimal,
    /// Final gross overlay cumulative return (%).
    pub overlay_gross_cum_pct: Decimal,
    /// Final net overlay cumulative return (%).
    pub overlay_net_cum_pct: Decimal,
    /// Maximum net overlay drawdown (%).
    pub overlay_max_drawdown_pct: Decimal,
    /// Maximum buy-and-hold drawdown (%).
    pub buy_hold_max_drawdown_pct: Decimal,
    /// Winning net periods.
    pub win_periods: usize,
    /// Total simulated periods.
    pub total_periods: usize,
    /// Equity comparison series.
    pub equity_curve: Vec<OverlayEquityPoint>,
    /// Strike liquidity & empirical exceedance lab rows.
    pub strike_lab: Vec<StrikeLabRow>,
    /// Honest data-source & quote availability explanation.
    pub availability_note: String,
}

/// Trailing volatility regime classification (empirical terciles).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum VolatilityRegime {
    /// Lower third (`<= P33`).
    Low,
    /// Middle third (`P33..P67`).
    Normal,
    /// Upper third (`>= P67`).
    Elevated,
}

/// Forward quantile step for Tab 11 (`Forecasting`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ForecastHorizonStep {
    /// Forward step index (`1..=horizon_steps`).
    pub step: usize,
    /// Analog sample count.
    pub sample_count: usize,
    /// 10th percentile compounded forward return (%).
    pub p10_pct: Decimal,
    /// 25th percentile compounded forward return (%).
    pub p25_pct: Decimal,
    /// 50th percentile (median) compounded forward return (%).
    pub p50_pct: Decimal,
    /// 75th percentile compounded forward return (%).
    pub p75_pct: Decimal,
    /// 90th percentile compounded forward return (%).
    pub p90_pct: Decimal,
    /// Mean compounded forward return (%).
    pub mean_pct: Decimal,
    /// Share (%) of analog paths finishing positive (`> 0`).
    pub positive_prob_pct: Decimal,
    /// Spot-anchored `P10` target price.
    pub p10_price: Decimal,
    /// Spot-anchored `P50` target price.
    pub p50_price: Decimal,
    /// Spot-anchored `P90` target price.
    pub p90_price: Decimal,
}

/// Historical analog episode matching active Markov + Volatility state.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct AnalogEpisode {
    /// Period end date of match (`YYYY-MM-DD`).
    pub trigger_end_date: String,
    /// Period return at trigger (%).
    pub trigger_return_pct: Decimal,
    /// Markov streak state at match date.
    pub markov_state: MarkovState,
    /// Volatility regime at match date.
    pub vol_regime: VolatilityRegime,
    /// Trailing 5-period realized volatility (%).
    pub trailing_vol_pct: Decimal,
    /// Next-1-period forward return (%).
    pub next_1_pct: Decimal,
    /// Compounded return through horizon end (%).
    pub horizon_end_pct: Decimal,
}

/// Request payload for `POST /studies/experimental-analysis/forecast`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ExperimentalForecastRequest {
    /// Canonical `EXCHANGE:SYMBOL` instrument ID (or `SYMBOL.EXCHANGE`).
    pub instrument_id: String,
    /// Asset class context.
    pub asset_class: StudyAssetClass,
    /// Period schedule.
    pub period: PeriodScheduleKind,
    /// Return formula (`open_to_close`, `high_to_low`, `close_to_close`).
    #[serde(default)]
    pub calc_type: ReturnCalcKind,
    /// Inclusive start date (`YYYY-MM-DD`).
    pub start_date: String,
    /// Inclusive end date (`YYYY-MM-DD`).
    pub end_date: String,
    /// Forward forecast horizon in periods (`1..=12`, default `6`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub horizon_steps: Option<usize>,
    /// Optional F&O spread/basis adjustment mode.
    #[serde(default)]
    pub roll_basis_mode: RollBasisMode,
    /// Optional spread slippage in bps.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spread_slippage_bps: Option<Decimal>,
    /// Optional futures roll basis adjustment in bps.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub futures_roll_basis_bps: Option<Decimal>,
    /// Whether to deduct statutory costs when in net mode.
    #[serde(default)]
    pub include_statutory_costs: bool,
}

/// Response payload for `POST /studies/experimental-analysis/forecast`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ExperimentalForecastResponse {
    /// Analyzed canonical instrument ID.
    pub instrument_id: String,
    /// Symbol.
    pub symbol: String,
    /// Exchange code.
    pub exchange: String,
    /// Asset class evaluated.
    pub asset_class: StudyAssetClass,
    /// Period schedule applied.
    pub period: PeriodScheduleKind,
    /// Effective start date (`YYYY-MM-DD`).
    pub start_date: String,
    /// Effective end date (`YYYY-MM-DD`).
    pub end_date: String,
    /// Reference spot price.
    pub spot_price: Decimal,
    /// Active Markov streak state.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_markov_state: Option<MarkovState>,
    /// Active Markov display label.
    pub current_markov_label: String,
    /// Active trailing volatility regime.
    pub current_vol_regime: VolatilityRegime,
    /// Trailing 5-period realized std dev (%).
    pub current_trailing_vol_pct: Decimal,
    /// Analog conditioning tier (`"exact_state_and_vol"`, `"markov_state_only"`, `"direction_only"`, `"unconditional"`).
    pub match_tier: String,
    /// Count of historical analog periods selected.
    pub analog_count: usize,
    /// Multi-step forward quantile fan (`1..=horizon_steps`).
    pub horizon_fan: Vec<ForecastHorizonStep>,
    /// Top matching historical episodes (up to 15).
    pub top_analogs: Vec<AnalogEpisode>,
}

/// Event study category aligned across Phase 3 Event Modes & Tab 12.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum StudyEventKind {
    /// Share buyback / tender / bonus / corporate action.
    Buyback,
    /// Quarterly earnings or financial result release.
    Earnings,
    /// Sovereign / central bank macro release (RBI / FOMC / CPI / GDP).
    Macro,
    /// F&O inclusion, exclusion, or index rebalance corporate event.
    FnoRebalance,
}

/// Average & Cumulative Abnormal Return sample for offset `k ∈ [-20..=+20]`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct EventWindowPoint {
    /// Trading-day offset relative to event `T+0` (`-20..=+20`).
    pub day_offset: i32,
    /// Events contributing at this offset.
    pub sample_count: usize,
    /// Cross-event average abnormal daily return (%).
    pub avg_abnormal_return_pct: Decimal,
    /// Cumulative average abnormal return (`T-20..=day_offset`, %).
    pub cum_abnormal_return_pct: Decimal,
    /// Cross-event median abnormal return (%) at `day_offset`.
    pub median_return_pct: Decimal,
    /// Share (%) of positive abnormal returns at `day_offset`.
    pub win_rate_pct: Decimal,
}

/// Per-event drift summary row for event study tables.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct EventStudyRow {
    /// Aligned trading date (`YYYY-MM-DD`).
    pub event_date: String,
    /// Instrument symbol.
    pub symbol: String,
    /// Event classification.
    pub kind: StudyEventKind,
    /// Human-readable event title.
    pub title: String,
    /// Optional numeric or category detail.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    /// Pre-event drift (`T-20` to `T-1`, %).
    pub pre_drift_pct: Decimal,
    /// Event-day abnormal return (`T+0`, %).
    pub event_day_pct: Decimal,
    /// Post-event 5-session drift (`T+1` to `T+5`, %).
    pub post_5d_pct: Decimal,
    /// Post-event 20-session drift (`T+1` to `T+20`, %).
    pub post_20d_pct: Decimal,
    /// Event-day volume relative to trailing 20-day pre-event average (`×`).
    pub volume_ratio: Decimal,
}

/// Request payload for `POST /studies/experimental-analysis/events`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ExperimentalEventStudyRequest {
    /// Canonical `EXCHANGE:SYMBOL` instrument ID (or `SYMBOL.EXCHANGE`).
    pub instrument_id: String,
    /// Event study category (`buyback`, `earnings`, `macro`, `fno_rebalance`).
    pub mode: StudyEventKind,
    /// Optional benchmark instrument ID for abnormal return subtraction (e.g. `"NSE:NIFTY50"`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub benchmark_id: Option<String>,
    /// Optional ISO country filter for `macro` mode (`"IN"`, `"US"`, etc.).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub macro_country: Option<String>,
    /// Inclusive start date (`YYYY-MM-DD`).
    pub start_date: String,
    /// Inclusive end date (`YYYY-MM-DD`).
    pub end_date: String,
}

/// Response payload for `POST /studies/experimental-analysis/events`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ExperimentalEventStudyResponse {
    /// Analyzed canonical instrument ID.
    pub instrument_id: String,
    /// Symbol.
    pub symbol: String,
    /// Exchange code.
    pub exchange: String,
    /// Requested event study mode.
    pub mode: StudyEventKind,
    /// `true` when at least one real Lakehouse event aligned with daily bars.
    pub available: bool,
    /// `true` when returns were benchmark-adjusted against `benchmark_id`.
    pub benchmark_adjusted: bool,
    /// Benchmark instrument label if adjusted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub benchmark_symbol: Option<String>,
    /// Total aligned events.
    pub event_count: usize,
    /// Mean pre-event drift (`[-20d, -1d]`, %).
    pub avg_pre_drift_pct: Decimal,
    /// Mean event-day return (`T+0`, %).
    pub avg_event_day_pct: Decimal,
    /// Mean `+5d` drift (`[+1d, +5d]`, %).
    pub avg_post_5d_pct: Decimal,
    /// Mean `+20d` drift (`[+1d, +20d]`, %).
    pub avg_post_20d_pct: Decimal,
    /// Post `T+1` win rate (%).
    pub win_rate_post_1d_pct: Decimal,
    /// Post `T+5` win rate (%).
    pub win_rate_post_5d_pct: Decimal,
    /// Post `T+20` win rate (%).
    pub win_rate_post_20d_pct: Decimal,
    /// Mean event-day volume surge ratio (`×`).
    pub avg_volume_surge_ratio: Decimal,
    /// 41-point `[-20..=+20]` AAR / CAR trajectory.
    pub window_curve: Vec<EventWindowPoint>,
    /// Matched dated events table (up to 100 rows).
    pub events: Vec<EventStudyRow>,
    /// Honest data-source & projection availability message.
    pub availability_note: String,
}

/// Dated historical news headline, macro release, or corporate/filing catalyst item.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct OutlierNewsItem {
    /// Source category (`"news"`, `"macro_calendar"`, `"filing"`, `"corporate_action"`).
    pub kind: String,
    /// Event/article date (`YYYY-MM-DD`).
    pub date: String,
    /// Full timestamp / ISO datetime string.
    pub published_at: String,
    /// Headline or release title.
    pub title: String,
    /// Publisher or official calendar feed name.
    pub source: String,
    /// Optional article or filing URL.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    /// Optional summary or release actual-vs-forecast string.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
    /// Related instruments.
    pub instruments: Vec<String>,
}

/// Request payload for `POST /studies/experimental-analysis/outlier-news`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ExperimentalOutlierNewsRequest {
    /// Canonical `EXCHANGE:SYMBOL` instrument ID (or `SYMBOL.EXCHANGE`).
    pub instrument_id: String,
    /// Asset class hint for default macro/catalyst keywords.
    pub asset_class: StudyAssetClass,
    /// Outlier period start date (`YYYY-MM-DD`).
    pub window_start: String,
    /// Outlier period end date (`YYYY-MM-DD`).
    pub window_end: String,
    /// Optional search keywords (space-separated).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub keywords: Option<String>,
    /// Maximum items to return (`1..=50`, default `20`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
}

/// Response payload for `POST /studies/experimental-analysis/outlier-news`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ExperimentalOutlierNewsResponse {
    /// Canonical instrument ID.
    pub instrument_id: String,
    /// Evaluated asset class.
    pub asset_class: StudyAssetClass,
    /// Expanded window start (`window_start - 2d`, `YYYY-MM-DD`).
    pub expanded_start: String,
    /// Expanded window end (`window_end + 1d`, `YYYY-MM-DD`).
    pub expanded_end: String,
    /// Effective search keywords applied.
    pub keywords: String,
    /// Matched real catalyst items sorted newest-first.
    pub items: Vec<OutlierNewsItem>,
    /// Total matched items returned.
    pub count: usize,
    /// Whether `NewsProjectionReader` was available.
    pub news_archive_available: bool,
    /// Whether `EconomicCalendarProjectionReader` was available.
    pub macro_calendar_available: bool,
    /// Whether `FilingsProjectionReader` was available.
    pub filings_available: bool,
    /// Honest explanation when archives are empty or offline.
    pub availability_note: String,
}
