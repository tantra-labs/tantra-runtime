//! Versioned DTOs for generic Strategy Loop Evaluation, Parameter Sweeps,
//! Expiry-Aware F&O/Commodity Hedges, and the 6-Layer Unified `OverlayScene`.
//!
//! All monetary, return, price, and risk values use exact [`Decimal`] under the
//! Zero Floats Policy. The `OverlaySceneDto` schema matches
//! `tantra_ui_charts::scene::OverlayScene` field-for-field on the wire so Web
//! Canvas, TUI `ratatui::widgets::Canvas`, CLI exports, and Python strategies
//! share one unified visual contract.

use rust_decimal::Decimal;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::experimental_analysis::{RollBasisMode, StudyAssetClass};

fn default_true() -> bool {
    true
}

fn default_net_roll_basis() -> RollBasisMode {
    RollBasisMode::NetAfterSpreadAndCosts
}

// ============================================================================
// 1. Unified 6-Layer Graphical Scene Primitives (Wire-Identical to tantra-ui-charts)
// ============================================================================

/// Six top-level primitive layer groups in an [`OverlaySceneDto`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SceneLayerGroupDto {
    /// Buy/Sell/Sweep markers and entry-to-exit connector rays.
    Signals,
    /// Contextual zone boxes (`S1`/`S2`/`R1`/`R2`, `OR_HIGH`/`OR_LOW`) and level lines.
    Bands,
    /// Tight/Outer/Trailing/Breakeven stops and R-multiple target rays.
    Stops,
    /// Overlay and sub-pane indicator series.
    Indicators,
    /// Clickable/hoverable rationale pins explaining why a trade won/lost.
    Hints,
    /// F&O option strike lines, payoff breakeven, and hedge role overlays.
    Hedges,
}

/// Toggle mask for the six [`SceneLayerGroupDto`] layers.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SceneLayerMaskDto {
    /// Layer 1: Buy/Sell/Sweep signals & trade connectors.
    pub signals: bool,
    /// Layer 2: Contextual zone bands & equilibrium/pivot levels.
    pub bands: bool,
    /// Layer 3: Stops & R-multiple targets.
    pub stops: bool,
    /// Layer 4: Overlay/sub-pane indicator series.
    pub indicators: bool,
    /// Layer 5: Interactive rationale hint annotations.
    pub hints: bool,
    /// Layer 6: Hedge strike levels & breakeven overlays.
    pub hedges: bool,
}

impl Default for SceneLayerMaskDto {
    fn default() -> Self {
        Self {
            signals: true,
            bands: true,
            stops: true,
            indicators: true,
            hints: true,
            hedges: true,
        }
    }
}

/// Side of a signal marker.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum MarkerSideDto {
    /// Buy signal.
    Buy,
    /// Sell signal.
    Sell,
}

/// Signal kind drawn as a marker glyph.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum MarkerKindDto {
    /// Trend breakout.
    Breakout,
    /// Trend reversal.
    Reversal,
    /// Level retest.
    Retest,
    /// Stop-line touch.
    StopLine,
    /// Institutional liquidity sweep probe.
    Sweep,
    /// Tight stop-loss hunted while outer shield stop survived.
    SweepSaved,
    /// Profit target hit (e.g. `2R` / `3R` / `5R`).
    TargetHit,
    /// High-momentum institutional elephant candle.
    ElephantBar,
}

/// Signal marker anchored at a bar index and exact price.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SignalMarkerDto {
    /// Marker price.
    pub price: Decimal,
    /// 0-based candle index anchoring x coordinate.
    pub index: usize,
    /// Buy or sell.
    pub side: MarkerSideDto,
    /// Signal kind.
    pub kind: MarkerKindDto,
}

/// Entry-to-exit connector ray showing realized outcome of a simulated trade.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct TradeConnectorRayDto {
    /// Entry bar index (`<= to_index`).
    pub from_index: usize,
    /// Entry price (`>= 0`).
    pub from_price: Decimal,
    /// Exit bar index (`>= from_index`).
    pub to_index: usize,
    /// Exit price (`>= 0`).
    pub to_price: Decimal,
    /// Direction of trade (`buy` or `sell`).
    pub side: MarkerSideDto,
    /// Realized outcome in `R` multiples.
    pub realized_r: Decimal,
    /// Whether the trade exited via stop-loss.
    pub stopped_out: bool,
    /// Optional short label (e.g. `"+3R Target"` or `"Stopped (-1R)"`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
}

/// Shaded price-band zone spanning `x0..=x1` bar indices.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ZoneBandDto {
    /// Lower band price.
    pub low: Decimal,
    /// Upper band price (`high >= low`).
    pub high: Decimal,
    /// Left bar index.
    pub x0: usize,
    /// Right bar index (`x1 >= x0`).
    pub x1: usize,
    /// Palette fill / score index.
    pub fill_index: u8,
    /// Optional annotation label (`"S1"`, `"S2"`, `"R1"`, `"R2"`, `"OR_HIGH"`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
}

/// Drawing style for a [`LevelLineDto`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct LevelLineStyleDto {
    /// Whether the line is drawn with dashes.
    pub dashed: bool,
    /// Index into the renderer's palette / score color ramp.
    pub color_index: u8,
    /// Stroke width in pixels.
    pub line_width: u8,
}

impl Default for LevelLineStyleDto {
    fn default() -> Self {
        Self {
            dashed: true,
            color_index: 0,
            line_width: 1,
        }
    }
}

/// Horizontal reference level line spanning `x0..=x1`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct LevelLineDto {
    /// Level price (non-negative).
    pub price: Decimal,
    /// Left bar index.
    pub x0: usize,
    /// Right bar index (`x1 >= x0`).
    pub x1: usize,
    /// Optional annotation label.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    /// Line style.
    pub style: LevelLineStyleDto,
}

/// Kind of stop or target line.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum StopKindDto {
    /// Initial outer-buffer stop-loss.
    Initial,
    /// Trailing stop-loss.
    Trailing,
    /// Tight/breakeven reference stop.
    Breakeven,
    /// Profit target ray.
    Target,
}

/// Stop-loss or target line spanning `x0..=x1`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct StopLineDto {
    /// Stop or target price (`> 0`).
    pub price: Decimal,
    /// Left bar index.
    pub x0: usize,
    /// Right bar index (`x1 >= x0`).
    pub x1: usize,
    /// Stop kind.
    pub kind: StopKindDto,
    /// Optional prior price (for tight-vs-outer or trailing step visualization).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous: Option<Decimal>,
}

/// Single x/y point in an indicator series.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ChartPointDto {
    /// X coordinate (0-based bar index).
    pub x: Decimal,
    /// Y value.
    pub y: Decimal,
}

/// Named technical indicator series aligned with chart bars.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ChartSeriesDto {
    /// Series name (e.g. `"EMA(20)"`, `"RSI(14)"`, `"ATR(14)"`).
    pub name: String,
    /// Ordered points across visible bars.
    pub points: Vec<ChartPointDto>,
}

/// Visual severity for a [`HintAnnotationDto`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum HintSeverityDto {
    /// Neutral informational note.
    Info,
    /// Beneficial save / alpha edge (e.g. sweep survived by outer SL).
    Positive,
    /// Caution / tighter-stop vulnerability warning.
    Warning,
    /// Critical risk / split jump / trap alert.
    Critical,
}

/// Key-value metric badge attached to a [`HintAnnotationDto`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct HintMetricDto {
    /// Display label.
    pub key: String,
    /// Formatted value.
    pub value: String,
}

/// Clickable / hoverable rationale pin anchored to a candle and price.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct HintAnnotationDto {
    /// 0-based candle index anchoring x coordinate.
    pub bar_index: usize,
    /// Non-negative anchor price.
    pub price: Decimal,
    /// Visual severity badge.
    pub severity: HintSeverityDto,
    /// Short pin title.
    pub title: String,
    /// Detailed human-readable rationale.
    pub detail: String,
    /// Structured key-value metrics surfaced in inspector popovers.
    #[serde(default)]
    pub metrics: Vec<HintMetricDto>,
}

/// Leg action for an option/futures hedge overlay.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum HedgeLegSideDto {
    /// Long leg.
    Buy,
    /// Short leg.
    Sell,
}

/// Instrument type for a hedge leg strike overlay.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum HedgeOptionKindDto {
    /// Cash / Spot instrument.
    Spot,
    /// Futures contract.
    Future,
    /// Call option (`CE`).
    Call,
    /// Put option (`PE`).
    Put,
}

/// Strike & payoff ray rendered on the chart for synthesized F&O hedges.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct HedgeLevelOverlayDto {
    /// Human-readable role.
    pub role: String,
    /// Buy or Sell leg.
    pub side: HedgeLegSideDto,
    /// Option/Future/Spot kind.
    pub option_kind: HedgeOptionKindDto,
    /// Positive strike or entry reference price.
    pub strike: Decimal,
    /// Left bar index (`<= x1`).
    pub x0: usize,
    /// Right bar index (`>= x0`).
    pub x1: usize,
    /// Optional breakeven price.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub breakeven: Option<Decimal>,
    /// Optional worst-case rupee loss cap.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_loss: Option<Decimal>,
    /// Optional short summary tag rendered near the strike ray.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
}

/// Unified 6-layer interactive overlay scene shared across REST, Web, TUI, and CLI.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct OverlaySceneDto {
    /// Strategy identifier (e.g. `"elephant_edge_v3"`, `"orb_15"`).
    pub strategy_id: String,
    /// Canonical instrument symbol.
    pub symbol: String,
    /// Timeframe label (`"1d"`, `"5m"`, etc.).
    pub timeframe: String,
    /// Visibility toggle mask across the six layer groups.
    #[serde(default)]
    pub layers: SceneLayerMaskDto,
    /// Layer 1a: Buy/Sell/Sweep/Target markers.
    #[serde(default)]
    pub signals: Vec<SignalMarkerDto>,
    /// Layer 1b: Entry-to-exit connector rays.
    #[serde(default)]
    pub connectors: Vec<TradeConnectorRayDto>,
    /// Layer 2a: Contextual zone boxes.
    #[serde(default)]
    pub bands: Vec<ZoneBandDto>,
    /// Layer 2b: Dotted equilibrium/midpoint reference lines.
    #[serde(default)]
    pub levels: Vec<LevelLineDto>,
    /// Layer 3: Initial, Outer-Buffer, Tight-Reference, and Target stop rays.
    #[serde(default)]
    pub stops: Vec<StopLineDto>,
    /// Layer 4: Indicator series (`EMA(20)`, `RSI(14)`, `ATR(14)`).
    #[serde(default)]
    pub indicators: Vec<ChartSeriesDto>,
    /// Layer 5: Clickable/hoverable rationale hint pins.
    #[serde(default)]
    pub hints: Vec<HintAnnotationDto>,
    /// Layer 6: F&O / Commodity hedge strike overlays.
    #[serde(default)]
    pub hedges: Vec<HedgeLevelOverlayDto>,
}

// ============================================================================
// 2. Strategy Loop, Parameter Sweep & Expiry-Hedge Configuration / Result DTOs
// ============================================================================

/// Supported bar timeframe for strategy loop evaluation.
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
pub enum LoopTimeframe {
    /// 1-minute intraday bars.
    #[serde(rename = "1m", alias = "1M", alias = "m1")]
    M1,
    /// 5-minute intraday bars.
    #[serde(rename = "5m", alias = "5M", alias = "m5")]
    M5,
    /// 15-minute intraday bars.
    #[serde(rename = "15m", alias = "15M", alias = "m15")]
    M15,
    /// 1-hour intraday bars.
    #[serde(rename = "1h", alias = "1H", alias = "h1")]
    H1,
    /// Daily OHLCV bars (default).
    #[default]
    #[serde(
        rename = "1d",
        alias = "1D",
        alias = "d1",
        alias = "day",
        alias = "daily"
    )]
    D1,
}

impl LoopTimeframe {
    /// Canonical lowercase wire string (`"1d"`, `"5m"`, etc.).
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::M1 => "1m",
            Self::M5 => "5m",
            Self::M15 => "15m",
            Self::H1 => "1h",
            Self::D1 => "1d",
        }
    }

    /// True if this timeframe represents intraday bars rather than daily OHLCV.
    pub fn is_intraday(&self) -> bool {
        !matches!(self, Self::D1)
    }
}

/// Entry signal detection filter mode.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum LoopEntryTriggerMode {
    /// Automatically pick the strategy's native entry filter (`RejectionWick` for EE+, `OrbBreakout` for ORB-15).
    #[default]
    Auto,
    /// Raw zone-touch entry close (`ZoneTouchEntryFilter`).
    ZoneTouchClose,
    /// Selective wick-to-body reclaim inside or sweeping past the zone (`RejectionWickFilter`).
    RejectionWick,
    /// Opening range breakout (`OrbBreakoutEntryFilter`).
    OrbBreakout,
}

/// Stop-loss and trade exit management mode.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum LoopStopPolicyMode {
    /// Tight stop placed inside zone midpoint.
    TightCenterLine,
    /// Stop placed right at band outer edge (`+0.00 ATR`).
    BandOuterEdge,
    /// Stop placed outside band outer edge by `sl_buffer_atr * ATR` (default `0.25 ATR`).
    #[default]
    OuterAtrBuffer,
    /// Empirical penetration percentile shield (`sl_buffer_atr` clamped `0.10..=1.50 ATR`).
    PercentileShield,
    /// Wide fixed-horizon catastrophe shield so horizon close governs exit.
    FixedHorizon,
    /// Take partial profit at `partial_r`, shift SL to breakeven, trail runner to `runner_r`.
    PartialAndTrail,
    /// Exit after `time_stop_bars` elapsed if neither SL nor target hit.
    TimeStop,
}

/// Target derivative expiry / holding horizon for hedge synthesis.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum LoopExpiryHorizon {
    /// Intraday `0–1 DTE`.
    Day,
    /// Weekly `2–5 DTE`.
    #[default]
    Weekly,
    /// Monthly `5–30 DTE`.
    Monthly,
    /// MCX commodity roll cycle.
    CommodityCycle,
}

/// Multi-asset session grouping preset for `SessionClock`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum LoopSessionPreset {
    /// NSE equity / cash / regular F&O intraday sessions (`09:15–15:30 IST`).
    NseEquityFno,
    /// NSE Sep 3, 2025 expiry cutover-aware intraday sessions.
    #[default]
    NseSep2025Expiry,
    /// MCX commodity extended intraday sessions (`09:00–23:30 IST`).
    McxCommodity,
    /// Continuous FX sessions (`00:00–23:59`).
    FxSession,
    /// Daily bars grouped into calendar-month (`YYYY-MM`) regimes.
    DailyMonthly,
    /// Weekly bars grouped into calendar-quarter (`YYYY-Q#`) regimes.
    WeeklyQuarter,
    /// Monthly bars grouped into calendar-year (`YYYY`) regimes.
    MonthlyYear,
}

/// Recommended F&O option / futures hedge structure family.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum HedgeStructureKindDto {
    /// Naked underlying / spot direction with outer-buffer stop shield.
    Unhedged,
    /// Bullish defined-risk vertical call spread.
    BullCallSpread,
    /// Bearish defined-risk vertical put spread.
    BearPutSpread,
    /// Bullish credit spread anchored below `S1`/`S2` outer SL.
    BullPutCreditSpread,
    /// Bearish credit spread anchored above `R1`/`R2` outer SL.
    BearCallCreditSpread,
    /// Range-bound 4-leg Iron Condor anchored outside `S1`/`R1` buffers.
    ZoneIronCondor,
    /// Long/short future/spot paired with outer wing protection & OTM cap.
    ProtectiveCollar,
    /// Near/next expiry calendar roll.
    CalendarRoll,
}

/// Directional bias of a signal or hedge lookup.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SignalDirectionDto {
    /// Bullish / long bias.
    Long,
    /// Bearish / short bias.
    Short,
}

/// Aggregated quantitative performance KPIs for a strategy evaluation.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct StrategyLoopKpisDto {
    /// Total evaluated trades.
    pub trades: usize,
    /// Count of trades stopped out.
    pub stopped_out_count: usize,
    /// Percentage of trades stopped out.
    pub stop_out_rate_pct: Decimal,
    /// Count of stop-outs where deeper `+0.50 ATR` buffer survived & won.
    pub false_sweep_stop_count: usize,
    /// False sweep trap rate (%).
    pub false_sweep_stop_rate_pct: Decimal,
    /// Winning trade count (`gross_r > 0`).
    pub win_count: usize,
    /// Win rate (%).
    pub win_rate_pct: Decimal,
    /// Rate (%) hitting at least `2R` MFE before exit.
    pub target_2r_hit_rate_pct: Decimal,
    /// Average gross realized R per trade.
    pub avg_realized_r: Decimal,
    /// Average net realized R per trade after spread + statutory costs.
    pub net_avg_realized_r: Decimal,
    /// Gross profit factor.
    pub profit_factor: Decimal,
    /// Net profit factor after friction.
    pub net_profit_factor: Decimal,
    /// Per-trade Sharpe ratio computed in R-space.
    pub sharpe_ratio: Decimal,
    /// Maximum peak-to-trough equity drawdown measured in R units.
    pub max_drawdown_r: Decimal,
}

/// Empirical excursion percentiles beyond zone outer edges (expressed in ATR units).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct PenetrationDistributionDto {
    /// Number of setups measured.
    pub sample_count: usize,
    /// 50th percentile penetration beyond outer edge (ATR).
    pub p50_atr: Decimal,
    /// 75th percentile penetration beyond outer edge (ATR).
    pub p75_atr: Decimal,
    /// 90th percentile penetration beyond outer edge (ATR).
    pub p90_atr: Decimal,
    /// 95th percentile penetration beyond outer edge (ATR).
    pub p95_atr: Decimal,
    /// Maximum observed penetration beyond outer edge (ATR).
    pub max_atr: Decimal,
    /// Percentage of setups breaching outer edge that still closed profitable by horizon.
    pub pct_recovering_after_outer_touch: Decimal,
}

/// One stop-loss policy row inside a 4-model comparison benchmark.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct StopComparisonRowDto {
    /// Canonical identifier (`"tight_center"`, `"outer_edge_0"`, `"outer_025_atr"`, `"outer_050_atr"`).
    pub policy_id: String,
    /// Human-readable label.
    pub label: String,
    /// Explanation of stop placement rule.
    pub description: String,
    /// ATR buffer relative to band outer edge.
    pub buffer_atr: Decimal,
    /// Complete KPIs under this stop policy.
    pub kpis: StrategyLoopKpisDto,
}

/// Side-by-side 4-policy stop-hunt comparison summary.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct StopComparisonSummaryDto {
    /// Total qualifying setups detected.
    pub total_setups: usize,
    /// Count of bullish setups.
    pub bullish_setups: usize,
    /// Count of bearish setups.
    pub bearish_setups: usize,
    /// Four compared stop models (`tight_center`, `outer_edge_0`, `outer_025_atr`, `outer_050_atr`).
    pub models: Vec<StopComparisonRowDto>,
    /// Policy ID achieving highest net expectancy.
    pub recommended_policy_id: String,
    /// Concise verdict summary highlighting trap reduction.
    pub verdict_summary: String,
}

/// Individual simulated trade record returned alongside KPIs and scene rays.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct StrategyTradeSummaryDto {
    /// Entry bar index relative to returned `bars` when in visible window (or full history index).
    pub entry_bar_index: usize,
    /// Exit bar index.
    pub exit_bar_index: usize,
    /// Entry date/time label (`YYYY-MM-DD` or `YYYY-MM-DD HH:MM`).
    pub entry_time: String,
    /// Exit date/time label.
    pub exit_time: String,
    /// Directional bias.
    pub direction: SignalDirectionDto,
    /// Entry price.
    pub entry_price: Decimal,
    /// Stop-loss price under selected policy.
    pub stop_price: Decimal,
    /// Primary profit target price.
    pub target_price: Decimal,
    /// Realized exit price.
    pub exit_price: Decimal,
    /// Risk unit (`1R`).
    pub one_r: Decimal,
    /// True if stopped out before target/horizon.
    pub stopped_out: bool,
    /// True if stopped out while a deep shield (`+0.50 ATR`) survived and won.
    pub false_sweep_trap: bool,
    /// True if price reached `2R` before exiting.
    pub hit_2r: bool,
    /// True if primary target was hit.
    pub hit_target: bool,
    /// Gross realized R multiple.
    pub gross_r: Decimal,
    /// Net realized R multiple after friction.
    pub net_r: Decimal,
    /// Associated zone band label.
    pub band_label: String,
    /// Human-readable setup hint.
    pub hint: String,
}

/// Chart candlestick bar aligned 1:1 with `OverlaySceneDto` indices `0..bars.len()`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct LoopBarDto {
    /// 0-based index within the returned `bars` array.
    pub index: usize,
    /// IST timestamp/date label (`YYYY-MM-DD` for `1d`, `YYYY-MM-DD HH:MM` for intraday).
    pub timestamp: String,
    /// Open price.
    pub open: Decimal,
    /// High price.
    pub high: Decimal,
    /// Low price.
    pub low: Decimal,
    /// Close price.
    pub close: Decimal,
    /// Volume.
    pub volume: Decimal,
}

/// Single leg inside a synthesized option/futures hedge recommendation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct HedgeLegDto {
    /// `"BUY"` or `"SELL"`.
    pub side: String,
    /// `"FUT"`, `"CE"`, or `"PE"`.
    pub option_type: String,
    /// Strike price (or entry spot for futures leg).
    pub strike: Decimal,
    /// Expiry date (`YYYY-MM-DD`) when sourced from live chain.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expiry: Option<String>,
    /// Lot-rounded contract quantity.
    pub lot_qty: Decimal,
    /// Quoted or theoretical ATR-implied leg price per unit.
    pub leg_price: Decimal,
    /// Quote sourcing transparency (`"live_bid_ask"`, `"live_ltp"`, `"theoretical_atr"`, `"spot_anchor"`).
    pub quote_source: String,
}

/// Complete synthesized hedge recommendation for one target horizon.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct HedgeRecommendationDto {
    /// Selected hedge structure family.
    pub structure: HedgeStructureKindDto,
    /// Target expiry horizon.
    pub horizon: LoopExpiryHorizon,
    /// `"live_quotes"` if live option quotes matched, else `"theoretical_model"`.
    pub chain_status: String,
    /// Number of live option chain contracts inspected for this symbol.
    pub live_contract_count: usize,
    /// Individual legs of the hedge structure.
    pub legs: Vec<HedgeLegDto>,
    /// Estimated net credit (`> 0`) or debit (`< 0`) across 1 lot.
    pub net_credit_or_debit: Decimal,
    /// Worst-case bounded loss (₹ across lot qty) including friction.
    pub max_loss_inr: Decimal,
    /// Max loss as percentage of allocated capital.
    pub max_loss_pct_equity: Decimal,
    /// `P95` adverse gap stress loss estimate (₹).
    pub p95_stress_loss_inr: Decimal,
    /// Estimated round-trip spread + statutory cost (₹).
    pub estimated_friction_inr: Decimal,
    /// Breakeven price levels if bounded.
    pub breakevens: Vec<Decimal>,
    /// Clear rationale explaining why this structure matches horizon & zone geometry.
    pub rationale: String,
}

/// Concrete primitive bindings active inside a strategy loop run.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct LoopPipelineSpecDto {
    /// Strategy identifier requested.
    pub strategy_id: String,
    /// Active zone extractor primitive ID.
    pub zone_extractor_id: String,
    /// Active entry signal primitive ID.
    pub entry_filter_id: String,
    /// Active stop/exit policy ID.
    pub stop_policy_id: String,
    /// Active hedge synthesizer primitive ID.
    pub hedge_policy_id: String,
}

/// Lightweight summary of a prior stateful loop run stored in `LoopSessionStore`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct LoopSessionSnapshotSummaryDto {
    /// Session identifier.
    pub session_id: String,
    /// Monotonically increasing run sequence number within this session.
    pub run_seq: u64,
    /// Evaluated strategy identifier.
    pub strategy_id: String,
    /// Canonical symbol.
    pub symbol: String,
    /// Bar timeframe.
    pub timeframe: LoopTimeframe,
    /// Active SL ATR buffer.
    pub sl_buffer_atr: Decimal,
    /// Active holding horizon in bars.
    pub horizon_bars: usize,
    /// Active target R multiple.
    pub target_r: Decimal,
    /// Active stop policy mode.
    pub stop_policy_mode: LoopStopPolicyMode,
    /// Active expiry horizon.
    pub expiry_horizon: LoopExpiryHorizon,
    /// Net avg realized R achieved.
    pub net_avg_realized_r: Decimal,
    /// Win rate (%).
    pub win_rate_pct: Decimal,
    /// Stop-out rate (%).
    pub stop_out_rate_pct: Decimal,
    /// Sharpe ratio.
    pub sharpe_ratio: Decimal,
    /// Net profit factor.
    pub profit_factor: Decimal,
}

/// Exact KPI delta between two evaluations (e.g. winner vs baseline, or current vs previous run).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SweepKpiDeltaDto {
    /// Change in net average realized R per trade (`after - before`).
    pub net_avg_realized_r_delta: Decimal,
    /// Change in win rate percentage points (`after - before`).
    pub win_rate_pct_delta: Decimal,
    /// Change in stop-out rate percentage points (`after - before`; negative = improvement).
    pub stop_out_rate_pct_delta: Decimal,
    /// Change in false-sweep trap rate percentage points (`after - before`; negative = improvement).
    pub false_sweep_stop_rate_pct_delta: Decimal,
    /// Change in Sharpe ratio (`after - before`).
    pub sharpe_ratio_delta: Decimal,
    /// Change in net profit factor (`after - before`).
    pub profit_factor_delta: Decimal,
}

// ============================================================================
// 3. Top-Level Endpoint Requests & Responses
// ============================================================================

/// Request for `POST /api/v2/studies/strategy-loop/evaluate`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct StrategyLoopRequest {
    /// Registered strategy identifier (`"elephant_edge_v3"`, `"elephant_edge"`, `"orb_15"`, `"orb15"`).
    pub strategy_id: String,
    /// Canonical `EXCHANGE:SYMBOL` or `SYMBOL.EXCHANGE` instrument ID.
    pub instrument_id: String,
    /// Asset class context.
    pub asset_class: StudyAssetClass,
    /// Inclusive study start date (`YYYY-MM-DD`).
    pub start_date: String,
    /// Inclusive study end date (`YYYY-MM-DD`).
    pub end_date: String,
    /// Bar timeframe (`"1d"`, `"5m"`, `"15m"`, `"1m"`, `"1h"`). Defaults to `"1d"`.
    #[serde(default)]
    pub timeframe: LoopTimeframe,
    /// Optional session slicing preset override.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_preset: Option<LoopSessionPreset>,
    /// Entry trigger mode override.
    #[serde(default)]
    pub entry_trigger_mode: LoopEntryTriggerMode,
    /// Stop/exit management policy mode.
    #[serde(default)]
    pub stop_policy_mode: LoopStopPolicyMode,
    /// Outer stop-loss ATR buffer multiplier (`0.00..=3.00`, default `0.25`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sl_buffer_atr: Option<Decimal>,
    /// Holding horizon in bars after signal entry (default `5` daily / `12` intraday).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub horizon_bars: Option<usize>,
    /// Primary reward target R multiple (`>= 1`, default `3`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_r: Option<Decimal>,
    /// ATR lookback period (`2..=100`, default `14`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub atr_period: Option<usize>,
    /// Partial target R for `PartialAndTrail` mode (default `2`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partial_r: Option<Decimal>,
    /// Runner target R for `PartialAndTrail` mode (default `5`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runner_r: Option<Decimal>,
    /// Max holding bars for `TimeStop` mode.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub time_stop_bars: Option<usize>,
    /// Target expiry horizon for derivative hedge synthesis.
    #[serde(default)]
    pub expiry_horizon: LoopExpiryHorizon,
    /// Spread/roll/statutory cost mode.
    #[serde(default = "default_net_roll_basis")]
    pub roll_basis_mode: RollBasisMode,
    /// Optional bid-ask slippage in basis points (`0..=500`, default `2`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spread_slippage_bps: Option<Decimal>,
    /// Optional futures rollover basis in basis points (`-500..=500`, default `0`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub futures_roll_basis_bps: Option<Decimal>,
    /// Deduct Indian statutory costs (STT, stamp, SEBI, GST). Defaults to `true`.
    #[serde(default = "default_true")]
    pub include_statutory_costs: bool,
    /// Optional lot size override (defaults to instrument master lot size or `50`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lot_size: Option<Decimal>,
    /// Optional tick size override (defaults to instrument master tick size or `0.05`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tick_size: Option<Decimal>,
    /// Optional allocated strategy capital in INR for equity risk % sizing (default `500000`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capital: Option<Decimal>,
    /// Optional interactive session ID to track iterative refinement runs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    /// Optional cap on returned chart bars (default `300`, clamped `20..=1500`); KPIs still use full date range.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_chart_bars: Option<usize>,
}

/// Response for `POST /api/v2/studies/strategy-loop/evaluate`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct StrategyLoopResponse {
    /// Stateful refinement session ID.
    pub session_id: String,
    /// Monotonically increasing run sequence number within `session_id`.
    pub run_seq: u64,
    /// Canonical `EXCHANGE:SYMBOL` instrument ID.
    pub instrument_id: String,
    /// Underlying ticker symbol.
    pub symbol: String,
    /// Exchange code.
    pub exchange: String,
    /// Asset class.
    pub asset_class: StudyAssetClass,
    /// Evaluated bar timeframe.
    pub timeframe: LoopTimeframe,
    /// Actual first bar date (`YYYY-MM-DD`).
    pub start_date: String,
    /// Actual last bar date (`YYYY-MM-DD`).
    pub end_date: String,
    /// Active composable pipeline primitives.
    pub pipeline: LoopPipelineSpecDto,
    /// Total clean bars evaluated in study.
    pub total_bars: usize,
    /// Number of corporate splits detected & backward-adjusted.
    pub splits_adjusted: usize,
    /// Latest bar / live spot reference price.
    pub spot_price: Decimal,
    /// Latest ATR value.
    pub atr: Decimal,
    /// Primary strategy KPIs under selected stop policy.
    pub kpis: StrategyLoopKpisDto,
    /// Empirical penetration distribution past band outer edges.
    pub penetration: PenetrationDistributionDto,
    /// 4-policy stop-hunt benchmark comparison.
    pub stop_comparison: StopComparisonSummaryDto,
    /// Simulated trade ledger with hints.
    pub trades: Vec<StrategyTradeSummaryDto>,
    /// Expiry-aware F&O / Commodity hedge recommendation.
    pub hedge: HedgeRecommendationDto,
    /// Unified 6-layer graphical scene synchronized with `bars`.
    pub scene: OverlaySceneDto,
    /// Chart bars aligned 1:1 with `scene` indices `0..bars.len()`.
    pub bars: Vec<LoopBarDto>,
    /// Snapshot of the immediately preceding run in this `session_id`, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous_run: Option<LoopSessionSnapshotSummaryDto>,
    /// KPI deltas vs `previous_run` when iterative tweaking occurs within the same session.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delta_vs_previous: Option<SweepKpiDeltaDto>,
}

/// Bounded parameter grid bounds for `POST /api/v2/studies/strategy-loop/sweep`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct StrategyParameterBoundsDto {
    /// Candidate ATR stop buffer values (e.g. `[0.00, 0.25, 0.50]`).
    #[serde(default)]
    pub sl_buffer_atr_grid: Vec<Decimal>,
    /// Candidate holding horizon bar counts (e.g. `[3, 5, 10]`).
    #[serde(default)]
    pub horizon_bars_grid: Vec<usize>,
    /// Candidate reward target R multiples (e.g. `[2, 3, 5]`).
    #[serde(default)]
    pub target_r_grid: Vec<Decimal>,
    /// Hard upper cap on evaluated combinations (clamped `1..=64`, default `27`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_candidates: Option<usize>,
}

/// Request for `POST /api/v2/studies/strategy-loop/sweep`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct StrategySweepRequest {
    /// Base evaluation request acting as baseline and parameter template.
    pub base: StrategyLoopRequest,
    /// Bounded parameter grid bounds.
    pub bounds: StrategyParameterBoundsDto,
}

/// One candidate parameter configuration scored on IS (70%), OOS (30%), Full, WFE & DSR.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SweepCandidateResultDto {
    /// Zero-based candidate ordinal.
    pub candidate_index: usize,
    /// Tested ATR buffer.
    pub sl_buffer_atr: Decimal,
    /// Tested holding horizon bars.
    pub horizon_bars: usize,
    /// Tested target R.
    pub target_r: Decimal,
    /// Full-window KPIs.
    pub full_kpis: StrategyLoopKpisDto,
    /// In-sample (70%) KPIs.
    pub is_kpis: StrategyLoopKpisDto,
    /// Out-of-sample (30%) KPIs.
    pub oos_kpis: StrategyLoopKpisDto,
    /// Walk-Forward Efficiency `(OOS avg R / IS avg R) * 100`.
    pub walk_forward_efficiency_pct: Decimal,
    /// Deflated Sharpe Ratio proxy adjusting for multiple testing.
    pub deflated_sharpe_ratio: Decimal,
    /// Recommended hedge for this candidate configuration.
    pub hedge: HedgeRecommendationDto,
}

/// Response for `POST /api/v2/studies/strategy-loop/sweep`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct StrategySweepResponse {
    /// Stateful refinement session ID.
    pub session_id: String,
    /// Monotonically increasing run sequence number within `session_id`.
    pub run_seq: u64,
    /// Canonical `EXCHANGE:SYMBOL` instrument ID.
    pub instrument_id: String,
    /// Underlying ticker symbol.
    pub symbol: String,
    /// Evaluated strategy identifier.
    pub strategy_id: String,
    /// Evaluated bar timeframe.
    pub timeframe: LoopTimeframe,
    /// Number of parameter combinations evaluated (`<= 64`).
    pub trial_count: usize,
    /// Ranked candidates (best OOS expectancy first).
    pub candidates: Vec<SweepCandidateResultDto>,
    /// Top-ranked winning candidate configuration.
    pub winner: SweepCandidateResultDto,
    /// Baseline KPIs from `base` configuration before tuning.
    pub baseline_kpis: StrategyLoopKpisDto,
    /// Exact KPI delta of `winner.full_kpis` vs `baseline_kpis`.
    pub delta_winner_vs_baseline: SweepKpiDeltaDto,
    /// Baseline 6-layer `OverlaySceneDto` for visual before/after diff.
    pub baseline_scene: OverlaySceneDto,
    /// Winner 6-layer `OverlaySceneDto` for visual before/after diff.
    pub winner_scene: OverlaySceneDto,
    /// Chart bars aligned 1:1 with `baseline_scene` & `winner_scene`.
    pub bars: Vec<LoopBarDto>,
}

/// Request for `POST /api/v2/studies/strategy-loop/recommend-hedge`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ExpiryHedgeRequest {
    /// Canonical `EXCHANGE:SYMBOL` or `SYMBOL.EXCHANGE` instrument ID.
    pub instrument_id: String,
    /// Asset class context.
    pub asset_class: StudyAssetClass,
    /// Optional strategy ID to anchor S1/S2/R1/R2 zones & penetration stats (defaults to `"elephant_edge_v3"`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub strategy_id: Option<String>,
    /// Optional historical lookback start date (`YYYY-MM-DD`, defaults to 180 days before `end_date`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_date: Option<String>,
    /// Optional historical lookback end date (`YYYY-MM-DD`, defaults to today UTC).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end_date: Option<String>,
    /// Bar timeframe (defaults to `"1d"`).
    #[serde(default)]
    pub timeframe: LoopTimeframe,
    /// Optional explicit directional bias (`None` uses latest strategy signal or neutral `ZoneIronCondor`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub direction: Option<SignalDirectionDto>,
    /// Primary target expiry horizon.
    #[serde(default)]
    pub expiry_horizon: LoopExpiryHorizon,
    /// Optional outer stop-loss buffer multiplier in ATR (default `0.25`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sl_buffer_atr: Option<Decimal>,
    /// Optional evaluation holding horizon in bars for penetration percentiles (default `5`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub horizon_bars: Option<usize>,
    /// Optional ATR lookback period (default `14`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub atr_period: Option<usize>,
    /// Optional lot size override.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lot_size: Option<Decimal>,
    /// Optional tick size override.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tick_size: Option<Decimal>,
    /// Optional allocated capital in INR (default `500000`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capital: Option<Decimal>,
    /// Optional spread slippage in bps (default `2`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spread_slippage_bps: Option<Decimal>,
    /// Include Indian statutory costs (STT, GST, SEBI). Defaults to `true`.
    #[serde(default = "default_true")]
    pub include_statutory_costs: bool,
    /// Set true when implied volatility exceeds realized volatility (`IV > RV`).
    #[serde(default)]
    pub iv_gt_rv: bool,
}

/// Response for `POST /api/v2/studies/strategy-loop/recommend-hedge`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ExpiryHedgeResponse {
    /// Canonical `EXCHANGE:SYMBOL` instrument ID.
    pub instrument_id: String,
    /// Underlying ticker symbol.
    pub symbol: String,
    /// Exchange code.
    pub exchange: String,
    /// Active spot/close reference price.
    pub spot_price: Decimal,
    /// Active ATR value.
    pub atr: Decimal,
    /// Resolved directional bias (explicit or latest strategy signal; `None` = neutral/range).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub direction: Option<SignalDirectionDto>,
    /// True if live `ChainReader` quotes matched strikes for this underlying.
    pub live_chain_available: bool,
    /// Count of live option contracts discovered in `ChainReader`.
    pub live_contract_count: usize,
    /// Primary recommendation matching `request.expiry_horizon`.
    pub recommended: HedgeRecommendationDto,
    /// Matrix across all 4 derivative horizons (`day`, `weekly`, `monthly`, `commodity_cycle`).
    pub all_horizons: Vec<HedgeRecommendationDto>,
    /// Chart Layer 6 strike/breakeven overlays corresponding to `recommended`.
    pub hedges: Vec<HedgeLevelOverlayDto>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn overlay_scene_dto_serializes_expected_wire_keys() {
        let mut scene = OverlaySceneDto {
            strategy_id: "elephant_edge_v3".to_string(),
            symbol: "NSE:NIFTYBEES".to_string(),
            timeframe: "1d".to_string(),
            layers: SceneLayerMaskDto::default(),
            signals: vec![SignalMarkerDto {
                price: Decimal::new(26550, 2),
                index: 12,
                side: MarkerSideDto::Buy,
                kind: MarkerKindDto::SweepSaved,
            }],
            connectors: vec![TradeConnectorRayDto {
                from_index: 12,
                from_price: Decimal::new(26550, 2),
                to_index: 17,
                to_price: Decimal::new(27100, 2),
                side: MarkerSideDto::Buy,
                realized_r: Decimal::new(24, 1),
                stopped_out: false,
                label: Some("+2.4R".to_string()),
            }],
            bands: vec![ZoneBandDto {
                low: Decimal::new(26200, 2),
                high: Decimal::new(26450, 2),
                x0: 0,
                x1: 20,
                fill_index: 0,
                label: Some("S1".to_string()),
            }],
            levels: vec![LevelLineDto {
                price: Decimal::new(26600, 2),
                x0: 0,
                x1: 20,
                label: Some("Equilibrium".to_string()),
                style: LevelLineStyleDto::default(),
            }],
            stops: vec![StopLineDto {
                price: Decimal::new(26050, 2),
                x0: 12,
                x1: 17,
                kind: StopKindDto::Initial,
                previous: Some(Decimal::new(26325, 2)),
            }],
            indicators: vec![ChartSeriesDto {
                name: "EMA(20)".to_string(),
                points: vec![ChartPointDto {
                    x: Decimal::from(12),
                    y: Decimal::new(26500, 2),
                }],
            }],
            hints: vec![HintAnnotationDto {
                bar_index: 12,
                price: Decimal::new(26550, 2),
                severity: HintSeverityDto::Positive,
                title: "Sweep Saved".to_string(),
                detail: "Outer buffer survived".to_string(),
                metrics: vec![HintMetricDto {
                    key: "Net R".to_string(),
                    value: "+2.40R".to_string(),
                }],
            }],
            hedges: vec![HedgeLevelOverlayDto {
                role: "Short OTM PE".to_string(),
                side: HedgeLegSideDto::Sell,
                option_kind: HedgeOptionKindDto::Put,
                strike: Decimal::from(260),
                x0: 0,
                x1: 20,
                breakeven: Some(Decimal::new(25850, 2)),
                max_loss: Some(Decimal::from(1200)),
                summary: Some("Bull Put Spread".to_string()),
            }],
        };
        scene.layers.hedges = true;
        let json = serde_json::to_value(&scene).expect("scene serializes");
        assert_eq!(json["signals"][0]["kind"], "sweep_saved");
        assert_eq!(json["stops"][0]["kind"], "initial");
        assert_eq!(json["stops"][0]["previous"], "263.25");
        assert_eq!(json["hedges"][0]["option_kind"], "put");

        let decoded: OverlaySceneDto = serde_json::from_value(json).expect("round-trip");
        assert_eq!(decoded, scene);
    }
}
