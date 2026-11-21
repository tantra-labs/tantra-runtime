//! Stateful Strategy Loop Evaluation, Bounded Parameter Sweeps, Expiry-Aware
//! F&O/Commodity Hedges, and Real `RuntimeStrategyRunner` Composition Root.
//!
//! Endpoints (served under `/api/v2/studies/strategy-loop/*` and `/api/v1/studies/strategy-loop/*`):
//! - `POST /studies/strategy-loop/evaluate`
//! - `POST /studies/strategy-loop/sweep`
//! - `POST /studies/strategy-loop/recommend-hedge`

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::{Arc, OnceLock, Weak};

use async_trait::async_trait;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::Response;
use chrono::{DateTime, Duration, NaiveDate, Utc};
use serde_json::{Value, json};
use tantra_agent_chat_tools::ports::strategy::StrategyRunner;
use tantra_agent_core::strategy_loop::{
    StrategyHedgeRequest, StrategyLoopEvalRequest, StrategyLoopPort, StrategyLoopPortError,
    StrategyLoopSweepRequest,
};
use tantra_core::rust_decimal::Decimal;
use tantra_core::rust_decimal::prelude::ToPrimitive;
use tantra_core::{Exchange, InstrumentId};
use tantra_market_finance::studies::pipeline::{
    ElephantZonesExtractor, EntrySignal, EntrySignalPrimitive, ExpiryHorizon as DomainHorizon,
    HedgeOverlayPolicy, HedgeRecommendation as DomainHedge, HedgeStructureKind as DomainStructure,
    HedgeSynthesisInput, HedgeSynthesizer, IndicatorPrimitive, IndicatorSpec, LoopEvalReport,
    LoopEvalRequest, Orb15Extractor, OrbBreakoutEntryFilter, OuterBufferStopPolicy,
    ParameterSweepRequest, ParameterSweepRunner, PenetrationDistribution, RejectionWickFilter,
    SessionPreset as DomainSessionPreset, SignalDirection, SimulatedTrade, SplitArtifactGuard,
    StandardIndicatorEvaluator, StopComparisonRow, StopExitPolicy, StopPolicyMode, StrategyKpis,
    StrategyLoopKernel, StrategyParameterBounds, StudyBar, SweepCandidateResult,
    ZoneExtractionOutput, ZoneExtractor, ZoneTouchEntryFilter,
};
use tantra_market_finance::studies::{
    LiveOptionStrikeQuote, SpreadCostConfig, StudyAssetClass as DomainAssetClass, StudyError,
    estimate_statutory_roundtrip_bps,
};
use tantra_market_store::{ChainReader as _, InstrumentChainReader};
use tantra_runtime_api::{
    ChartPointDto, ChartSeriesDto, ErrorCode, ErrorDetail, ExpiryHedgeRequest, ExpiryHedgeResponse,
    HedgeLegDto, HedgeLegSideDto, HedgeLevelOverlayDto, HedgeOptionKindDto, HedgeRecommendationDto,
    HedgeStructureKindDto, HintAnnotationDto, HintMetricDto, HintSeverityDto, LevelLineDto,
    LevelLineStyleDto, LoopBarDto, LoopEntryTriggerMode, LoopExpiryHorizon, LoopPipelineSpecDto,
    LoopSessionPreset, LoopSessionSnapshotSummaryDto, LoopStopPolicyMode, LoopTimeframe,
    MarkerKindDto, MarkerSideDto, OverlaySceneDto, PenetrationDistributionDto, SceneLayerMaskDto,
    SignalDirectionDto, SignalMarkerDto, StopComparisonRowDto, StopComparisonSummaryDto,
    StopKindDto, StopLineDto, StrategyLoopKpisDto, StrategyLoopRequest, StrategyLoopResponse,
    StrategySweepRequest, StrategySweepResponse, StrategyTradeSummaryDto, StudyAssetClass,
    SweepCandidateResultDto, SweepKpiDeltaDto, TradeConnectorRayDto, ZoneBandDto,
};
use tantra_runtime_backtest::NseTradingCalendar;

use crate::experimental_analysis::{
    RuntimeNseCalendar, invalid_field, load_daily_study_bars, load_intraday_study_bars,
    parse_date_range, parse_study_instrument_id, read_live_spot, resolve_study_instrument,
    to_domain_asset_class, to_domain_roll_mode,
};
use crate::market::{failure, success, unprocessable};
use crate::market_feed::ports::{BarReader, BarRequest, InstrumentMaster, QuoteReader};
use crate::market_feed::wire::{
    BarAggregation, BarSpecification, Instrument, PriceType, UnixNanos,
};
use crate::{ApiJson, AppState};

// ============================================================================
// 1. Strategy Pipeline Factory & Parameter Validation
// ============================================================================

struct StrategyPipeline {
    canonical_strategy_id: &'static str,
    zone_extractor: Box<dyn ZoneExtractor>,
    entry_filter: Box<dyn EntrySignalPrimitive>,
    default_session_preset: DomainSessionPreset,
}

fn build_strategy_pipeline(
    strategy_id: &str,
    entry_mode: LoopEntryTriggerMode,
    sl_buffer_atr: Decimal,
    timeframe: LoopTimeframe,
    asset_class: StudyAssetClass,
    session_override: Option<LoopSessionPreset>,
) -> Result<StrategyPipeline, ErrorDetail> {
    let norm = strategy_id.trim().to_ascii_lowercase();
    let default_preset = match session_override {
        Some(p) => to_domain_session_preset(p),
        None => {
            if !timeframe.is_intraday() {
                DomainSessionPreset::DailyMonthly
            } else {
                match asset_class {
                    StudyAssetClass::Commodity => DomainSessionPreset::McxCommodity,
                    StudyAssetClass::Forex => DomainSessionPreset::FxSession,
                    _ => DomainSessionPreset::NseSep2025Expiry,
                }
            }
        }
    };

    match norm.as_str() {
        "elephant_edge_v3" | "elephant_edge" | "ee" | "ee_plus" | "ee+" => {
            let entry_filter: Box<dyn EntrySignalPrimitive> = match entry_mode {
                LoopEntryTriggerMode::ZoneTouchClose => Box::new(ZoneTouchEntryFilter),
                LoopEntryTriggerMode::OrbBreakout => Box::new(OrbBreakoutEntryFilter),
                LoopEntryTriggerMode::Auto | LoopEntryTriggerMode::RejectionWick => {
                    Box::new(RejectionWickFilter::default())
                }
            };
            Ok(StrategyPipeline {
                canonical_strategy_id: "elephant_edge_v3",
                zone_extractor: Box::new(
                    ElephantZonesExtractor::default().with_sl_buffer_atr(sl_buffer_atr),
                ),
                entry_filter,
                default_session_preset: default_preset,
            })
        }
        "orb_15" | "orb15" | "opening_range_breakout" => {
            let entry_filter: Box<dyn EntrySignalPrimitive> = match entry_mode {
                LoopEntryTriggerMode::ZoneTouchClose => Box::new(ZoneTouchEntryFilter),
                LoopEntryTriggerMode::RejectionWick => Box::new(RejectionWickFilter::default()),
                LoopEntryTriggerMode::Auto | LoopEntryTriggerMode::OrbBreakout => {
                    Box::new(OrbBreakoutEntryFilter)
                }
            };
            Ok(StrategyPipeline {
                canonical_strategy_id: "orb_15",
                zone_extractor: Box::new(Orb15Extractor {
                    opening_range_minutes: 15,
                    sl_buffer_atr,
                }),
                entry_filter,
                default_session_preset: default_preset,
            })
        }
        _ => Err(invalid_field(
            "strategy_id",
            "unknown_strategy_id",
            format!(
                "unsupported strategy_id {strategy_id:?}; expected \"elephant_edge_v3\" or \"orb_15\""
            ),
        )),
    }
}

struct ValidatedLoopParams {
    sl_buffer_atr: Decimal,
    horizon_bars: usize,
    target_r: Decimal,
    atr_period: usize,
    stop_mode: StopPolicyMode,
    spread_cost: SpreadCostConfig,
    lot_size_override: Option<Decimal>,
    tick_size_override: Option<Decimal>,
    capital: Decimal,
    max_chart_bars: usize,
}

fn validate_loop_params(req: &StrategyLoopRequest) -> Result<ValidatedLoopParams, ErrorDetail> {
    let sl_buffer_atr = req.sl_buffer_atr.unwrap_or_else(|| Decimal::new(25, 2));
    if sl_buffer_atr < Decimal::ZERO || sl_buffer_atr > Decimal::from(3) {
        return Err(invalid_field(
            "sl_buffer_atr",
            "out_of_range",
            "sl_buffer_atr must be between 0.00 and 3.00 ATR",
        ));
    }

    let default_horizon = if req.timeframe.is_intraday() { 12 } else { 5 };
    let horizon_bars = req.horizon_bars.unwrap_or(default_horizon);
    if !(1..=120).contains(&horizon_bars) {
        return Err(invalid_field(
            "horizon_bars",
            "out_of_range",
            "horizon_bars must be between 1 and 120",
        ));
    }

    let target_r = req.target_r.unwrap_or_else(|| Decimal::from(3));
    if target_r < Decimal::ONE || target_r > Decimal::from(20) {
        return Err(invalid_field(
            "target_r",
            "out_of_range",
            "target_r must be between 1 and 20",
        ));
    }

    let atr_period = req.atr_period.unwrap_or(14);
    if !(2..=100).contains(&atr_period) {
        return Err(invalid_field(
            "atr_period",
            "out_of_range",
            "atr_period must be between 2 and 100",
        ));
    }

    let spread_slippage_bps = req.spread_slippage_bps.unwrap_or_else(|| Decimal::from(2));
    if spread_slippage_bps < Decimal::ZERO || spread_slippage_bps > Decimal::from(500) {
        return Err(invalid_field(
            "spread_slippage_bps",
            "out_of_range",
            "spread_slippage_bps must be between 0 and 500 bps",
        ));
    }

    let futures_roll_basis_bps = req.futures_roll_basis_bps.unwrap_or(Decimal::ZERO);
    if futures_roll_basis_bps < Decimal::from(-500) || futures_roll_basis_bps > Decimal::from(500) {
        return Err(invalid_field(
            "futures_roll_basis_bps",
            "out_of_range",
            "futures_roll_basis_bps must be between -500 and 500 bps",
        ));
    }

    let stop_mode = match req.stop_policy_mode {
        LoopStopPolicyMode::TightCenterLine => StopPolicyMode::TightCenterLine,
        LoopStopPolicyMode::BandOuterEdge => StopPolicyMode::BandOuterEdge,
        LoopStopPolicyMode::OuterAtrBuffer => StopPolicyMode::OuterAtrBuffer {
            buffer_atr: sl_buffer_atr,
        },
        LoopStopPolicyMode::PercentileShield => StopPolicyMode::PercentileShield {
            buffer_atr: sl_buffer_atr.clamp(Decimal::new(10, 2), Decimal::new(150, 2)),
        },
        LoopStopPolicyMode::FixedHorizon => StopPolicyMode::FixedHorizon,
        LoopStopPolicyMode::PartialAndTrail => {
            let partial_r = req.partial_r.unwrap_or_else(|| Decimal::from(2));
            let runner_r = req.runner_r.unwrap_or_else(|| Decimal::from(5));
            if partial_r < Decimal::ONE {
                return Err(invalid_field(
                    "partial_r",
                    "out_of_range",
                    "partial_r must be >= 1",
                ));
            }
            if runner_r < partial_r {
                return Err(invalid_field(
                    "runner_r",
                    "out_of_range",
                    "runner_r must be >= partial_r",
                ));
            }
            StopPolicyMode::PartialAndTrail {
                partial_r,
                runner_r,
            }
        }
        LoopStopPolicyMode::TimeStop => {
            let ts_bars = req.time_stop_bars.unwrap_or(horizon_bars);
            if !(1..=120).contains(&ts_bars) {
                return Err(invalid_field(
                    "time_stop_bars",
                    "out_of_range",
                    "time_stop_bars must be between 1 and 120",
                ));
            }
            StopPolicyMode::TimeStop { bars: ts_bars }
        }
    };

    let capital = req.capital.unwrap_or_else(|| Decimal::from(500_000));
    if capital <= Decimal::ZERO {
        return Err(invalid_field(
            "capital",
            "out_of_range",
            "capital must be positive",
        ));
    }

    let max_chart_bars = req.max_chart_bars.unwrap_or(300).clamp(20, 1500);

    Ok(ValidatedLoopParams {
        sl_buffer_atr,
        horizon_bars,
        target_r,
        atr_period,
        stop_mode,
        spread_cost: SpreadCostConfig {
            mode: to_domain_roll_mode(req.roll_basis_mode),
            spread_slippage_bps,
            futures_roll_basis_bps,
            include_statutory_costs: req.include_statutory_costs,
        },
        lot_size_override: req.lot_size,
        tick_size_override: req.tick_size,
        capital,
        max_chart_bars,
    })
}

fn to_domain_session_preset(v: LoopSessionPreset) -> DomainSessionPreset {
    match v {
        LoopSessionPreset::NseEquityFno => DomainSessionPreset::NseEquityFno,
        LoopSessionPreset::NseSep2025Expiry => DomainSessionPreset::NseSep2025Expiry,
        LoopSessionPreset::McxCommodity => DomainSessionPreset::McxCommodity,
        LoopSessionPreset::FxSession => DomainSessionPreset::FxSession,
        LoopSessionPreset::DailyMonthly => DomainSessionPreset::DailyMonthly,
        LoopSessionPreset::WeeklyQuarter => DomainSessionPreset::WeeklyQuarter,
        LoopSessionPreset::MonthlyYear => DomainSessionPreset::MonthlyYear,
    }
}

fn to_domain_horizon(v: LoopExpiryHorizon) -> DomainHorizon {
    match v {
        LoopExpiryHorizon::Day => DomainHorizon::Day,
        LoopExpiryHorizon::Weekly => DomainHorizon::Weekly,
        LoopExpiryHorizon::Monthly => DomainHorizon::Monthly,
        LoopExpiryHorizon::CommodityCycle => DomainHorizon::CommodityCycle,
    }
}

fn from_domain_horizon(v: DomainHorizon) -> LoopExpiryHorizon {
    match v {
        DomainHorizon::Day => LoopExpiryHorizon::Day,
        DomainHorizon::Weekly => LoopExpiryHorizon::Weekly,
        DomainHorizon::Monthly => LoopExpiryHorizon::Monthly,
        DomainHorizon::CommodityCycle => LoopExpiryHorizon::CommodityCycle,
    }
}

fn to_domain_dir(v: SignalDirectionDto) -> SignalDirection {
    match v {
        SignalDirectionDto::Long => SignalDirection::Long,
        SignalDirectionDto::Short => SignalDirection::Short,
    }
}

fn from_domain_dir(v: SignalDirection) -> SignalDirectionDto {
    match v {
        SignalDirection::Long => SignalDirectionDto::Long,
        SignalDirection::Short => SignalDirectionDto::Short,
    }
}

fn from_domain_structure(v: DomainStructure) -> HedgeStructureKindDto {
    match v {
        DomainStructure::Unhedged => HedgeStructureKindDto::Unhedged,
        DomainStructure::BullCallSpread => HedgeStructureKindDto::BullCallSpread,
        DomainStructure::BearPutSpread => HedgeStructureKindDto::BearPutSpread,
        DomainStructure::BullPutCreditSpread => HedgeStructureKindDto::BullPutCreditSpread,
        DomainStructure::BearCallCreditSpread => HedgeStructureKindDto::BearCallCreditSpread,
        DomainStructure::ZoneIronCondor => HedgeStructureKindDto::ZoneIronCondor,
        DomainStructure::ProtectiveCollar => HedgeStructureKindDto::ProtectiveCollar,
        DomainStructure::CalendarRoll => HedgeStructureKindDto::CalendarRoll,
    }
}

fn resolve_lot_and_tick(
    inst: &Instrument,
    lot_override: Option<Decimal>,
    tick_override: Option<Decimal>,
) -> (Decimal, Decimal) {
    let inst_lot = Decimal::from_f64_retain(inst.lot_size())
        .filter(|v| *v > Decimal::ZERO)
        .unwrap_or_else(|| Decimal::from(50));
    let inst_tick = Decimal::from_f64_retain(inst.tick_size())
        .filter(|v| *v > Decimal::ZERO)
        .unwrap_or_else(|| Decimal::new(5, 2));
    (
        lot_override
            .filter(|v| *v > Decimal::ZERO)
            .unwrap_or(inst_lot),
        tick_override
            .filter(|v| *v > Decimal::ZERO)
            .unwrap_or(inst_tick),
    )
}

async fn load_study_bars_for_timeframe(
    state: &AppState,
    canonical_id: &InstrumentId,
    start_date: NaiveDate,
    end_date: NaiveDate,
    timeframe: LoopTimeframe,
) -> Result<Vec<StudyBar>, Response> {
    let bars = match timeframe {
        LoopTimeframe::D1 => {
            let daily =
                load_daily_study_bars(state, canonical_id, start_date, end_date, true).await?;
            daily.iter().map(StudyBar::from).collect::<Vec<_>>()
        }
        LoopTimeframe::M1 => load_intraday_study_bars(
            state,
            canonical_id,
            start_date,
            end_date,
            1,
            BarAggregation::Minute,
        )
        .await
        .iter()
        .map(StudyBar::from)
        .collect(),
        LoopTimeframe::M5 => load_intraday_study_bars(
            state,
            canonical_id,
            start_date,
            end_date,
            5,
            BarAggregation::Minute,
        )
        .await
        .iter()
        .map(StudyBar::from)
        .collect(),
        LoopTimeframe::M15 => load_intraday_study_bars(
            state,
            canonical_id,
            start_date,
            end_date,
            15,
            BarAggregation::Minute,
        )
        .await
        .iter()
        .map(StudyBar::from)
        .collect(),
        LoopTimeframe::H1 => load_intraday_study_bars(
            state,
            canonical_id,
            start_date,
            end_date,
            1,
            BarAggregation::Hour,
        )
        .await
        .iter()
        .map(StudyBar::from)
        .collect(),
    };

    if bars.is_empty() {
        return Err(failure(
            StatusCode::NOT_FOUND,
            ErrorDetail::new(
                ErrorCode::NotFound,
                format!(
                    "no {} bars available for {} between {} and {}",
                    timeframe.as_str(),
                    canonical_id,
                    start_date,
                    end_date
                ),
            ),
        ));
    }

    Ok(bars)
}

async fn load_live_chain_quotes(
    state: &AppState,
    canonical_id: &InstrumentId,
) -> (Vec<LiveOptionStrikeQuote>, usize) {
    let chain_reader = InstrumentChainReader::new(state.instruments.clone(), state.quotes.clone());
    match chain_reader
        .options_chain(canonical_id, None, Some(200))
        .await
    {
        Ok(contracts) => {
            let quotes: Vec<LiveOptionStrikeQuote> = contracts
                .into_iter()
                .map(|c| LiveOptionStrikeQuote {
                    option_type: match c.option_type {
                        tantra_core::OptionType::Call => "CE".to_string(),
                        tantra_core::OptionType::Put => "PE".to_string(),
                    },
                    strike: c.strike.value(),
                    expiry: c.expiry,
                    lot_size: c.lot_size.value(),
                    bid: c.bid.map(|p| p.value()),
                    ask: c.ask.map(|p| p.value()),
                    last_price: c.last_price.map(|p| p.value()),
                    open_interest: c.open_interest.map(|q| q.value()),
                })
                .collect();
            let count = quotes.len();
            (quotes, count)
        }
        Err(_) => (Vec::new(), 0),
    }
}

fn map_study_err(err: StudyError) -> Response {
    match err {
        StudyError::EmptyBars { symbol } => failure(
            StatusCode::NOT_FOUND,
            ErrorDetail::new(
                ErrorCode::NotFound,
                format!("no bars available for {symbol}"),
            ),
        ),
        other => unprocessable(invalid_field(
            "strategy_loop",
            "study_error",
            other.to_string(),
        )),
    }
}

// ============================================================================
// 2. Domain -> DTO Conversion & 6-Layer OverlayScene Assembly
// ============================================================================

fn map_kpis(k: &StrategyKpis) -> StrategyLoopKpisDto {
    StrategyLoopKpisDto {
        trades: k.trades,
        stopped_out_count: k.stopped_out_count,
        stop_out_rate_pct: k.stop_out_rate_pct,
        false_sweep_stop_count: k.false_sweep_stop_count,
        false_sweep_stop_rate_pct: k.false_sweep_stop_rate_pct,
        win_count: k.win_count,
        win_rate_pct: k.win_rate_pct,
        target_2r_hit_rate_pct: k.target_2r_hit_rate_pct,
        avg_realized_r: k.avg_realized_r,
        net_avg_realized_r: k.net_avg_realized_r,
        profit_factor: k.profit_factor,
        net_profit_factor: k.net_profit_factor,
        sharpe_ratio: k.sharpe_ratio,
        max_drawdown_r: k.max_drawdown_r,
    }
}

fn map_penetration(p: &PenetrationDistribution) -> PenetrationDistributionDto {
    PenetrationDistributionDto {
        sample_count: p.sample_count,
        p50_atr: p.p50_atr,
        p75_atr: p.p75_atr,
        p90_atr: p.p90_atr,
        p95_atr: p.p95_atr,
        max_atr: p.max_atr,
        pct_recovering_after_outer_touch: p.pct_recovering_after_outer_touch,
    }
}

fn map_stop_comparison_row(r: &StopComparisonRow) -> StopComparisonRowDto {
    StopComparisonRowDto {
        policy_id: r.policy_id.clone(),
        label: r.label.clone(),
        description: r.description.clone(),
        buffer_atr: r.buffer_atr,
        kpis: map_kpis(&r.kpis),
    }
}

fn map_hedge_dto(h: &DomainHedge, live_contract_count: usize) -> HedgeRecommendationDto {
    HedgeRecommendationDto {
        structure: from_domain_structure(h.structure),
        horizon: from_domain_horizon(h.horizon),
        chain_status: h.chain_status.clone(),
        live_contract_count,
        legs: h
            .legs
            .iter()
            .map(|l| HedgeLegDto {
                side: l.side.clone(),
                option_type: l.option_type.clone(),
                strike: l.strike,
                expiry: l.expiry.map(|d| d.format("%Y-%m-%d").to_string()),
                lot_qty: l.lot_qty,
                leg_price: l.leg_price,
                quote_source: l.quote_source.clone(),
            })
            .collect(),
        net_credit_or_debit: h.net_credit_or_debit,
        max_loss_inr: h.max_loss_inr,
        max_loss_pct_equity: h.max_loss_pct_equity,
        p95_stress_loss_inr: h.p95_stress_loss_inr,
        estimated_friction_inr: h.estimated_friction_inr,
        breakevens: h.breakevens.clone(),
        rationale: h.rationale.clone(),
    }
}

fn build_hedge_overlays(
    hedge: &HedgeRecommendationDto,
    x0: usize,
    x1: usize,
) -> Vec<HedgeLevelOverlayDto> {
    hedge
        .legs
        .iter()
        .enumerate()
        .map(|(idx, leg)| HedgeLevelOverlayDto {
            role: format!("{} {}", leg.side, leg.option_type),
            side: if leg.side.eq_ignore_ascii_case("BUY") {
                HedgeLegSideDto::Buy
            } else {
                HedgeLegSideDto::Sell
            },
            option_kind: match leg.option_type.to_ascii_uppercase().as_str() {
                "FUT" => HedgeOptionKindDto::Future,
                "CE" => HedgeOptionKindDto::Call,
                "PE" => HedgeOptionKindDto::Put,
                _ => HedgeOptionKindDto::Spot,
            },
            strike: leg.strike,
            x0,
            x1,
            breakeven: hedge.breakevens.get(idx).copied(),
            max_loss: if idx == 0 {
                Some(hedge.max_loss_inr)
            } else {
                None
            },
            summary: Some(format!(
                "{} {} {} ({})",
                leg.side, leg.option_type, leg.strike, leg.quote_source
            )),
        })
        .collect()
}

fn compute_kpi_delta(
    after: &StrategyLoopKpisDto,
    before: &StrategyLoopKpisDto,
) -> SweepKpiDeltaDto {
    SweepKpiDeltaDto {
        net_avg_realized_r_delta: (after.net_avg_realized_r - before.net_avg_realized_r)
            .round_dp(4),
        win_rate_pct_delta: (after.win_rate_pct - before.win_rate_pct).round_dp(2),
        stop_out_rate_pct_delta: (after.stop_out_rate_pct - before.stop_out_rate_pct).round_dp(2),
        false_sweep_stop_rate_pct_delta: (after.false_sweep_stop_rate_pct
            - before.false_sweep_stop_rate_pct)
            .round_dp(2),
        sharpe_ratio_delta: (after.sharpe_ratio - before.sharpe_ratio).round_dp(2),
        profit_factor_delta: (after.net_profit_factor - before.net_profit_factor).round_dp(2),
    }
}

fn compute_snapshot_delta(
    after: &StrategyLoopKpisDto,
    before: &LoopSessionSnapshotSummaryDto,
) -> SweepKpiDeltaDto {
    SweepKpiDeltaDto {
        net_avg_realized_r_delta: (after.net_avg_realized_r - before.net_avg_realized_r)
            .round_dp(4),
        win_rate_pct_delta: (after.win_rate_pct - before.win_rate_pct).round_dp(2),
        stop_out_rate_pct_delta: (after.stop_out_rate_pct - before.stop_out_rate_pct).round_dp(2),
        false_sweep_stop_rate_pct_delta: Decimal::ZERO,
        sharpe_ratio_delta: (after.sharpe_ratio - before.sharpe_ratio).round_dp(2),
        profit_factor_delta: (after.net_profit_factor - before.profit_factor).round_dp(2),
    }
}

fn format_bar_timestamp(bar: &StudyBar) -> String {
    match bar.minute_of_day {
        Some(m) => format!(
            "{} {:02}:{:02}",
            bar.date.format("%Y-%m-%d"),
            m / 60,
            m % 60
        ),
        None => bar.date.format("%Y-%m-%d").to_string(),
    }
}

#[allow(clippy::too_many_arguments)]
fn build_overlay_scene_and_bars(
    strategy_id: &str,
    symbol: &str,
    timeframe: LoopTimeframe,
    clean_bars: &[StudyBar],
    atr_period: usize,
    zones: &ZoneExtractionOutput,
    signals: &[EntrySignal],
    trades: &[SimulatedTrade],
    stop_mode: StopPolicyMode,
    hedge: &HedgeRecommendationDto,
    max_chart_bars: usize,
) -> (OverlaySceneDto, Vec<LoopBarDto>) {
    let total = clean_bars.len();
    let offset = total.saturating_sub(max_chart_bars);
    let visible_bars = &clean_bars[offset..];
    let last_vis = visible_bars.len().saturating_sub(1);

    let loop_bars: Vec<LoopBarDto> = visible_bars
        .iter()
        .enumerate()
        .map(|(vis_idx, b)| LoopBarDto {
            index: vis_idx,
            timestamp: format_bar_timestamp(b),
            open: b.open,
            high: b.high,
            low: b.low,
            close: b.close,
            volume: b.volume,
        })
        .collect();

    let sig_by_bar: HashMap<usize, &EntrySignal> =
        signals.iter().map(|s| (s.bar_index, s)).collect();

    // Evaluate indicators across full clean series once, then slice trailing visible window
    let indicators = StandardIndicatorEvaluator.evaluate(
        clean_bars,
        &[
            IndicatorSpec::Atr {
                period: atr_period.max(2),
            },
            IndicatorSpec::Ema { period: 20 },
            IndicatorSpec::Rsi { period: 14 },
        ],
    );

    let mut scene_signals = Vec::new();
    let mut scene_connectors = Vec::new();
    let mut scene_stops = Vec::new();
    let mut scene_hints = Vec::new();

    let shows_outer_diff = matches!(
        stop_mode,
        StopPolicyMode::OuterAtrBuffer { .. }
            | StopPolicyMode::PercentileShield { .. }
            | StopPolicyMode::BandOuterEdge
            | StopPolicyMode::PartialAndTrail { .. }
    );

    for trade in trades {
        let Some(sig) = sig_by_bar.get(&trade.entry_bar_index).copied() else {
            continue;
        };
        let tight_sl =
            OuterBufferStopPolicy::compute_stop_for_mode(StopPolicyMode::TightCenterLine, sig);
        let tight_touched = clean_bars
            .iter()
            .take(trade.exit_bar_index.min(total.saturating_sub(1)) + 1)
            .skip(trade.entry_bar_index + 1)
            .any(|b| match sig.direction {
                SignalDirection::Long => b.low <= tight_sl,
                SignalDirection::Short => b.high >= tight_sl,
            });
        let saved_by_outer = shows_outer_diff
            && tight_touched
            && !trade.stopped_out
            && trade.gross_r > Decimal::ZERO;

        let side = match trade.direction {
            SignalDirection::Long => MarkerSideDto::Buy,
            SignalDirection::Short => MarkerSideDto::Sell,
        };

        if trade.entry_bar_index >= offset {
            let vis_entry = (trade.entry_bar_index - offset).min(last_vis);
            let kind = if saved_by_outer || sig.is_sweep_reclaim {
                MarkerKindDto::SweepSaved
            } else if strategy_id == "orb_15" {
                MarkerKindDto::Breakout
            } else {
                MarkerKindDto::Reversal
            };
            scene_signals.push(SignalMarkerDto {
                price: trade.entry_price,
                index: vis_entry,
                side,
                kind,
            });

            if trade.exit_bar_index >= offset {
                let vis_exit = (trade.exit_bar_index - offset).min(last_vis);
                if trade.hit_target && vis_exit > vis_entry {
                    scene_signals.push(SignalMarkerDto {
                        price: trade.exit_price,
                        index: vis_exit,
                        side,
                        kind: MarkerKindDto::TargetHit,
                    });
                }
                if vis_entry <= vis_exit {
                    let label = if trade.stopped_out {
                        format!("Stopped ({:+.2}R)", trade.net_r)
                    } else if trade.hit_target {
                        format!("{:+.1}R Target", trade.net_r)
                    } else {
                        format!("{:+.2}R Horizon", trade.net_r)
                    };
                    scene_connectors.push(TradeConnectorRayDto {
                        from_index: vis_entry,
                        from_price: trade.entry_price,
                        to_index: vis_exit,
                        to_price: trade.exit_price,
                        side,
                        realized_r: trade.net_r,
                        stopped_out: trade.stopped_out,
                        label: Some(label),
                    });
                }
            }

            let severity = if trade.false_sweep_trap {
                HintSeverityDto::Warning
            } else if saved_by_outer || trade.hit_target {
                HintSeverityDto::Positive
            } else if trade.stopped_out {
                HintSeverityDto::Critical
            } else {
                HintSeverityDto::Info
            };

            let title = if saved_by_outer {
                "Sweep Saved — Outer Buffer Survived".to_string()
            } else if trade.false_sweep_trap {
                "False Sweep Trap (Tight SL Hit)".to_string()
            } else if trade.hit_target {
                format!("Target Hit ({:+.2}R)", trade.net_r)
            } else if trade.stopped_out {
                "Stopped Out".to_string()
            } else {
                format!(
                    "{} Setup ({})",
                    match sig.direction {
                        SignalDirection::Long => "Long",
                        SignalDirection::Short => "Short",
                    },
                    sig.band_label
                )
            };

            let detail = if saved_by_outer {
                format!(
                    "{}. Tight SL ({:.2}) was wicked inside the horizon while Outer SL ({:.2}) held and netted {:+.2}R.",
                    sig.hint, tight_sl, trade.stop_price, trade.net_r
                )
            } else {
                format!(
                    "{}. Entry {:.2}, SL {:.2}, Exit {:.2} -> Net {:+.2}R.",
                    sig.hint, trade.entry_price, trade.stop_price, trade.exit_price, trade.net_r
                )
            };

            scene_hints.push(HintAnnotationDto {
                bar_index: vis_entry,
                price: trade.entry_price,
                severity,
                title,
                detail,
                metrics: vec![
                    HintMetricDto {
                        key: "Entry".to_string(),
                        value: format!("{:.2}", trade.entry_price),
                    },
                    HintMetricDto {
                        key: "Active SL".to_string(),
                        value: format!("{:.2}", trade.stop_price),
                    },
                    HintMetricDto {
                        key: "Tight SL".to_string(),
                        value: format!("{tight_sl:.2}"),
                    },
                    HintMetricDto {
                        key: "Target".to_string(),
                        value: format!("{:.2}", trade.target_price),
                    },
                    HintMetricDto {
                        key: "Net R".to_string(),
                        value: format!("{:+.2}R", trade.net_r),
                    },
                ],
            });
        }

        if trade.exit_bar_index >= offset {
            let x0 = trade.entry_bar_index.saturating_sub(offset).min(last_vis);
            let x1 = (trade.exit_bar_index - offset).min(last_vis);
            if x0 <= x1 {
                scene_stops.push(StopLineDto {
                    price: trade.stop_price,
                    x0,
                    x1,
                    kind: StopKindDto::Initial,
                    previous: shows_outer_diff.then_some(tight_sl),
                });
                scene_stops.push(StopLineDto {
                    price: trade.target_price,
                    x0,
                    x1,
                    kind: StopKindDto::Target,
                    previous: None,
                });
            }
        }
    }

    let scene_bands: Vec<ZoneBandDto> = zones
        .bands
        .iter()
        .filter_map(|band| {
            if band.end_index < offset || visible_bars.is_empty() {
                return None;
            }
            let x0 = band.start_index.saturating_sub(offset).min(last_vis);
            let x1 = (band.end_index - offset).min(last_vis);
            if x0 > x1 {
                return None;
            }
            let fill_index = match band.label.as_str() {
                "S1" => 0,
                "S2" => 1,
                "R1" => 2,
                "R2" => 3,
                "OR_LOW" => 4,
                "OR_HIGH" => 5,
                _ => 0,
            };
            Some(ZoneBandDto {
                low: band.low,
                high: band.high,
                x0,
                x1,
                fill_index,
                label: Some(band.label.clone()),
            })
        })
        .collect();

    let scene_levels: Vec<LevelLineDto> = zones
        .levels
        .iter()
        .filter_map(|lvl| {
            if lvl.end_index < offset || visible_bars.is_empty() {
                return None;
            }
            let x0 = lvl.start_index.saturating_sub(offset).min(last_vis);
            let x1 = (lvl.end_index - offset).min(last_vis);
            if x0 > x1 {
                return None;
            }
            Some(LevelLineDto {
                price: lvl.price,
                x0,
                x1,
                label: Some(lvl.label.clone()),
                style: LevelLineStyleDto::default(),
            })
        })
        .collect();

    let mut scene_indicators = Vec::new();
    if let Some(ema20) = indicators.ema.get(&20)
        && ema20.len() == total
    {
        scene_indicators.push(ChartSeriesDto {
            name: "EMA(20)".to_string(),
            points: ema20[offset..]
                .iter()
                .enumerate()
                .map(|(i, y)| ChartPointDto {
                    x: Decimal::from(i),
                    y: *y,
                })
                .collect(),
        });
    }
    if indicators.rsi.len() == total {
        scene_indicators.push(ChartSeriesDto {
            name: "RSI(14)".to_string(),
            points: indicators.rsi[offset..]
                .iter()
                .enumerate()
                .map(|(i, y)| ChartPointDto {
                    x: Decimal::from(i),
                    y: *y,
                })
                .collect(),
        });
    }
    if indicators.atr.len() == total {
        scene_indicators.push(ChartSeriesDto {
            name: format!("ATR({})", atr_period.max(2)),
            points: indicators.atr[offset..]
                .iter()
                .enumerate()
                .map(|(i, y)| ChartPointDto {
                    x: Decimal::from(i),
                    y: *y,
                })
                .collect(),
        });
    }

    let hedge_x0 = last_vis.saturating_sub(20).min(last_vis);
    let scene_hedges = if visible_bars.is_empty() {
        Vec::new()
    } else {
        build_hedge_overlays(hedge, hedge_x0, last_vis)
    };

    let scene = OverlaySceneDto {
        strategy_id: strategy_id.to_string(),
        symbol: symbol.to_string(),
        timeframe: timeframe.as_str().to_string(),
        layers: SceneLayerMaskDto::default(),
        signals: scene_signals,
        connectors: scene_connectors,
        bands: scene_bands,
        levels: scene_levels,
        stops: scene_stops,
        indicators: scene_indicators,
        hints: scene_hints,
        hedges: scene_hedges,
    };

    (scene, loop_bars)
}

fn map_trade_summaries(
    clean_bars: &[StudyBar],
    signals: &[EntrySignal],
    trades: &[SimulatedTrade],
) -> Vec<StrategyTradeSummaryDto> {
    let sig_by_bar: HashMap<usize, &EntrySignal> =
        signals.iter().map(|s| (s.bar_index, s)).collect();
    trades
        .iter()
        .map(|t| {
            let entry_time = clean_bars
                .get(t.entry_bar_index)
                .map(format_bar_timestamp)
                .unwrap_or_default();
            let exit_time = clean_bars
                .get(t.exit_bar_index)
                .map(format_bar_timestamp)
                .unwrap_or_default();
            let sig_meta = sig_by_bar.get(&t.entry_bar_index).copied();
            StrategyTradeSummaryDto {
                entry_bar_index: t.entry_bar_index,
                exit_bar_index: t.exit_bar_index,
                entry_time,
                exit_time,
                direction: from_domain_dir(t.direction),
                entry_price: t.entry_price,
                stop_price: t.stop_price,
                target_price: t.target_price,
                exit_price: t.exit_price,
                one_r: t.one_r,
                stopped_out: t.stopped_out,
                false_sweep_trap: t.false_sweep_trap,
                hit_2r: t.hit_2r,
                hit_target: t.hit_target,
                gross_r: t.gross_r,
                net_r: t.net_r,
                band_label: sig_meta
                    .map(|s| s.band_label.clone())
                    .unwrap_or_else(|| "Zone".to_string()),
                hint: sig_meta
                    .map(|s| s.hint.clone())
                    .unwrap_or_else(|| "Evaluated setup".to_string()),
            }
        })
        .collect()
}

fn latest_spot_and_atr(
    clean_bars: &[StudyBar],
    atr_period: usize,
    live_spot: Option<Decimal>,
) -> (Decimal, Decimal) {
    let close = clean_bars
        .last()
        .map(|b| b.close)
        .unwrap_or_else(|| Decimal::from(100));
    let spot = live_spot.filter(|p| *p > Decimal::ZERO).unwrap_or(close);
    let atr_series = StandardIndicatorEvaluator::compute_atr_series(clean_bars, atr_period.max(2));
    let atr = atr_series
        .last()
        .copied()
        .filter(|a| *a > Decimal::ZERO)
        .unwrap_or_else(|| (spot * Decimal::new(1, 2)).max(Decimal::ONE))
        .round_dp(2);
    (spot.round_dp(2), atr)
}

// ============================================================================
// 3. REST Endpoint Handlers (`/api/v2/studies/strategy-loop/*`)
// ============================================================================

/// `POST /studies/strategy-loop/evaluate`: evaluates any registered strategy pipeline with
/// configurable stop policy, 4-policy stop-hunt benchmark, expiry hedge synthesis, 6-layer
/// `OverlaySceneDto`, and stateful session KPI tracking.
pub(crate) async fn post_evaluate(
    State(state): State<Arc<AppState>>,
    ApiJson(req): ApiJson<StrategyLoopRequest>,
) -> Response {
    let parsed_id = match parse_study_instrument_id(&req.instrument_id) {
        Ok(id) => id,
        Err(detail) => return unprocessable(detail),
    };
    let (start_date, end_date) = match parse_date_range(&req.start_date, &req.end_date) {
        Ok(range) => range,
        Err(detail) => return unprocessable(detail),
    };
    let params = match validate_loop_params(&req) {
        Ok(p) => p,
        Err(detail) => return unprocessable(detail),
    };
    let pipeline = match build_strategy_pipeline(
        &req.strategy_id,
        req.entry_trigger_mode,
        params.sl_buffer_atr,
        req.timeframe,
        req.asset_class,
        req.session_preset,
    ) {
        Ok(p) => p,
        Err(detail) => return unprocessable(detail),
    };

    let instrument = match resolve_study_instrument(&state, &parsed_id).await {
        Ok(inst) => inst,
        Err(resp) => return resp,
    };
    let canonical_id = instrument.id().clone();
    let (lot_size, tick_size) = resolve_lot_and_tick(
        &instrument,
        params.lot_size_override,
        params.tick_size_override,
    );

    let raw_bars = match load_study_bars_for_timeframe(
        &state,
        &canonical_id,
        start_date,
        end_date,
        req.timeframe,
    )
    .await
    {
        Ok(b) => b,
        Err(resp) => return resp,
    };

    let (live_chain, live_contract_count) = load_live_chain_quotes(&state, &canonical_id).await;
    let live_spot = read_live_spot(&state, &canonical_id).await;
    let domain_asset = to_domain_asset_class(req.asset_class);

    let cal_inner = match state.lakehouse_root() {
        Some(root) => NseTradingCalendar::from_data_root(root),
        None => NseTradingCalendar::new(),
    };
    let calendar = RuntimeNseCalendar(cal_inner);

    let eval_req = LoopEvalRequest {
        strategy_id: pipeline.canonical_strategy_id.to_string(),
        symbol: canonical_id.to_string(),
        asset_class: domain_asset,
        session_preset: pipeline.default_session_preset,
        bars: raw_bars.clone(),
        horizon_bars: params.horizon_bars,
        target_r: params.target_r,
        atr_period: params.atr_period,
        sl_buffer_atr: params.sl_buffer_atr,
        expiry_horizon: to_domain_horizon(req.expiry_horizon),
        spread_cost: params.spread_cost.clone(),
        live_chain: live_chain.clone(),
    };

    let stop_policy = OuterBufferStopPolicy::new(params.stop_mode);
    let hedge_policy = HedgeSynthesizer;

    let report: LoopEvalReport = match StrategyLoopKernel::evaluate(
        &eval_req,
        &calendar,
        pipeline.zone_extractor.as_ref(),
        pipeline.entry_filter.as_ref(),
        &stop_policy,
        &hedge_policy,
    ) {
        Ok(r) => r,
        Err(e) => return map_study_err(e),
    };

    let cmp_report = match StrategyLoopKernel::compare_stop_policies(
        &eval_req,
        &calendar,
        pipeline.zone_extractor.as_ref(),
        pipeline.entry_filter.as_ref(),
    ) {
        Ok(c) => c,
        Err(e) => return map_study_err(e),
    };

    let clean = SplitArtifactGuard::default().clean(&raw_bars);
    let clean_bars = &clean.adjusted_bars;
    let (spot_price, atr) = latest_spot_and_atr(clean_bars, params.atr_period, live_spot);

    // Re-synthesize hedge with true resolved instrument lot_size, tick_size, live spot & capital
    let domain_hedge = hedge_policy.synthesize(&HedgeSynthesisInput {
        symbol: canonical_id.symbol.clone(),
        asset_class: domain_asset,
        spot: spot_price,
        atr,
        horizon: to_domain_horizon(req.expiry_horizon),
        direction: report.signals.last().map(|s| s.direction),
        zones: Some(&report.zones),
        penetration: Some(&report.penetration),
        iv_gt_rv: false,
        trap_rate_pct: report.kpis.false_sweep_stop_rate_pct,
        live_chain: &live_chain,
        lot_size,
        tick_size,
        capital: params.capital,
        spread_cost: &params.spread_cost,
    });
    let hedge_dto = map_hedge_dto(&domain_hedge, live_contract_count);

    let (scene, bars) = build_overlay_scene_and_bars(
        pipeline.canonical_strategy_id,
        &canonical_id.to_string(),
        req.timeframe,
        clean_bars,
        params.atr_period,
        &report.zones,
        &report.signals,
        &report.trades,
        params.stop_mode,
        &hedge_dto,
        params.max_chart_bars,
    );

    let kpis_dto = map_kpis(&report.kpis);
    let trade_summaries = map_trade_summaries(clean_bars, &report.signals, &report.trades);

    let actual_start = clean_bars
        .first()
        .map(|b| b.date.format("%Y-%m-%d").to_string())
        .unwrap_or_else(|| req.start_date.clone());
    let actual_end = clean_bars
        .last()
        .map(|b| b.date.format("%Y-%m-%d").to_string())
        .unwrap_or_else(|| req.end_date.clone());

    let snapshot = LoopSessionSnapshotSummaryDto {
        session_id: String::new(),
        run_seq: 0,
        strategy_id: pipeline.canonical_strategy_id.to_string(),
        symbol: canonical_id.to_string(),
        timeframe: req.timeframe,
        sl_buffer_atr: params.sl_buffer_atr,
        horizon_bars: params.horizon_bars,
        target_r: params.target_r,
        stop_policy_mode: req.stop_policy_mode,
        expiry_horizon: req.expiry_horizon,
        net_avg_realized_r: kpis_dto.net_avg_realized_r,
        win_rate_pct: kpis_dto.win_rate_pct,
        stop_out_rate_pct: kpis_dto.stop_out_rate_pct,
        sharpe_ratio: kpis_dto.sharpe_ratio,
        profit_factor: kpis_dto.net_profit_factor,
    };

    let (session_id, run_seq, previous_run) = state
        .strategy_loops
        .record_run(req.session_id.as_deref(), snapshot);
    let delta_vs_previous = previous_run
        .as_ref()
        .map(|prev| compute_snapshot_delta(&kpis_dto, prev));

    success(StrategyLoopResponse {
        session_id,
        run_seq,
        instrument_id: canonical_id.to_string(),
        symbol: canonical_id.symbol.clone(),
        exchange: canonical_id.exchange.to_string(),
        asset_class: req.asset_class,
        timeframe: req.timeframe,
        start_date: actual_start,
        end_date: actual_end,
        pipeline: LoopPipelineSpecDto {
            strategy_id: pipeline.canonical_strategy_id.to_string(),
            zone_extractor_id: pipeline.zone_extractor.id().to_string(),
            entry_filter_id: pipeline.entry_filter.id().to_string(),
            stop_policy_id: stop_policy.policy_id(),
            hedge_policy_id: "hedge_synthesizer".to_string(),
        },
        total_bars: report.total_bars,
        splits_adjusted: report.splits_adjusted,
        spot_price,
        atr,
        kpis: kpis_dto,
        penetration: map_penetration(&report.penetration),
        stop_comparison: StopComparisonSummaryDto {
            total_setups: cmp_report.total_setups,
            bullish_setups: cmp_report.bullish_setups,
            bearish_setups: cmp_report.bearish_setups,
            models: cmp_report
                .models
                .iter()
                .map(map_stop_comparison_row)
                .collect(),
            recommended_policy_id: cmp_report.recommended_policy_id,
            verdict_summary: cmp_report.verdict_summary,
        },
        trades: trade_summaries,
        hedge: hedge_dto,
        scene,
        bars,
        previous_run,
        delta_vs_previous,
    })
}

/// `POST /studies/strategy-loop/sweep`: runs bounded IS (70%) / OOS (30%) parameter grid sweep
/// (`<= 64` combinations) with Walk-Forward Efficiency (`WFE %`), Deflated Sharpe Ratio (`DSR`),
/// and baseline-vs-winner 6-layer `OverlaySceneDto` diff.
pub(crate) async fn post_sweep(
    State(state): State<Arc<AppState>>,
    ApiJson(req): ApiJson<StrategySweepRequest>,
) -> Response {
    let parsed_id = match parse_study_instrument_id(&req.base.instrument_id) {
        Ok(id) => id,
        Err(detail) => return unprocessable(detail),
    };
    let (start_date, end_date) = match parse_date_range(&req.base.start_date, &req.base.end_date) {
        Ok(range) => range,
        Err(detail) => return unprocessable(detail),
    };
    let params = match validate_loop_params(&req.base) {
        Ok(p) => p,
        Err(detail) => return unprocessable(detail),
    };

    for &b in &req.bounds.sl_buffer_atr_grid {
        if b < Decimal::ZERO || b > Decimal::from(3) {
            return unprocessable(invalid_field(
                "bounds.sl_buffer_atr_grid",
                "out_of_range",
                "sl_buffer_atr_grid entries must be between 0.00 and 3.00",
            ));
        }
    }
    for &h in &req.bounds.horizon_bars_grid {
        if !(1..=120).contains(&h) {
            return unprocessable(invalid_field(
                "bounds.horizon_bars_grid",
                "out_of_range",
                "horizon_bars_grid entries must be between 1 and 120",
            ));
        }
    }
    for &t in &req.bounds.target_r_grid {
        if t < Decimal::ONE || t > Decimal::from(20) {
            return unprocessable(invalid_field(
                "bounds.target_r_grid",
                "out_of_range",
                "target_r_grid entries must be between 1 and 20",
            ));
        }
    }

    let base_pipeline = match build_strategy_pipeline(
        &req.base.strategy_id,
        req.base.entry_trigger_mode,
        params.sl_buffer_atr,
        req.base.timeframe,
        req.base.asset_class,
        req.base.session_preset,
    ) {
        Ok(p) => p,
        Err(detail) => return unprocessable(detail),
    };

    let instrument = match resolve_study_instrument(&state, &parsed_id).await {
        Ok(inst) => inst,
        Err(resp) => return resp,
    };
    let canonical_id = instrument.id().clone();
    let (lot_size, tick_size) = resolve_lot_and_tick(
        &instrument,
        params.lot_size_override,
        params.tick_size_override,
    );

    let raw_bars = match load_study_bars_for_timeframe(
        &state,
        &canonical_id,
        start_date,
        end_date,
        req.base.timeframe,
    )
    .await
    {
        Ok(b) => b,
        Err(resp) => return resp,
    };

    let (live_chain, live_contract_count) = load_live_chain_quotes(&state, &canonical_id).await;
    let live_spot = read_live_spot(&state, &canonical_id).await;
    let domain_asset = to_domain_asset_class(req.base.asset_class);

    let cal_inner = match state.lakehouse_root() {
        Some(root) => NseTradingCalendar::from_data_root(root),
        None => NseTradingCalendar::new(),
    };
    let calendar = RuntimeNseCalendar(cal_inner);

    let base_eval_req = LoopEvalRequest {
        strategy_id: base_pipeline.canonical_strategy_id.to_string(),
        symbol: canonical_id.to_string(),
        asset_class: domain_asset,
        session_preset: base_pipeline.default_session_preset,
        bars: raw_bars.clone(),
        horizon_bars: params.horizon_bars,
        target_r: params.target_r,
        atr_period: params.atr_period,
        sl_buffer_atr: params.sl_buffer_atr,
        expiry_horizon: to_domain_horizon(req.base.expiry_horizon),
        spread_cost: params.spread_cost.clone(),
        live_chain: live_chain.clone(),
    };

    let base_stop_policy = OuterBufferStopPolicy::new(params.stop_mode);
    let hedge_policy = HedgeSynthesizer;

    let baseline_report = match StrategyLoopKernel::evaluate(
        &base_eval_req,
        &calendar,
        base_pipeline.zone_extractor.as_ref(),
        base_pipeline.entry_filter.as_ref(),
        &base_stop_policy,
        &hedge_policy,
    ) {
        Ok(r) => r,
        Err(e) => return map_study_err(e),
    };

    let sweep_req = ParameterSweepRequest {
        base: base_eval_req.clone(),
        bounds: StrategyParameterBounds {
            sl_buffer_atr_grid: req.bounds.sl_buffer_atr_grid.clone(),
            horizon_bars_grid: req.bounds.horizon_bars_grid.clone(),
            target_r_grid: req.bounds.target_r_grid.clone(),
            max_candidates: req.bounds.max_candidates.unwrap_or(27).clamp(1, 64),
        },
    };

    let sweep_report = match ParameterSweepRunner::run(
        &sweep_req,
        &calendar,
        base_pipeline.zone_extractor.as_ref(),
        base_pipeline.entry_filter.as_ref(),
        &hedge_policy,
    ) {
        Ok(r) => r,
        Err(e) => return map_study_err(e),
    };

    // Winner pipeline evaluated with winner parameters so winner_scene reflects winning SL buffer/horizon/target
    let winner_sl_buf = sweep_report.winner.sl_buffer_atr;
    let winner_stop_mode = if winner_sl_buf.is_zero() {
        StopPolicyMode::BandOuterEdge
    } else {
        StopPolicyMode::OuterAtrBuffer {
            buffer_atr: winner_sl_buf,
        }
    };
    let winner_pipeline = match build_strategy_pipeline(
        &req.base.strategy_id,
        req.base.entry_trigger_mode,
        winner_sl_buf,
        req.base.timeframe,
        req.base.asset_class,
        req.base.session_preset,
    ) {
        Ok(p) => p,
        Err(detail) => return unprocessable(detail),
    };
    let winner_eval_req = LoopEvalRequest {
        strategy_id: winner_pipeline.canonical_strategy_id.to_string(),
        symbol: canonical_id.to_string(),
        asset_class: domain_asset,
        session_preset: winner_pipeline.default_session_preset,
        bars: raw_bars.clone(),
        horizon_bars: sweep_report.winner.horizon_bars,
        target_r: sweep_report.winner.target_r,
        atr_period: params.atr_period,
        sl_buffer_atr: winner_sl_buf,
        expiry_horizon: to_domain_horizon(req.base.expiry_horizon),
        spread_cost: params.spread_cost.clone(),
        live_chain: live_chain.clone(),
    };
    let winner_stop_policy = OuterBufferStopPolicy::new(winner_stop_mode);
    let winner_report = match StrategyLoopKernel::evaluate(
        &winner_eval_req,
        &calendar,
        winner_pipeline.zone_extractor.as_ref(),
        winner_pipeline.entry_filter.as_ref(),
        &winner_stop_policy,
        &hedge_policy,
    ) {
        Ok(r) => r,
        Err(e) => return map_study_err(e),
    };

    let clean = SplitArtifactGuard::default().clean(&raw_bars);
    let clean_bars = &clean.adjusted_bars;
    let (spot_price, atr) = latest_spot_and_atr(clean_bars, params.atr_period, live_spot);

    // Re-synthesize candidate hedges with real resolved lot_size / tick_size / capital
    let synthesize_for_candidate = |cand: &SweepCandidateResult| -> HedgeRecommendationDto {
        let h = hedge_policy.synthesize(&HedgeSynthesisInput {
            symbol: canonical_id.symbol.clone(),
            asset_class: domain_asset,
            spot: spot_price,
            atr,
            horizon: to_domain_horizon(req.base.expiry_horizon),
            direction: winner_report.signals.last().map(|s| s.direction),
            zones: Some(&winner_report.zones),
            penetration: Some(&winner_report.penetration),
            iv_gt_rv: false,
            trap_rate_pct: cand.full_kpis.false_sweep_stop_rate_pct,
            live_chain: &live_chain,
            lot_size,
            tick_size,
            capital: params.capital,
            spread_cost: &params.spread_cost,
        });
        map_hedge_dto(&h, live_contract_count)
    };

    let candidates_dto: Vec<SweepCandidateResultDto> = sweep_report
        .candidates
        .iter()
        .map(|c| SweepCandidateResultDto {
            candidate_index: c.candidate_index,
            sl_buffer_atr: c.sl_buffer_atr,
            horizon_bars: c.horizon_bars,
            target_r: c.target_r,
            full_kpis: map_kpis(&c.full_kpis),
            is_kpis: map_kpis(&c.is_kpis),
            oos_kpis: map_kpis(&c.oos_kpis),
            walk_forward_efficiency_pct: c.walk_forward_efficiency_pct,
            deflated_sharpe_ratio: c.deflated_sharpe_ratio,
            hedge: synthesize_for_candidate(c),
        })
        .collect();

    let winner_dto = candidates_dto[0].clone();
    let baseline_kpis = map_kpis(&baseline_report.kpis);
    let delta_winner_vs_baseline = compute_kpi_delta(&winner_dto.full_kpis, &baseline_kpis);

    let baseline_hedge = map_hedge_dto(
        &hedge_policy.synthesize(&HedgeSynthesisInput {
            symbol: canonical_id.symbol.clone(),
            asset_class: domain_asset,
            spot: spot_price,
            atr,
            horizon: to_domain_horizon(req.base.expiry_horizon),
            direction: baseline_report.signals.last().map(|s| s.direction),
            zones: Some(&baseline_report.zones),
            penetration: Some(&baseline_report.penetration),
            iv_gt_rv: false,
            trap_rate_pct: baseline_report.kpis.false_sweep_stop_rate_pct,
            live_chain: &live_chain,
            lot_size,
            tick_size,
            capital: params.capital,
            spread_cost: &params.spread_cost,
        }),
        live_contract_count,
    );

    let (baseline_scene, bars) = build_overlay_scene_and_bars(
        base_pipeline.canonical_strategy_id,
        &canonical_id.to_string(),
        req.base.timeframe,
        clean_bars,
        params.atr_period,
        &baseline_report.zones,
        &baseline_report.signals,
        &baseline_report.trades,
        params.stop_mode,
        &baseline_hedge,
        params.max_chart_bars,
    );

    let (winner_scene, _) = build_overlay_scene_and_bars(
        winner_pipeline.canonical_strategy_id,
        &canonical_id.to_string(),
        req.base.timeframe,
        clean_bars,
        params.atr_period,
        &winner_report.zones,
        &winner_report.signals,
        &winner_report.trades,
        winner_stop_mode,
        &winner_dto.hedge,
        params.max_chart_bars,
    );

    let snapshot = LoopSessionSnapshotSummaryDto {
        session_id: String::new(),
        run_seq: 0,
        strategy_id: winner_pipeline.canonical_strategy_id.to_string(),
        symbol: canonical_id.to_string(),
        timeframe: req.base.timeframe,
        sl_buffer_atr: winner_dto.sl_buffer_atr,
        horizon_bars: winner_dto.horizon_bars,
        target_r: winner_dto.target_r,
        stop_policy_mode: if winner_dto.sl_buffer_atr.is_zero() {
            LoopStopPolicyMode::BandOuterEdge
        } else {
            LoopStopPolicyMode::OuterAtrBuffer
        },
        expiry_horizon: req.base.expiry_horizon,
        net_avg_realized_r: winner_dto.full_kpis.net_avg_realized_r,
        win_rate_pct: winner_dto.full_kpis.win_rate_pct,
        stop_out_rate_pct: winner_dto.full_kpis.stop_out_rate_pct,
        sharpe_ratio: winner_dto.full_kpis.sharpe_ratio,
        profit_factor: winner_dto.full_kpis.net_profit_factor,
    };
    let (session_id, run_seq, _) = state
        .strategy_loops
        .record_run(req.base.session_id.as_deref(), snapshot);

    success(StrategySweepResponse {
        session_id,
        run_seq,
        instrument_id: canonical_id.to_string(),
        symbol: canonical_id.symbol,
        strategy_id: winner_pipeline.canonical_strategy_id.to_string(),
        timeframe: req.base.timeframe,
        trial_count: sweep_report.trial_count,
        candidates: candidates_dto,
        winner: winner_dto,
        baseline_kpis,
        delta_winner_vs_baseline,
        baseline_scene,
        winner_scene,
        bars,
    })
}

/// `POST /studies/strategy-loop/recommend-hedge`: synthesizes Day / Weekly / Monthly /
/// CommodityCycle F&O hedges anchored on extracted strategy zones, outer-buffer stop tails,
/// and live `ChainReader` option quotes (falling back honestly to `"theoretical_model"`).
pub(crate) async fn post_recommend_hedge(
    State(state): State<Arc<AppState>>,
    ApiJson(req): ApiJson<ExpiryHedgeRequest>,
) -> Response {
    let parsed_id = match parse_study_instrument_id(&req.instrument_id) {
        Ok(id) => id,
        Err(detail) => return unprocessable(detail),
    };

    let default_end = Utc::now().date_naive();
    let default_start = default_end - Duration::days(180);
    let end_str = req
        .end_date
        .clone()
        .unwrap_or_else(|| default_end.format("%Y-%m-%d").to_string());
    let start_str = req
        .start_date
        .clone()
        .unwrap_or_else(|| default_start.format("%Y-%m-%d").to_string());
    let (start_date, end_date) = match parse_date_range(&start_str, &end_str) {
        Ok(range) => range,
        Err(detail) => return unprocessable(detail),
    };

    let sl_buffer_atr = req.sl_buffer_atr.unwrap_or_else(|| Decimal::new(25, 2));
    if sl_buffer_atr < Decimal::ZERO || sl_buffer_atr > Decimal::from(3) {
        return unprocessable(invalid_field(
            "sl_buffer_atr",
            "out_of_range",
            "sl_buffer_atr must be between 0.00 and 3.00",
        ));
    }
    let horizon_bars = req
        .horizon_bars
        .unwrap_or(if req.timeframe.is_intraday() { 12 } else { 5 });
    if !(1..=120).contains(&horizon_bars) {
        return unprocessable(invalid_field(
            "horizon_bars",
            "out_of_range",
            "horizon_bars must be between 1 and 120",
        ));
    }
    let atr_period = req.atr_period.unwrap_or(14);
    if !(2..=100).contains(&atr_period) {
        return unprocessable(invalid_field(
            "atr_period",
            "out_of_range",
            "atr_period must be between 2 and 100",
        ));
    }
    let spread_slippage_bps = req.spread_slippage_bps.unwrap_or_else(|| Decimal::from(2));
    if spread_slippage_bps < Decimal::ZERO || spread_slippage_bps > Decimal::from(500) {
        return unprocessable(invalid_field(
            "spread_slippage_bps",
            "out_of_range",
            "spread_slippage_bps must be between 0 and 500",
        ));
    }
    let capital = req.capital.unwrap_or_else(|| Decimal::from(500_000));
    if capital <= Decimal::ZERO {
        return unprocessable(invalid_field(
            "capital",
            "out_of_range",
            "capital must be positive",
        ));
    }

    let strategy_id = req.strategy_id.as_deref().unwrap_or("elephant_edge_v3");
    let pipeline = match build_strategy_pipeline(
        strategy_id,
        LoopEntryTriggerMode::Auto,
        sl_buffer_atr,
        req.timeframe,
        req.asset_class,
        None,
    ) {
        Ok(p) => p,
        Err(detail) => return unprocessable(detail),
    };

    let instrument = match resolve_study_instrument(&state, &parsed_id).await {
        Ok(inst) => inst,
        Err(resp) => return resp,
    };
    let canonical_id = instrument.id().clone();
    let (lot_size, tick_size) = resolve_lot_and_tick(&instrument, req.lot_size, req.tick_size);

    let raw_bars = match load_study_bars_for_timeframe(
        &state,
        &canonical_id,
        start_date,
        end_date,
        req.timeframe,
    )
    .await
    {
        Ok(b) => b,
        Err(resp) => return resp,
    };

    let (live_chain, live_contract_count) = load_live_chain_quotes(&state, &canonical_id).await;
    let live_spot = read_live_spot(&state, &canonical_id).await;
    let domain_asset = to_domain_asset_class(req.asset_class);
    let spread_cost = SpreadCostConfig {
        mode: tantra_market_finance::studies::RollBasisMode::NetAfterSpreadAndCosts,
        spread_slippage_bps,
        futures_roll_basis_bps: Decimal::ZERO,
        include_statutory_costs: req.include_statutory_costs,
    };

    let cal_inner = match state.lakehouse_root() {
        Some(root) => NseTradingCalendar::from_data_root(root),
        None => NseTradingCalendar::new(),
    };
    let calendar = RuntimeNseCalendar(cal_inner);

    let eval_req = LoopEvalRequest {
        strategy_id: pipeline.canonical_strategy_id.to_string(),
        symbol: canonical_id.to_string(),
        asset_class: domain_asset,
        session_preset: pipeline.default_session_preset,
        bars: raw_bars.clone(),
        horizon_bars,
        target_r: Decimal::from(3),
        atr_period,
        sl_buffer_atr,
        expiry_horizon: to_domain_horizon(req.expiry_horizon),
        spread_cost: spread_cost.clone(),
        live_chain: live_chain.clone(),
    };

    let stop_policy = OuterBufferStopPolicy::new(StopPolicyMode::OuterAtrBuffer {
        buffer_atr: sl_buffer_atr,
    });
    let hedge_policy = HedgeSynthesizer;

    let report = match StrategyLoopKernel::evaluate(
        &eval_req,
        &calendar,
        pipeline.zone_extractor.as_ref(),
        pipeline.entry_filter.as_ref(),
        &stop_policy,
        &hedge_policy,
    ) {
        Ok(r) => r,
        Err(e) => return map_study_err(e),
    };

    let clean = SplitArtifactGuard::default().clean(&raw_bars);
    let clean_bars = &clean.adjusted_bars;
    let (spot_price, atr) = latest_spot_and_atr(clean_bars, atr_period, live_spot);

    let resolved_dir = req
        .direction
        .map(to_domain_dir)
        .or_else(|| report.signals.last().map(|s| s.direction));

    let synthesize_for = |horizon: LoopExpiryHorizon| -> HedgeRecommendationDto {
        let h = hedge_policy.synthesize(&HedgeSynthesisInput {
            symbol: canonical_id.symbol.clone(),
            asset_class: domain_asset,
            spot: spot_price,
            atr,
            horizon: to_domain_horizon(horizon),
            direction: resolved_dir,
            zones: Some(&report.zones),
            penetration: Some(&report.penetration),
            iv_gt_rv: req.iv_gt_rv,
            trap_rate_pct: report.kpis.false_sweep_stop_rate_pct,
            live_chain: &live_chain,
            lot_size,
            tick_size,
            capital,
            spread_cost: &spread_cost,
        });
        map_hedge_dto(&h, live_contract_count)
    };

    let recommended = synthesize_for(req.expiry_horizon);
    let all_horizons = vec![
        synthesize_for(LoopExpiryHorizon::Day),
        synthesize_for(LoopExpiryHorizon::Weekly),
        synthesize_for(LoopExpiryHorizon::Monthly),
        synthesize_for(LoopExpiryHorizon::CommodityCycle),
    ];

    let x1 = clean_bars.len().min(300).saturating_sub(1);
    let x0 = x1.saturating_sub(20).min(x1);
    let hedges = build_hedge_overlays(&recommended, x0, x1);

    success(ExpiryHedgeResponse {
        instrument_id: canonical_id.to_string(),
        symbol: canonical_id.symbol,
        exchange: canonical_id.exchange.to_string(),
        spot_price,
        atr,
        direction: resolved_dir.map(from_domain_dir),
        live_chain_available: live_contract_count > 0,
        live_contract_count,
        recommended,
        all_horizons,
        hedges,
    })
}

// ============================================================================
// 4. Real RuntimeStrategyRunner (Replaces DefaultStrategyRunner Stubs)
// ============================================================================

type RunnerPrimitives = (
    &'static str,
    Box<dyn ZoneExtractor>,
    Box<dyn EntrySignalPrimitive>,
);

/// Production [`StrategyRunner`] implementation backed by real `AppState` market data
/// ports (`InstrumentMaster`, `BarReader`, `QuoteReader`) and `tantra-market-finance`'s
/// composable `StrategyLoopKernel`, `ParameterSweepRunner`, and `HedgeSynthesizer`.
///
/// Never fabricates prices, quotes, or bars: returns an explicit `Err` when the requested
/// instrument or historical bars are absent.
#[derive(Clone)]
pub struct RuntimeStrategyRunner {
    instruments: Arc<dyn InstrumentMaster>,
    bars: Arc<dyn BarReader>,
    quotes: Arc<dyn QuoteReader>,
    lakehouse_root: Option<PathBuf>,
}

impl RuntimeStrategyRunner {
    /// Constructs a real [`StrategyRunner`] wired to the given runtime read ports.
    pub fn new(
        instruments: Arc<dyn InstrumentMaster>,
        bars: Arc<dyn BarReader>,
        quotes: Arc<dyn QuoteReader>,
        lakehouse_root: Option<PathBuf>,
    ) -> Self {
        Self {
            instruments,
            bars,
            quotes,
            lakehouse_root,
        }
    }

    async fn resolve_instrument(&self, raw_symbol: &str) -> Result<Instrument, String> {
        let trimmed = raw_symbol.trim();
        if trimmed.is_empty() {
            return Err("symbol cannot be empty".to_string());
        }

        if let Ok(parsed) = parse_study_instrument_id(trimmed) {
            if let Ok(Some(inst)) = self.instruments.get_instrument(&parsed).await {
                return Ok(inst);
            }
            if let Ok(all) = self.instruments.list_instruments().await {
                if let Some(found) = all.into_iter().find(|item| {
                    item.id().exchange == parsed.exchange
                        && item.id().symbol.eq_ignore_ascii_case(&parsed.symbol)
                }) {
                    return Ok(found);
                }
            }
        }

        // Bare symbol fallback (e.g. "RELIANCE" or "NIFTYBEES")
        let all = self
            .instruments
            .list_instruments()
            .await
            .map_err(|e| format!("failed to list instruments: {e:?}"))?;
        let upper = trimmed.to_ascii_uppercase();
        all.into_iter()
            .find(|item| {
                item.id().symbol.eq_ignore_ascii_case(&upper)
                    || item.id().to_string().eq_ignore_ascii_case(&upper)
            })
            .ok_or_else(|| format!("instrument {raw_symbol:?} not found in market catalog"))
    }

    async fn load_daily_bars(&self, canonical_id: &InstrumentId) -> Result<Vec<StudyBar>, String> {
        let end_date = Utc::now().date_naive();
        let start_date = end_date - Duration::days(365 * 5);
        let from_utc: DateTime<Utc> = (start_date - Duration::days(1))
            .and_hms_opt(0, 0, 0)
            .unwrap()
            .and_utc();
        let to_utc: DateTime<Utc> = (end_date + Duration::days(2))
            .and_hms_opt(0, 0, 0)
            .unwrap()
            .and_utc();
        let spec = BarSpecification::new(1, BarAggregation::Day, PriceType::Last);
        let bar_req = BarRequest::new(
            canonical_id.clone(),
            spec,
            Some(UnixNanos::from_datetime(from_utc)),
            Some(UnixNanos::from_datetime(to_utc)),
        )
        .map_err(|e| format!("invalid bar request: {e:?}"))?;
        let raw_bars = self
            .bars
            .read_bars(&bar_req)
            .await
            .map_err(|e| format!("failed to read bars for {canonical_id}: {e:?}"))?;

        let ist_offset = chrono::FixedOffset::east_opt(5 * 3600 + 30 * 60).unwrap();
        let mut dedup: BTreeMap<NaiveDate, StudyBar> = BTreeMap::new();
        for bar in raw_bars {
            let d = bar
                .ts_event()
                .to_datetime()
                .with_timezone(&ist_offset)
                .date_naive();
            dedup.insert(
                d,
                StudyBar {
                    date: d,
                    minute_of_day: None,
                    open: Decimal::from_f64_retain(bar.open())
                        .unwrap_or_default()
                        .round_dp(4),
                    high: Decimal::from_f64_retain(bar.high())
                        .unwrap_or_default()
                        .round_dp(4),
                    low: Decimal::from_f64_retain(bar.low())
                        .unwrap_or_default()
                        .round_dp(4),
                    close: Decimal::from_f64_retain(bar.close())
                        .unwrap_or_default()
                        .round_dp(4),
                    volume: Decimal::from_f64_retain(bar.volume())
                        .unwrap_or_default()
                        .round_dp(2),
                    dividend: Decimal::ZERO,
                    split_factor: Decimal::ZERO,
                },
            );
        }

        let bars: Vec<StudyBar> = dedup.into_values().collect();
        if bars.is_empty() {
            return Err(format!(
                "no daily bars available for {canonical_id}; cannot run strategy without market data"
            ));
        }
        Ok(bars)
    }

    async fn read_spot(&self, canonical_id: &InstrumentId) -> Option<Decimal> {
        if let Ok(Some(q)) = self.quotes.read_quote(canonical_id, None).await {
            let bid = Decimal::from_f64_retain(q.bid_price()).unwrap_or_default();
            let ask = Decimal::from_f64_retain(q.ask_price()).unwrap_or_default();
            let mid = if bid > Decimal::ZERO && ask > Decimal::ZERO {
                (bid + ask) / Decimal::from(2)
            } else {
                bid.max(ask)
            };
            if mid > Decimal::ZERO {
                return Some(mid.round_dp(2));
            }
        }
        None
    }

    async fn load_chain(&self, canonical_id: &InstrumentId) -> Vec<LiveOptionStrikeQuote> {
        let reader = InstrumentChainReader::new(self.instruments.clone(), self.quotes.clone());
        match reader.options_chain(canonical_id, None, Some(200)).await {
            Ok(contracts) => contracts
                .into_iter()
                .map(|c| LiveOptionStrikeQuote {
                    option_type: match c.option_type {
                        tantra_core::OptionType::Call => "CE".to_string(),
                        tantra_core::OptionType::Put => "PE".to_string(),
                    },
                    strike: c.strike.value(),
                    expiry: c.expiry,
                    lot_size: c.lot_size.value(),
                    bid: c.bid.map(|p| p.value()),
                    ask: c.ask.map(|p| p.value()),
                    last_price: c.last_price.map(|p| p.value()),
                    open_interest: c.open_interest.map(|q| q.value()),
                })
                .collect(),
            Err(_) => Vec::new(),
        }
    }

    fn calendar(&self) -> RuntimeNseCalendar {
        let cal = match &self.lakehouse_root {
            Some(root) => NseTradingCalendar::from_data_root(root),
            None => NseTradingCalendar::new(),
        };
        RuntimeNseCalendar(cal)
    }

    fn infer_asset_class(inst: &Instrument) -> DomainAssetClass {
        match inst.id().exchange {
            Exchange::Mcx => DomainAssetClass::Commodity,
            Exchange::Cds => DomainAssetClass::Forex,
            _ => match inst.kind() {
                tantra_core::InstrumentKind::Index => DomainAssetClass::Index,
                _ => DomainAssetClass::FnoStock,
            },
        }
    }

    fn resolve_runner_primitives(
        strategy: &str,
        sl_buffer_atr: Decimal,
    ) -> Result<RunnerPrimitives, String> {
        match strategy.trim().to_ascii_lowercase().as_str() {
            "elephant_edge_v3" | "elephant_edge" | "ee" | "ee+" | "ee_plus" => Ok((
                "elephant_edge_v3",
                Box::new(ElephantZonesExtractor::default().with_sl_buffer_atr(sl_buffer_atr)),
                Box::new(RejectionWickFilter::default()),
            )),
            "orb_15" | "orb15" | "opening_range_breakout" | "turtle_breakout" => Ok((
                "orb_15",
                Box::new(Orb15Extractor {
                    opening_range_minutes: 15,
                    sl_buffer_atr,
                }),
                Box::new(OrbBreakoutEntryFilter),
            )),
            "sma_crossover" | "rsi_reversal" => Ok((
                "elephant_edge_v3",
                Box::new(ElephantZonesExtractor::default().with_sl_buffer_atr(sl_buffer_atr)),
                Box::new(ZoneTouchEntryFilter),
            )),
            other => Err(format!(
                "unsupported strategy {other:?}; expected elephant_edge_v3, orb_15, turtle_breakout, sma_crossover, or rsi_reversal"
            )),
        }
    }
}

#[async_trait]
impl StrategyRunner for RuntimeStrategyRunner {
    async fn run_backtest(
        &self,
        strategy: &str,
        symbol: &str,
        capital: Decimal,
        params: Option<BTreeMap<String, Decimal>>,
    ) -> Result<Value, String> {
        let base_capital = if capital > Decimal::ZERO {
            capital
        } else {
            Decimal::from(500_000)
        };
        let inst = self.resolve_instrument(symbol).await?;
        let canonical_id = inst.id().clone();
        let bars = self.load_daily_bars(&canonical_id).await?;
        let live_chain = self.load_chain(&canonical_id).await;
        let asset_class = Self::infer_asset_class(&inst);

        let sl_buffer_atr = params
            .as_ref()
            .and_then(|p| p.get("sl_buffer_atr").copied())
            .unwrap_or_else(|| Decimal::new(25, 2))
            .clamp(Decimal::ZERO, Decimal::from(3));
        let horizon_bars = params
            .as_ref()
            .and_then(|p| p.get("horizon_bars").copied())
            .and_then(|d| d.to_usize())
            .unwrap_or(5)
            .clamp(1, 120);
        let target_r = params
            .as_ref()
            .and_then(|p| p.get("target_r").copied())
            .unwrap_or_else(|| Decimal::from(3))
            .clamp(Decimal::ONE, Decimal::from(20));

        let (canonical_strategy, zone_extractor, entry_filter) =
            Self::resolve_runner_primitives(strategy, sl_buffer_atr)?;
        let calendar = self.calendar();
        let stop_mode = if sl_buffer_atr.is_zero() {
            StopPolicyMode::BandOuterEdge
        } else {
            StopPolicyMode::OuterAtrBuffer {
                buffer_atr: sl_buffer_atr,
            }
        };
        let stop_policy = OuterBufferStopPolicy::new(stop_mode);
        let hedge_policy = HedgeSynthesizer;

        let req = LoopEvalRequest {
            strategy_id: canonical_strategy.to_string(),
            symbol: canonical_id.to_string(),
            asset_class,
            session_preset: DomainSessionPreset::DailyMonthly,
            bars,
            horizon_bars,
            target_r,
            atr_period: 14,
            sl_buffer_atr,
            expiry_horizon: DomainHorizon::Weekly,
            spread_cost: SpreadCostConfig::default(),
            live_chain,
        };

        let report = StrategyLoopKernel::evaluate(
            &req,
            &calendar,
            zone_extractor.as_ref(),
            entry_filter.as_ref(),
            &stop_policy,
            &hedge_policy,
        )
        .map_err(|e| e.to_string())?;

        // Convert R-space performance using 1% equity risk per trade into INR P&L & % return
        let risk_fraction = Decimal::new(1, 2); // 1% risk per trade
        let mut equity = base_capital;
        let mut peak = base_capital;
        let mut max_dd_pct = Decimal::ZERO;
        for t in &report.trades {
            let trade_pnl = equity * risk_fraction * t.net_r;
            equity = (equity + trade_pnl).max(Decimal::ONE);
            if equity > peak {
                peak = equity;
            }
            if peak > Decimal::ZERO {
                let dd = ((peak - equity) / peak * Decimal::from(100)).round_dp(2);
                if dd > max_dd_pct {
                    max_dd_pct = dd;
                }
            }
        }
        let net_pnl = (equity - base_capital).round_dp(2);
        let total_return_pct = (net_pnl / base_capital * Decimal::from(100)).round_dp(2);

        Ok(json!({
            "strategy": strategy,
            "canonical_strategy": canonical_strategy,
            "symbol": canonical_id.to_string(),
            "initial_capital": base_capital,
            "final_equity": equity.round_dp(2),
            "net_pnl": net_pnl,
            "total_return_pct": total_return_pct,
            "sharpe_ratio": report.kpis.sharpe_ratio,
            "max_drawdown_pct": max_dd_pct,
            "total_trades": report.kpis.trades,
            "win_rate_pct": report.kpis.win_rate_pct,
            "profit_factor": report.kpis.net_profit_factor,
            "loop_kpis": report.kpis,
            "penetration": report.penetration,
            "chain_status": report.hedge.chain_status,
        }))
    }

    async fn tune_strategy(
        &self,
        strategy: &str,
        symbol: &str,
        capital: Decimal,
        metric: &str,
    ) -> Result<Value, String> {
        let base_capital = if capital > Decimal::ZERO {
            capital
        } else {
            Decimal::from(500_000)
        };
        let inst = self.resolve_instrument(symbol).await?;
        let canonical_id = inst.id().clone();
        let bars = self.load_daily_bars(&canonical_id).await?;
        let live_chain = self.load_chain(&canonical_id).await;
        let asset_class = Self::infer_asset_class(&inst);

        let default_sl = Decimal::new(25, 2);
        let (canonical_strategy, zone_extractor, entry_filter) =
            Self::resolve_runner_primitives(strategy, default_sl)?;
        let calendar = self.calendar();
        let hedge_policy = HedgeSynthesizer;

        let base_req = LoopEvalRequest {
            strategy_id: canonical_strategy.to_string(),
            symbol: canonical_id.to_string(),
            asset_class,
            session_preset: DomainSessionPreset::DailyMonthly,
            bars,
            horizon_bars: 5,
            target_r: Decimal::from(3),
            atr_period: 14,
            sl_buffer_atr: default_sl,
            expiry_horizon: DomainHorizon::Weekly,
            spread_cost: SpreadCostConfig::default(),
            live_chain,
        };

        let baseline = StrategyLoopKernel::evaluate(
            &base_req,
            &calendar,
            zone_extractor.as_ref(),
            entry_filter.as_ref(),
            &OuterBufferStopPolicy::new(StopPolicyMode::OuterAtrBuffer {
                buffer_atr: default_sl,
            }),
            &hedge_policy,
        )
        .map_err(|e| e.to_string())?;

        let sweep_req = ParameterSweepRequest {
            base: base_req,
            bounds: StrategyParameterBounds {
                sl_buffer_atr_grid: vec![
                    Decimal::ZERO,
                    Decimal::new(15, 2),
                    Decimal::new(25, 2),
                    Decimal::new(50, 2),
                ],
                horizon_bars_grid: vec![3, 5, 10],
                target_r_grid: vec![Decimal::from(2), Decimal::from(3), Decimal::from(5)],
                max_candidates: 36,
            },
        };

        let mut report = ParameterSweepRunner::run(
            &sweep_req,
            &calendar,
            zone_extractor.as_ref(),
            entry_filter.as_ref(),
            &hedge_policy,
        )
        .map_err(|e| e.to_string())?;

        let best = if metric.trim().eq_ignore_ascii_case("sharpe") {
            report
                .candidates
                .iter()
                .max_by(|a, b| a.oos_kpis.sharpe_ratio.cmp(&b.oos_kpis.sharpe_ratio))
                .cloned()
                .unwrap_or_else(|| report.winner.clone())
        } else {
            report.winner.clone()
        };
        report.winner = best.clone();

        let verdict = if best.deflated_sharpe_ratio > Decimal::ZERO
            && best.walk_forward_efficiency_pct >= Decimal::from(50)
        {
            format!(
                "Low Overfitting Risk (DSR {:.2}, WFE {:.1}% across {} trials)",
                best.deflated_sharpe_ratio, best.walk_forward_efficiency_pct, report.trial_count
            )
        } else {
            format!(
                "Caution: Mixed OOS Stability (DSR {:.2}, WFE {:.1}% across {} trials)",
                best.deflated_sharpe_ratio, best.walk_forward_efficiency_pct, report.trial_count
            )
        };

        let max_drawdown_pct = (best.full_kpis.max_drawdown_r).round_dp(2);

        Ok(json!({
            "strategy": strategy,
            "canonical_strategy": canonical_strategy,
            "symbol": canonical_id.to_string(),
            "capital": base_capital,
            "metric": metric,
            "default_params": {
                "sl_buffer_atr": default_sl,
                "horizon_bars": 5,
                "target_r": Decimal::from(3),
            },
            "tuned_params": {
                "sl_buffer_atr": best.sl_buffer_atr,
                "horizon_bars": best.horizon_bars,
                "target_r": best.target_r,
            },
            "default_sharpe": baseline.kpis.sharpe_ratio,
            "tuned_sharpe": best.full_kpis.sharpe_ratio,
            "deflated_sharpe": best.deflated_sharpe_ratio,
            "walk_forward_efficiency_pct": best.walk_forward_efficiency_pct,
            "max_drawdown_pct": max_drawdown_pct,
            "win_rate_pct": best.full_kpis.win_rate_pct,
            "total_trials": report.trial_count,
            "overfitting_verdict": verdict,
        }))
    }

    async fn generate_trade_suggestion(
        &self,
        strategy: &str,
        symbol: &str,
        capital: Decimal,
        risk_pct: Decimal,
    ) -> Result<Value, String> {
        let base_capital = if capital > Decimal::ZERO {
            capital
        } else {
            Decimal::from(500_000)
        };
        let base_risk = if risk_pct > Decimal::ZERO {
            risk_pct
        } else {
            Decimal::ONE
        };

        let inst = self.resolve_instrument(symbol).await?;
        let canonical_id = inst.id().clone();
        let (lot_size, tick_size) = resolve_lot_and_tick(&inst, None, None);
        let bars = self.load_daily_bars(&canonical_id).await?;
        let live_chain = self.load_chain(&canonical_id).await;
        let live_spot = self.read_spot(&canonical_id).await;
        let asset_class = Self::infer_asset_class(&inst);

        let sl_buffer_atr = Decimal::new(25, 2);
        let (canonical_strategy, zone_extractor, entry_filter) =
            Self::resolve_runner_primitives(strategy, sl_buffer_atr)?;
        let calendar = self.calendar();
        let stop_policy = OuterBufferStopPolicy::new(StopPolicyMode::OuterAtrBuffer {
            buffer_atr: sl_buffer_atr,
        });
        let hedge_policy = HedgeSynthesizer;

        let req = LoopEvalRequest {
            strategy_id: canonical_strategy.to_string(),
            symbol: canonical_id.to_string(),
            asset_class,
            session_preset: DomainSessionPreset::DailyMonthly,
            bars: bars.clone(),
            horizon_bars: 5,
            target_r: Decimal::from(3),
            atr_period: 14,
            sl_buffer_atr,
            expiry_horizon: DomainHorizon::Weekly,
            spread_cost: SpreadCostConfig::default(),
            live_chain: live_chain.clone(),
        };

        let report = StrategyLoopKernel::evaluate(
            &req,
            &calendar,
            zone_extractor.as_ref(),
            entry_filter.as_ref(),
            &stop_policy,
            &hedge_policy,
        )
        .map_err(|e| e.to_string())?;

        let clean = SplitArtifactGuard::default().clean(&bars);
        let clean_bars = &clean.adjusted_bars;
        let (spot, atr) = latest_spot_and_atr(clean_bars, 14, live_spot);

        let last_sig = report.signals.last().cloned();
        let direction = last_sig.as_ref().map(|s| s.direction);
        let is_short = matches!(direction, Some(SignalDirection::Short));

        let entry_price = last_sig
            .as_ref()
            .map(|s| s.entry_price)
            .unwrap_or(spot)
            .max(Decimal::ONE);
        let stop_loss = last_sig
            .as_ref()
            .map(|s| stop_policy.stop_price(s))
            .unwrap_or_else(|| {
                if is_short {
                    (entry_price + atr * Decimal::new(125, 2)).round_dp(2)
                } else {
                    (entry_price - atr * Decimal::new(125, 2))
                        .max(Decimal::new(5, 2))
                        .round_dp(2)
                }
            });

        let per_unit_risk = (entry_price - stop_loss)
            .abs()
            .max(atr * Decimal::new(25, 2))
            .max(Decimal::new(5, 2));
        let target_1 = if is_short {
            (entry_price - per_unit_risk * Decimal::from(2)).max(Decimal::new(5, 2))
        } else {
            entry_price + per_unit_risk * Decimal::from(2)
        }
        .round_dp(2);
        let target_2 = if is_short {
            (entry_price - per_unit_risk * Decimal::from(3)).max(Decimal::new(5, 2))
        } else {
            entry_price + per_unit_risk * Decimal::from(3)
        }
        .round_dp(2);

        let risk_amt = (base_capital * (base_risk / Decimal::from(100))).round_dp(2);
        let raw_units = (risk_amt / per_unit_risk).floor();
        let lots = if lot_size > Decimal::ONE {
            (raw_units / lot_size).floor().max(Decimal::ONE)
        } else {
            raw_units.max(Decimal::ONE)
        };
        let qty = (lots * lot_size).round_dp(0);
        let notional = (qty * entry_price).round_dp(2);
        let margin_util = ((notional / base_capital) * Decimal::from(100)).round_dp(2);

        let stat_bps = estimate_statutory_roundtrip_bps(&canonical_id.symbol, asset_class);
        let total_charges = (notional * stat_bps / Decimal::from(10_000)).round_dp(2);
        let gross_reward = per_unit_risk * Decimal::from(2) * qty;
        let net_rr = if risk_amt > Decimal::ZERO {
            ((gross_reward - total_charges) / risk_amt)
                .max(Decimal::ZERO)
                .round_dp(2)
        } else {
            Decimal::from(2)
        };

        let hedge = hedge_policy.synthesize(&HedgeSynthesisInput {
            symbol: canonical_id.symbol.clone(),
            asset_class,
            spot,
            atr,
            horizon: DomainHorizon::Weekly,
            direction,
            zones: Some(&report.zones),
            penetration: Some(&report.penetration),
            iv_gt_rv: false,
            trap_rate_pct: report.kpis.false_sweep_stop_rate_pct,
            live_chain: &live_chain,
            lot_size,
            tick_size,
            capital: base_capital,
            spread_cost: &SpreadCostConfig::default(),
        });

        let bias = if is_short {
            "SELL / SHORT"
        } else {
            "BUY / LONG"
        };
        let reason = last_sig
            .as_ref()
            .map(|s| s.hint.clone())
            .unwrap_or_else(|| {
                format!("Zone-anchored {canonical_strategy} setup on {canonical_id} (ATR {atr:.2})")
            });

        Ok(json!({
            "strategy": strategy,
            "canonical_strategy": canonical_strategy,
            "symbol": canonical_id.to_string(),
            "signal": {
                "bias": bias,
                "reason": reason,
                "entry_price": entry_price.round_dp(2),
                "stop_loss": stop_loss.round_dp(2),
                "target_1": target_1,
                "target_2": target_2,
                "risk_reward_ratio": Decimal::from(2),
            },
            "sizing": {
                "account_capital": base_capital,
                "risk_pct": base_risk,
                "risk_amount": risk_amt,
                "quantity": qty,
                "lot_size": lot_size,
                "notional_value": notional,
                "margin_utilization_pct": margin_util,
            },
            "statutory_charges": {
                "statutory_bps": stat_bps,
                "total_charges": total_charges,
                "net_risk_reward_ratio": net_rr,
            },
            "hedge": map_hedge_dto(&hedge, live_chain.len()),
        }))
    }
}

/// In-process [`StrategyLoopPort`] that invokes the `/studies/strategy-loop/*` handlers
/// directly (no HTTP hop). Holds a *weak*, late-bound `AppState` handle: `AppState` owns the
/// `ChatRuntime`, which owns the tool registry, which owns this port, so a strong `Arc` would
/// form a reference cycle. The cell is filled by `ChatRuntime::bind_state` when the router is
/// built (root plan 0015 T21.6 / agent plan 0018).
pub struct RuntimeStrategyLoopPort {
    state: Arc<OnceLock<Weak<AppState>>>,
}

/// Upper bound on a handler response body read by the port.
const PORT_MAX_BODY_BYTES: usize = 32 * 1024 * 1024;

impl RuntimeStrategyLoopPort {
    /// Creates a port over a late-bound state cell.
    pub fn new(cell: Arc<OnceLock<Weak<AppState>>>) -> Self {
        Self { state: cell }
    }

    fn state(&self) -> Result<Arc<AppState>, StrategyLoopPortError> {
        self.state.get().and_then(Weak::upgrade).ok_or_else(|| {
            StrategyLoopPortError::unavailable("strategy loop engine not bound to a running server")
        })
    }

    fn convert<A: serde::Serialize, R: serde::de::DeserializeOwned>(
        req: &A,
    ) -> Result<R, StrategyLoopPortError> {
        let v = serde_json::to_value(req)
            .map_err(|e| StrategyLoopPortError::invalid_request(e.to_string()))?;
        serde_json::from_value(v).map_err(|e| StrategyLoopPortError::invalid_request(e.to_string()))
    }

    async fn unwrap_response(resp: Response) -> Result<Value, StrategyLoopPortError> {
        let status = resp.status();
        let bytes = axum::body::to_bytes(resp.into_body(), PORT_MAX_BODY_BYTES)
            .await
            .map_err(|e| StrategyLoopPortError::upstream(format!("response body: {e}")))?;
        let mut body: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
        if status.is_success() {
            return match body.get_mut("data") {
                Some(data) => Ok(data.take()),
                None => Err(StrategyLoopPortError::upstream(
                    "strategy loop response missing `data`",
                )),
            };
        }
        let message = body
            .pointer("/error/message")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .unwrap_or_else(|| status.to_string());
        Err(match status.as_u16() {
            400 | 422 => StrategyLoopPortError::invalid_request(message),
            404 => StrategyLoopPortError::not_found(message),
            429 => StrategyLoopPortError::busy(message),
            503 => StrategyLoopPortError::unavailable(message),
            408 | 504 => StrategyLoopPortError::timeout(message),
            _ => StrategyLoopPortError::upstream(message),
        })
    }
}

#[async_trait]
impl StrategyLoopPort for RuntimeStrategyLoopPort {
    async fn evaluate(&self, req: StrategyLoopEvalRequest) -> Result<Value, StrategyLoopPortError> {
        let state = self.state()?;
        let rt: StrategyLoopRequest = Self::convert(&req)?;
        Self::unwrap_response(post_evaluate(State(state), crate::ApiJson(rt)).await).await
    }

    async fn sweep(&self, req: StrategyLoopSweepRequest) -> Result<Value, StrategyLoopPortError> {
        let state = self.state()?;
        let rt: StrategySweepRequest = Self::convert(&req)?;
        Self::unwrap_response(post_sweep(State(state), crate::ApiJson(rt)).await).await
    }

    async fn recommend_hedge(
        &self,
        req: StrategyHedgeRequest,
    ) -> Result<Value, StrategyLoopPortError> {
        let state = self.state()?;
        let rt: ExpiryHedgeRequest = Self::convert(&req)?;
        Self::unwrap_response(post_recommend_hedge(State(state), crate::ApiJson(rt)).await).await
    }
}
