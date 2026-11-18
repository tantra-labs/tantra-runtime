//! REST handlers for `Experimental-Analysis` (`/experimental-analysis/catalog`,
//! `/experimental-analysis/returns`, `/experimental-analysis/intraday`, and
//! `/experimental-analysis/correlation`).

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::Response;
use chrono::{DateTime, Duration, FixedOffset, NaiveDate, Timelike as _, Utc};
use serde_json::json;
use tantra_core::rust_decimal::Decimal;
use tantra_core::{Exchange, InstrumentId};
use tantra_market_finance::calendar::{MarketCalendar, Session};
use tantra_market_finance::studies::{
    self, ExperimentalStudyConfig, IntradayBar, OhlcvBar, OrbConfig,
    PeriodScheduleKind as DomainSchedule, ReturnCalcKind as DomainCalc,
    RollBasisMode as DomainRollMode, SpreadCostConfig, StudyAssetClass as DomainAssetClass,
    StudyError, SubCycleKind as DomainSubCycle,
};
use async_trait::async_trait;
use tantra_agent_chat_tools::ports::error::PortError;
use tantra_agent_chat_tools::ports::study::{
    StudyProbabilityLevel, StudyReader, StudySummarySnapshot,
};
use tantra_market_store::{
    CorporateActionsProjectionReader, EconomicCalendarProjectionReader, FilingsProjectionReader,
    InstrumentChainReader, NewsProjectionReader,
};
use tantra_runtime_api::{
    AssetClassCatalogGroup, CatalogInstrumentEntry, CycleProgressAnalysis, CycleProgressDecile,
    DailyOhlcRow, DailyOhlcSummary, DataSourceStatus, ErrorCode, ErrorDetail,
    ExperimentalCatalogQuery, ExperimentalCatalogResponse, ExperimentalCorrelationRequest,
    ExperimentalCorrelationResponse, ExperimentalIntradayRequest, ExperimentalIntradayResponse,
    ExperimentalReturnsRequest, ExperimentalReturnsResponse, ForwardConditionEvent,
    ForwardConditionalGroup, FrictionSummary, GapAnalysis, GapBucketRow, GapEventRow,
    GreenVsRedComparison, IntradayMovementAnalysis, IntradaySlotStat, MagnitudePdfBucket,
    MarkovAnalysis, MarkovCurrentPrediction, MarkovState, MarkovStateProbability,
    MarkovTransitionRow, OrbBacktestResult, OrbEquityPoint, OrbTradeRow, OutlierAnalysis,
    OutlierKpis, OutlierPeriodPoint, OutlierStatus, PeriodPoint, PeriodScheduleKind, PolarityStats,
    ProbabilityLevelRow, ReturnBucketRow, ReturnCalcKind, ReturnKpis, ReturnPercentiles,
    ExperimentalEventStudyRequest, ExperimentalEventStudyResponse, ExperimentalForecastRequest,
    ExperimentalForecastResponse, ExperimentalOptionsOverlayRequest,
    ExperimentalOptionsOverlayResponse, ExperimentalOutlierNewsRequest,
    ExperimentalOutlierNewsResponse, AnalogEpisode, EventStudyRow, EventWindowPoint,
    ForecastHorizonStep, OptionLegSpec, OptionOverlayStrategy, OutlierNewsItem,
    OverlayEquityPoint, RollBasisMode, RollingFiveDayAnalysis, RollingFiveDayPoint,
    RollingPairPoint, ScheduleOption, SideStreakStats, StreakAnalysis, StreakDetail,
    StreakDirection, StreakLengthBucket, StreakSummary, StrikeLabRow, StudyAssetClass,
    StudyEventKind, SubCycleKind, VolatilityRegime, wire::InstrumentKind,
};
use tantra_runtime_backtest::NseTradingCalendar;

use crate::market::{
    ApiQuery, failure, instrument_not_found, port_failure, success, unprocessable,
};
use crate::market_feed::ports::{
    BarReader, BarRequest, ChainReader as _, InstrumentMaster, QuoteReader, TradeReader as _,
};
use crate::market_feed::wire::{BarAggregation, BarSpecification, PriceType, UnixNanos};
use crate::{ApiJson, AppState};

struct RuntimeNseCalendar(NseTradingCalendar);

impl MarketCalendar for RuntimeNseCalendar {
    fn session(&self) -> Session {
        Session::regular()
    }

    fn is_holiday(&self, date: NaiveDate) -> bool {
        self.0.is_holiday(date)
    }

    fn is_trading_day(&self, date: NaiveDate) -> bool {
        self.0.is_trading_day(date)
    }
}

fn invalid_field(field: &str, reason: &str, message: impl Into<String>) -> ErrorDetail {
    ErrorDetail::new(ErrorCode::ValidationInvalidRequest, message)
        .with_context(json!({ "field": field, "reason": reason }))
}

fn parse_study_instrument_id(raw: &str) -> Result<InstrumentId, ErrorDetail> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(invalid_field(
            "instrument_id",
            "missing_instrument_id",
            "instrument_id cannot be empty",
        ));
    }
    if let Some((ex_str, sym_str)) = trimmed.split_once(':') {
        let ex_str = ex_str.trim();
        let sym_str = sym_str.trim();
        if !ex_str.is_empty() && !sym_str.is_empty() {
            if let Ok(exchange) = Exchange::from_str(ex_str) {
                return Ok(InstrumentId::new(exchange, sym_str));
            }
            return Err(invalid_field(
                "instrument_id",
                "invalid_exchange",
                format!("unknown exchange {ex_str:?} in instrument_id {trimmed:?}"),
            ));
        }
    }
    tantra_runtime_api::parse_instrument_id(trimmed).map_err(|_| {
        invalid_field(
            "instrument_id",
            "invalid_instrument_id",
            format!("instrument_id {trimmed:?} must be EXCHANGE:SYMBOL or SYMBOL.EXCHANGE"),
        )
    })
}

fn parse_date_range(start_raw: &str, end_raw: &str) -> Result<(NaiveDate, NaiveDate), ErrorDetail> {
    let start_date = match NaiveDate::parse_from_str(start_raw.trim(), "%Y-%m-%d") {
        Ok(d) => d,
        Err(_) => {
            return Err(invalid_field(
                "start_date",
                "invalid_date",
                "start_date must be YYYY-MM-DD",
            ));
        }
    };
    let end_date = match NaiveDate::parse_from_str(end_raw.trim(), "%Y-%m-%d") {
        Ok(d) => d,
        Err(_) => {
            return Err(invalid_field(
                "end_date",
                "invalid_date",
                "end_date must be YYYY-MM-DD",
            ));
        }
    };
    if start_date >= end_date {
        return Err(invalid_field(
            "end_date",
            "empty_range",
            "start_date must be strictly before end_date",
        ));
    }
    Ok((start_date, end_date))
}

async fn resolve_study_instrument(
    state: &AppState,
    parsed_id: &InstrumentId,
) -> Result<crate::market_feed::wire::Instrument, Response> {
    match state.instruments.get_instrument(parsed_id).await {
        Ok(Some(inst)) => Ok(inst),
        Ok(None) => match state.instruments.list_instruments().await {
            Ok(all) => match all.into_iter().find(|item| {
                item.id().exchange == parsed_id.exchange
                    && item.id().symbol.eq_ignore_ascii_case(&parsed_id.symbol)
            }) {
                Some(found) => Ok(found),
                None => Err(instrument_not_found(parsed_id)),
            },
            Err(err) => Err(port_failure(err)),
        },
        Err(err) => Err(port_failure(err)),
    }
}

async fn load_daily_study_bars(
    state: &AppState,
    canonical_id: &InstrumentId,
    start_date: NaiveDate,
    end_date: NaiveDate,
    with_corporate_actions: bool,
) -> Result<Vec<OhlcvBar>, Response> {
    let from_utc: DateTime<Utc> = (start_date - Duration::days(1))
        .and_hms_opt(0, 0, 0)
        .unwrap()
        .and_utc();
    let to_utc: DateTime<Utc> = (end_date + Duration::days(2))
        .and_hms_opt(0, 0, 0)
        .unwrap()
        .and_utc();
    let spec = BarSpecification::new(1, BarAggregation::Day, PriceType::Last);
    let bar_req = match BarRequest::new(
        canonical_id.clone(),
        spec,
        Some(UnixNanos::from_datetime(from_utc)),
        Some(UnixNanos::from_datetime(to_utc)),
    ) {
        Ok(r) => r,
        Err(err) => return Err(port_failure(err)),
    };
    let raw_bars = match state.bars.read_bars(&bar_req).await {
        Ok(b) => b,
        Err(err) => return Err(port_failure(err)),
    };

    let mut dividends: HashMap<NaiveDate, Decimal> = HashMap::new();
    let mut splits: HashMap<NaiveDate, Decimal> = HashMap::new();
    if with_corporate_actions && let Some(root) = state.lakehouse_root() {
        let ca_reader = CorporateActionsProjectionReader::new(root);
        if let Ok(actions) = ca_reader.read() {
            let id_upper = canonical_id.to_string().to_ascii_uppercase();
            let sym_upper = canonical_id.symbol.to_ascii_uppercase();
            for act in actions {
                let act_upper = act.instrument.to_ascii_uppercase();
                let matches_inst = act_upper == id_upper
                    || act_upper.ends_with(&format!(":{sym_upper}"))
                    || act_upper.contains(&format!(":{sym_upper}-"));
                if !matches_inst {
                    continue;
                }
                match act.action_type.to_ascii_lowercase().as_str() {
                    "dividend" => {
                        if let Some(val) = act.per_share {
                            *dividends.entry(act.ex_date).or_insert(Decimal::ZERO) += val;
                        }
                    }
                    "split" => {
                        if let (Some(old_s), Some(new_s)) = (act.old_shares, act.new_shares)
                            && old_s > 0
                        {
                            let ratio = (Decimal::from(new_s) / Decimal::from(old_s)).round_dp(4);
                            splits.insert(act.ex_date, ratio);
                        }
                    }
                    _ => {}
                }
            }
        }
    }

    let ist_offset = FixedOffset::east_opt(5 * 3600 + 30 * 60).unwrap();
    let mut dedup_bars: BTreeMap<NaiveDate, OhlcvBar> = BTreeMap::new();
    for bar in raw_bars {
        let d = bar
            .ts_event()
            .to_datetime()
            .with_timezone(&ist_offset)
            .date_naive();
        if d < start_date || d > end_date {
            continue;
        }
        dedup_bars.insert(
            d,
            OhlcvBar {
                date: d,
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
                dividend: dividends.get(&d).copied().unwrap_or(Decimal::ZERO),
                split_factor: splits.get(&d).copied().unwrap_or(Decimal::ZERO),
            },
        );
    }

    Ok(dedup_bars.into_values().collect())
}

async fn load_5m_intraday_bars(
    state: &AppState,
    canonical_id: &InstrumentId,
    start_date: NaiveDate,
    end_date: NaiveDate,
) -> Vec<IntradayBar> {
    let from_utc: DateTime<Utc> = (start_date - Duration::days(1))
        .and_hms_opt(0, 0, 0)
        .unwrap()
        .and_utc();
    let to_utc: DateTime<Utc> = (end_date + Duration::days(2))
        .and_hms_opt(0, 0, 0)
        .unwrap()
        .and_utc();
    let spec = BarSpecification::new(5, BarAggregation::Minute, PriceType::Last);
    let Ok(bar_req) = BarRequest::new(
        canonical_id.clone(),
        spec,
        Some(UnixNanos::from_datetime(from_utc)),
        Some(UnixNanos::from_datetime(to_utc)),
    ) else {
        return Vec::new();
    };
    let Ok(raw_bars) = state.bars.read_bars(&bar_req).await else {
        return Vec::new();
    };

    let ist_offset = FixedOffset::east_opt(5 * 3600 + 30 * 60).unwrap();
    let mut dedup: BTreeMap<(NaiveDate, u16), IntradayBar> = BTreeMap::new();
    for bar in raw_bars {
        let dt_ist = bar.ts_event().to_datetime().with_timezone(&ist_offset);
        let d = dt_ist.date_naive();
        if d < start_date || d > end_date {
            continue;
        }
        let minute_of_day = (dt_ist.hour() * 60 + dt_ist.minute()) as u16;
        dedup.insert(
            (d, minute_of_day),
            IntradayBar {
                date: d,
                minute_of_day,
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
            },
        );
    }
    dedup.into_values().collect()
}

fn to_domain_asset_class(v: StudyAssetClass) -> DomainAssetClass {
    match v {
        StudyAssetClass::Index => DomainAssetClass::Index,
        StudyAssetClass::FnoStock => DomainAssetClass::FnoStock,
        StudyAssetClass::Etf => DomainAssetClass::Etf,
        StudyAssetClass::Equity => DomainAssetClass::Equity,
        StudyAssetClass::Forex => DomainAssetClass::Forex,
        StudyAssetClass::Commodity => DomainAssetClass::Commodity,
    }
}

fn to_domain_schedule(v: PeriodScheduleKind) -> DomainSchedule {
    match v {
        PeriodScheduleKind::WeeklyExpiry => DomainSchedule::WeeklyExpiry,
        PeriodScheduleKind::WeekPlusOne => DomainSchedule::WeekPlusOne,
        PeriodScheduleKind::MonthlyExpiry => DomainSchedule::MonthlyExpiry,
        PeriodScheduleKind::CalendarWeekly => DomainSchedule::CalendarWeekly,
        PeriodScheduleKind::CalendarMonthly => DomainSchedule::CalendarMonthly,
        PeriodScheduleKind::FrontMonthCycle => DomainSchedule::FrontMonthCycle,
        PeriodScheduleKind::McxExpiryCycle => DomainSchedule::McxExpiryCycle,
        PeriodScheduleKind::Daily => DomainSchedule::Daily,
    }
}

fn to_domain_sub_cycle(v: SubCycleKind) -> DomainSubCycle {
    match v {
        SubCycleKind::Current => DomainSubCycle::Current,
        SubCycleKind::PlusOne => DomainSubCycle::PlusOne,
    }
}

fn to_domain_calc(v: ReturnCalcKind) -> DomainCalc {
    match v {
        ReturnCalcKind::OpenToClose => DomainCalc::OpenToClose,
        ReturnCalcKind::HighToLow => DomainCalc::HighToLow,
        ReturnCalcKind::CloseToClose => DomainCalc::CloseToClose,
    }
}

fn to_domain_roll_mode(v: RollBasisMode) -> DomainRollMode {
    match v {
        RollBasisMode::GrossSpot => DomainRollMode::GrossSpot,
        RollBasisMode::IncludeRollBasis => DomainRollMode::IncludeRollBasis,
        RollBasisMode::NetAfterSpreadAndCosts => DomainRollMode::NetAfterSpreadAndCosts,
    }
}

fn from_domain_roll_mode(v: DomainRollMode) -> RollBasisMode {
    match v {
        DomainRollMode::GrossSpot => RollBasisMode::GrossSpot,
        DomainRollMode::IncludeRollBasis => RollBasisMode::IncludeRollBasis,
        DomainRollMode::NetAfterSpreadAndCosts => RollBasisMode::NetAfterSpreadAndCosts,
    }
}

fn to_wire_kind(k: tantra_core::InstrumentKind) -> InstrumentKind {
    match k {
        tantra_core::InstrumentKind::Equity => InstrumentKind::Equity,
        tantra_core::InstrumentKind::Future => InstrumentKind::Future,
        tantra_core::InstrumentKind::Option => InstrumentKind::Option,
        tantra_core::InstrumentKind::Index => InstrumentKind::Index,
    }
}

fn looks_like_index_or_proxy(symbol: &str) -> bool {
    let s = symbol.trim().to_ascii_uppercase();
    matches!(
        s.as_str(),
        "NIFTY"
            | "NIFTY50"
            | "NIFTY 50"
            | "BANKNIFTY"
            | "FINNIFTY"
            | "MIDCPNIFTY"
            | "NIFTYNXT50"
            | "SENSEX"
            | "BANKEX"
            | "INDIAVIX"
            | "NIFTYBEES"
            | "BANKBEES"
            | "JUNIORBEES"
            | "MID150BEES"
            | "SENSEXETF"
            | "SENSEXIETF"
            | "SETFNIF50"
            | "SETFNIFBK"
            | "SETFNN50"
            | "NIFTYETF"
            | "NIFTYIETF"
            | "MONIFTY100"
            | "MONIFTY500"
            | "NIFTY1"
            | "BANKNIFTY1"
            | "HDFCNIFTY"
            | "KOTAKNIFTY"
            | "AXISNIFTY"
            | "BSLNIFTY"
            | "GROWWNIFTY"
            | "MID150"
            | "MIDCAPETF"
            | "MIDCAPIETF"
            | "SMALLCAP"
            | "ICICIB22"
    )
}

fn index_sort_rank(item: &CatalogInstrumentEntry) -> (u8, usize, &str) {
    let kind_rank = if item.instrument_kind == InstrumentKind::Index { 0 } else { 1 };
    let sym = item.symbol.trim().to_ascii_uppercase();
    let preferred = [
        "NIFTY50",
        "BANKNIFTY",
        "FINNIFTY",
        "SENSEX",
        "NIFTYBEES",
        "BANKBEES",
        "JUNIORBEES",
        "MID150BEES",
        "SENSEXETF",
        "SETFNIF50",
        "MONIFTY100",
        "MONIFTY500",
    ];
    let pref_rank = preferred.iter().position(|p| *p == sym).unwrap_or(preferred.len());
    (kind_rank, pref_rank, item.symbol.as_str())
}

fn looks_like_etf(symbol: &str) -> bool {
    let s = symbol.trim().to_ascii_uppercase();
    s.ends_with("BEES")
        || s.ends_with("ETF")
        || s.ends_with("IETF")
        || matches!(
            s.as_str(),
            "GOLDSHARE" | "SETFNIF50" | "SETFNN50" | "SETF10GILT" | "CPSEETF" | "BHARAT22"
        )
}

fn looks_like_forex(exchange: Exchange, symbol: &str) -> bool {
    if exchange == Exchange::Cds {
        return true;
    }
    let s = symbol.trim().to_ascii_uppercase();
    s.starts_with("USDINR")
        || s.starts_with("EURINR")
        || s.starts_with("GBPINR")
        || s.starts_with("JPYINR")
        || matches!(s.as_str(), "EURUSD" | "GBPUSD" | "USDJPY" | "DXY")
}

fn looks_like_commodity(exchange: Exchange, symbol: &str) -> bool {
    if exchange == Exchange::Mcx {
        return true;
    }
    let s = symbol.trim().to_ascii_uppercase();
    matches!(
        s.as_str(),
        "GOLD" | "GOLDM" | "SILVER" | "SILVERM" | "CRUDEOIL" | "NATURALGAS" | "COPPER" | "ZINC"
    )
}

fn looks_like_known_fno_underlying(symbol: &str, fno_symbols: &BTreeSet<String>) -> bool {
    let s = symbol.trim().to_ascii_uppercase();
    if fno_symbols.contains(&s) {
        return true;
    }
    matches!(
        s.as_str(),
        "RELIANCE"
            | "TCS"
            | "HDFCBANK"
            | "ICICIBANK"
            | "INFY"
            | "SBIN"
            | "BHARTIARTL"
            | "ITC"
            | "LT"
            | "KOTAKBANK"
            | "AXISBANK"
            | "HINDUNILVR"
            | "BAJFINANCE"
            | "MARUTI"
            | "SUNPHARMA"
            | "TATAMOTORS"
            | "M&M"
            | "NTPC"
            | "POWERGRID"
            | "WIPRO"
            | "ULTRACEMCO"
            | "TITAN"
            | "ADANIENT"
            | "ADANIPORTS"
            | "TATASTEEL"
            | "JSWSTEEL"
            | "COALINDIA"
            | "ONGC"
            | "TRENT"
            | "BEL"
            | "HAL"
    )
}

fn schedules_for_asset_class(
    asset_class: StudyAssetClass,
) -> (PeriodScheduleKind, Vec<ScheduleOption>) {
    match asset_class {
        StudyAssetClass::Index => (
            PeriodScheduleKind::WeeklyExpiry,
            vec![
                ScheduleOption {
                    kind: PeriodScheduleKind::WeeklyExpiry,
                    label: "Weekly Expiry".to_string(),
                    description:
                        "Exchange weekly expiry cycle (NSE Fri→Thu / Wed→Tue post Sep 3, 2025)"
                            .to_string(),
                    is_default: true,
                },
                ScheduleOption {
                    kind: PeriodScheduleKind::WeekPlusOne,
                    label: "Week +1 Carry".to_string(),
                    description: "Two-week rollover window (Cycle k Open → Cycle k+1 Close)"
                        .to_string(),
                    is_default: false,
                },
                ScheduleOption {
                    kind: PeriodScheduleKind::MonthlyExpiry,
                    label: "Monthly Expiry".to_string(),
                    description: "Last designated expiry trading day of each month".to_string(),
                    is_default: false,
                },
                ScheduleOption {
                    kind: PeriodScheduleKind::Daily,
                    label: "Daily".to_string(),
                    description: "Individual trading session returns".to_string(),
                    is_default: false,
                },
            ],
        ),
        StudyAssetClass::FnoStock => (
            PeriodScheduleKind::MonthlyExpiry,
            vec![
                ScheduleOption {
                    kind: PeriodScheduleKind::MonthlyExpiry,
                    label: "Monthly Expiry".to_string(),
                    description: "NSE F&O monthly contract expiry cycle".to_string(),
                    is_default: true,
                },
                ScheduleOption {
                    kind: PeriodScheduleKind::WeeklyExpiry,
                    label: "Weekly Expiry".to_string(),
                    description: "Weekly expiry aligned benchmark windows".to_string(),
                    is_default: false,
                },
                ScheduleOption {
                    kind: PeriodScheduleKind::WeekPlusOne,
                    label: "Week +1 Carry".to_string(),
                    description: "Rolling two-cycle weekly carry window".to_string(),
                    is_default: false,
                },
                ScheduleOption {
                    kind: PeriodScheduleKind::CalendarWeekly,
                    label: "Calendar Weekly".to_string(),
                    description: "Monday-to-Friday trading week".to_string(),
                    is_default: false,
                },
                ScheduleOption {
                    kind: PeriodScheduleKind::Daily,
                    label: "Daily".to_string(),
                    description: "Individual trading session returns".to_string(),
                    is_default: false,
                },
            ],
        ),
        StudyAssetClass::Etf | StudyAssetClass::Equity => (
            PeriodScheduleKind::CalendarWeekly,
            vec![
                ScheduleOption {
                    kind: PeriodScheduleKind::CalendarWeekly,
                    label: "Calendar Weekly (Mon→Fri)".to_string(),
                    description: "Monday-to-Friday ISO trading week".to_string(),
                    is_default: true,
                },
                ScheduleOption {
                    kind: PeriodScheduleKind::CalendarMonthly,
                    label: "Calendar Monthly".to_string(),
                    description: "First to last trading session per calendar month".to_string(),
                    is_default: false,
                },
                ScheduleOption {
                    kind: PeriodScheduleKind::WeeklyExpiry,
                    label: "F&O Expiry Aligned".to_string(),
                    description: "Aligned to NSE weekly derivative expiry regime".to_string(),
                    is_default: false,
                },
                ScheduleOption {
                    kind: PeriodScheduleKind::Daily,
                    label: "Daily".to_string(),
                    description: "Individual trading session returns".to_string(),
                    is_default: false,
                },
            ],
        ),
        StudyAssetClass::Forex => (
            PeriodScheduleKind::CalendarWeekly,
            vec![
                ScheduleOption {
                    kind: PeriodScheduleKind::CalendarWeekly,
                    label: "Weekly Session (Mon→Fri)".to_string(),
                    description: "Weekly FX session return cycle".to_string(),
                    is_default: true,
                },
                ScheduleOption {
                    kind: PeriodScheduleKind::CalendarMonthly,
                    label: "Monthly".to_string(),
                    description: "Calendar monthly FX return cycle".to_string(),
                    is_default: false,
                },
                ScheduleOption {
                    kind: PeriodScheduleKind::Daily,
                    label: "Daily (24h)".to_string(),
                    description: "Daily FX settlement-to-settlement returns".to_string(),
                    is_default: false,
                },
            ],
        ),
        StudyAssetClass::Commodity => (
            PeriodScheduleKind::FrontMonthCycle,
            vec![
                ScheduleOption {
                    kind: PeriodScheduleKind::FrontMonthCycle,
                    label: "Front-Month Cycle".to_string(),
                    description: "Monthly front-contract roll cycle".to_string(),
                    is_default: true,
                },
                ScheduleOption {
                    kind: PeriodScheduleKind::McxExpiryCycle,
                    label: "MCX Expiry Cycle (20th)".to_string(),
                    description: "MCX monthly roll cycle ending on or before the 20th".to_string(),
                    is_default: false,
                },
                ScheduleOption {
                    kind: PeriodScheduleKind::CalendarWeekly,
                    label: "Weekly (Mon→Fri)".to_string(),
                    description: "Calendar weekly commodity returns".to_string(),
                    is_default: false,
                },
                ScheduleOption {
                    kind: PeriodScheduleKind::Daily,
                    label: "Daily".to_string(),
                    description: "Individual trading session returns".to_string(),
                    is_default: false,
                },
            ],
        ),
    }
}

fn asset_class_label(c: StudyAssetClass) -> &'static str {
    match c {
        StudyAssetClass::Index => "Indices",
        StudyAssetClass::FnoStock => "F&O Stocks",
        StudyAssetClass::Etf => "ETFs",
        StudyAssetClass::Equity => "Equities",
        StudyAssetClass::Forex => "Forex",
        StudyAssetClass::Commodity => "Commodities",
    }
}

/// `GET /experimental-analysis/catalog`: returns grouped multi-asset catalog, schedule rules,
/// and live/historical feed status.
pub(crate) async fn get_catalog(
    State(state): State<Arc<AppState>>,
    ApiQuery(query): ApiQuery<ExperimentalCatalogQuery>,
) -> Response {
    let mut all = match state.instruments.list_instruments().await {
        Ok(list) => list,
        Err(err) => return port_failure(err),
    };
    all.sort_by(|a, b| a.id().cmp(b.id()));

    let mut fno_underlyings: BTreeSet<String> = BTreeSet::new();
    let mut fno_contract_count = 0usize;
    for inst in &all {
        let is_deriv = matches!(
            inst.kind(),
            tantra_core::InstrumentKind::Future | tantra_core::InstrumentKind::Option
        ) || inst.id().exchange == Exchange::Nfo;
        if is_deriv {
            fno_contract_count += 1;
            let sym = inst.id().symbol.to_ascii_uppercase();
            fno_underlyings.insert(sym.clone());
            let base: String = sym
                .chars()
                .take_while(|c| c.is_ascii_alphabetic() || *c == '&')
                .collect();
            if !base.is_empty() {
                fno_underlyings.insert(base);
            }
        }
    }

    let ordered_classes = [
        StudyAssetClass::Index,
        StudyAssetClass::FnoStock,
        StudyAssetClass::Etf,
        StudyAssetClass::Equity,
        StudyAssetClass::Forex,
        StudyAssetClass::Commodity,
    ];

    let mut buckets: BTreeMap<StudyAssetClass, Vec<CatalogInstrumentEntry>> = BTreeMap::new();
    for class in ordered_classes {
        buckets.insert(class, Vec::new());
    }

    for inst in &all {
        let id = inst.id();
        let entry = |asset_class: StudyAssetClass| CatalogInstrumentEntry {
            instrument_id: id.to_string(),
            symbol: id.symbol.clone(),
            exchange: id.exchange.to_string(),
            asset_class,
            instrument_kind: to_wire_kind(inst.kind()),
            currency: inst.currency().to_string(),
            lot_size: Decimal::from_f64_retain(inst.lot_size()).unwrap_or(Decimal::ONE),
            tick_size: Decimal::from_f64_retain(inst.tick_size())
                .unwrap_or_else(|| Decimal::new(5, 2)),
        };

        if inst.kind() == tantra_core::InstrumentKind::Index {
            buckets
                .get_mut(&StudyAssetClass::Index)
                .unwrap()
                .push(entry(StudyAssetClass::Index));
            continue;
        }
        if looks_like_commodity(id.exchange, &id.symbol) {
            buckets
                .get_mut(&StudyAssetClass::Commodity)
                .unwrap()
                .push(entry(StudyAssetClass::Commodity));
            continue;
        }
        if looks_like_forex(id.exchange, &id.symbol) {
            buckets
                .get_mut(&StudyAssetClass::Forex)
                .unwrap()
                .push(entry(StudyAssetClass::Forex));
            continue;
        }
        if matches!(
            inst.kind(),
            tantra_core::InstrumentKind::Future | tantra_core::InstrumentKind::Option
        ) {
            buckets
                .get_mut(&StudyAssetClass::FnoStock)
                .unwrap()
                .push(entry(StudyAssetClass::FnoStock));
            continue;
        }
        if id.exchange == Exchange::Nse && looks_like_index_or_proxy(&id.symbol) {
            buckets
                .get_mut(&StudyAssetClass::Index)
                .unwrap()
                .push(entry(StudyAssetClass::Index));
        }
        if looks_like_etf(&id.symbol) {
            let mut e = entry(StudyAssetClass::Etf);
            e.instrument_kind = InstrumentKind::Etf;
            buckets.get_mut(&StudyAssetClass::Etf).unwrap().push(e);
            continue;
        }

        buckets
            .get_mut(&StudyAssetClass::Equity)
            .unwrap()
            .push(entry(StudyAssetClass::Equity));
        if looks_like_known_fno_underlying(&id.symbol, &fno_underlyings) {
            buckets
                .get_mut(&StudyAssetClass::FnoStock)
                .unwrap()
                .push(entry(StudyAssetClass::FnoStock));
        }
    }

    if let Some(index_items) = buckets.get_mut(&StudyAssetClass::Index) {
        index_items.sort_by(|a, b| index_sort_rank(a).cmp(&index_sort_rank(b)));
    }

    let groups: Vec<AssetClassCatalogGroup> = ordered_classes
        .into_iter()
        .filter(|c| query.asset_class.is_none_or(|wanted| wanted == *c))
        .map(|class| {
            let (default_schedule, schedules) = schedules_for_asset_class(class);
            AssetClassCatalogGroup {
                asset_class: class,
                label: asset_class_label(class).to_string(),
                default_schedule,
                schedules,
                instruments: buckets.remove(&class).unwrap_or_default(),
            }
        })
        .collect();

    let lakehouse_connected = state.lakehouse_root().is_some();
    let live_feed_connected =
        state.live_quotes().is_some() || state.market_data_broadcast_tx.is_some();
    let source_status = DataSourceStatus {
        lakehouse_connected,
        lakehouse_commit_id: state.data_commit().map(|c| c.commit_sha.clone()),
        live_feed_connected,
        adapter_label: if lakehouse_connected && live_feed_connected {
            "Lakehouse v3 + Live Quote Feed".to_string()
        } else if lakehouse_connected {
            "Lakehouse v3 Historical".to_string()
        } else {
            "Runtime Bar Store".to_string()
        },
        indexed_instruments: all.len(),
        fno_contract_count,
    };

    success(ExperimentalCatalogResponse {
        groups,
        source_status,
    })
}

fn map_streak_dir(d: studies::StreakDirection) -> StreakDirection {
    match d {
        studies::StreakDirection::Positive => StreakDirection::Positive,
        studies::StreakDirection::Negative => StreakDirection::Negative,
    }
}

fn map_markov_state(s: studies::MarkovState) -> MarkovState {
    match s {
        studies::MarkovState::ShortPositive => MarkovState::ShortPositive,
        studies::MarkovState::MediumPositive => MarkovState::MediumPositive,
        studies::MarkovState::LongPositive => MarkovState::LongPositive,
        studies::MarkovState::ShortNegative => MarkovState::ShortNegative,
        studies::MarkovState::MediumNegative => MarkovState::MediumNegative,
        studies::MarkovState::LongNegative => MarkovState::LongNegative,
    }
}

fn map_outlier_status(s: studies::OutlierStatus) -> OutlierStatus {
    match s {
        studies::OutlierStatus::WithinBoundary => OutlierStatus::WithinBoundary,
        studies::OutlierStatus::UpsideOutlier => OutlierStatus::UpsideOutlier,
        studies::OutlierStatus::DownsideOutlier => OutlierStatus::DownsideOutlier,
    }
}

fn map_forward_group(g: &studies::ForwardConditionalGroup) -> ForwardConditionalGroup {
    ForwardConditionalGroup {
        threshold_pct: g.threshold_pct,
        after_large_pos_mean_pct: g.after_large_pos_mean_pct,
        after_large_pos_median_pct: g.after_large_pos_median_pct,
        after_large_pos_win_pct: g.after_large_pos_win_pct,
        after_large_pos_events: g
            .after_large_pos_events
            .iter()
            .map(|e| ForwardConditionEvent {
                trigger_end_date: e.trigger_end_date.format("%Y-%m-%d").to_string(),
                trigger_return_pct: e.trigger_return_pct,
                next_end_date: e.next_end_date.format("%Y-%m-%d").to_string(),
                next_return_pct: e.next_return_pct,
            })
            .collect(),
        after_large_neg_mean_pct: g.after_large_neg_mean_pct,
        after_large_neg_median_pct: g.after_large_neg_median_pct,
        after_large_neg_win_pct: g.after_large_neg_win_pct,
        after_large_neg_events: g
            .after_large_neg_events
            .iter()
            .map(|e| ForwardConditionEvent {
                trigger_end_date: e.trigger_end_date.format("%Y-%m-%d").to_string(),
                trigger_return_pct: e.trigger_return_pct,
                next_end_date: e.next_end_date.format("%Y-%m-%d").to_string(),
                next_return_pct: e.next_return_pct,
            })
            .collect(),
    }
}

fn map_bucket_row(b: &studies::ReturnBucketRow) -> ReturnBucketRow {
    ReturnBucketRow {
        label: b.label.clone(),
        min_pct: b.min_pct,
        max_pct: b.max_pct,
        count: b.count,
        pct_of_total: b.pct_of_total,
        percentile_range: b.percentile_range.clone(),
        probability_label: b.probability_label.clone(),
        price_range_low: b.price_range_low,
        price_range_high: b.price_range_high,
    }
}

fn map_slot_stat(s: studies::IntradaySlotStat) -> IntradaySlotStat {
    IntradaySlotStat {
        slot_label: s.slot_label,
        minute_of_day: s.minute_of_day,
        bar_count: s.bar_count,
        avg_range_pct: s.avg_range_pct,
        max_range_pct: s.max_range_pct,
        avg_abs_move_pct: s.avg_abs_move_pct,
        avg_volume: s.avg_volume,
        hod_count: s.hod_count,
        lod_count: s.lod_count,
    }
}

/// `POST /experimental-analysis/returns`: runs complete multi-asset period return,
/// F&O spread/basis/cost adjustment, streak, Markov, distribution, probability-level,
/// conditional forward, and survivability outlier computations on `1d` historical bars.
pub(crate) async fn post_returns(
    State(state): State<Arc<AppState>>,
    ApiJson(req): ApiJson<ExperimentalReturnsRequest>,
) -> Response {
    let parsed_id = match parse_study_instrument_id(&req.instrument_id) {
        Ok(id) => id,
        Err(detail) => return unprocessable(detail),
    };
    let (start_date, end_date) = match parse_date_range(&req.start_date, &req.end_date) {
        Ok(range) => range,
        Err(detail) => return unprocessable(detail),
    };

    let ma_window = req.ma_window.unwrap_or(20);
    if !(2..=120).contains(&ma_window) {
        return unprocessable(invalid_field(
            "ma_window",
            "out_of_range",
            "ma_window must be between 2 and 120 periods",
        ));
    }
    let outlier_coverage_pct = req
        .outlier_coverage_pct
        .unwrap_or_else(|| Decimal::from(90));
    if outlier_coverage_pct < Decimal::from(50) || outlier_coverage_pct > Decimal::from(99) {
        return unprocessable(invalid_field(
            "outlier_coverage_pct",
            "out_of_range",
            "outlier_coverage_pct must be between 50 and 99",
        ));
    }
    let tail_threshold_pct = req.tail_threshold_pct.unwrap_or_else(|| Decimal::from(2));
    if tail_threshold_pct <= Decimal::ZERO || tail_threshold_pct > Decimal::from(50) {
        return unprocessable(invalid_field(
            "tail_threshold_pct",
            "out_of_range",
            "tail_threshold_pct must be > 0 and <= 50",
        ));
    }
    let spread_slippage_bps = req.spread_slippage_bps.unwrap_or(Decimal::ZERO);
    if spread_slippage_bps < Decimal::ZERO || spread_slippage_bps > Decimal::from(500) {
        return unprocessable(invalid_field(
            "spread_slippage_bps",
            "out_of_range",
            "spread_slippage_bps must be between 0 and 500 bps",
        ));
    }
    let futures_roll_basis_bps = req.futures_roll_basis_bps.unwrap_or(Decimal::ZERO);
    if futures_roll_basis_bps < Decimal::from(-500) || futures_roll_basis_bps > Decimal::from(500) {
        return unprocessable(invalid_field(
            "futures_roll_basis_bps",
            "out_of_range",
            "futures_roll_basis_bps must be between -500 and +500 bps",
        ));
    }

    let ohlc_page = req.ohlc_page.unwrap_or(1).max(1);
    let ohlc_page_size = req.ohlc_page_size.unwrap_or(50).clamp(10, 500);

    let instrument = match resolve_study_instrument(&state, &parsed_id).await {
        Ok(inst) => inst,
        Err(resp) => return resp,
    };
    let canonical_id = instrument.id().clone();

    let study_bars =
        match load_daily_study_bars(&state, &canonical_id, start_date, end_date, true).await {
            Ok(bars) => bars,
            Err(resp) => return resp,
        };
    if study_bars.is_empty() {
        return failure(
            StatusCode::NOT_FOUND,
            ErrorDetail::new(
                ErrorCode::NotFound,
                format!(
                    "no daily bars available for {canonical_id} between {start_date} and {end_date}"
                ),
            ),
        );
    }

    // Optional live quote spot enrichment (honest: falls back to None/latest close if offline).
    let mut live_spot_price: Option<Decimal> = None;
    if let Some(live) = state.live_quotes()
        && let Ok(Some(snap)) = live.read_trade_snapshot(&canonical_id).await
    {
        let p = Decimal::from_f64_retain(snap.last.price()).unwrap_or_default();
        if p > Decimal::ZERO {
            live_spot_price = Some(p.round_dp(2));
        }
    }
    if live_spot_price.is_none()
        && let Ok(Some(q)) = state.quotes.read_quote(&canonical_id, None).await
    {
        let bid = Decimal::from_f64_retain(q.bid_price()).unwrap_or_default();
        let ask = Decimal::from_f64_retain(q.ask_price()).unwrap_or_default();
        let mid = if bid > Decimal::ZERO && ask > Decimal::ZERO {
            (bid + ask) / Decimal::from(2)
        } else {
            bid.max(ask)
        };
        if mid > Decimal::ZERO {
            live_spot_price = Some(mid.round_dp(2));
        }
    }

    let cal_inner = match state.lakehouse_root() {
        Some(root) => NseTradingCalendar::from_data_root(root),
        None => NseTradingCalendar::new(),
    };
    let calendar = RuntimeNseCalendar(cal_inner);

    let spread_cost = SpreadCostConfig {
        mode: to_domain_roll_mode(req.roll_basis_mode),
        spread_slippage_bps,
        futures_roll_basis_bps,
        include_statutory_costs: req.include_statutory_costs,
    };

    let cfg = ExperimentalStudyConfig {
        symbol: canonical_id.symbol.clone(),
        asset_class: to_domain_asset_class(req.asset_class),
        schedule: to_domain_schedule(req.period),
        sub_cycle: to_domain_sub_cycle(req.sub_cycle),
        calc_kind: to_domain_calc(req.calc_type),
        ma_window,
        outlier_coverage_pct,
        tail_threshold_pct,
        live_spot_override: live_spot_price,
        spread_cost,
    };

    let report = match studies::evaluate_returns_study(&study_bars, &calendar, &cfg) {
        Ok(r) => r,
        Err(StudyError::EmptyBars { .. }) => {
            return unprocessable(invalid_field(
                "start_date",
                "empty_bars",
                "no daily bars available in selected date range",
            ));
        }
        Err(StudyError::NoPeriods { .. }) => {
            return unprocessable(invalid_field(
                "period",
                "insufficient_periods",
                "selected date range does not produce at least one complete period",
            ));
        }
    };

    // Compute optional Period+1 forward conditional group so both accordions render when applicable.
    let forward_conditional_plus_one = if req.sub_cycle == SubCycleKind::Current
        && req.period != PeriodScheduleKind::WeekPlusOne
        && req.period != PeriodScheduleKind::Daily
    {
        let (mut p1_periods, _) = studies::slice_periods(
            &study_bars,
            &cfg.symbol,
            cfg.asset_class,
            cfg.schedule,
            DomainSubCycle::PlusOne,
            cfg.calc_kind,
            &calendar,
        );
        studies::apply_spread_and_basis(
            &mut p1_periods,
            cfg.asset_class,
            &canonical_id.to_string(),
            cfg.calc_kind,
            &cfg.spread_cost,
            ma_window,
        );
        if p1_periods.len() >= 2 {
            let b1 = studies::analyze_distributions_and_outliers(
                &p1_periods,
                report.distributions.current_price,
                report.distributions.current_reference_price,
                outlier_coverage_pct,
                tail_threshold_pct,
            );
            Some(map_forward_group(&b1.forward_conditional))
        } else {
            None
        }
    } else {
        None
    };

    // Paginate Daily OHLC rows newest-first cleanly.
    let all_daily_desc: Vec<DailyOhlcRow> = report
        .daily_ohlc_rows
        .iter()
        .rev()
        .map(|r| DailyOhlcRow {
            date: r.date.format("%Y-%m-%d").to_string(),
            open: r.open,
            high: r.high,
            low: r.low,
            close: r.close,
            volume: r.volume,
            dividend: r.dividend,
            split_factor: r.split_factor,
            daily_return_pct: r.daily_return_pct,
        })
        .collect();
    let total_daily_rows = all_daily_desc.len();
    let total_pages = total_daily_rows.div_ceil(ohlc_page_size).max(1);
    let safe_page = ohlc_page.min(total_pages);
    let start_idx = (safe_page - 1) * ohlc_page_size;
    let end_idx = (start_idx + ohlc_page_size).min(total_daily_rows);
    let daily_ohlc_rows = if start_idx < end_idx {
        all_daily_desc[start_idx..end_idx].to_vec()
    } else {
        Vec::new()
    };

    let friction_summary = report.friction_summary.map(|f| FrictionSummary {
        mode: from_domain_roll_mode(f.mode),
        gross_avg_return_pct: f.gross_avg_return_pct,
        net_avg_return_pct: f.net_avg_return_pct,
        gross_sharpe_ratio: f.gross_sharpe_ratio,
        net_sharpe_ratio: f.net_sharpe_ratio,
        gross_profit_factor: f.gross_profit_factor,
        net_profit_factor: f.net_profit_factor,
        per_period_spread_bps: f.per_period_spread_bps,
        per_period_statutory_bps: f.per_period_statutory_bps,
        per_period_basis_bps: f.per_period_basis_bps,
        cumulative_net_delta_pct: f.cumulative_net_delta_pct,
    });

    let resp = ExperimentalReturnsResponse {
        instrument_id: canonical_id.to_string(),
        symbol: canonical_id.symbol.clone(),
        exchange: canonical_id.exchange.to_string(),
        currency: instrument.currency().to_string(),
        asset_class: req.asset_class,
        period: req.period,
        sub_cycle: req.sub_cycle,
        calc_type: req.calc_type,
        start_date: report
            .period_series
            .first()
            .map(|p| p.start_date.format("%Y-%m-%d").to_string())
            .unwrap_or_else(|| req.start_date.clone()),
        end_date: report
            .period_series
            .last()
            .map(|p| p.end_date.format("%Y-%m-%d").to_string())
            .unwrap_or_else(|| req.end_date.clone()),
        regime_note: report.regime_note,
        source_label: if state.lakehouse_root().is_some() {
            "Lakehouse v3".to_string()
        } else {
            "Store BarReader".to_string()
        },
        period_count: report.period_series.len(),
        friction_summary,
        kpis: ReturnKpis {
            total_periods: report.period_series.len(),
            avg_return_pct: report.kpis.avg_return_pct,
            max_gain_pct: report.kpis.max_gain_pct,
            max_loss_pct: report.kpis.max_loss_pct,
            std_dev_pct: report.kpis.std_dev_pct,
            positive_count: report.kpis.positive_count,
            positive_pct: report.kpis.positive_pct,
            negative_count: report.kpis.negative_count,
            negative_pct: report.kpis.negative_pct,
            neutral_count: report.kpis.neutral_count,
            sharpe_ratio: report.kpis.sharpe_ratio,
            sortino_ratio: report.kpis.sortino_ratio,
            profit_factor: report.kpis.profit_factor,
        },
        series: report
            .period_series
            .into_iter()
            .map(|p| PeriodPoint {
                index: p.index,
                start_date: p.start_date.format("%Y-%m-%d").to_string(),
                end_date: p.end_date.format("%Y-%m-%d").to_string(),
                entry_price: p.entry_price,
                high_price: p.high_price,
                low_price: p.low_price,
                exit_price: p.exit_price,
                return_pct: p.return_pct,
                ma_pct: p.ma_pct,
                regime_label: p.regime_label,
            })
            .collect(),
        streaks: StreakAnalysis {
            summary: StreakSummary {
                current_streak_type: report
                    .streaks
                    .summary
                    .current_streak_type
                    .map(map_streak_dir),
                current_streak_length: report.streaks.summary.current_streak_length,
                current_streak_return_pct: report.streaks.summary.current_streak_return_pct,
                current_markov_state: report
                    .streaks
                    .summary
                    .current_markov_state
                    .map(map_markov_state),
                positive_streak_count: report.streaks.summary.positive_streak_count,
                negative_streak_count: report.streaks.summary.negative_streak_count,
                longest_positive: report.streaks.summary.longest_positive,
                longest_negative: report.streaks.summary.longest_negative,
                best_positive_return_pct: report.streaks.summary.best_positive_return_pct,
                worst_negative_return_pct: report.streaks.summary.worst_negative_return_pct,
                avg_positive_length: report.streaks.summary.avg_positive_length,
                avg_negative_length: report.streaks.summary.avg_negative_length,
                excluded_neutral_count: report.streaks.summary.excluded_neutral_count,
            },
            details: report
                .streaks
                .details
                .into_iter()
                .map(|d| StreakDetail {
                    index: d.index,
                    streak_type: map_streak_dir(d.streak_type),
                    length: d.length,
                    start_date: d.start_date.format("%Y-%m-%d").to_string(),
                    end_date: d.end_date.format("%Y-%m-%d").to_string(),
                    start_price: d.start_price,
                    end_price: d.end_price,
                    total_return_pct: d.total_return_pct,
                    avg_period_return_pct: d.avg_period_return_pct,
                })
                .collect(),
            length_distribution: report
                .streaks
                .length_distribution
                .into_iter()
                .map(|b| StreakLengthBucket {
                    length: b.length,
                    positive_count: b.positive_count,
                    negative_count: b.negative_count,
                })
                .collect(),
            positive_stats: SideStreakStats {
                most_common_length: report.streaks.positive_stats.most_common_length,
                most_common_count: report.streaks.positive_stats.most_common_count,
                median_length: report.streaks.positive_stats.median_length,
                std_dev_length: report.streaks.positive_stats.std_dev_length,
            },
            negative_stats: SideStreakStats {
                most_common_length: report.streaks.negative_stats.most_common_length,
                most_common_count: report.streaks.negative_stats.most_common_count,
                median_length: report.streaks.negative_stats.median_length,
                std_dev_length: report.streaks.negative_stats.std_dev_length,
            },
        },
        markov: MarkovAnalysis {
            transition_matrix: report
                .markov
                .transition_matrix
                .into_iter()
                .map(|row| MarkovTransitionRow {
                    from_state: map_markov_state(row.from_state),
                    from_label: row.from_label,
                    observation_count: row.observation_count,
                    probs_pct: row.probs_pct,
                })
                .collect(),
            current_prediction: MarkovCurrentPrediction {
                current_state: report
                    .markov
                    .current_prediction
                    .current_state
                    .map(map_markov_state),
                current_label: report.markov.current_prediction.current_label,
                positive_next_pct: report.markov.current_prediction.positive_next_pct,
                negative_next_pct: report.markov.current_prediction.negative_next_pct,
                strongest_next_state: report
                    .markov
                    .current_prediction
                    .strongest_next_state
                    .map(map_markov_state),
                strongest_next_label: report.markov.current_prediction.strongest_next_label,
                strongest_probability_pct: report
                    .markov
                    .current_prediction
                    .strongest_probability_pct,
                state_breakdown: report
                    .markov
                    .current_prediction
                    .state_breakdown
                    .into_iter()
                    .map(|p| MarkovStateProbability {
                        state: map_markov_state(p.state),
                        label: p.label,
                        probability_pct: p.probability_pct,
                    })
                    .collect(),
            },
            steady_state: report
                .markov
                .steady_state
                .into_iter()
                .map(|p| MarkovStateProbability {
                    state: map_markov_state(p.state),
                    label: p.label,
                    probability_pct: p.probability_pct,
                })
                .collect(),
        },
        percentiles: ReturnPercentiles {
            p99: report.distributions.percentiles.p99,
            p95: report.distributions.percentiles.p95,
            p75: report.distributions.percentiles.p75,
            p50: report.distributions.percentiles.p50,
            p25: report.distributions.percentiles.p25,
            p5: report.distributions.percentiles.p5,
            p1: report.distributions.percentiles.p1,
        },
        current_price: report.distributions.current_price,
        current_reference_price: report.distributions.current_reference_price,
        current_reference_change_pct: report.distributions.current_reference_change_pct,
        coarse_buckets: report
            .distributions
            .coarse_buckets
            .iter()
            .map(map_bucket_row)
            .collect(),
        granular_10bps: report
            .distributions
            .granular_10bps
            .iter()
            .map(map_bucket_row)
            .collect(),
        probability_levels: report
            .distributions
            .probability_levels
            .into_iter()
            .map(|r| ProbabilityLevelRow {
                prob_above_pct: r.prob_above_pct,
                return_pct: r.return_pct,
                implied_price: r.implied_price,
                change_pct: r.change_pct,
                scenario: r.scenario,
            })
            .collect(),
        green_vs_red: GreenVsRedComparison {
            green: PolarityStats {
                count: report.distributions.green_vs_red.green.count,
                share_pct: report.distributions.green_vs_red.green.share_pct,
                mean_pct: report.distributions.green_vs_red.green.mean_pct,
                median_pct: report.distributions.green_vs_red.green.median_pct,
                std_dev_pct: report.distributions.green_vs_red.green.std_dev_pct,
                extreme_pct: report.distributions.green_vs_red.green.extreme_pct,
            },
            red: PolarityStats {
                count: report.distributions.green_vs_red.red.count,
                share_pct: report.distributions.green_vs_red.red.share_pct,
                mean_pct: report.distributions.green_vs_red.red.mean_pct,
                median_pct: report.distributions.green_vs_red.red.median_pct,
                std_dev_pct: report.distributions.green_vs_red.red.std_dev_pct,
                extreme_pct: report.distributions.green_vs_red.red.extreme_pct,
            },
            magnitude_buckets: report
                .distributions
                .green_vs_red
                .magnitude_buckets
                .into_iter()
                .map(|m| MagnitudePdfBucket {
                    label: m.label,
                    green_count: m.green_count,
                    green_pct: m.green_pct,
                    red_count: m.red_count,
                    red_pct: m.red_pct,
                })
                .collect(),
        },
        forward_conditional: map_forward_group(&report.distributions.forward_conditional),
        forward_conditional_plus_one,
        outliers: OutlierAnalysis {
            kpis: OutlierKpis {
                coverage_pct: report.distributions.outliers.kpis.coverage_pct,
                upper_bound_pct: report.distributions.outliers.kpis.upper_bound_pct,
                lower_bound_pct: report.distributions.outliers.kpis.lower_bound_pct,
                total_periods: report.distributions.outliers.kpis.total_periods,
                total_outliers: report.distributions.outliers.kpis.total_outliers,
                outlier_pct: report.distributions.outliers.kpis.outlier_pct,
                upside_outliers: report.distributions.outliers.kpis.upside_outliers,
                downside_outliers: report.distributions.outliers.kpis.downside_outliers,
            },
            scatter_points: report
                .distributions
                .outliers
                .scatter_points
                .into_iter()
                .map(|p| OutlierPeriodPoint {
                    index: p.index,
                    start_date: p.start_date.format("%Y-%m-%d").to_string(),
                    end_date: p.end_date.format("%Y-%m-%d").to_string(),
                    return_pct: p.return_pct,
                    status: map_outlier_status(p.status),
                })
                .collect(),
            outlier_table: report
                .distributions
                .outliers
                .outlier_table
                .into_iter()
                .map(|p| OutlierPeriodPoint {
                    index: p.index,
                    start_date: p.start_date.format("%Y-%m-%d").to_string(),
                    end_date: p.end_date.format("%Y-%m-%d").to_string(),
                    return_pct: p.return_pct,
                    status: map_outlier_status(p.status),
                })
                .collect(),
        },
        daily_ohlc_summary: DailyOhlcSummary {
            latest_close: report.daily_ohlc_summary.latest_close,
            period_high: report.daily_ohlc_summary.period_high,
            period_low: report.daily_ohlc_summary.period_low,
            total_trading_days: report.daily_ohlc_summary.total_trading_days,
            avg_daily_return_pct: report.daily_ohlc_summary.avg_daily_return_pct,
            daily_return_std_pct: report.daily_ohlc_summary.daily_return_std_pct,
            avg_volume: report.daily_ohlc_summary.avg_volume,
        },
        daily_ohlc_total_rows: total_daily_rows,
        daily_ohlc_page: safe_page,
        daily_ohlc_page_size: ohlc_page_size,
        daily_ohlc_rows,
    };

    success(resp)
}

/// `POST /experimental-analysis/intraday`: computes Phase 2 Intraday Movement (`5m`),
/// Overnight Gaps (`1d`), Rolling 5-Day Moves (`1d`), Cycle Progress Cone (`0-100%`),
/// and 15m ORB Flip Backtest (`5m`, honest `available: false` when `5m` bars are missing).
pub(crate) async fn post_intraday(
    State(state): State<Arc<AppState>>,
    ApiJson(req): ApiJson<ExperimentalIntradayRequest>,
) -> Response {
    let parsed_id = match parse_study_instrument_id(&req.instrument_id) {
        Ok(id) => id,
        Err(detail) => return unprocessable(detail),
    };
    let (start_date, end_date) = match parse_date_range(&req.start_date, &req.end_date) {
        Ok(range) => range,
        Err(detail) => return unprocessable(detail),
    };

    let tail_threshold_pct = req.tail_threshold_pct.unwrap_or_else(|| Decimal::from(2));
    if tail_threshold_pct <= Decimal::ZERO || tail_threshold_pct > Decimal::from(50) {
        return unprocessable(invalid_field(
            "tail_threshold_pct",
            "out_of_range",
            "tail_threshold_pct must be > 0 and <= 50",
        ));
    }
    let opening_range_minutes = req.opening_range_minutes.unwrap_or(15);
    if !(5..=60).contains(&opening_range_minutes) {
        return unprocessable(invalid_field(
            "opening_range_minutes",
            "out_of_range",
            "opening_range_minutes must be between 5 and 60 minutes",
        ));
    }
    let max_flips_per_day = req.max_flips_per_day.unwrap_or(2);
    if max_flips_per_day > 10 {
        return unprocessable(invalid_field(
            "max_flips_per_day",
            "out_of_range",
            "max_flips_per_day must be <= 10",
        ));
    }
    let stop_loss_pct = req.stop_loss_pct.unwrap_or(Decimal::ZERO);
    if stop_loss_pct < Decimal::ZERO || stop_loss_pct > Decimal::from(25) {
        return unprocessable(invalid_field(
            "stop_loss_pct",
            "out_of_range",
            "stop_loss_pct must be between 0 and 25",
        ));
    }
    let target_pct = req.target_pct.unwrap_or(Decimal::ZERO);
    if target_pct < Decimal::ZERO || target_pct > Decimal::from(50) {
        return unprocessable(invalid_field(
            "target_pct",
            "out_of_range",
            "target_pct must be between 0 and 50",
        ));
    }
    let spread_slippage_bps = req.spread_slippage_bps.unwrap_or(Decimal::ZERO);
    if spread_slippage_bps < Decimal::ZERO || spread_slippage_bps > Decimal::from(500) {
        return unprocessable(invalid_field(
            "spread_slippage_bps",
            "out_of_range",
            "spread_slippage_bps must be between 0 and 500",
        ));
    }

    let instrument = match resolve_study_instrument(&state, &parsed_id).await {
        Ok(inst) => inst,
        Err(resp) => return resp,
    };
    let canonical_id = instrument.id().clone();

    let daily_bars =
        match load_daily_study_bars(&state, &canonical_id, start_date, end_date, false).await {
            Ok(bars) => bars,
            Err(resp) => return resp,
        };
    if daily_bars.is_empty() {
        return failure(
            StatusCode::NOT_FOUND,
            ErrorDetail::new(
                ErrorCode::NotFound,
                format!(
                    "no daily bars available for {canonical_id} between {start_date} and {end_date}"
                ),
            ),
        );
    }

    // Load 5m bars honestly (if absent, return `available: false` for intraday & ORB).
    let bars_5m = load_5m_intraday_bars(&state, &canonical_id, start_date, end_date).await;

    let cal_inner = match state.lakehouse_root() {
        Some(root) => NseTradingCalendar::from_data_root(root),
        None => NseTradingCalendar::new(),
    };
    let calendar = RuntimeNseCalendar(cal_inner);
    let domain_asset = to_domain_asset_class(req.asset_class);
    let domain_schedule = to_domain_schedule(req.period);

    let (periods, _) = studies::slice_periods(
        &daily_bars,
        &canonical_id.symbol,
        domain_asset,
        domain_schedule,
        DomainSubCycle::Current,
        DomainCalc::OpenToClose,
        &calendar,
    );

    let intra = studies::analyze_intraday_movement(&bars_5m);
    let gaps = studies::analyze_gaps(&daily_bars, &bars_5m);
    let five_day = studies::analyze_rolling_5day(&daily_bars, tail_threshold_pct);
    let cycle_progress = studies::analyze_cycle_progress(&daily_bars, &periods);
    let orb = studies::run_orb15_flip_backtest(
        &bars_5m,
        domain_asset,
        &canonical_id.to_string(),
        &OrbConfig {
            opening_range_minutes,
            max_flips_per_day,
            stop_loss_pct,
            target_pct,
            spread_slippage_bps,
            include_statutory_costs: req.include_statutory_costs,
        },
    );

    let resp = ExperimentalIntradayResponse {
        instrument_id: canonical_id.to_string(),
        symbol: canonical_id.symbol.clone(),
        exchange: canonical_id.exchange.to_string(),
        asset_class: req.asset_class,
        period: req.period,
        start_date: req.start_date,
        end_date: req.end_date,
        intraday: IntradayMovementAnalysis {
            available: intra.available,
            total_sessions: intra.total_sessions,
            avg_session_range_pct: intra.avg_session_range_pct,
            peak_volatility_slot: intra.peak_volatility_slot,
            most_common_hod_slot: intra.most_common_hod_slot,
            most_common_lod_slot: intra.most_common_lod_slot,
            slots_15m: intra.slots_15m.into_iter().map(map_slot_stat).collect(),
            slots_hourly: intra.slots_hourly.into_iter().map(map_slot_stat).collect(),
        },
        gaps: GapAnalysis {
            total_sessions: gaps.total_sessions,
            gap_up_count: gaps.gap_up_count,
            gap_down_count: gaps.gap_down_count,
            avg_gap_up_pct: gaps.avg_gap_up_pct,
            avg_gap_down_pct: gaps.avg_gap_down_pct,
            gap_up_fill_rate_pct: gaps.gap_up_fill_rate_pct,
            gap_down_fill_rate_pct: gaps.gap_down_fill_rate_pct,
            gap_up_follow_pct: gaps.gap_up_follow_pct,
            gap_down_follow_pct: gaps.gap_down_follow_pct,
            buckets: gaps
                .buckets
                .into_iter()
                .map(|b| GapBucketRow {
                    label: b.label,
                    count: b.count,
                    share_pct: b.share_pct,
                    fill_rate_pct: b.fill_rate_pct,
                    avg_intraday_follow_pct: b.avg_intraday_follow_pct,
                })
                .collect(),
            recent_events: gaps
                .recent_events
                .into_iter()
                .map(|e| GapEventRow {
                    date: e.date.format("%Y-%m-%d").to_string(),
                    prev_close: e.prev_close,
                    open: e.open,
                    close: e.close,
                    gap_pct: e.gap_pct,
                    intraday_return_pct: e.intraday_return_pct,
                    filled: e.filled,
                })
                .collect(),
        },
        five_day: RollingFiveDayAnalysis {
            sample_count: five_day.sample_count,
            avg_return_pct: five_day.avg_return_pct,
            std_dev_pct: five_day.std_dev_pct,
            positive_pct: five_day.positive_pct,
            p99_pct: five_day.p99_pct,
            p95_pct: five_day.p95_pct,
            p50_pct: five_day.p50_pct,
            p5_pct: five_day.p5_pct,
            p1_pct: five_day.p1_pct,
            after_surge_next_5d_avg_pct: five_day.after_surge_next_5d_avg_pct,
            after_drop_next_5d_avg_pct: five_day.after_drop_next_5d_avg_pct,
            series: five_day
                .series
                .into_iter()
                .map(|pt| RollingFiveDayPoint {
                    date: pt.date.format("%Y-%m-%d").to_string(),
                    close: pt.close,
                    return_5d_pct: pt.return_5d_pct,
                })
                .collect(),
        },
        cycle_progress: CycleProgressAnalysis {
            cycle_count: cycle_progress.cycle_count,
            deciles: cycle_progress
                .deciles
                .into_iter()
                .map(|d| CycleProgressDecile {
                    progress_pct: d.progress_pct,
                    sample_count: d.sample_count,
                    mean_pct: d.mean_pct,
                    p10_pct: d.p10_pct,
                    p25_pct: d.p25_pct,
                    p50_pct: d.p50_pct,
                    p75_pct: d.p75_pct,
                    p90_pct: d.p90_pct,
                })
                .collect(),
        },
        orb_backtest: OrbBacktestResult {
            available: orb.available,
            total_trades: orb.total_trades,
            winning_trades: orb.winning_trades,
            win_rate_pct: orb.win_rate_pct,
            gross_return_pct: orb.gross_return_pct,
            total_friction_pct: orb.total_friction_pct,
            net_return_pct: orb.net_return_pct,
            profit_factor: orb.profit_factor,
            max_drawdown_pct: orb.max_drawdown_pct,
            avg_trade_net_pct: orb.avg_trade_net_pct,
            equity_curve: orb
                .equity_curve
                .into_iter()
                .map(|pt| OrbEquityPoint {
                    date: pt.date.format("%Y-%m-%d").to_string(),
                    cumulative_gross_pct: pt.cumulative_gross_pct,
                    cumulative_net_pct: pt.cumulative_net_pct,
                })
                .collect(),
            trades: orb
                .trades
                .into_iter()
                .map(|t| OrbTradeRow {
                    date: t.date.format("%Y-%m-%d").to_string(),
                    direction: t.direction,
                    flip_index: t.flip_index,
                    or_high: t.or_high,
                    or_low: t.or_low,
                    entry_price: t.entry_price,
                    exit_price: t.exit_price,
                    exit_reason: t.exit_reason,
                    gross_return_pct: t.gross_return_pct,
                    friction_bps: t.friction_bps,
                    net_return_pct: t.net_return_pct,
                })
                .collect(),
        },
    };

    success(resp)
}

/// `POST /experimental-analysis/correlation`: computes N×N Pearson & Spearman correlation
/// matrices, rolling pair beta, and normalized relative spread curves across 2..=12 instruments.
pub(crate) async fn post_correlation(
    State(state): State<Arc<AppState>>,
    ApiJson(req): ApiJson<ExperimentalCorrelationRequest>,
) -> Response {
    let (start_date, end_date) = match parse_date_range(&req.start_date, &req.end_date) {
        Ok(range) => range,
        Err(detail) => return unprocessable(detail),
    };

    let primary_parsed = match parse_study_instrument_id(&req.primary_id) {
        Ok(id) => id,
        Err(detail) => return unprocessable(detail),
    };
    let benchmark_parsed = match parse_study_instrument_id(&req.benchmark_id) {
        Ok(id) => id,
        Err(detail) => return unprocessable(detail),
    };

    let rolling_window = req.rolling_window.unwrap_or(20);
    if !(5..=250).contains(&rolling_window) {
        return unprocessable(invalid_field(
            "rolling_window",
            "out_of_range",
            "rolling_window must be between 5 and 250 sessions",
        ));
    }

    // Collect deduplicated symbol list ensuring both primary and benchmark are included.
    let mut requested: Vec<InstrumentId> = Vec::new();
    let mut seen: BTreeSet<String> = BTreeSet::new();
    for raw in std::iter::once(&req.primary_id)
        .chain(std::iter::once(&req.benchmark_id))
        .chain(req.symbols.iter())
    {
        let Ok(id) = parse_study_instrument_id(raw) else {
            return unprocessable(invalid_field(
                "symbols",
                "invalid_instrument_id",
                format!("invalid instrument id {raw:?} in correlation symbols"),
            ));
        };
        let key = id.to_string().to_ascii_uppercase();
        if seen.insert(key) {
            requested.push(id);
        }
    }

    if !(2..=12).contains(&requested.len()) {
        return unprocessable(invalid_field(
            "symbols",
            "invalid_symbol_count",
            "correlation analysis requires between 2 and 12 distinct instruments",
        ));
    }

    let primary_inst = match resolve_study_instrument(&state, &primary_parsed).await {
        Ok(inst) => inst,
        Err(resp) => return resp,
    };
    let primary_canon = primary_inst.id().to_string();

    let benchmark_inst = match resolve_study_instrument(&state, &benchmark_parsed).await {
        Ok(inst) => inst,
        Err(resp) => return resp,
    };
    let benchmark_canon = benchmark_inst.id().to_string();

    let mut series: Vec<(String, Vec<OhlcvBar>)> = Vec::with_capacity(requested.len());
    for candidate_id in &requested {
        let Ok(inst) = resolve_study_instrument(&state, candidate_id).await else {
            continue;
        };
        let cid = inst.id().clone();
        if let Ok(bars) = load_daily_study_bars(&state, &cid, start_date, end_date, false).await
            && !bars.is_empty()
        {
            series.push((cid.to_string(), bars));
        }
    }

    if series.len() < 2 {
        return unprocessable(invalid_field(
            "symbols",
            "insufficient_instruments",
            "at least 2 instruments must have daily bars in the selected date window",
        ));
    }

    let corr = studies::analyze_cross_asset_correlation(
        &series,
        &primary_canon,
        &benchmark_canon,
        rolling_window,
    );
    if corr.aligned_sessions == 0 {
        return unprocessable(invalid_field(
            "benchmark_id",
            "no_overlapping_sessions",
            "no overlapping trading dates between primary and benchmark instruments",
        ));
    }

    success(ExperimentalCorrelationResponse {
        labels: corr.labels,
        primary_id: corr.primary_id,
        benchmark_id: corr.benchmark_id,
        rolling_window: corr.rolling_window,
        aligned_sessions: corr.aligned_sessions,
        pearson_matrix: corr.pearson_matrix,
        spearman_matrix: corr.spearman_matrix,
        pair_beta: corr.pair_beta,
        pair_pearson: corr.pair_pearson,
        pair_spearman: corr.pair_spearman,
        latest_raw_spread: corr.latest_raw_spread,
        latest_relative_spread_pct: corr.latest_relative_spread_pct,
        rolling_series: corr
            .rolling_series
            .into_iter()
            .map(|pt| RollingPairPoint {
                date: pt.date.format("%Y-%m-%d").to_string(),
                rolling_corr: pt.rolling_corr,
                relative_spread_pct: pt.relative_spread_pct,
                raw_spread: pt.raw_spread,
            })
            .collect(),
    })
}


// ─────────────────────────────────────────────────────────────────────────────
// Phase 3 Handlers: Options Overlay, Analog Forecast, Event Studies & News
// ─────────────────────────────────────────────────────────────────────────────

fn map_to_domain_strategy(s: OptionOverlayStrategy) -> studies::OptionOverlayStrategy {
    match s {
        OptionOverlayStrategy::CoveredCall => studies::OptionOverlayStrategy::CoveredCall,
        OptionOverlayStrategy::BullCallSpread => studies::OptionOverlayStrategy::BullCallSpread,
        OptionOverlayStrategy::BearPutSpread => studies::OptionOverlayStrategy::BearPutSpread,
        OptionOverlayStrategy::IronCondor => studies::OptionOverlayStrategy::IronCondor,
        OptionOverlayStrategy::CalendarSpread => studies::OptionOverlayStrategy::CalendarSpread,
    }
}

fn map_from_domain_vol_regime(v: studies::VolatilityRegime) -> VolatilityRegime {
    match v {
        studies::VolatilityRegime::Low => VolatilityRegime::Low,
        studies::VolatilityRegime::Normal => VolatilityRegime::Normal,
        studies::VolatilityRegime::Elevated => VolatilityRegime::Elevated,
    }
}

fn map_from_domain_event_kind(k: studies::StudyEventKind) -> StudyEventKind {
    match k {
        studies::StudyEventKind::Buyback => StudyEventKind::Buyback,
        studies::StudyEventKind::Earnings => StudyEventKind::Earnings,
        studies::StudyEventKind::Macro => StudyEventKind::Macro,
        studies::StudyEventKind::FnoRebalance => StudyEventKind::FnoRebalance,
    }
}

fn instrument_text_matches(candidate: &str, canonical_id: &InstrumentId) -> bool {
    let cand_upper = candidate.trim().to_ascii_uppercase();
    let id_upper = canonical_id.to_string().to_ascii_uppercase();
    let sym_upper = canonical_id.symbol.to_ascii_uppercase();
    cand_upper == id_upper
        || cand_upper == sym_upper
        || cand_upper.ends_with(&format!(":{sym_upper}"))
        || cand_upper.contains(&format!(":{sym_upper}-"))
}

async fn read_live_spot(state: &AppState, canonical_id: &InstrumentId) -> Option<Decimal> {
    if let Some(live) = state.live_quotes()
        && let Ok(Some(snap)) = live.read_trade_snapshot(canonical_id).await
    {
        let p = Decimal::from_f64_retain(snap.last.price()).unwrap_or_default();
        if p > Decimal::ZERO {
            return Some(p.round_dp(2));
        }
    }
    if let Ok(Some(q)) = state.quotes.read_quote(canonical_id, None).await {
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

/// `POST /experimental-analysis/options-overlay`: evaluates multi-leg F&O option overlay
/// payoffs & live option chain bid-ask strike liquidity lab.
pub(crate) async fn post_options_overlay(
    State(state): State<Arc<AppState>>,
    ApiJson(req): ApiJson<ExperimentalOptionsOverlayRequest>,
) -> Response {
    let parsed_id = match parse_study_instrument_id(&req.instrument_id) {
        Ok(id) => id,
        Err(detail) => return unprocessable(detail),
    };
    let (start_date, end_date) = match parse_date_range(&req.start_date, &req.end_date) {
        Ok(range) => range,
        Err(detail) => return unprocessable(detail),
    };

    let target_short_prob_pct = req.target_short_prob_pct.unwrap_or_else(|| Decimal::from(80));
    if target_short_prob_pct < Decimal::from(55) || target_short_prob_pct > Decimal::from(98) {
        return unprocessable(invalid_field(
            "target_short_prob_pct",
            "out_of_range",
            "target_short_prob_pct must be between 55 and 98",
        ));
    }
    let wing_width_pct = req
        .wing_width_pct
        .unwrap_or_else(|| Decimal::from(3));
    if wing_width_pct < Decimal::new(5, 1) || wing_width_pct > Decimal::from(15) {
        return unprocessable(invalid_field(
            "wing_width_pct",
            "out_of_range",
            "wing_width_pct must be between 0.5 and 15",
        ));
    }
    let spread_slippage_bps = req.spread_slippage_bps.unwrap_or_else(|| Decimal::from(15));
    if spread_slippage_bps < Decimal::ZERO || spread_slippage_bps > Decimal::from(500) {
        return unprocessable(invalid_field(
            "spread_slippage_bps",
            "out_of_range",
            "spread_slippage_bps must be between 0 and 500 bps",
        ));
    }

    let instrument = match resolve_study_instrument(&state, &parsed_id).await {
        Ok(inst) => inst,
        Err(resp) => return resp,
    };
    let canonical_id = instrument.id().clone();
    let lot_size = Decimal::from_f64_retain(instrument.lot_size()).unwrap_or(Decimal::ONE);
    let tick_size = Decimal::from_f64_retain(instrument.tick_size())
        .unwrap_or_else(|| Decimal::new(5, 2));

    let daily_bars =
        match load_daily_study_bars(&state, &canonical_id, start_date, end_date, false).await {
            Ok(bars) => bars,
            Err(resp) => return resp,
        };
    if daily_bars.is_empty() {
        return failure(
            StatusCode::NOT_FOUND,
            ErrorDetail::new(
                ErrorCode::NotFound,
                format!(
                    "no daily bars available for {canonical_id} between {start_date} and {end_date}"
                ),
            ),
        );
    }

    let cal_inner = match state.lakehouse_root() {
        Some(root) => NseTradingCalendar::from_data_root(root),
        None => NseTradingCalendar::new(),
    };
    let calendar = RuntimeNseCalendar(cal_inner);
    let domain_asset = to_domain_asset_class(req.asset_class);
    let domain_schedule = to_domain_schedule(req.period);

    let (periods, _) = studies::slice_periods(
        &daily_bars,
        &canonical_id.symbol,
        domain_asset,
        domain_schedule,
        DomainSubCycle::Current,
        DomainCalc::OpenToClose,
        &calendar,
    );
    if periods.is_empty() {
        return unprocessable(invalid_field(
            "period",
            "insufficient_periods",
            "selected date range does not produce at least one complete period",
        ));
    }

    let live_spot = read_live_spot(&state, &canonical_id).await;

    // Query live option chain through InstrumentChainReader (never fabricates quotes if empty).
    let chain_reader = InstrumentChainReader::new(state.instruments.clone(), state.quotes.clone());
    let live_quotes: Vec<studies::LiveOptionStrikeQuote> = match chain_reader
        .options_chain(&canonical_id, None, Some(200))
        .await
    {
        Ok(contracts) => contracts
            .into_iter()
            .map(|c| studies::LiveOptionStrikeQuote {
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
    };

    let report = studies::evaluate_options_overlay(
        &periods,
        &live_quotes,
        &studies::OptionsOverlayConfig {
            symbol: canonical_id.to_string(),
            asset_class: domain_asset,
            strategy: map_to_domain_strategy(req.strategy),
            target_short_prob_pct,
            wing_width_pct,
            lot_size,
            tick_size,
            spread_slippage_bps,
            include_statutory_costs: req.include_statutory_costs,
            live_spot_override: live_spot,
        },
    );

    success(ExperimentalOptionsOverlayResponse {
        instrument_id: canonical_id.to_string(),
        symbol: canonical_id.symbol.clone(),
        exchange: canonical_id.exchange.to_string(),
        asset_class: req.asset_class,
        period: req.period,
        start_date: req.start_date,
        end_date: req.end_date,
        strategy: req.strategy,
        spot_price: report.spot_price,
        empirical_rv_pct: report.empirical_rv_pct,
        quoted_iv_proxy_pct: report.quoted_iv_proxy_pct,
        iv_minus_rv_pct: report.iv_minus_rv_pct,
        live_chain_available: report.live_chain_available,
        live_contract_count: report.live_contract_count,
        nearest_expiry: report
            .nearest_expiry
            .map(|d| d.format("%Y-%m-%d").to_string()),
        legs: report
            .legs
            .into_iter()
            .map(|l| OptionLegSpec {
                role: l.role,
                side: l.side,
                option_type: l.option_type,
                strike: l.strike,
                fair_value_pct: l.fair_value_pct,
                bid: l.bid,
                ask: l.ask,
                spread_abs: l.spread_abs,
                spread_bps: l.spread_bps,
                execution_price: l.execution_price,
            })
            .collect(),
        net_premium_pct: report.net_premium_pct,
        total_spread_drag_pct: report.total_spread_drag_pct,
        statutory_cost_pct: report.statutory_cost_pct,
        net_after_friction_pct: report.net_after_friction_pct,
        max_profit_pct: report.max_profit_pct,
        max_loss_pct: report.max_loss_pct,
        probability_of_profit_pct: report.probability_of_profit_pct,
        buy_hold_cum_pct: report.buy_hold_cum_pct,
        overlay_gross_cum_pct: report.overlay_gross_cum_pct,
        overlay_net_cum_pct: report.overlay_net_cum_pct,
        overlay_max_drawdown_pct: report.overlay_max_drawdown_pct,
        buy_hold_max_drawdown_pct: report.buy_hold_max_drawdown_pct,
        win_periods: report.win_periods,
        total_periods: report.total_periods,
        equity_curve: report
            .equity_curve
            .into_iter()
            .map(|pt| OverlayEquityPoint {
                date: pt.date.format("%Y-%m-%d").to_string(),
                buy_hold_cum_pct: pt.buy_hold_cum_pct,
                overlay_gross_cum_pct: pt.overlay_gross_cum_pct,
                overlay_net_cum_pct: pt.overlay_net_cum_pct,
                period_payoff_pct: pt.period_payoff_pct,
            })
            .collect(),
        strike_lab: report
            .strike_lab
            .into_iter()
            .map(|row| StrikeLabRow {
                option_type: row.option_type,
                strike: row.strike,
                expiry: row.expiry.map(|d| d.format("%Y-%m-%d").to_string()),
                bid: row.bid,
                ask: row.ask,
                mid: row.mid,
                spread_abs: row.spread_abs,
                spread_bps: row.spread_bps,
                liquidity_tier: row.liquidity_tier,
                open_interest: row.open_interest,
                exceedance_prob_pct: row.exceedance_prob_pct,
                moneyness_pct: row.moneyness_pct,
            })
            .collect(),
        availability_note: report.availability_note,
    })
}

/// `POST /experimental-analysis/forecast`: computes Markov + trailing volatility regime
/// conditioned multi-step quantile fans & historical analog episodes (Tab 11).
pub(crate) async fn post_forecast(
    State(state): State<Arc<AppState>>,
    ApiJson(req): ApiJson<ExperimentalForecastRequest>,
) -> Response {
    let parsed_id = match parse_study_instrument_id(&req.instrument_id) {
        Ok(id) => id,
        Err(detail) => return unprocessable(detail),
    };
    let (start_date, end_date) = match parse_date_range(&req.start_date, &req.end_date) {
        Ok(range) => range,
        Err(detail) => return unprocessable(detail),
    };

    let horizon_steps = req.horizon_steps.unwrap_or(6);
    if !(1..=12).contains(&horizon_steps) {
        return unprocessable(invalid_field(
            "horizon_steps",
            "out_of_range",
            "horizon_steps must be between 1 and 12",
        ));
    }
    let spread_slippage_bps = req.spread_slippage_bps.unwrap_or(Decimal::ZERO);
    if spread_slippage_bps < Decimal::ZERO || spread_slippage_bps > Decimal::from(500) {
        return unprocessable(invalid_field(
            "spread_slippage_bps",
            "out_of_range",
            "spread_slippage_bps must be between 0 and 500 bps",
        ));
    }
    let futures_roll_basis_bps = req.futures_roll_basis_bps.unwrap_or(Decimal::ZERO);
    if futures_roll_basis_bps < Decimal::from(-500) || futures_roll_basis_bps > Decimal::from(500) {
        return unprocessable(invalid_field(
            "futures_roll_basis_bps",
            "out_of_range",
            "futures_roll_basis_bps must be between -500 and +500 bps",
        ));
    }

    let instrument = match resolve_study_instrument(&state, &parsed_id).await {
        Ok(inst) => inst,
        Err(resp) => return resp,
    };
    let canonical_id = instrument.id().clone();

    let daily_bars =
        match load_daily_study_bars(&state, &canonical_id, start_date, end_date, false).await {
            Ok(bars) => bars,
            Err(resp) => return resp,
        };
    if daily_bars.is_empty() {
        return failure(
            StatusCode::NOT_FOUND,
            ErrorDetail::new(
                ErrorCode::NotFound,
                format!(
                    "no daily bars available for {canonical_id} between {start_date} and {end_date}"
                ),
            ),
        );
    }

    let cal_inner = match state.lakehouse_root() {
        Some(root) => NseTradingCalendar::from_data_root(root),
        None => NseTradingCalendar::new(),
    };
    let calendar = RuntimeNseCalendar(cal_inner);
    let domain_asset = to_domain_asset_class(req.asset_class);
    let domain_schedule = to_domain_schedule(req.period);
    let domain_calc = to_domain_calc(req.calc_type);

    let (mut periods, _) = studies::slice_periods(
        &daily_bars,
        &canonical_id.symbol,
        domain_asset,
        domain_schedule,
        DomainSubCycle::Current,
        domain_calc,
        &calendar,
    );
    if periods.is_empty() {
        return unprocessable(invalid_field(
            "period",
            "insufficient_periods",
            "selected date range does not produce at least one complete period",
        ));
    }

    studies::apply_spread_and_basis(
        &mut periods,
        domain_asset,
        &canonical_id.to_string(),
        domain_calc,
        &SpreadCostConfig {
            mode: to_domain_roll_mode(req.roll_basis_mode),
            spread_slippage_bps,
            futures_roll_basis_bps,
            include_statutory_costs: req.include_statutory_costs,
        },
        20,
    );

    let live_spot = read_live_spot(&state, &canonical_id).await;
    let forecast = studies::evaluate_forecasting(&periods, live_spot, horizon_steps);

    success(ExperimentalForecastResponse {
        instrument_id: canonical_id.to_string(),
        symbol: canonical_id.symbol.clone(),
        exchange: canonical_id.exchange.to_string(),
        asset_class: req.asset_class,
        period: req.period,
        start_date: req.start_date,
        end_date: req.end_date,
        spot_price: forecast.spot_price,
        current_markov_state: forecast.current_markov_state.map(map_markov_state),
        current_markov_label: forecast.current_markov_label,
        current_vol_regime: map_from_domain_vol_regime(forecast.current_vol_regime),
        current_trailing_vol_pct: forecast.current_trailing_vol_pct,
        match_tier: forecast.match_tier,
        analog_count: forecast.analog_count,
        horizon_fan: forecast
            .horizon_fan
            .into_iter()
            .map(|h| ForecastHorizonStep {
                step: h.step,
                sample_count: h.sample_count,
                p10_pct: h.p10_pct,
                p25_pct: h.p25_pct,
                p50_pct: h.p50_pct,
                p75_pct: h.p75_pct,
                p90_pct: h.p90_pct,
                mean_pct: h.mean_pct,
                positive_prob_pct: h.positive_prob_pct,
                p10_price: h.p10_price,
                p50_price: h.p50_price,
                p90_price: h.p90_price,
            })
            .collect(),
        top_analogs: forecast
            .top_analogs
            .into_iter()
            .map(|a| AnalogEpisode {
                trigger_end_date: a.trigger_end_date.format("%Y-%m-%d").to_string(),
                trigger_return_pct: a.trigger_return_pct,
                markov_state: map_markov_state(a.markov_state),
                vol_regime: map_from_domain_vol_regime(a.vol_regime),
                trailing_vol_pct: a.trailing_vol_pct,
                next_1_pct: a.next_1_pct,
                horizon_end_pct: a.horizon_end_pct,
            })
            .collect(),
    })
}

/// `POST /experimental-analysis/events`: computes `[-20d, +20d]` benchmark-adjusted CAR & AAR
/// event reaction curves over real Lakehouse corporate actions, filings, and economic calendar releases.
pub(crate) async fn post_events(
    State(state): State<Arc<AppState>>,
    ApiJson(req): ApiJson<ExperimentalEventStudyRequest>,
) -> Response {
    let parsed_id = match parse_study_instrument_id(&req.instrument_id) {
        Ok(id) => id,
        Err(detail) => return unprocessable(detail),
    };
    let (start_date, end_date) = match parse_date_range(&req.start_date, &req.end_date) {
        Ok(range) => range,
        Err(detail) => return unprocessable(detail),
    };

    let instrument = match resolve_study_instrument(&state, &parsed_id).await {
        Ok(inst) => inst,
        Err(resp) => return resp,
    };
    let canonical_id = instrument.id().clone();

    let daily_bars =
        match load_daily_study_bars(&state, &canonical_id, start_date, end_date, false).await {
            Ok(bars) => bars,
            Err(resp) => return resp,
        };
    if daily_bars.is_empty() {
        return failure(
            StatusCode::NOT_FOUND,
            ErrorDetail::new(
                ErrorCode::NotFound,
                format!(
                    "no daily bars available for {canonical_id} between {start_date} and {end_date}"
                ),
            ),
        );
    }

    // Optional benchmark bars for market-adjusted CAR subtraction.
    let mut benchmark_bars: Option<Vec<OhlcvBar>> = None;
    let mut benchmark_symbol: Option<String> = None;
    if let Some(raw_bench) = req
        .benchmark_id
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        && let Ok(bench_parsed) = parse_study_instrument_id(raw_bench)
        && let Ok(bench_inst) = resolve_study_instrument(&state, &bench_parsed).await
    {
        let bench_id = bench_inst.id().clone();
        if bench_id != canonical_id
            && let Ok(bb) = load_daily_study_bars(&state, &bench_id, start_date, end_date, false).await
            && !bb.is_empty()
        {
            benchmark_symbol = Some(bench_id.to_string());
            benchmark_bars = Some(bb);
        }
    }

    let mut events: Vec<studies::StudyEventInput> = Vec::new();
    let sym_label = canonical_id.symbol.clone();

    if let Some(root) = state.lakehouse_root() {
        match req.mode {
            StudyEventKind::Buyback => {
                if let Ok(actions) = CorporateActionsProjectionReader::new(root).read() {
                    for act in actions {
                        if act.ex_date < start_date || act.ex_date > end_date {
                            continue;
                        }
                        if !instrument_text_matches(&act.instrument, &canonical_id) {
                            continue;
                        }
                        let ty = act.action_type.to_ascii_lowercase();
                        if matches!(
                            ty.as_str(),
                            "buyback" | "bonus" | "split" | "rights"
                        ) {
                            let detail = if let Some(p) = act.per_share {
                                Some(format!("Amount/Share: {p} {}", act.currency))
                            } else if let (Some(o), Some(n)) = (act.old_shares, act.new_shares) {
                                Some(format!("Ratio {n}:{o}"))
                            } else {
                                act.issue_price.map(|ip| format!("Issue price {ip}"))
                            };
                            events.push(studies::StudyEventInput {
                                event_date: act.ex_date,
                                symbol: sym_label.clone(),
                                kind: studies::StudyEventKind::Buyback,
                                title: format!("Corporate Action: {}", act.action_type.to_uppercase()),
                                detail,
                            });
                        }
                    }
                }
                if let Ok(filings) = FilingsProjectionReader::new(root).read() {
                    for f in filings {
                        if f.filing_date < start_date || f.filing_date > end_date {
                            continue;
                        }
                        if !instrument_text_matches(&f.instrument, &canonical_id) {
                            continue;
                        }
                        let lower_title = f.title.to_ascii_lowercase();
                        if lower_title.contains("buyback")
                            || lower_title.contains("tender")
                            || lower_title.contains("bonus")
                            || lower_title.contains("split")
                        {
                            events.push(studies::StudyEventInput {
                                event_date: f.filing_date,
                                symbol: sym_label.clone(),
                                kind: studies::StudyEventKind::Buyback,
                                title: f.title,
                                detail: f.source_url,
                            });
                        }
                    }
                }
            }
            StudyEventKind::Earnings => {
                if let Ok(filings) = FilingsProjectionReader::new(root).read() {
                    for f in filings {
                        if f.filing_date < start_date || f.filing_date > end_date {
                            continue;
                        }
                        if !instrument_text_matches(&f.instrument, &canonical_id) {
                            continue;
                        }
                        let lower_type = f.filing_type.to_ascii_lowercase();
                        let lower_title = f.title.to_ascii_lowercase();
                        if lower_type == "quarterly_result"
                            || lower_title.contains("result")
                            || lower_title.contains("earnings")
                            || lower_title.contains("financial")
                        {
                            events.push(studies::StudyEventInput {
                                event_date: f.filing_date,
                                symbol: sym_label.clone(),
                                kind: studies::StudyEventKind::Earnings,
                                title: f.title,
                                detail: f.source_url,
                            });
                        }
                    }
                }
            }
            StudyEventKind::Macro => {
                let country_filter = req
                    .macro_country
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty());
                if let Ok(cal_rows) = EconomicCalendarProjectionReader::new(root).read() {
                    for ev in cal_rows {
                        if ev.scheduled_date < start_date || ev.scheduled_date > end_date {
                            continue;
                        }
                        if let Some(c) = country_filter
                            && !ev.country.eq_ignore_ascii_case(c)
                        {
                            continue;
                        }
                        if !ev.impact_level.eq_ignore_ascii_case("high")
                            && !ev.impact_level.eq_ignore_ascii_case("medium")
                        {
                            continue;
                        }
                        events.push(studies::StudyEventInput {
                            event_date: ev.scheduled_date,
                            symbol: ev.country.clone(),
                            kind: studies::StudyEventKind::Macro,
                            title: format!("{} ({})", ev.event_name, ev.country),
                            detail: Some(format!(
                                "{} impact={} actual={} forecast={} prior={}",
                                ev.category,
                                ev.impact_level,
                                ev.actual
                                    .map(|d| d.to_string())
                                    .unwrap_or_else(|| "—".to_string()),
                                ev.forecast
                                    .map(|d| d.to_string())
                                    .unwrap_or_else(|| "—".to_string()),
                                ev.prior
                                    .map(|d| d.to_string())
                                    .unwrap_or_else(|| "—".to_string()),
                            )),
                        });
                    }
                }
            }
            StudyEventKind::FnoRebalance => {
                if let Ok(filings) = FilingsProjectionReader::new(root).read() {
                    for f in filings {
                        if f.filing_date < start_date || f.filing_date > end_date {
                            continue;
                        }
                        if !instrument_text_matches(&f.instrument, &canonical_id) {
                            continue;
                        }
                        let lower_title = f.title.to_ascii_lowercase();
                        if f.filing_type.eq_ignore_ascii_case("corporate_action")
                            || lower_title.contains("f&o")
                            || lower_title.contains("fno")
                            || lower_title.contains("rebalance")
                            || lower_title.contains("inclusion")
                            || lower_title.contains("exclusion")
                            || lower_title.contains("index")
                        {
                            events.push(studies::StudyEventInput {
                                event_date: f.filing_date,
                                symbol: sym_label.clone(),
                                kind: studies::StudyEventKind::FnoRebalance,
                                title: f.title,
                                detail: f.source_url,
                            });
                        }
                    }
                }
            }
        }
    }

    // Deduplicate same date + title events cleanly.
    let mut seen_events: BTreeSet<(NaiveDate, String)> = BTreeSet::new();
    events.retain(|ev| seen_events.insert((ev.event_date, ev.title.clone())));

    let report = studies::evaluate_event_study(
        &daily_bars,
        benchmark_bars.as_deref(),
        benchmark_symbol.as_deref(),
        &events,
    );

    success(ExperimentalEventStudyResponse {
        instrument_id: canonical_id.to_string(),
        symbol: canonical_id.symbol.clone(),
        exchange: canonical_id.exchange.to_string(),
        mode: req.mode,
        available: report.available,
        benchmark_adjusted: report.benchmark_adjusted,
        benchmark_symbol: report.benchmark_symbol,
        event_count: report.event_count,
        avg_pre_drift_pct: report.avg_pre_drift_pct,
        avg_event_day_pct: report.avg_event_day_pct,
        avg_post_5d_pct: report.avg_post_5d_pct,
        avg_post_20d_pct: report.avg_post_20d_pct,
        win_rate_post_1d_pct: report.win_rate_post_1d_pct,
        win_rate_post_5d_pct: report.win_rate_post_5d_pct,
        win_rate_post_20d_pct: report.win_rate_post_20d_pct,
        avg_volume_surge_ratio: report.avg_volume_surge_ratio,
        window_curve: report
            .window_curve
            .into_iter()
            .map(|pt| EventWindowPoint {
                day_offset: pt.day_offset,
                sample_count: pt.sample_count,
                avg_abnormal_return_pct: pt.avg_abnormal_return_pct,
                cum_abnormal_return_pct: pt.cum_abnormal_return_pct,
                median_return_pct: pt.median_return_pct,
                win_rate_pct: pt.win_rate_pct,
            })
            .collect(),
        events: report
            .events
            .into_iter()
            .map(|r| EventStudyRow {
                event_date: r.event_date.format("%Y-%m-%d").to_string(),
                symbol: r.symbol,
                kind: map_from_domain_event_kind(r.kind),
                title: r.title,
                detail: r.detail,
                pre_drift_pct: r.pre_drift_pct,
                event_day_pct: r.event_day_pct,
                post_5d_pct: r.post_5d_pct,
                post_20d_pct: r.post_20d_pct,
                volume_ratio: r.volume_ratio,
            })
            .collect(),
        availability_note: report.availability_note,
    })
}

/// `POST /experimental-analysis/outlier-news`: retrieves real dated news headlines,
/// macro calendar releases, corporate actions & filings inside `[window_start - 2d, window_end + 1d]`.
pub(crate) async fn post_outlier_news(
    State(state): State<Arc<AppState>>,
    ApiJson(req): ApiJson<ExperimentalOutlierNewsRequest>,
) -> Response {
    let parsed_id = match parse_study_instrument_id(&req.instrument_id) {
        Ok(id) => id,
        Err(detail) => return unprocessable(detail),
    };
    let window_start = match NaiveDate::parse_from_str(req.window_start.trim(), "%Y-%m-%d") {
        Ok(d) => d,
        Err(_) => {
            return unprocessable(invalid_field(
                "window_start",
                "invalid_date",
                "window_start must be YYYY-MM-DD",
            ));
        }
    };
    let window_end = match NaiveDate::parse_from_str(req.window_end.trim(), "%Y-%m-%d") {
        Ok(d) => d,
        Err(_) => {
            return unprocessable(invalid_field(
                "window_end",
                "invalid_date",
                "window_end must be YYYY-MM-DD",
            ));
        }
    };
    if window_start > window_end {
        return unprocessable(invalid_field(
            "window_end",
            "empty_range",
            "window_start cannot be after window_end",
        ));
    }

    let limit = req.limit.unwrap_or(20).clamp(1, 50);
    let canonical_id = match resolve_study_instrument(&state, &parsed_id).await {
        Ok(inst) => inst.id().clone(),
        Err(_) => parsed_id,
    };

    let expanded_start = window_start - Duration::days(2);
    let expanded_end = window_end + Duration::days(1);

    let keywords = req
        .keywords
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| match req.asset_class {
            StudyAssetClass::Commodity => "crude oil opec gold fed macro".to_string(),
            StudyAssetClass::Forex => "rbi fed currency dollar usd inr macro".to_string(),
            StudyAssetClass::Index => format!("{} market economy finance macro", canonical_id.symbol),
            StudyAssetClass::Etf => format!("{} etf flows macro index", canonical_id.symbol),
            StudyAssetClass::FnoStock | StudyAssetClass::Equity => {
                format!("{} earnings macro corporate event", canonical_id.symbol)
            }
        });

    let tokens: Vec<String> = keywords
        .split(|c: char| !c.is_alphanumeric())
        .map(|t| t.trim().to_ascii_lowercase())
        .filter(|t| t.len() >= 2)
        .collect();
    let matches_kw = |text: &str| -> bool {
        if tokens.is_empty() {
            return true;
        }
        let lower = text.to_ascii_lowercase();
        tokens.iter().any(|tok| lower.contains(tok))
    };

    let mut items: Vec<OutlierNewsItem> = Vec::new();
    let mut news_archive_available = false;
    let mut macro_calendar_available = false;
    let mut filings_available = false;

    if let Some(root) = state.lakehouse_root() {
        if let Ok(articles) = NewsProjectionReader::new(root).read() {
            news_archive_available = true;
            for a in articles {
                let d = a.published_date;
                if d < expanded_start || d > expanded_end {
                    continue;
                }
                let mentions = a
                    .instruments
                    .iter()
                    .any(|tag| instrument_text_matches(tag, &canonical_id));
                let haystack = format!(
                    "{} {} {}",
                    a.title,
                    a.summary.as_deref().unwrap_or(""),
                    a.instruments.join(" ")
                );
                if mentions || matches_kw(&haystack) {
                    items.push(OutlierNewsItem {
                        kind: "news".to_string(),
                        date: d.format("%Y-%m-%d").to_string(),
                        published_at: a.published_at.to_rfc3339(),
                        title: a.title,
                        source: a.source,
                        url: if a.url.is_empty() { None } else { Some(a.url) },
                        summary: a.summary,
                        instruments: a.instruments,
                    });
                }
            }
        }

        if let Ok(cal_rows) = EconomicCalendarProjectionReader::new(root).read() {
            macro_calendar_available = true;
            for ev in cal_rows {
                if ev.scheduled_date < expanded_start || ev.scheduled_date > expanded_end {
                    continue;
                }
                let haystack = format!("{} {} {}", ev.event_name, ev.category, ev.country);
                let important = ev.impact_level.eq_ignore_ascii_case("high");
                if important || matches_kw(&haystack) {
                    items.push(OutlierNewsItem {
                        kind: "macro_calendar".to_string(),
                        date: ev.scheduled_date.format("%Y-%m-%d").to_string(),
                        published_at: ev.scheduled_at.clone(),
                        title: ev.event_name,
                        source: format!("Macro Calendar ({})", ev.country),
                        url: None,
                        summary: Some(format!(
                            "{} | impact={} | actual={} | forecast={} | prior={}",
                            ev.category,
                            ev.impact_level,
                            ev.actual
                                .map(|d| d.to_string())
                                .unwrap_or_else(|| "—".to_string()),
                            ev.forecast
                                .map(|d| d.to_string())
                                .unwrap_or_else(|| "—".to_string()),
                            ev.prior
                                .map(|d| d.to_string())
                                .unwrap_or_else(|| "—".to_string()),
                        )),
                        instruments: vec![canonical_id.to_string()],
                    });
                }
            }
        }

        if let Ok(filings) = FilingsProjectionReader::new(root).read() {
            filings_available = true;
            for f in filings {
                if f.filing_date < expanded_start || f.filing_date > expanded_end {
                    continue;
                }
                if !instrument_text_matches(&f.instrument, &canonical_id) {
                    continue;
                }
                items.push(OutlierNewsItem {
                    kind: "filing".to_string(),
                    date: f.filing_date.format("%Y-%m-%d").to_string(),
                    published_at: format!("{}T00:00:00Z", f.filing_date),
                    title: f.title,
                    source: format!("Exchange Filing ({})", f.filing_type),
                    url: f.source_url,
                    summary: None,
                    instruments: vec![f.instrument],
                });
            }
        }

        if let Ok(actions) = CorporateActionsProjectionReader::new(root).read() {
            for act in actions {
                if act.ex_date < expanded_start || act.ex_date > expanded_end {
                    continue;
                }
                if !instrument_text_matches(&act.instrument, &canonical_id) {
                    continue;
                }
                items.push(OutlierNewsItem {
                    kind: "corporate_action".to_string(),
                    date: act.ex_date.format("%Y-%m-%d").to_string(),
                    published_at: format!("{}T00:00:00Z", act.ex_date),
                    title: format!("Corporate Action: {} ({})", act.action_type, canonical_id.symbol),
                    source: "Corporate Actions Registry".to_string(),
                    url: None,
                    summary: act
                        .per_share
                        .map(|v| format!("Per share: {v} {}", act.currency)),
                    instruments: vec![act.instrument],
                });
            }
        }
    }

    items.sort_by(|a, b| b.published_at.cmp(&a.published_at));
    items.truncate(limit);
    let count = items.len();
    let availability_note = if count > 0 {
        format!(
            "Found {count} archived news, macro, or filing catalysts across {}..{}.",
            expanded_start, expanded_end
        )
    } else if !news_archive_available && !macro_calendar_available && !filings_available {
        "Lakehouse news, macro calendar, and filings projections are offline or not ingested in this data root. Consistent with the zero-fabrication policy, no synthetic headlines were created.".to_string()
    } else {
        format!(
            "No archived news headlines, macro releases, or filings matched inside {}..{} for {}.",
            expanded_start, expanded_end, canonical_id
        )
    };

    success(ExperimentalOutlierNewsResponse {
        instrument_id: canonical_id.to_string(),
        asset_class: req.asset_class,
        expanded_start: expanded_start.format("%Y-%m-%d").to_string(),
        expanded_end: expanded_end.format("%Y-%m-%d").to_string(),
        keywords,
        items,
        count,
        news_archive_available,
        macro_calendar_available,
        filings_available,
        availability_note,
    })
}

/// Read-through `StudyReader` adapter wiring `Experimental-Analysis` statistical summaries
/// directly into `tantra-agent-chat-tools` (`experimental_study_summary`).
pub(crate) struct FeedStudyReader {
    instruments: Arc<dyn InstrumentMaster>,
    bars: Arc<dyn BarReader>,
    quotes: Arc<dyn QuoteReader>,
    data_root: Option<PathBuf>,
}

impl FeedStudyReader {
    pub(crate) fn new(
        instruments: Arc<dyn InstrumentMaster>,
        bars: Arc<dyn BarReader>,
        quotes: Arc<dyn QuoteReader>,
        data_root: PathBuf,
    ) -> Self {
        Self {
            instruments,
            bars,
            quotes,
            data_root: if data_root.exists() {
                Some(data_root)
            } else {
                None
            },
        }
    }
}

fn parse_chat_asset_class(raw: Option<&str>, ex: Exchange, sym: &str) -> DomainAssetClass {
    match raw.map(|s| s.trim().to_ascii_lowercase()).as_deref() {
        Some("index") => DomainAssetClass::Index,
        Some("fno_stock") | Some("fno") => DomainAssetClass::FnoStock,
        Some("etf") => DomainAssetClass::Etf,
        Some("equity") => DomainAssetClass::Equity,
        Some("forex") | Some("fx") => DomainAssetClass::Forex,
        Some("commodity") => DomainAssetClass::Commodity,
        _ => {
            if looks_like_commodity(ex, sym) {
                DomainAssetClass::Commodity
            } else if looks_like_forex(ex, sym) {
                DomainAssetClass::Forex
            } else if looks_like_etf(sym) {
                DomainAssetClass::Etf
            } else if sym.contains("NIFTY") || sym.contains("SENSEX") || sym.contains("VIX") {
                DomainAssetClass::Index
            } else {
                DomainAssetClass::FnoStock
            }
        }
    }
}

fn parse_chat_schedule(raw: Option<&str>, asset_class: DomainAssetClass) -> DomainSchedule {
    match raw.map(|s| s.trim().to_ascii_lowercase()).as_deref() {
        Some("weekly_expiry") => DomainSchedule::WeeklyExpiry,
        Some("week_plus_one") => DomainSchedule::WeekPlusOne,
        Some("monthly_expiry") => DomainSchedule::MonthlyExpiry,
        Some("calendar_weekly") => DomainSchedule::CalendarWeekly,
        Some("calendar_monthly") => DomainSchedule::CalendarMonthly,
        Some("front_month_cycle") => DomainSchedule::FrontMonthCycle,
        Some("mcx_expiry_cycle") => DomainSchedule::McxExpiryCycle,
        Some("daily") => DomainSchedule::Daily,
        _ => match asset_class {
            DomainAssetClass::Index => DomainSchedule::WeeklyExpiry,
            DomainAssetClass::FnoStock => DomainSchedule::MonthlyExpiry,
            DomainAssetClass::Commodity => DomainSchedule::FrontMonthCycle,
            DomainAssetClass::Etf | DomainAssetClass::Equity | DomainAssetClass::Forex => {
                DomainSchedule::CalendarWeekly
            }
        },
    }
}

fn parse_chat_calc(raw: Option<&str>) -> DomainCalc {
    match raw.map(|s| s.trim().to_ascii_lowercase()).as_deref() {
        Some("high_to_low") | Some("hl") => DomainCalc::HighToLow,
        Some("close_to_close") | Some("cc") => DomainCalc::CloseToClose,
        _ => DomainCalc::OpenToClose,
    }
}

#[async_trait]
impl StudyReader for FeedStudyReader {
    async fn returns_study_summary(
        &self,
        instrument: &InstrumentId,
        asset_class: Option<&str>,
        period: Option<&str>,
        calc_type: Option<&str>,
        start_date: Option<NaiveDate>,
        end_date: Option<NaiveDate>,
    ) -> Result<StudySummarySnapshot, PortError> {
        let end = end_date.unwrap_or_else(|| Utc::now().date_naive());
        let start = start_date.unwrap_or_else(|| end - Duration::days(365 * 3));

        let inst_match = match self.instruments.get_instrument(instrument).await {
            Ok(Some(inst)) => inst,
            Ok(None) => self
                .instruments
                .list_instruments()
                .await
                .map_err(|e| PortError::internal(format!("{e:?}")))?
                .into_iter()
                .find(|item| {
                    item.id().exchange == instrument.exchange
                        && item.id().symbol.eq_ignore_ascii_case(&instrument.symbol)
                })
                .ok_or_else(|| {
                    PortError::not_found(format!("instrument {instrument} not found"))
                })?,
            Err(e) => return Err(PortError::internal(format!("{e:?}"))),
        };
        let canonical_id = inst_match.id().clone();

        let from_utc: DateTime<Utc> = (start - Duration::days(1))
            .and_hms_opt(0, 0, 0)
            .unwrap()
            .and_utc();
        let to_utc: DateTime<Utc> = (end + Duration::days(2))
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
        .map_err(|e| PortError::internal(format!("{e:?}")))?;
        let raw_bars = self
            .bars
            .read_bars(&bar_req)
            .await
            .map_err(|e| PortError::internal(format!("{e:?}")))?;

        let ist_offset = FixedOffset::east_opt(5 * 3600 + 30 * 60).unwrap();
        let mut dedup_bars: BTreeMap<NaiveDate, OhlcvBar> = BTreeMap::new();
        for bar in raw_bars {
            let d = bar
                .ts_event()
                .to_datetime()
                .with_timezone(&ist_offset)
                .date_naive();
            if d < start || d > end {
                continue;
            }
            dedup_bars.insert(
                d,
                OhlcvBar {
                    date: d,
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
        let study_bars: Vec<OhlcvBar> = dedup_bars.into_values().collect();
        if study_bars.is_empty() {
            return Err(PortError::not_found(format!(
                "no daily bars found for {canonical_id} in {start}..{end}"
            )));
        }

        let live_spot = match self.quotes.read_quote(&canonical_id, None).await {
            Ok(Some(q)) => {
                let bid = Decimal::from_f64_retain(q.bid_price()).unwrap_or_default();
                let ask = Decimal::from_f64_retain(q.ask_price()).unwrap_or_default();
                let mid = if bid > Decimal::ZERO && ask > Decimal::ZERO {
                    (bid + ask) / Decimal::from(2)
                } else {
                    bid.max(ask)
                };
                if mid > Decimal::ZERO {
                    Some(mid.round_dp(2))
                } else {
                    None
                }
            }
            _ => None,
        };

        let cal_inner = match &self.data_root {
            Some(root) => NseTradingCalendar::from_data_root(root),
            None => NseTradingCalendar::new(),
        };
        let calendar = RuntimeNseCalendar(cal_inner);
        let domain_asset = parse_chat_asset_class(
            asset_class,
            canonical_id.exchange,
            &canonical_id.symbol,
        );
        let domain_schedule = parse_chat_schedule(period, domain_asset);
        let domain_calc = parse_chat_calc(calc_type);

        let report = studies::evaluate_returns_study(
            &study_bars,
            &calendar,
            &ExperimentalStudyConfig {
                symbol: canonical_id.symbol.clone(),
                asset_class: domain_asset,
                schedule: domain_schedule,
                sub_cycle: DomainSubCycle::Current,
                calc_kind: domain_calc,
                ma_window: 20,
                outlier_coverage_pct: Decimal::from(90),
                tail_threshold_pct: Decimal::from(2),
                live_spot_override: live_spot,
                spread_cost: SpreadCostConfig::default(),
            },
        )
        .map_err(|e| PortError::unavailable(format!("{e}")))?;

        let key_probs = [95usize, 90, 80, 50, 20, 10, 5];
        let probability_levels = report
            .distributions
            .probability_levels
            .iter()
            .filter(|row| key_probs.contains(&row.prob_above_pct))
            .map(|row| StudyProbabilityLevel {
                prob_above_pct: row.prob_above_pct,
                return_pct: row.return_pct,
                implied_price: row.implied_price,
                scenario: row.scenario.clone(),
            })
            .collect();

        Ok(StudySummarySnapshot {
            instrument: canonical_id,
            asset_class: format!("{domain_asset:?}").to_ascii_lowercase(),
            period_schedule: format!("{domain_schedule:?}").to_ascii_lowercase(),
            calc_type: format!("{domain_calc:?}").to_ascii_lowercase(),
            start_date: study_bars.first().map(|b| b.date).unwrap_or(start),
            end_date: study_bars.last().map(|b| b.date).unwrap_or(end),
            total_periods: report.period_series.len(),
            regime_note: report.regime_note,
            avg_return_pct: report.kpis.avg_return_pct,
            std_dev_pct: report.kpis.std_dev_pct,
            sharpe_ratio: report.kpis.sharpe_ratio,
            sortino_ratio: report.kpis.sortino_ratio,
            profit_factor: report.kpis.profit_factor,
            max_gain_pct: report.kpis.max_gain_pct,
            max_loss_pct: report.kpis.max_loss_pct,
            win_rate_pct: report.kpis.positive_pct,
            current_streak_direction: report
                .streaks
                .summary
                .current_streak_type
                .map(|d| format!("{d:?}").to_ascii_lowercase()),
            current_streak_length: report.streaks.summary.current_streak_length,
            current_streak_return_pct: report.streaks.summary.current_streak_return_pct,
            current_markov_state: report
                .markov
                .current_prediction
                .current_state
                .map(|s| s.label().to_string()),
            strongest_next_markov_state: report.markov.current_prediction.strongest_next_label,
            strongest_next_probability_pct: report
                .markov
                .current_prediction
                .strongest_probability_pct,
            probability_levels,
            outlier_90_lower_pct: report.distributions.outliers.kpis.lower_bound_pct,
            outlier_90_upper_pct: report.distributions.outliers.kpis.upper_bound_pct,
            outlier_count: report.distributions.outliers.kpis.total_outliers,
            after_big_up_next_avg_pct: (!report
                .distributions
                .forward_conditional
                .after_large_pos_events
                .is_empty())
            .then_some(report.distributions.forward_conditional.after_large_pos_mean_pct),
            after_big_down_next_avg_pct: (!report
                .distributions
                .forward_conditional
                .after_large_neg_events
                .is_empty())
            .then_some(report.distributions.forward_conditional.after_large_neg_mean_pct),
        })
    }
}
