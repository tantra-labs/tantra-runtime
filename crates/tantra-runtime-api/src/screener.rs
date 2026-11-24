//! Query resolution and per-instrument evaluation for `GET /screener/scans`.
//!
//! Pure like [`crate::market`]: the REST handler reads bars through its port and hands each
//! instrument's history to [`ResolvedScreenerQuery::evaluate`]; nothing here does I/O. The
//! filter grammar is the canonical [`tantra_core::screener`] AST (`ScreenerFilterGroup`,
//! `ScreenerFilterPredicate`, `Timeframe`): a metric is a bar column (`open`, `high`, `low`,
//! `close`, `volume`), `SMA<n>`, or `RSI`, and every threshold is an exact [`Decimal`] — no float
//! ever touches a price.

use std::collections::BTreeMap;

use chrono::{DateTime, Duration, Utc};
use rust_decimal::Decimal;
use serde_json::{Value, json};
pub use tantra_core::screener::{
    FilterOp, LogicalOp, MetricKeySpec, MetricPeriod, MetricRef, PredicateValue,
    ScreenerFilterGroup, ScreenerFilterItem, ScreenerFilterPredicate,
};
pub use tantra_core::signal::Timeframe;
use tantra_core::{Bar, InstrumentId};

use crate::market::{invalid, parse_bound, parse_instrument_id, parse_timeframe};
use crate::requests::ScreenerQuery;
use crate::responses::ScreenerResultRow;
use crate::wire::{BarAggregation, BarSpecification, ErrorCode, ErrorDetail};

/// Backwards-compatible alias for [`ScreenerFilterPredicate`].
pub type ScreenerPredicate = ScreenerFilterPredicate;

/// The timeframe used when `tf` is omitted from a scan.
pub const DEFAULT_SCREENER_TIMEFRAME: &str = "1d";

/// The most instruments one scan may name.
pub const MAX_SCREENER_UNIVERSE: usize = 1_000;

/// The most matching rows one scan may return; there is no pagination, so the caller narrows the
/// universe or the filter.
pub const MAX_SCREENER_ROWS: usize = 500;

/// The most bars one scan may read in total across the universe.
pub const MAX_SCREENER_BARS: usize = 2_000_000;

/// A [`ScreenerQuery`] resolved into domain values.
#[derive(Clone, Debug, PartialEq)]
pub struct ResolvedScreenerQuery {
    /// Instruments to scan, sorted and de-duplicated.
    pub universe: Vec<InstrumentId>,
    /// The parsed canonical filter (empty group when none was given); validated against the
    /// metrics a bar dataset can compute.
    pub filters: ScreenerFilterGroup,
    /// Every metric key the filter reads, in first-seen order; the keys of a row's `metrics`.
    pub metric_keys: Vec<String>,
    /// Bar specification evaluated (always last-price).
    pub spec: BarSpecification,
    /// Exclusive end of the bars read: just past `as_of`, or `None` for all history.
    pub to: Option<DateTime<Utc>>,
}

fn filter_error(error: &ScreenerError) -> ErrorDetail {
    invalid("filters", error.reason(), error.to_string())
}

/// Whether a wire timeframe names the same bar length as `spec`.
fn timeframe_matches(tf: Timeframe, spec: &BarSpecification) -> bool {
    let (step, seconds): (usize, Option<u64>) = match tf {
        Timeframe::M1 => (1, Some(60)),
        Timeframe::M5 => (5, Some(60)),
        Timeframe::M15 => (15, Some(60)),
        Timeframe::M30 => (30, Some(60)),
        Timeframe::H1 => (60, Some(60)),
        Timeframe::H2 => (120, Some(60)),
        Timeframe::H4 => (240, Some(60)),
        Timeframe::D1 => (1, Some(86_400)),
        Timeframe::W1 => (1, Some(604_800)),
        Timeframe::Month1 => {
            return spec.aggregation() == BarAggregation::Month && spec.step() == 1;
        }
    };
    let unit: u64 = match spec.aggregation() {
        BarAggregation::Second => 1,
        BarAggregation::Minute => 60,
        BarAggregation::Hour => 3_600,
        BarAggregation::Day => 86_400,
        BarAggregation::Week => 604_800,
        _ => return false,
    };
    let wanted = seconds.map(|s| s.saturating_mul(step as u64));
    wanted == Some(unit.saturating_mul(spec.step()))
}

fn parse_universe(text: Option<&str>) -> Result<Vec<InstrumentId>, ErrorDetail> {
    let bad = |message: String| invalid("universe", "invalid_universe", message);
    let Some(text) = text else {
        return Err(invalid(
            "universe",
            "missing_universe",
            "`universe` must be a JSON array of instrument ids, e.g. [\"TCS.NSE\"]",
        ));
    };
    let raw: Vec<serde_json::Value> = serde_json::from_str(text)
        .map_err(|e| bad(format!("`universe` is not a JSON array: {e}")))?;
    if raw.is_empty() {
        return Err(invalid(
            "universe",
            "missing_universe",
            "`universe` must name at least one instrument",
        ));
    }
    if raw.len() > MAX_SCREENER_UNIVERSE {
        return Err(too_many(
            "universe",
            MAX_SCREENER_UNIVERSE,
            format!(
                "the universe names {} instruments, over the limit of {MAX_SCREENER_UNIVERSE}",
                raw.len()
            ),
        ));
    }
    let mut ids = Vec::with_capacity(raw.len());
    for item in &raw {
        let text = item
            .as_str()
            .ok_or_else(|| bad(format!("universe entry {item} is not a string")))?;
        let id = parse_instrument_id(text).map_err(|_| {
            invalid(
                "universe",
                "invalid_instrument_id",
                format!("universe entry {text:?} is not SYMBOL.EXCHANGE"),
            )
        })?;
        ids.push(id);
    }
    // Canonical instrument ids have no `Ord` (exchange, symbol), so sort by the canonical
    // textual form; dedup then drops exact repeats.
    ids.sort_by_key(|id| id.to_string());
    ids.dedup();
    Ok(ids)
}

fn too_many(field: &str, limit: usize, message: String) -> ErrorDetail {
    ErrorDetail::new(ErrorCode::ValidationInvalidRequest, message).with_context(json!({
        "field": field,
        "reason": "too_many_rows",
        "limit": limit,
    }))
}

/// Rejects a scan that has read more than [`MAX_SCREENER_BARS`] bars so far.
pub fn check_scan_budget(bars_read: usize) -> Result<(), ErrorDetail> {
    if bars_read <= MAX_SCREENER_BARS {
        return Ok(());
    }
    Err(too_many(
        "universe",
        MAX_SCREENER_BARS,
        format!(
            "the scan reads over {MAX_SCREENER_BARS} bars; narrow the universe or use as_of with a \
             coarser tf"
        ),
    ))
}

/// Rejects a scan with more than [`MAX_SCREENER_ROWS`] matches.
pub fn check_screener_rows(rows: usize) -> Result<(), ErrorDetail> {
    if rows <= MAX_SCREENER_ROWS {
        return Ok(());
    }
    Err(too_many(
        "universe",
        MAX_SCREENER_ROWS,
        format!(
            "the scan matched {rows} instruments, over the limit of {MAX_SCREENER_ROWS}; narrow the universe or the filter"
        ),
    ))
}

/// Normalizes REST query JSON numbers inside predicate `value` / `value_upper` fields into exact
/// decimal strings (`Value::Number` -> `Value::String(n.to_string())`) so REST callers like
/// `?filters={"operator":"AND","items":[{"key":"close","op":"gt","value":150.0}]}` interoperate
/// cleanly with L0 `tantra_core::PredicateValue` (which forbids raw JSON floats).
fn normalize_predicate_numbers(value: &mut Value) {
    match value {
        Value::Object(map) => {
            for k in ["value", "value_upper"] {
                if let Some(v) = map.get_mut(k) {
                    normalize_numeric_operand(v);
                }
            }
            for child_list_key in ["items", "predicates"] {
                if let Some(arr) = map.get_mut(child_list_key) {
                    normalize_predicate_numbers(arr);
                }
            }
        }
        Value::Array(items) => {
            for item in items {
                normalize_predicate_numbers(item);
            }
        }
        _ => {}
    }
}

fn normalize_numeric_operand(v: &mut Value) {
    match v {
        Value::Number(n) => {
            *v = Value::String(n.to_string());
        }
        Value::Array(items) => {
            for item in items {
                normalize_numeric_operand(item);
            }
        }
        _ => {}
    }
}

impl ScreenerQuery {
    /// Resolves the query: bounded universe, parsed and validated filter, timeframe and `as_of`.
    ///
    /// A filter that reads a metric a bar dataset cannot compute is rejected here, before any
    /// bar is read.
    pub fn resolve(&self) -> Result<ResolvedScreenerQuery, ErrorDetail> {
        let universe = parse_universe(self.universe.as_deref())?;
        let spec = parse_timeframe(self.tf.as_deref().unwrap_or(DEFAULT_SCREENER_TIMEFRAME))?;
        let to = self
            .as_of
            .as_deref()
            .map(|text| parse_bound("as_of", text))
            .transpose()?
            .map(|t| t + Duration::nanoseconds(1));
        let filters: ScreenerFilterGroup = match self.filters.as_deref() {
            None => ScreenerFilterGroup::empty(),
            Some(text) => {
                let mut raw: Value = serde_json::from_str(text).map_err(|e| {
                    invalid(
                        "filters",
                        "invalid_filters",
                        format!("`filters` is not a JSON filter group: {e}"),
                    )
                })?;
                normalize_predicate_numbers(&mut raw);
                serde_json::from_value(raw).map_err(|e| {
                    invalid(
                        "filters",
                        "invalid_filters",
                        format!("`filters` is not a JSON filter group: {e}"),
                    )
                })?
            }
        };
        validate_group(&filters).map_err(|e| filter_error(&e))?;
        for tf in group_timeframes(&filters).map_err(|e| filter_error(&e))? {
            if !timeframe_matches(tf, &spec) {
                return Err(invalid(
                    "filters",
                    "unsupported_timeframe",
                    format!(
                        "a predicate asks for timeframe {tf} but the scan evaluates `tf` bars; \
                         set `tf` to match"
                    ),
                ));
            }
        }
        let metric_keys = group_metric_keys(&filters).map_err(|e| filter_error(&e))?;
        Ok(ResolvedScreenerQuery {
            universe,
            filters,
            metric_keys,
            spec,
            to,
        })
    }
}

impl ResolvedScreenerQuery {
    /// Evaluates the filter on one instrument's bars (ascending, latest last).
    ///
    /// `Some(row)` when the instrument matches, carrying the latest value of every metric the
    /// filter reads; `None` when it does not.
    pub fn evaluate(
        &self,
        id: &InstrumentId,
        bars: &[Bar],
    ) -> Result<Option<ScreenerResultRow>, ErrorDetail> {
        if !evaluate_group(&self.filters, bars).map_err(|e| filter_error(&e))? {
            return Ok(None);
        }
        let metrics: BTreeMap<String, Option<Decimal>> =
            latest_metrics(&self.metric_keys, bars).map_err(|e| filter_error(&e))?;
        Ok(Some(ScreenerResultRow {
            instrument_id: id.clone(),
            metrics,
        }))
    }
}

/// Why a filter was refused. `reason()` is the stable `context.reason` of the wire error.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum ScreenerError {
    /// A malformed group, operator, op or value shape.
    #[error("invalid filter: {0}")]
    InvalidFilter(String),
    /// A metric key a bar dataset cannot compute.
    #[error("unsupported metric: {0}")]
    UnsupportedMetric(String),
    /// A timeframe dimension the scan cannot satisfy.
    #[error("unsupported timeframe: {0}")]
    UnsupportedTimeframe(String),
}

impl ScreenerError {
    /// The stable `context.reason` for this error.
    pub fn reason(&self) -> &'static str {
        match self {
            Self::InvalidFilter(_) => "invalid_filter",
            Self::UnsupportedMetric(_) => "unsupported_metric",
            Self::UnsupportedTimeframe(_) => "unsupported_timeframe",
        }
    }
}

/// The metrics a bar dataset computes.
const BASE_METRICS: [&str; 5] = ["open", "high", "low", "close", "volume"];

/// The period of an `SMA<n>` metric key, or `None` when `key` is not one.
fn parse_sma(key: &str) -> Option<u32> {
    let digits = key
        .strip_prefix("SMA")
        .or_else(|| key.strip_prefix("sma"))?;
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let period: u32 = digits.parse().ok()?;
    (period >= 1).then_some(period)
}

fn is_rsi_key(key: &str) -> bool {
    matches!(key, "RSI" | "rsi" | "RSI14" | "rsi14")
}

fn validate_metric_key(key: &str) -> Result<(), ScreenerError> {
    if BASE_METRICS.contains(&key) || parse_sma(key).is_some() || is_rsi_key(key) {
        Ok(())
    } else {
        Err(ScreenerError::UnsupportedMetric(key.to_owned()))
    }
}

fn validate_period(key: &str, period: Option<MetricPeriod>) -> Result<(), ScreenerError> {
    match period {
        None
        | Some(
            MetricPeriod::Snapshot
            | MetricPeriod::Current
            | MetricPeriod::Daily
            | MetricPeriod::Weekly
            | MetricPeriod::Monthly
            | MetricPeriod::Intraday,
        ) => Ok(()),
        Some(_) => Err(ScreenerError::UnsupportedMetric(key.to_owned())),
    }
}

fn validate_operand(value: &PredicateValue) -> Result<(), ScreenerError> {
    match value {
        PredicateValue::Decimal(_) => Ok(()),
        PredicateValue::MetricRef(m) => {
            validate_metric_key(&m.0.key)?;
            validate_period(&m.0.key, m.0.period)
        }
        other => Err(ScreenerError::InvalidFilter(format!(
            "value {other:?} is not a numeric threshold or metric reference"
        ))),
    }
}

fn validate_predicate_value(op: FilterOp, value: &PredicateValue) -> Result<(), ScreenerError> {
    match op {
        FilterOp::Between => {
            let Some(bounds) = value.as_list().filter(|a| a.len() == 2) else {
                return Err(ScreenerError::InvalidFilter(
                    "`between` needs [low, high] as its value".to_owned(),
                ));
            };
            for bound in bounds {
                validate_operand(bound)?;
            }
            Ok(())
        }
        FilterOp::In | FilterOp::NotIn => {
            let Some(items) = value.as_list() else {
                return Err(ScreenerError::InvalidFilter(format!(
                    "`{op}` requires a list value"
                )));
            };
            for item in items {
                validate_operand(item)?;
            }
            Ok(())
        }
        FilterOp::Like | FilterOp::Has => {
            if matches!(value, PredicateValue::List(_)) {
                return Err(ScreenerError::InvalidFilter(format!(
                    "`{op}` requires a scalar operand"
                )));
            }
            Ok(())
        }
        FilterOp::Eq
        | FilterOp::Ne
        | FilterOp::Gt
        | FilterOp::Gte
        | FilterOp::Lt
        | FilterOp::Lte
        | FilterOp::CrossesAbove
        | FilterOp::CrossesBelow => validate_operand(value),
    }
}

/// Validates a group: operators, ops, value shapes and metric keys, recursively.
///
/// Rejected before any bar is read, so a typo costs nothing.
pub fn validate_group(group: &ScreenerFilterGroup) -> Result<(), ScreenerError> {
    if group.classification.as_ref().is_some_and(|c| !c.is_empty()) {
        return Err(ScreenerError::InvalidFilter(
            "classification filters are not supported on bar-only /screener/scans".to_owned(),
        ));
    }
    for item in &group.items {
        match item {
            ScreenerFilterItem::Group(nested) => validate_group(nested)?,
            ScreenerFilterItem::Predicate(predicate) => {
                if predicate.relative_to.is_some() || predicate.relative_stat.is_some() {
                    return Err(ScreenerError::InvalidFilter(
                        "relative_to predicates are not supported on bar-only /screener/scans"
                            .to_owned(),
                    ));
                }
                validate_metric_key(&predicate.metric.0.key)?;
                validate_period(&predicate.metric.0.key, predicate.metric.0.period)?;
                predicate
                    .validate()
                    .map_err(|e| ScreenerError::InvalidFilter(e.to_string()))?;
                validate_predicate_value(predicate.op, &predicate.value)?;
            }
        }
    }
    Ok(())
}

/// Every metric key the group reads, in first-seen order, de-duplicated.
pub fn group_metric_keys(group: &ScreenerFilterGroup) -> Result<Vec<String>, ScreenerError> {
    Ok(group.metric_keys())
}

/// Every timeframe dimension the group asks for, in first-seen order, de-duplicated.
pub fn group_timeframes(group: &ScreenerFilterGroup) -> Result<Vec<Timeframe>, ScreenerError> {
    let mut timeframes = Vec::new();
    collect_timeframes(group, &mut timeframes);
    Ok(timeframes)
}

fn collect_timeframes(group: &ScreenerFilterGroup, timeframes: &mut Vec<Timeframe>) {
    for item in &group.items {
        match item {
            ScreenerFilterItem::Group(nested) => collect_timeframes(nested, timeframes),
            ScreenerFilterItem::Predicate(predicate) => {
                if let Some(tf) = predicate.metric.0.timeframe
                    && !timeframes.contains(&tf)
                {
                    timeframes.push(tf);
                }
                match &predicate.value {
                    PredicateValue::MetricRef(m) => {
                        if let Some(tf) = m.0.timeframe
                            && !timeframes.contains(&tf)
                        {
                            timeframes.push(tf);
                        }
                    }
                    PredicateValue::List(items) => {
                        for elem in items {
                            if let PredicateValue::MetricRef(m) = elem
                                && let Some(tf) = m.0.timeframe
                                && !timeframes.contains(&tf)
                            {
                                timeframes.push(tf);
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
    }
}

/// The latest value of one metric over the bars, or `None` while it is warming up.
pub(crate) fn metric_value(key: &str, bars: &[Bar]) -> Option<Decimal> {
    if let Some(period) = parse_sma(key) {
        let period = period as usize;
        if bars.len() < period {
            return None;
        }
        let window = &bars[bars.len() - period..];
        let sum: Decimal = window.iter().map(|bar| bar.close.0).sum();
        // Exact decimal mean: the sum over the period, at the period's scale.
        return Some(sum / Decimal::from(period as u32));
    }
    if is_rsi_key(key) {
        if bars.len() < 15 {
            return None;
        }
        let period = 14;
        let window = &bars[bars.len() - (period + 1)..];
        let mut gains = Decimal::ZERO;
        let mut losses = Decimal::ZERO;
        for i in 1..=period {
            let diff = window[i].close.0 - window[i - 1].close.0;
            if diff > Decimal::ZERO {
                gains += diff;
            } else {
                losses -= diff;
            }
        }
        if losses == Decimal::ZERO {
            return Some(Decimal::from(100));
        }
        if gains == Decimal::ZERO {
            return Some(Decimal::ZERO);
        }
        let rs = gains / losses;
        return Some(Decimal::from(100) - (Decimal::from(100) / (Decimal::ONE + rs)));
    }
    let last = bars.last()?;
    match key {
        "open" => Some(last.open.0),
        "high" => Some(last.high.0),
        "low" => Some(last.low.0),
        "close" => Some(last.close.0),
        "volume" => Some(last.volume.0),
        _ => None,
    }
}

fn operand_value(value: &PredicateValue, bars: &[Bar]) -> Result<Option<Decimal>, ScreenerError> {
    match value {
        PredicateValue::Decimal(d) => Ok(Some(*d)),
        PredicateValue::MetricRef(m) => Ok(metric_value(&m.0.key, bars)),
        other => Err(ScreenerError::InvalidFilter(format!(
            "value {other:?} is not a number or a metric reference"
        ))),
    }
}

/// Evaluates one item: a nested group, or a predicate whose metrics are missing (still
/// warming up) counts as not matching rather than an error.
fn evaluate_item(item: &ScreenerFilterItem, bars: &[Bar]) -> Result<bool, ScreenerError> {
    match item {
        ScreenerFilterItem::Group(nested) => evaluate_group(nested, bars),
        ScreenerFilterItem::Predicate(predicate) => {
            let Some(left) = metric_value(&predicate.metric.0.key, bars) else {
                return Ok(false);
            };
            match predicate.op {
                FilterOp::Between => {
                    let Some((lo, hi)) = predicate.between_bounds() else {
                        return Err(ScreenerError::InvalidFilter(
                            "`between` needs [low, high]".to_owned(),
                        ));
                    };
                    let (Some(low), Some(high)) =
                        (operand_value(lo, bars)?, operand_value(hi, bars)?)
                    else {
                        return Ok(false);
                    };
                    Ok(low <= left && left <= high)
                }
                FilterOp::CrossesAbove | FilterOp::CrossesBelow => {
                    if bars.len() < 2 {
                        return Ok(false);
                    }
                    let prev_bars = &bars[..bars.len() - 1];
                    let Some(left_prev) = metric_value(&predicate.metric.0.key, prev_bars) else {
                        return Ok(false);
                    };
                    let Some(right_curr) = operand_value(&predicate.value, bars)? else {
                        return Ok(false);
                    };
                    let Some(right_prev) = operand_value(&predicate.value, prev_bars)? else {
                        return Ok(false);
                    };
                    Ok(if predicate.op == FilterOp::CrossesAbove {
                        left_prev <= right_prev && left > right_curr
                    } else {
                        left_prev >= right_prev && left < right_curr
                    })
                }
                FilterOp::In | FilterOp::NotIn => {
                    let Some(items) = predicate.value.as_list() else {
                        return Err(ScreenerError::InvalidFilter(format!(
                            "`{}` needs a list value",
                            predicate.op
                        )));
                    };
                    let mut matched = false;
                    for item in items {
                        if let Some(v) = operand_value(item, bars)?
                            && left == v
                        {
                            matched = true;
                            break;
                        }
                    }
                    Ok(if predicate.op == FilterOp::In {
                        matched
                    } else {
                        !matched
                    })
                }
                FilterOp::Like | FilterOp::Has => {
                    let needle = match &predicate.value {
                        PredicateValue::String(s) => s.to_lowercase(),
                        PredicateValue::Decimal(d) => d.normalize().to_string().to_lowercase(),
                        PredicateValue::Bool(b) => b.to_string().to_lowercase(),
                        other => {
                            return Err(ScreenerError::InvalidFilter(format!(
                                "cannot pattern-match {other:?}"
                            )));
                        }
                    };
                    Ok(left
                        .normalize()
                        .to_string()
                        .to_lowercase()
                        .contains(&needle))
                }
                FilterOp::Eq
                | FilterOp::Ne
                | FilterOp::Gt
                | FilterOp::Gte
                | FilterOp::Lt
                | FilterOp::Lte => {
                    let Some(right) = operand_value(&predicate.value, bars)? else {
                        return Ok(false);
                    };
                    Ok(match predicate.op {
                        FilterOp::Eq => left == right,
                        FilterOp::Ne => left != right,
                        FilterOp::Gt => left > right,
                        FilterOp::Gte => left >= right,
                        FilterOp::Lt => left < right,
                        FilterOp::Lte => left <= right,
                        _ => unreachable!(),
                    })
                }
            }
        }
    }
}

/// Evaluates a group over one instrument's bars (ascending, latest last).
pub fn evaluate_group(group: &ScreenerFilterGroup, bars: &[Bar]) -> Result<bool, ScreenerError> {
    match group.operator {
        LogicalOp::And => {
            for item in &group.items {
                if !evaluate_item(item, bars)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        // An OR group matches as soon as one item does; an empty one matches everything.
        LogicalOp::Or => {
            if group.items.is_empty() {
                return Ok(true);
            }
            for item in &group.items {
                if evaluate_item(item, bars)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
    }
}

/// The latest value of every metric key over the bars, by key; `None` while warming up.
pub fn latest_metrics(
    keys: &[String],
    bars: &[Bar],
) -> Result<BTreeMap<String, Option<Decimal>>, ScreenerError> {
    let mut metrics = BTreeMap::new();
    for key in keys {
        metrics.insert(key.clone(), metric_value(key, bars));
    }
    Ok(metrics)
}
