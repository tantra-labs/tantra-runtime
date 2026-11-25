//! Query resolution and response mapping for the market-data read endpoints
//! (`/instruments`, `/bars/{id}`, `/quotes`, `/depth/{id}`).
//!
//! Everything here is pure: it turns the string-typed query DTOs into domain
//! values and reports a bad request as an [`ErrorDetail`] that names the
//! offending field in `context.field`, so every transport rejects the same way.

use std::str::FromStr;

use chrono::{DateTime, NaiveDate, Utc};
use serde_json::{Value, json};

use tantra_core::{Exchange, InstrumentId};

use crate::requests::{BarsQuery, DepthQuery, InstrumentsQuery, QuotesQuery};
use crate::wire::{
    BarAggregation, BarSpecification, ErrorCode, ErrorDetail, Instrument, InstrumentKind, PriceType,
};

/// The timeframe used when `tf` is omitted from a bars query.
pub const DEFAULT_TIMEFRAME: &str = "1m";

/// Levels per side returned by `GET /depth/{id}` when `depth` is omitted.
pub const DEFAULT_DEPTH_LEVELS: usize = 5;

/// The most levels per side `GET /depth/{id}` accepts.
pub const MAX_DEPTH_LEVELS: u32 = 50;

/// Maximum number of instruments a `GET /instruments?limit=...` query may request.
pub const MAX_INSTRUMENTS_LIMIT: u32 = 500;

/// An [`InstrumentsQuery`] resolved into validated filter and search parameters.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolvedInstrumentsQuery<'a> {
    /// Exact exchange filter, if set.
    pub exchange: Option<&'a str>,
    /// Exact symbol filter, if set.
    pub symbol: Option<&'a str>,
    /// Trimmed non-empty fuzzy search query, if set.
    pub q: Option<&'a str>,
    /// Maximum number of rows to return, if set.
    pub limit: Option<usize>,
}

/// A [`QuotesQuery`] resolved into domain values.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolvedQuotesQuery {
    /// Requested symbols, trimmed, de-duplicated, in request order.
    pub symbols: Vec<String>,
    /// Exchange the symbols are restricted to, if any.
    pub venue: Option<Exchange>,
    /// Inclusive point in time the quotes are known at; `None` means the latest.
    pub as_of: Option<DateTime<Utc>>,
}

/// A [`BarsQuery`] resolved into domain values.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ResolvedBarsQuery {
    /// Bar specification (always last-price).
    pub spec: BarSpecification,
    /// Inclusive start, if bounded.
    pub from: Option<DateTime<Utc>>,
    /// Exclusive end, if bounded.
    pub to: Option<DateTime<Utc>>,
}

pub(crate) fn invalid(field: &str, reason: &str, message: impl Into<String>) -> ErrorDetail {
    ErrorDetail::new(ErrorCode::ValidationInvalidRequest, message)
        .with_context(json!({"field": field, "reason": reason}))
}

/// Parses a timeframe such as `30s`, `1m`, `4h`, `1d`, `2w` or `1mo`.
///
/// The step is a positive integer and the unit is lower-case; the result is a
/// last-price [`BarSpecification`].
pub fn parse_timeframe(text: &str) -> Result<BarSpecification, ErrorDetail> {
    let bad = || {
        invalid(
            "tf",
            "invalid_timeframe",
            format!("timeframe {text:?} is not <n><s|m|h|d|w|mo>, e.g. 1m or 1d"),
        )
    };
    let digits = text.chars().take_while(char::is_ascii_digit).count();
    let (step, unit) = text.split_at(digits);
    let step: u64 = step.parse().map_err(|_| bad())?;
    if step == 0 {
        return Err(bad());
    }
    let aggregation = match unit {
        "s" => BarAggregation::Second,
        "m" => BarAggregation::Minute,
        "h" => BarAggregation::Hour,
        "d" => BarAggregation::Day,
        "w" => BarAggregation::Week,
        "mo" => BarAggregation::Month,
        _ => return Err(bad()),
    };
    Ok(BarSpecification::new(step, aggregation, PriceType::Last))
}

pub(crate) fn parse_bound(field: &str, text: &str) -> Result<DateTime<Utc>, ErrorDetail> {
    let bad = || {
        invalid(
            field,
            "invalid_time",
            format!("{field} {text:?} is not an RFC3339 time or a YYYY-MM-DD date"),
        )
    };
    let instant = match DateTime::parse_from_rfc3339(text) {
        Ok(parsed) => parsed.to_utc(),
        Err(_) => NaiveDate::parse_from_str(text, "%Y-%m-%d")
            .map_err(|_| bad())?
            .and_hms_opt(0, 0, 0)
            .ok_or_else(bad)?
            .and_utc(),
    };
    if instant.timestamp() < 0 {
        return Err(invalid(
            field,
            "before_epoch",
            format!("{field} {text:?} is before 1970-01-01"),
        ));
    }
    Ok(instant)
}

impl BarsQuery {
    /// Resolves the query: default timeframe, parsed bounds, and a non-empty range.
    pub fn resolve(&self) -> Result<ResolvedBarsQuery, ErrorDetail> {
        let spec = parse_timeframe(self.tf.as_deref().unwrap_or(DEFAULT_TIMEFRAME))?;
        let from = self
            .from
            .as_deref()
            .map(|text| parse_bound("from", text))
            .transpose()?;
        let to = self
            .to
            .as_deref()
            .map(|text| parse_bound("to", text))
            .transpose()?;
        if let (Some(from), Some(to)) = (from, to) {
            if from >= to {
                return Err(invalid(
                    "to",
                    "empty_range",
                    "`from` must be strictly before `to`",
                ));
            }
        }
        Ok(ResolvedBarsQuery { spec, from, to })
    }
}

fn subsequence_score(needle: &[u8], haystack: &[u8]) -> Option<u32> {
    if needle.is_empty() {
        return Some(0);
    }
    let mut score: i64 = 100;
    let mut consecutive: i64 = 0;
    let mut prev_match: Option<usize> = None;
    let mut j = 0usize;

    for &nc in needle {
        let mut found = None;
        while j < haystack.len() {
            if haystack[j] == nc {
                found = Some(j);
                j += 1;
                break;
            }
            j += 1;
        }
        let pos = found?;
        if let Some(prev) = prev_match {
            if pos == prev + 1 {
                consecutive += 1;
                score += 20 + consecutive * 5;
            } else {
                consecutive = 0;
                let gap = i64::try_from(pos - prev - 1).unwrap_or(i64::MAX);
                score -= gap.saturating_mul(2);
            }
        } else if pos == 0 {
            score += 15;
        }
        if pos > 0 {
            let prev_byte = haystack[pos - 1];
            let curr_byte = haystack[pos];
            let is_boundary = matches!(prev_byte, b':' | b'-' | b'_' | b' ')
                || (prev_byte.is_ascii_digit() && curr_byte.is_ascii_alphabetic())
                || (prev_byte.is_ascii_alphabetic() && curr_byte.is_ascii_digit());
            if is_boundary {
                score += 15;
            }
        }
        prev_match = Some(pos);
    }

    let extra = i64::try_from(haystack.len().saturating_sub(needle.len())).unwrap_or(0);
    score -= extra;
    Some(u32::try_from(score.max(1)).unwrap_or(u32::MAX))
}

fn score_token(token: &str, symbol: &str, full_id: &str) -> Option<u32> {
    if symbol == token {
        return Some(2000);
    }
    if full_id == token {
        return Some(1900);
    }
    if symbol.starts_with(token) {
        let len_diff = u32::try_from(symbol.len().saturating_sub(token.len())).unwrap_or(u32::MAX);
        return Some(1500_u32.saturating_sub(len_diff));
    }
    if let Some(pos) = symbol.find(token) {
        let pos_u32 = u32::try_from(pos).unwrap_or(u32::MAX);
        let len_diff = u32::try_from(symbol.len().saturating_sub(token.len())).unwrap_or(u32::MAX);
        return Some(
            1100_u32
                .saturating_sub(pos_u32.saturating_mul(4))
                .saturating_sub(len_diff),
        );
    }
    if full_id.starts_with(token) {
        let len_diff = u32::try_from(full_id.len().saturating_sub(token.len())).unwrap_or(u32::MAX);
        return Some(950_u32.saturating_sub(len_diff));
    }
    if let Some(pos) = full_id.find(token) {
        let pos_u32 = u32::try_from(pos).unwrap_or(u32::MAX);
        return Some(850_u32.saturating_sub(pos_u32.saturating_mul(4)));
    }
    if let Some(sub) = subsequence_score(token.as_bytes(), symbol.as_bytes()) {
        return Some(150_u32.saturating_add(sub));
    }
    if let Some(sub) = subsequence_score(token.as_bytes(), full_id.as_bytes()) {
        return Some(50_u32.saturating_add(sub));
    }
    None
}

/// Scores how well `id` fuzzy-matches `query` (higher is better; `None` if no match).
///
/// An empty or whitespace-only query matches with score `0`. Multi-token queries
/// require every whitespace-delimited token to match either `id.symbol()` or the
/// full `EXCHANGE:SYMBOL` string (exact, prefix, substring, or subsequence).
pub fn fuzzy_score_instrument(query: &str, id: &InstrumentId) -> Option<u32> {
    let normalized = query.trim().to_ascii_uppercase();
    if normalized.is_empty() {
        return Some(0);
    }
    let symbol = id.symbol().to_ascii_uppercase();
    let full_id = id.to_string().to_ascii_uppercase();
    let mut total = 0_u32;
    for token in normalized.split_whitespace() {
        let score = score_token(token, &symbol, &full_id)?;
        total = total.saturating_add(score);
    }
    Some(total)
}

impl InstrumentsQuery {
    /// Resolves and validates the query parameters.
    pub fn resolve(&self) -> Result<ResolvedInstrumentsQuery<'_>, ErrorDetail> {
        let limit = match self.limit {
            None => None,
            Some(n) if (1..=MAX_INSTRUMENTS_LIMIT).contains(&n) => Some(n as usize),
            Some(n) => {
                return Err(invalid(
                    "limit",
                    "out_of_range",
                    format!("limit {n} is not between 1 and {MAX_INSTRUMENTS_LIMIT}"),
                ));
            }
        };
        let q = self
            .q
            .as_deref()
            .map(str::trim)
            .filter(|text| !text.is_empty());
        Ok(ResolvedInstrumentsQuery {
            exchange: self.exchange.as_deref(),
            symbol: self.symbol.as_deref(),
            q,
            limit,
        })
    }

    /// Reports whether `id` passes every exact filter that is set (case-sensitive).
    pub fn matches(&self, id: &InstrumentId) -> bool {
        self.exchange
            .as_deref()
            .is_none_or(|exchange| id.exchange().to_string() == exchange)
            && self
                .symbol
                .as_deref()
                .is_none_or(|symbol| id.symbol() == symbol)
    }
}

/// Parses the `{id}` path segment, `SYMBOL.EXCHANGE`, splitting on the last dot.
///
/// The exchange must be one of NSE, BSE or MCX; the canonical string form of
/// the parsed id is `EXCHANGE:SYMBOL` ([`InstrumentId`]'s `Display`), which is
/// what responses and the screener universe use.
pub fn parse_instrument_id(text: &str) -> Result<InstrumentId, ErrorDetail> {
    let (symbol, exchange) = match text.rsplit_once('.') {
        Some((symbol, exchange)) if !symbol.is_empty() && !exchange.is_empty() => {
            (symbol, exchange)
        }
        _ => {
            return Err(invalid(
                "id",
                "invalid_instrument_id",
                format!("instrument id {text:?} is not SYMBOL.EXCHANGE"),
            ));
        }
    };
    let exchange = Exchange::from_str(exchange).map_err(|_| {
        invalid(
            "id",
            "invalid_instrument_id",
            format!("instrument id {text:?} names unknown exchange {exchange:?} (NSE, BSE or MCX)"),
        )
    })?;
    InstrumentId::try_new(exchange, symbol).map_err(|_| {
        invalid(
            "id",
            "invalid_instrument_id",
            format!("instrument id {text:?} has invalid symbol"),
        )
    })
}

/// Maps an [`Instrument`] to its wire object.
///
/// `id` is the canonical display string (`NSE:INFY`), `currency` the ISO code
/// (`INR`), and `lot_size`/`tick_size` decimal strings.
pub fn instrument_json(instrument: &Instrument) -> Value {
    let kind = match instrument.kind() {
        InstrumentKind::Equity => "equity",
        InstrumentKind::Etf => "etf",
        InstrumentKind::Bond => "bond",
        InstrumentKind::Ipo => "ipo",
        InstrumentKind::Future => "future",
        InstrumentKind::Option => "option",
        InstrumentKind::Fx => "fx",
        InstrumentKind::Index => "index",
        InstrumentKind::MutualFund => "mutual_fund",
        InstrumentKind::Other => "other",
    };
    json!({
        "id": instrument.id().to_string(),
        "kind": kind,
        "currency": instrument.currency().to_string(),
        "lot_size": instrument.lot_size(),
        "tick_size": instrument.tick_size(),
    })
}

impl QuotesQuery {
    /// Resolves the query: at least one symbol, an optional venue, an optional `as_of` time.
    pub fn resolve(&self) -> Result<ResolvedQuotesQuery, ErrorDetail> {
        let mut symbols: Vec<String> = Vec::new();
        for symbol in self.symbols.as_deref().unwrap_or_default().split(',') {
            let symbol = symbol.trim();
            if !symbol.is_empty() && !symbols.iter().any(|seen| seen == symbol) {
                symbols.push(symbol.to_owned());
            }
        }
        if symbols.is_empty() {
            return Err(invalid(
                "symbols",
                "missing_symbols",
                "`symbols` must name at least one symbol, comma-separated",
            ));
        }
        let as_of = self
            .as_of
            .as_deref()
            .map(|text| parse_bound("as_of", text))
            .transpose()?;
        let venue = match self.venue.as_deref() {
            Some(text) => Some(Exchange::from_str(text).map_err(|_| {
                invalid(
                    "venue",
                    "invalid_exchange",
                    format!("venue {text:?} is not one of NSE, BSE or MCX"),
                )
            })?),
            None => None,
        };
        Ok(ResolvedQuotesQuery {
            symbols,
            venue,
            as_of,
        })
    }
}

impl DepthQuery {
    /// Resolves the number of levels per side: the default, or `1..=MAX_DEPTH_LEVELS`.
    pub fn levels(&self) -> Result<usize, ErrorDetail> {
        match self.depth {
            None => Ok(DEFAULT_DEPTH_LEVELS),
            Some(levels) if (1..=MAX_DEPTH_LEVELS).contains(&levels) => Ok(levels as usize),
            Some(levels) => Err(invalid(
                "depth",
                "out_of_range",
                format!("depth {levels} is not between 1 and {MAX_DEPTH_LEVELS}"),
            )),
        }
    }
}
