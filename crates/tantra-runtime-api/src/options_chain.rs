//! Pure wire DTOs and query resolution for `GET /api/v2/options/chain`.
//!
//! Zero I/O, zero wall-clock access, zero floating point: every price, strike,
//! OI, IV, and Greek field serializes as an exact decimal string (`Decimal`).

use std::str::FromStr;

use chrono::{Datelike, Days, NaiveDate};
use rust_decimal::Decimal;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tantra_core::{Exchange, InstrumentId};

use crate::market::invalid;
use crate::wire::ErrorDetail;

/// `GET /api/v2/options/chain` query parameters.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct OptionsChainQuery {
    /// Underlying symbol or instrument id (e.g. `NSE:NIFTY50-INDEX`, `NIFTY50-INDEX.NSE`, or `NIFTY`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub symbol: Option<String>,
    /// Optional expiry filter (`YYYY-MM-DD`). Defaults to the nearest active expiry.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expiry: Option<String>,
    /// Optional explicit evaluation date (`YYYY-MM-DD`) for deterministic expiry/DTE math.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub as_of: Option<String>,
}

/// Validated query inputs for [`OptionsChainQuery`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolvedOptionsChainQuery {
    /// Canonical underlying instrument id (`EXCHANGE:SYMBOL`).
    pub instrument_id: InstrumentId,
    /// Normalized F&O underlying root symbol (e.g. `NIFTY`, `BANKNIFTY`, `RELIANCE`).
    pub fno_root: String,
    /// Requested expiry date filter, if any.
    pub expiry: Option<NaiveDate>,
    /// Explicit as-of date if supplied by the caller.
    pub as_of: Option<NaiveDate>,
}

/// Origin of the option-chain snapshot rows.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum OptionChainSource {
    /// Populated from `InstrumentChainReader::options_chain` (with live quotes/OI when available).
    LiveChain,
    /// Theoretical Black-Scholes strike ladder over underlying spot when live contracts are offline.
    TheoreticalBs,
}

/// One expiry entry in the option chain's expiry selector.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct OptionExpiryDto {
    /// ISO date (`YYYY-MM-DD`).
    pub expiry: String,
    /// Compact display label (e.g. `"24 Nov"`).
    pub short_label: String,
    /// Descriptive label with horizon (e.g. `"24 Nov (1 week)"`).
    pub label: String,
    /// True for weekly expiries, false for monthly expiries.
    pub is_weekly: bool,
    /// Calendar days to expiry relative to `as_of` (at least `1`).
    pub dte: u32,
}

/// Call or Put contract leg metrics at one strike price.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct OptionLegSnapshotDto {
    /// Exchange contract symbol (e.g. `NFO:NIFTY24NOV25800CE`).
    pub contract_symbol: String,
    /// Last traded price, bid/ask mid, or theoretical Black-Scholes fair value when offline.
    pub ltp: Decimal,
    /// Session LTP percent change (`0` when unavailable).
    pub ltp_change_pct: Decimal,
    /// Live top-of-book bid price (`null` when the live feed is offline).
    pub bid: Option<Decimal>,
    /// Live top-of-book ask price (`null` when the live feed is offline).
    pub ask: Option<Decimal>,
    /// Open interest in contracts (`0` when no OI snapshot is available).
    pub oi: Decimal,
    /// Open interest percent change (`0` when no prior OI snapshot exists).
    pub oi_change_pct: Decimal,
    /// Session traded volume (`0` when no trade volume exists).
    pub volume: Decimal,
    /// Implied volatility percentage (e.g. `14.25` for 14.25%).
    pub iv_pct: Decimal,
    /// Black-Scholes `∂price/∂spot`.
    pub delta: Decimal,
    /// Black-Scholes `∂²price/∂spot²`.
    pub gamma: Decimal,
    /// Black-Scholes per-calendar-day time decay (`∂price/∂time / 365`).
    pub theta: Decimal,
    /// Black-Scholes sensitivity per 1% volatility (`∂price/∂vol / 100`).
    pub vega: Decimal,
    /// `true` when this leg reflects a live market quote rather than pure Black-Scholes fair value.
    pub live_quoted: bool,
}

/// Symmetrical Call + Strike + Put row for an option chain table.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct OptionChainRowDto {
    /// Strike price.
    pub strike: Decimal,
    /// `true` for the nearest at-the-money strike.
    pub is_atm: bool,
    /// `true` when `strike < spot_price`.
    pub call_itm: bool,
    /// `true` when `strike > spot_price`.
    pub put_itm: bool,
    /// Call (`CE`) leg snapshot.
    pub call: OptionLegSnapshotDto,
    /// Put (`PE`) leg snapshot.
    pub put: OptionLegSnapshotDto,
}

/// Full option-chain snapshot returned by `GET /api/v2/options/chain`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct OptionsChainResponse {
    /// Canonical underlying instrument identifier (e.g. `NSE:NIFTY50-INDEX`).
    pub instrument_id: String,
    /// ISO-4217 settlement currency code (e.g. `INR`).
    pub currency: String,
    /// Spot reference price of the underlying.
    pub spot_price: Decimal,
    /// Absolute session change in underlying spot (`0` when prior close is unavailable).
    pub spot_change: Decimal,
    /// Percentage session change in underlying spot (`0` when prior close is unavailable).
    pub spot_change_pct: Decimal,
    /// Standard F&O contract lot size.
    pub lot_size: Decimal,
    /// Available expiry entries ordered ascending.
    pub expiries: Vec<OptionExpiryDto>,
    /// Active expiry ISO date (`YYYY-MM-DD`) represented by `rows`.
    pub active_expiry: String,
    /// Put-Call OI ratio (`0` when Call OI sum is zero).
    pub pcr: Decimal,
    /// Strike minimizing total option-writer expiry payout.
    pub max_pain_strike: Decimal,
    /// Nearest at-the-money strike.
    pub atm_strike: Decimal,
    /// Implied volatility proxy at ATM in percentage points.
    pub atm_iv_pct: Decimal,
    /// Historical volatility / IV percentile in `[0, 100]`.
    pub iv_percentile: Decimal,
    /// Human-readable regime classification (`"Low"`, `"Mid"`, or `"High"`).
    pub iv_percentile_regime: String,
    /// Whether rows come from live chain contracts or theoretical Black-Scholes fallback.
    pub source: OptionChainSource,
    /// Count of live option contracts fetched from `InstrumentChainReader`.
    pub live_contract_count: usize,
    /// Honest note explaining live/offline feed status and whether bid/ask quotes are populated.
    pub availability_note: String,
    /// Strike-sorted Call/Put rows for `active_expiry`.
    pub rows: Vec<OptionChainRowDto>,
}

fn parse_naive_date(field: &str, reason: &str, text: &str) -> Result<NaiveDate, ErrorDetail> {
    NaiveDate::parse_from_str(text.trim(), "%Y-%m-%d").map_err(|_| {
        invalid(
            field,
            reason,
            format!("{field} {text:?} must be formatted as YYYY-MM-DD"),
        )
    })
}

/// Normalizes a user-supplied underlying reference into canonical [`InstrumentId`] and F&O root.
pub fn resolve_underlying_symbol(raw: &str) -> Result<(InstrumentId, String), ErrorDetail> {
    let trimmed = raw.trim().to_ascii_uppercase();
    if trimmed.is_empty() {
        return Err(invalid(
            "symbol",
            "missing_symbol",
            "`symbol` query parameter is required",
        ));
    }

    let (exchange, sym_part) = if let Some((ex, sym)) = trimmed.split_once(':') {
        let parsed_ex = Exchange::from_str(ex).map_err(|_| {
            invalid(
                "symbol",
                "invalid_exchange",
                format!("unknown exchange {ex:?} in symbol {raw:?}"),
            )
        })?;
        if sym.is_empty() {
            return Err(invalid(
                "symbol",
                "invalid_symbol",
                format!("empty symbol suffix in {raw:?}"),
            ));
        }
        (parsed_ex, sym.to_string())
    } else if let Some((sym, ex)) = trimmed.rsplit_once('.') {
        if let Ok(parsed_ex) = Exchange::from_str(ex) {
            if sym.is_empty() {
                return Err(invalid(
                    "symbol",
                    "invalid_symbol",
                    format!("empty symbol prefix in {raw:?}"),
                ));
            }
            (parsed_ex, sym.to_string())
        } else {
            (Exchange::Nse, trimmed.clone())
        }
    } else {
        (Exchange::Nse, trimmed.clone())
    };

    let canonical_symbol = match sym_part.as_str() {
        "NIFTY" | "NIFTY50" | "NIFTY 50" | "NIFTY50-INDEX" => "NIFTY50-INDEX".to_string(),
        "BANKNIFTY" | "NIFTY BANK" | "BANKNIFTY-INDEX" => "BANKNIFTY-INDEX".to_string(),
        "FINNIFTY" | "FINNIFTY-INDEX" => "FINNIFTY-INDEX".to_string(),
        "MIDCPNIFTY" | "MIDCPNIFTY-INDEX" => "MIDCPNIFTY-INDEX".to_string(),
        other => other.to_string(),
    };

    let fno_root = canonical_symbol
        .strip_suffix("-INDEX")
        .or_else(|| canonical_symbol.strip_suffix("-EQ"))
        .or_else(|| canonical_symbol.strip_suffix("-FUT"))
        .unwrap_or(&canonical_symbol)
        .to_string();
    let fno_root = match fno_root.as_str() {
        "NIFTY50" => "NIFTY".to_string(),
        other => other.to_string(),
    };

    let id = InstrumentId::try_new(exchange, canonical_symbol).map_err(|_| {
        invalid(
            "symbol",
            "invalid_symbol",
            format!("invalid underlying symbol {raw:?}"),
        )
    })?;
    Ok((id, fno_root))
}

impl OptionsChainQuery {
    /// Validates query parameters without reading any external clock or store.
    pub fn resolve(&self) -> Result<ResolvedOptionsChainQuery, ErrorDetail> {
        let raw_symbol = self
            .symbol
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                invalid(
                    "symbol",
                    "missing_symbol",
                    "`symbol` query parameter is required",
                )
            })?;
        let (instrument_id, fno_root) = resolve_underlying_symbol(raw_symbol)?;
        let expiry = self
            .expiry
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(|text| parse_naive_date("expiry", "invalid_expiry", text))
            .transpose()?;
        let as_of = self
            .as_of
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(|text| parse_naive_date("as_of", "invalid_as_of", text))
            .transpose()?;
        Ok(ResolvedOptionsChainQuery {
            instrument_id,
            fno_root,
            expiry,
            as_of,
        })
    }
}

const MONTH_SHORT: [&str; 12] = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
];

/// Formats one expiry date into UI/wire metadata relative to `as_of`.
pub fn format_expiry_entry(
    expiry: NaiveDate,
    as_of: NaiveDate,
    is_weekly: bool,
) -> OptionExpiryDto {
    let day = expiry.day();
    let month_idx = expiry.month0() as usize;
    let month_str = MONTH_SHORT.get(month_idx).copied().unwrap_or("Jan");
    let short_label = format!("{day} {month_str}");
    let raw_days = expiry.signed_duration_since(as_of).num_days();
    let dte = u32::try_from(raw_days.max(1)).unwrap_or(1);
    let weeks = ((dte + 3) / 7).max(1);
    let label = if dte <= 7 {
        format!("{short_label} (1 week)")
    } else {
        format!("{short_label} ({weeks} weeks)")
    };
    OptionExpiryDto {
        expiry: expiry.format("%Y-%m-%d").to_string(),
        short_label,
        label,
        is_weekly,
        dte,
    }
}

/// Deterministic 6-week NSE Tuesday expiry schedule starting after `as_of`.
pub fn build_tuesday_expiry_schedule(as_of: NaiveDate) -> Vec<OptionExpiryDto> {
    let dow = as_of.weekday().num_days_from_monday(); // Mon=0, Tue=1..Sun=6
    let offset = match (1i64 - i64::from(dow)).rem_euclid(7) {
        0 => 7u64,
        n => u64::try_from(n).unwrap_or(7),
    };
    let Some(first_tuesday) = as_of.checked_add_days(Days::new(offset)) else {
        return Vec::new();
    };
    let mut out = Vec::with_capacity(6);
    for idx in 0..6u64 {
        if let Some(dt) = first_tuesday.checked_add_days(Days::new(idx * 7)) {
            let is_monthly = dt
                .checked_add_days(Days::new(7))
                .is_some_and(|next| next.month() != dt.month());
            out.push(format_expiry_entry(dt, as_of, !is_monthly));
        }
    }
    out
}

/// Default strike increment for the given F&O root and spot price.
pub fn strike_step_for(fno_root: &str, spot: Decimal) -> Decimal {
    match fno_root {
        "BANKNIFTY" | "SENSEX" | "BANKEX" => Decimal::from(100),
        "NIFTY" | "FINNIFTY" => Decimal::from(50),
        "MIDCPNIFTY" => Decimal::from(25),
        _ => {
            if spot < Decimal::from(250) {
                Decimal::from(5)
            } else if spot < Decimal::from(1000) {
                Decimal::from(10)
            } else if spot < Decimal::from(2500) {
                Decimal::from(20)
            } else if spot < Decimal::from(5000) {
                Decimal::from(50)
            } else {
                Decimal::from(100)
            }
        }
    }
}

/// Formats an NFO option contract symbol (`NFO:NIFTY24NOV25800CE`).
pub fn format_contract_symbol(
    fno_root: &str,
    expiry: NaiveDate,
    strike: Decimal,
    is_call: bool,
) -> String {
    let dd = format!("{:02}", expiry.day());
    let mon = MONTH_SHORT
        .get(expiry.month0() as usize)
        .copied()
        .unwrap_or("JAN")
        .to_ascii_uppercase();
    let side = if is_call { "CE" } else { "PE" };
    format!("NFO:{fno_root}{dd}{mon}{}{side}", strike.round_dp(0))
}

/// Computes ATM strike nearest to `spot` (falling back to `spot` when empty).
pub fn compute_atm_strike(rows: &[OptionChainRowDto], spot: Decimal) -> Decimal {
    let Some(first) = rows.first() else {
        return spot;
    };
    let mut best = first.strike;
    let mut best_dist = (best - spot).abs();
    for row in &rows[1..] {
        let dist = (row.strike - spot).abs();
        if dist < best_dist {
            best_dist = dist;
            best = row.strike;
        }
    }
    best
}

/// Computes Put-Call OI ratio (`put_oi_sum / call_oi_sum`), returning `0` when Call OI is zero.
pub fn compute_pcr(rows: &[OptionChainRowDto]) -> Decimal {
    let put_sum: Decimal = rows.iter().map(|r| r.put.oi).sum();
    let call_sum: Decimal = rows.iter().map(|r| r.call.oi).sum();
    if call_sum <= Decimal::ZERO {
        Decimal::ZERO
    } else {
        (put_sum / call_sum).round_dp(2)
    }
}

/// Computes the Max Pain strike minimizing cumulative option-writer intrinsic payout at expiry.
pub fn compute_max_pain_strike(rows: &[OptionChainRowDto]) -> Decimal {
    let Some(first) = rows.first() else {
        return Decimal::ZERO;
    };
    let mut best_strike = first.strike;
    let mut min_payout: Option<Decimal> = None;
    for candidate in rows {
        let settle = candidate.strike;
        let mut payout = Decimal::ZERO;
        for row in rows {
            if settle > row.strike {
                payout += (settle - row.strike) * row.call.oi;
            }
            if settle < row.strike {
                payout += (row.strike - settle) * row.put.oi;
            }
        }
        if min_payout.is_none_or(|best| payout < best) {
            min_payout = Some(payout);
            best_strike = settle;
        }
    }
    best_strike
}

/// Classifies an IV percentile `[0, 100]` into `"Low"`, `"Mid"`, or `"High"`.
pub fn iv_regime_label(iv_percentile: Decimal) -> String {
    if iv_percentile >= Decimal::from(40) {
        "High".to_string()
    } else if iv_percentile <= Decimal::from(25) {
        "Low".to_string()
    } else {
        "Mid".to_string()
    }
}
