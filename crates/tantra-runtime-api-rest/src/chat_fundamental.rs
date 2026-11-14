//! Read-through `FundamentalReader` adapter for the chat tool catalog's fundamentals tools
//! (agent plan 0007 Track 3 Phase 6).
//!
//! Wires `tantra-data`'s fundamentals projections (`{root}/catalog/fundamentals` +
//! `{root}/catalog/fundamental_ratios`) through
//! [`tantra_market_store::FundamentalsProjectionReader`] with
//! [`FeedFundamentalReader::with_fundamentals`]; without a wiring the adapter answers its
//! explicit gap citation (closes `tantra-data/docs/gaps/0020`'s Rust side and
//! `tantra-runtime/docs/gaps/0007` item 14). Ratios and statements are never fabricated.

use std::collections::BTreeMap;

use async_trait::async_trait;
use chrono::{Datelike, NaiveDate, TimeZone, Utc};
use tantra_agent_chat_tools::ports::error::PortError;
use tantra_agent_chat_tools::ports::fundamental::{
    FinancialStatement, FundamentalRatios, FundamentalReader,
};
use tantra_core::{Currency, InstrumentId, Money};
use tantra_market_store::{FundamentalStatementProjection, FundamentalsProjectionReader};

/// Canonical statement line items the chat DTO maps from the projection.
const LINE_ITEM_REVENUE: &str = "total_revenue";
const LINE_ITEM_OPERATING_PROFIT: &str = "operating_income";
const LINE_ITEM_OPERATING_FALLBACK: &str = "ebit";
const LINE_ITEM_NET_INCOME: &str = "net_income";
const LINE_ITEM_EPS: &str = "diluted_eps";

/// Fundamentals adapter reading the `fundamental_ratios`/`financial_statements` chat tools
/// from the `tantra-data` projections when wired, answering its explicit gap citation otherwise.
pub struct FeedFundamentalReader {
    fundamentals: Option<FundamentalsProjectionReader>,
}

impl FeedFundamentalReader {
    /// Builds the fundamentals adapter — every call answers its explicit gap citation until wired.
    pub fn new() -> Self {
        Self { fundamentals: None }
    }

    /// Wires the `tantra-data` fundamentals projections (`{root}/catalog/fundamentals` +
    /// `{root}/catalog/fundamental_ratios`), closing `tantra-data/docs/gaps/0020`'s Rust side.
    pub fn with_fundamentals(mut self, root: &std::path::Path) -> Self {
        self.fundamentals = Some(FundamentalsProjectionReader::new(root));
        self
    }
}

impl Default for FeedFundamentalReader {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl FundamentalReader for FeedFundamentalReader {
    async fn fundamental_ratios(
        &self,
        instrument: &InstrumentId,
    ) -> Result<FundamentalRatios, PortError> {
        let Some(reader) = &self.fundamentals else {
            // No fundamentals projections configured — the reader is only wired by
            // [`FeedFundamentalReader::with_fundamentals`]. An explicit error, not an empty
            // success, so the model can't mistake "not wired up" for "this instrument has no
            // published ratios."
            return Err(PortError::unavailable(
                "fundamentals reader not wired; see tantra-data/docs/gaps/0020-fundamentals-ingestion.md",
            ));
        };
        let projections = reader.read_ratios().map_err(|e| {
            PortError::internal(format!("failed to read fundamental-ratio projections: {e}"))
        })?;
        let latest = projections
            .into_iter()
            .filter(|r| r.instrument.eq_ignore_ascii_case(&instrument.to_string()))
            .max_by_key(|r| r.as_of_date)
            .ok_or_else(|| {
                PortError::not_found(format!("no fundamental ratios for {instrument}"))
            })?;
        Ok(FundamentalRatios {
            instrument: instrument.clone(),
            as_of: midnight_utc(latest.as_of_date),
            pe_ratio: latest.pe,
            pb_ratio: latest.pb,
            ev_ebitda: latest.ev_to_ebitda,
            dividend_yield_pct: latest.dividend_yield,
            roce_pct: latest.roce,
            roe_pct: latest.roe,
            debt_equity: latest.debt_to_equity,
            interest_coverage: latest.interest_coverage,
        })
    }

    async fn financial_statements(
        &self,
        instrument: &InstrumentId,
        statement_type: Option<&str>,
        limit: Option<usize>,
    ) -> Result<Vec<FinancialStatement>, PortError> {
        let Some(reader) = &self.fundamentals else {
            return Err(PortError::unavailable(
                "fundamentals reader not wired; see tantra-data/docs/gaps/0020-fundamentals-ingestion.md",
            ));
        };
        let projections = reader.read_statements().map_err(|e| {
            PortError::internal(format!("failed to read fundamentals projections: {e}"))
        })?;
        let matching = projections.into_iter().filter(|p| {
            p.instrument.eq_ignore_ascii_case(&instrument.to_string())
                && statement_type.is_none_or(|t| p.statement_type == t)
        });
        // Group line items by (statement_type, period_end_date); within a group only the
        // latest filing is used (point-in-time: a backtest never sees an unfiled fact).
        let mut groups: BTreeMap<(String, NaiveDate), Vec<FundamentalStatementProjection>> =
            BTreeMap::new();
        for projection in matching {
            groups
                .entry((
                    projection.statement_type.clone(),
                    projection.period_end_date,
                ))
                .or_default()
                .push(projection);
        }
        let mut statements: Vec<FinancialStatement> = groups
            .into_iter()
            .rev()
            .filter_map(|((statement_type, period_end_date), rows)| {
                statement_from_rows(instrument, &statement_type, period_end_date, rows)
            })
            .collect();
        let limit = limit.unwrap_or(10).clamp(1, 50);
        statements.truncate(limit);
        Ok(statements)
    }
}

/// Builds one chat `FinancialStatement` from a period's line-item rows (latest filing wins).
fn statement_from_rows(
    instrument: &InstrumentId,
    statement_type: &str,
    period_end_date: NaiveDate,
    mut rows: Vec<FundamentalStatementProjection>,
) -> Option<FinancialStatement> {
    let latest_filing = rows.iter().map(|r| r.filing_date).max()?;
    rows.retain(|r| r.filing_date == latest_filing);
    let value_of = |line_item: &str| -> Option<Money> {
        rows.iter()
            .find(|r| r.line_item == line_item)
            .map(|r| Money::new(r.value, Currency::Inr))
    };
    let operating_profit = value_of(LINE_ITEM_OPERATING_PROFIT).or_else(|| {
        rows.iter()
            .find(|r| r.line_item == LINE_ITEM_OPERATING_FALLBACK)
            .map(|r| Money::new(r.value, Currency::Inr))
    });
    let eps = rows
        .iter()
        .find(|r| r.line_item == LINE_ITEM_EPS)
        .map(|r| r.value);
    Some(FinancialStatement {
        instrument: instrument.clone(),
        statement_type: statement_type.to_string(),
        fiscal_quarter: fiscal_label(&rows[0].period_type, period_end_date),
        revenue: value_of(LINE_ITEM_REVENUE),
        operating_profit,
        net_income: value_of(LINE_ITEM_NET_INCOME),
        eps,
    })
}

/// Labels a period the way the chat tool names it: `FY2026` for annual, `FY2026-Q1` quarterly
/// on the Indian fiscal calendar (Q1 = Apr–Jun, ..., Q4 = Jan–Mar).
fn fiscal_label(period_type: &str, period_end: NaiveDate) -> String {
    if period_type == "quarterly" {
        let quarter = ((period_end.month() + 8) % 12) / 3 + 1;
        format!("FY{}-Q{quarter}", period_end.year())
    } else {
        format!("FY{}", period_end.year())
    }
}

/// Midnight UTC of a projection date (the same `as_of` derivation the feed adapters use).
fn midnight_utc(date: NaiveDate) -> chrono::DateTime<Utc> {
    Utc.from_utc_datetime(
        &date
            .and_hms_opt(0, 0, 0)
            .expect("midnight is a valid time of day"),
    )
}
