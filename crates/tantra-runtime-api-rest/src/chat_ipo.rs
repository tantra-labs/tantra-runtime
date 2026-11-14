//! Read-through `IpoReader` adapter for the chat tool catalog's IPO tools
//! (agent plan 0007 Track 3 Phase 8).
//!
//! Wires `tantra-data`'s IPO projections (`{root}/catalog/ipos` +
//! `{root}/catalog/ipo_subscriptions`, NSE vendor, gap 0022 Python side landed `17c5b94`)
//! through [`tantra_market_store::FundsIposProjectionReader`] with
//! [`FeedIpoReader::with_ipos`] — closing `tantra-runtime/docs/gaps/0007` item 16's Rust side.
//! Issues and subscription multiples are never fabricated.

use async_trait::async_trait;
use chrono::{TimeZone, Utc};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use tantra_agent_chat_tools::ports::error::PortError;
use tantra_agent_chat_tools::ports::ipo::{IpoIssue, IpoReader, IpoSubscription};
use tantra_core::{Currency, Money, Price};
use tantra_market_store::FundsIposProjectionReader;

/// Default / cap for `upcoming_ipos` results.
const DEFAULT_IPO_LIMIT: usize = 20;
const MAX_IPO_LIMIT: usize = 50;

/// Issue statuses that mean "currently in the market / scheduled" (past issues are `Listed`
/// / `Closed` / `Withdrawn` / `Deferred`).
fn is_open(status: &str) -> bool {
    let status = status.to_lowercase();
    ["open", "current", "upcoming", "scheduled", "live"]
        .iter()
        .any(|word| status.contains(word))
}

/// IPO adapter reading the `ipo_calendar`/`ipo_subscription` chat tools from the
/// `tantra-data` IPO projections when wired, answering its explicit gap citation otherwise.
pub struct FeedIpoReader {
    ipos: Option<FundsIposProjectionReader>,
}

impl FeedIpoReader {
    /// Builds the IPO adapter — every call answers its explicit gap citation until wired.
    pub fn new() -> Self {
        Self { ipos: None }
    }

    /// Wires the `tantra-data` IPO projections (`{root}/catalog/ipos` +
    /// `{root}/catalog/ipo_subscriptions`), closing the Rust side of
    /// `tantra-data/docs/gaps/0022` / `tantra-runtime/docs/gaps/0007` item 16.
    pub fn with_ipos(mut self, root: &std::path::Path) -> Self {
        self.ipos = Some(FundsIposProjectionReader::new(root));
        self
    }
}

impl Default for FeedIpoReader {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl IpoReader for FeedIpoReader {
    async fn upcoming_ipos(&self, limit: Option<usize>) -> Result<Vec<IpoIssue>, PortError> {
        let Some(reader) = &self.ipos else {
            // No IPO projections configured — the reader is only wired by
            // [`FeedIpoReader::with_ipos`]. An explicit error, not an empty success, so the
            // model can't mistake "not wired up" for "no IPOs are scheduled."
            return Err(PortError::unavailable(
                "IPO reader not wired; see tantra-data/docs/gaps/0022-ipo-calendar-and-subscription-ingestion.md",
            ));
        };
        let projections = reader
            .read_ipos()
            .map_err(|e| PortError::internal(format!("failed to read IPO projections: {e}")))?;
        let mut issues: Vec<IpoIssue> = projections
            .into_iter()
            .filter(|issue| is_open(&issue.status))
            .filter_map(|issue| {
                let open_date = issue.open_date?;
                Some(IpoIssue {
                    symbol: issue.symbol,
                    company_name: issue.company_name,
                    open_date: midnight_utc(open_date),
                    close_date: midnight_utc(issue.close_date.unwrap_or(open_date)),
                    price_min: issue.price_band_low.map(Price::new),
                    price_max: issue.price_band_high.map(Price::new),
                    lot_size: issue.lot_size.and_then(|lot| lot.to_u64()),
                    issue_size: issue
                        .issue_size_cr
                        .map(|cr| Money::new(cr * Decimal::from(ONE_CRORE), Currency::Inr)),
                })
            })
            .collect();
        issues.sort_by_key(|issue| issue.open_date);
        let limit = limit.unwrap_or(DEFAULT_IPO_LIMIT).min(MAX_IPO_LIMIT);
        issues.truncate(limit);
        Ok(issues)
    }

    async fn ipo_subscription(&self, symbol: &str) -> Result<IpoSubscription, PortError> {
        let Some(reader) = &self.ipos else {
            return Err(PortError::unavailable(
                "IPO reader not wired; see tantra-data/docs/gaps/0022-ipo-calendar-and-subscription-ingestion.md",
            ));
        };
        let projections = reader.read_ipo_subscriptions().map_err(|e| {
            PortError::internal(format!("failed to read IPO-subscription projections: {e}"))
        })?;
        let latest = projections
            .into_iter()
            .filter(|row| row.symbol.eq_ignore_ascii_case(symbol))
            .max_by_key(|row| row.as_of_date)
            .ok_or_else(|| PortError::not_found(format!("no subscription data for {symbol}")))?;
        Ok(IpoSubscription {
            symbol: symbol.to_string(),
            as_of: midnight_utc(latest.as_of_date),
            qib_multiple: latest.qib,
            nii_multiple: latest.nii,
            rii_multiple: latest.rii,
            total_multiple: latest.total,
        })
    }
}

/// Midnight UTC of a projection date (the same `as_of` derivation the feed adapters use).
fn midnight_utc(date: chrono::NaiveDate) -> chrono::DateTime<Utc> {
    Utc.from_utc_datetime(
        &date
            .and_hms_opt(0, 0, 0)
            .expect("midnight is a valid time of day"),
    )
}

/// 1 ₹ crore = 10,000,000 (issue sizes are stored in crore).
const ONE_CRORE: u64 = 10_000_000;
