//! Read-through `FundReader` adapter for the chat tool catalog's mutual-fund tools
//! (agent plan 0007 Track 3 Phase 7).
//!
//! Wires `tantra-data`'s mutual-fund projections (`{root}/catalog/mutual_funds` +
//! `{root}/catalog/mf_nav` + `{root}/catalog/mf_returns`, AMFI vendor, gap 0021 Python side
//! landed `17c5b94`) through [`tantra_market_store::FundsIposProjectionReader`] with
//! [`FeedFundReader::with_funds`] — closing `tantra-runtime/docs/gaps/0007` item 15's Rust
//! side. Scheme *holdings* have no free machine-readable source verified live (documented in
//! `tantra-data/docs/gaps/0021`), so `fund_holdings` keeps an explicit gap citation rather
//! than fabricating a portfolio. Everything else is never fabricated.

use async_trait::async_trait;
use tantra_agent_chat_tools::ports::error::PortError;
use tantra_agent_chat_tools::ports::fund::{FundHolding, FundReader, MutualFundMeta};
use tantra_core::{Currency, Money};
use tantra_market_store::FundsIposProjectionReader;

/// Default / cap for `fund_search` results.
const DEFAULT_FUND_LIMIT: usize = 10;
const MAX_FUND_LIMIT: usize = 50;

/// Funds adapter reading the `fund_search`/`fund_holdings` chat tools from the `tantra-data`
/// mutual-fund projections when wired, answering its explicit gap citation otherwise.
pub struct FeedFundReader {
    funds: Option<FundsIposProjectionReader>,
}

impl FeedFundReader {
    /// Builds the funds adapter — every call answers its explicit gap citation until wired.
    pub fn new() -> Self {
        Self { funds: None }
    }

    /// Wires the `tantra-data` mutual-fund projections (`{root}/catalog/mutual_funds` +
    /// `{root}/catalog/mf_nav` + `{root}/catalog/mf_returns`), closing the Rust side of
    /// `tantra-data/docs/gaps/0021` / `tantra-runtime/docs/gaps/0007` item 15.
    pub fn with_funds(mut self, root: &std::path::Path) -> Self {
        self.funds = Some(FundsIposProjectionReader::new(root));
        self
    }
}

impl Default for FeedFundReader {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl FundReader for FeedFundReader {
    async fn fund_search(
        &self,
        query: &str,
        limit: Option<usize>,
    ) -> Result<Vec<MutualFundMeta>, PortError> {
        let Some(reader) = &self.funds else {
            // No mutual-fund projections configured — the reader is only wired by
            // [`FeedFundReader::with_funds`]. An explicit error, not an empty success, so the
            // model can't mistake "not wired up" for "no matching funds exist."
            return Err(PortError::unavailable(
                "mutual-fund reader not wired; see tantra-data/docs/gaps/0021-mutual-fund-data-ingestion.md",
            ));
        };
        let metas = reader.read_mutual_funds().map_err(|e| {
            PortError::internal(format!("failed to read mutual-fund projections: {e}"))
        })?;
        let navs = reader
            .read_mf_nav()
            .map_err(|e| PortError::internal(format!("failed to read MF NAV projections: {e}")))?;
        let returns = reader.read_mf_returns().map_err(|e| {
            PortError::internal(format!("failed to read MF return projections: {e}"))
        })?;
        let needle = query.to_lowercase();
        let mut hits: Vec<MutualFundMeta> = metas
            .into_iter()
            .filter(|meta| {
                meta.scheme_name.to_lowercase().contains(&needle)
                    || meta.scheme_code.to_lowercase().contains(&needle)
            })
            .map(|meta| {
                // Latest NAV and return metrics for the scheme, joined by scheme code.
                let nav = navs
                    .iter()
                    .filter(|n| n.scheme_code == meta.scheme_code)
                    .max_by_key(|n| n.nav_date)
                    .map(|n| Money::new(n.nav, Currency::Inr));
                let ret = returns
                    .iter()
                    .filter(|r| r.scheme_code == meta.scheme_code)
                    .max_by_key(|r| r.as_of_date);
                MutualFundMeta {
                    scheme_code: meta.scheme_code,
                    scheme_name: meta.scheme_name,
                    category: meta.category,
                    nav,
                    cagr_1y_pct: ret.and_then(|r| r.cagr_1y),
                    cagr_3y_pct: None,
                    cagr_5y_pct: None,
                    expense_ratio_pct: ret.and_then(|r| r.expense_ratio),
                }
            })
            .collect();
        let limit = limit.unwrap_or(DEFAULT_FUND_LIMIT).min(MAX_FUND_LIMIT);
        hits.truncate(limit);
        Ok(hits)
    }

    async fn fund_holdings(&self, scheme_code: &str) -> Result<Vec<FundHolding>, PortError> {
        let _ = scheme_code;
        if self.funds.is_none() {
            return Err(PortError::unavailable(
                "mutual-fund reader not wired; see tantra-data/docs/gaps/0021-mutual-fund-data-ingestion.md",
            ));
        }
        // No free machine-readable holdings source was verified live during gap 0021's
        // ingestion (fund houses / aggregators are paywalled); holdings are not fabricated.
        Err(PortError::unavailable(
            "mutual-fund holdings have no verified data source yet; see tantra-data/docs/gaps/0021-mutual-fund-data-ingestion.md (holdings note)",
        ))
    }
}
