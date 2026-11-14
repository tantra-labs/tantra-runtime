//! Read-through `FundReader` adapter for the chat tool catalog's mutual-fund tools
//! (agent plan 0007 Track 3 Phase 7).
//!
//! No mutual-fund NAV/holdings data source exists anywhere yet, so every call answers an
//! explicit [`PortError::unavailable`] with the gap citation the chat agent surfaces to the
//! user — never fabricated funds (same honest-stub shape as
//! [`crate::chat_market::FeedMarketReader`] per
//! `docs/gaps/0007-chat-market-reader-approximations-and-missing-sources.md`):
//! - Mutual funds: no ingestion exists yet — see
//!   `tantra-data/docs/gaps/0021-mutual-fund-data-ingestion.md`.

use async_trait::async_trait;
use tantra_agent_chat_tools::ports::error::PortError;
use tantra_agent_chat_tools::ports::fund::{FundHolding, FundReader, MutualFundMeta};

/// Funds adapter answering the `fund_search`/`fund_holdings` chat tools with an explicit gap
/// citation until `tantra-data` mutual-fund ingestion exists.
pub struct FeedFundReader;

impl FeedFundReader {
    /// Builds the funds adapter — every call answers its explicit gap citation.
    pub fn new() -> Self {
        Self
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
        _query: &str,
        _limit: Option<usize>,
    ) -> Result<Vec<MutualFundMeta>, PortError> {
        // No mutual-fund data source exists anywhere yet — see
        // tantra-data/docs/gaps/0021-mutual-fund-data-ingestion.md. An explicit error, not
        // an empty success, so the model can't mistake "not wired up" for "no matching
        // funds exist."
        Err(PortError::unavailable(
            "mutual-fund data not ingested yet; see tantra-data/docs/gaps/0021-mutual-fund-data-ingestion.md",
        ))
    }

    async fn fund_holdings(&self, _scheme_code: &str) -> Result<Vec<FundHolding>, PortError> {
        Err(PortError::unavailable(
            "mutual-fund data not ingested yet; see tantra-data/docs/gaps/0021-mutual-fund-data-ingestion.md",
        ))
    }
}
