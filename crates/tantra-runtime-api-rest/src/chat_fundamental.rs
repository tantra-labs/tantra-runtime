//! Read-through `FundamentalReader` adapter for the chat tool catalog's fundamentals tools
//! (agent plan 0007 Track 3 Phase 6).
//!
//! No fundamentals/financial-statement data source exists anywhere yet, so every call answers
//! an explicit [`PortError::unavailable`] with the gap citation the chat agent surfaces to the
//! user — never a fabricated ratio (same honest-stub shape as
//! [`crate::chat_market::FeedMarketReader`] per
//! `docs/gaps/0007-chat-market-reader-approximations-and-missing-sources.md`):
//! - Fundamentals: no ingestion exists yet — see
//!   `tantra-data/docs/gaps/0020-fundamentals-ingestion.md`.

use async_trait::async_trait;
use tantra_agent_chat_tools::ports::error::PortError;
use tantra_agent_chat_tools::ports::fundamental::{
    FinancialStatement, FundamentalRatios, FundamentalReader,
};
use tantra_core::InstrumentId;

/// Fundamentals adapter answering the `fundamental_ratios`/`financial_statements` chat tools
/// with an explicit gap citation until `tantra-data` fundamentals ingestion exists.
pub struct FeedFundamentalReader;

impl FeedFundamentalReader {
    /// Builds the fundamentals adapter — every call answers its explicit gap citation.
    pub fn new() -> Self {
        Self
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
        _instrument: &InstrumentId,
    ) -> Result<FundamentalRatios, PortError> {
        // No fundamentals/financial-statement data source exists anywhere yet — see
        // tantra-data/docs/gaps/0020-fundamentals-ingestion.md. An explicit error, not an
        // empty success, so the model can't mistake "not wired up" for "this instrument
        // has no published ratios."
        Err(PortError::unavailable(
            "fundamentals data not ingested yet; see tantra-data/docs/gaps/0020-fundamentals-ingestion.md",
        ))
    }

    async fn financial_statements(
        &self,
        _instrument: &InstrumentId,
        _statement_type: Option<&str>,
        _limit: Option<usize>,
    ) -> Result<Vec<FinancialStatement>, PortError> {
        Err(PortError::unavailable(
            "fundamentals data not ingested yet; see tantra-data/docs/gaps/0020-fundamentals-ingestion.md",
        ))
    }
}
