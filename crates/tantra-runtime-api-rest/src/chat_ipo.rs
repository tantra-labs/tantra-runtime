//! Read-through `IpoReader` adapter for the chat tool catalog's IPO tools
//! (agent plan 0007 Track 3 Phase 8).
//!
//! No IPO calendar/subscription data source exists anywhere yet, so every call answers an
//! explicit [`PortError::unavailable`] with the gap citation the chat agent surfaces to the
//! user — never fabricated issues (same honest-stub shape as
//! [`crate::chat_market::FeedMarketReader`] per
//! `docs/gaps/0007-chat-market-reader-approximations-and-missing-sources.md`):
//! - IPO calendar/subscriptions: no ingestion exists yet — see
//!   `tantra-data/docs/gaps/0022-ipo-calendar-and-subscription-ingestion.md`.

use async_trait::async_trait;
use tantra_agent_chat_tools::ports::error::PortError;
use tantra_agent_chat_tools::ports::ipo::{IpoIssue, IpoReader, IpoSubscription};

/// IPO adapter answering the `ipo_calendar`/`ipo_subscription` chat tools with an explicit gap
/// citation until `tantra-data` IPO ingestion exists.
pub struct FeedIpoReader;

impl FeedIpoReader {
    /// Builds the IPO adapter — every call answers its explicit gap citation.
    pub fn new() -> Self {
        Self
    }
}

impl Default for FeedIpoReader {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl IpoReader for FeedIpoReader {
    async fn upcoming_ipos(&self, _limit: Option<usize>) -> Result<Vec<IpoIssue>, PortError> {
        // No IPO data source exists anywhere yet — see
        // tantra-data/docs/gaps/0022-ipo-calendar-and-subscription-ingestion.md. An explicit
        // error, not an empty success, so the model can't mistake "not wired up" for "no
        // IPOs are scheduled."
        Err(PortError::unavailable(
            "IPO data not ingested yet; see tantra-data/docs/gaps/0022-ipo-calendar-and-subscription-ingestion.md",
        ))
    }

    async fn ipo_subscription(&self, _symbol: &str) -> Result<IpoSubscription, PortError> {
        Err(PortError::unavailable(
            "IPO data not ingested yet; see tantra-data/docs/gaps/0022-ipo-calendar-and-subscription-ingestion.md",
        ))
    }
}
