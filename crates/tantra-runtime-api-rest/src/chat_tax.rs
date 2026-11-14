//! Tax-report adapter for the chat tool catalog's `tax_report`/`ledger_statement` tools
//! (agent plan 0007 Track 5 Phase 14).

use async_trait::async_trait;
use tantra_agent_chat_tools::ports::error::PortError;
use tantra_agent_chat_tools::ports::portfolio::DateRange;
use tantra_agent_chat_tools::ports::tax::{ClosedLotRecord, TaxReportReader};
use tantra_core::AccountId;

/// Tax/statement adapter answering `tax_report`/`ledger_statement` with an explicit gap
/// citation until the runtime exposes `tantra-ledger-portfolio`'s matched lots to the chat
/// agent (see `docs/gaps/0007` item 19).
pub struct FeedTaxReportReader;

impl FeedTaxReportReader {
    /// Builds the tax-report adapter — every call answers its explicit gap citation.
    pub fn new() -> Self {
        Self
    }
}

impl Default for FeedTaxReportReader {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl TaxReportReader for FeedTaxReportReader {
    async fn closed_lots(
        &self,
        _account: &AccountId,
        _range: DateRange,
    ) -> Result<Vec<ClosedLotRecord>, PortError> {
        // `tantra-ledger-portfolio` computes matched lots but nothing exposes them to the
        // chat agent yet — see tantra-runtime/docs/gaps/0007 item 19. An explicit error,
        // never fabricated capital-gains figures.
        Err(PortError::unavailable(
            "matched-lot bridge not wired in the runtime; see tantra-runtime/docs/gaps/0007 item 19",
        ))
    }
}
