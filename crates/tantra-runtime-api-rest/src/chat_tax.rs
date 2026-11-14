//! Tax-report adapter for the chat tool catalog's `tax_report`/`ledger_statement` tools
//! (agent plan 0007 Track 5 Phase 14, docs/gaps/0007 item 19).

use std::sync::Arc;

use async_trait::async_trait;
use tantra_agent_chat_tools::ports::error::PortError;
use tantra_agent_chat_tools::ports::portfolio::DateRange;
use tantra_agent_chat_tools::ports::tax::{ClosedLotRecord, TaxReportReader};
use tantra_core::AccountId;
use tantra_runtime_api::SharedLedger;

/// Tax/statement adapter answering `tax_report`/`ledger_statement` from the shared ledger's
/// FIFO matched-lot history (docs/gaps/0007 item 19): `tantra-ledger-portfolio` computes the
/// matched lots on every fill, `SharedLedger::record_fill` accumulates them, and this adapter
/// exposes them per account over a date range. The STCG/LTCG tax math itself lives and is
/// unit-tested in `tantra-agent-chat-tools`.
pub struct FeedTaxReportReader {
    ledger: Option<Arc<SharedLedger>>,
}

impl FeedTaxReportReader {
    /// Builds the tax-report adapter over the runtime's shared ledger handle.
    pub fn new(ledger: Arc<SharedLedger>) -> Self {
        Self {
            ledger: Some(ledger),
        }
    }

    /// Builds a tax-report adapter with no ledger handle — every call answers its explicit gap
    /// citation (defensive; `ChatRuntime::from_env` always wires the real handle).
    pub fn stub() -> Self {
        Self { ledger: None }
    }
}

impl Default for FeedTaxReportReader {
    fn default() -> Self {
        Self::stub()
    }
}

#[async_trait]
impl TaxReportReader for FeedTaxReportReader {
    async fn closed_lots(
        &self,
        account: &AccountId,
        range: DateRange,
    ) -> Result<Vec<ClosedLotRecord>, PortError> {
        let Some(ledger) = &self.ledger else {
            return Err(PortError::unavailable(
                "matched-lot bridge not wired in the runtime; see tantra-runtime/docs/gaps/0007 item 19",
            ));
        };
        let mut records: Vec<ClosedLotRecord> = ledger
            .closed_lots(account)
            .into_iter()
            .filter(|entry| {
                range.start.is_none_or(|start| entry.matched_at >= start)
                    && range.end.is_none_or(|end| entry.matched_at < end)
            })
            .map(|entry| ClosedLotRecord {
                lot_id: entry.lot_id,
                instrument: entry.instrument,
                quantity: entry.matched_quantity,
                entry_price: entry.entry_price,
                exit_price: entry.exit_price,
                realized_pnl: entry.realized_pnl,
                holding_period_days: entry.holding_period_days,
                is_long_term: entry.is_long_term,
            })
            .collect();
        records.sort_by(|a, b| a.lot_id.cmp(&b.lot_id));
        Ok(records)
    }
}
