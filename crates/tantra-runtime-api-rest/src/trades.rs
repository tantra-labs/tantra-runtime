//! The journal `Event::Fill` -> `Trade` mapping (ADR 0017 decision 7), pure.

use tantra_core::Currency;
use tantra_runtime_api::wire::Trade;
use tantra_runtime_api::{ErrorDetail, Event, Message};

/// One [`Trade`] per `Event::Fill` record, in journal order.
///
/// A fill record ([`tantra_core::FillReport`], aliased [`Trade`] on the wire) already carries
/// its own instrument, side, price, quantity and timestamp, so — unlike the old
/// `tantra_runtime_api` journal event model this replaced — no correlation with an earlier order
/// record is needed to resolve a fill's context.
pub(crate) fn trades_from_journal(
    records: &[Message],
    _currency: Currency,
) -> Result<Vec<Trade>, ErrorDetail> {
    Ok(records
        .iter()
        .filter_map(|record| match &record.event {
            Event::Fill(trade) => Some(trade.clone()),
            _ => None,
        })
        .collect())
}
