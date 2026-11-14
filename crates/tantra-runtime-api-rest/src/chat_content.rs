//! Read-through `ContentReader` adapter for the chat tool catalog's news/filings tools
//! (agent plan 0006 Phase 3).
//!
//! Filings read through the `tantra-data` projection lakehouse via
//! [`tantra_market_store::FilingsProjectionReader`] (closes
//! `tantra-data/docs/gaps/0007-filings-rust-bridge.md`). News has no ingestion
//! anywhere yet, so `news_search` answers an explicit [`PortError::unavailable`]
//! with the gap citation the chat agent surfaces to the user — never a fabricated
//! empty success (same honest-stub shape as
//! [`crate::chat_market::FeedMarketReader`] per
//! `docs/gaps/0007-chat-market-reader-approximations-and-missing-sources.md`):
//! - News: no news ingestion exists anywhere yet — see
//!   `tantra-data/docs/gaps/0006-news-ingestion.md`.

use async_trait::async_trait;
use chrono::{TimeZone, Utc};
use tantra_agent_chat_tools::ports::content::{ContentReader, FilingRecord, NewsArticle};
use tantra_agent_chat_tools::ports::error::PortError;
use tantra_core::{Exchange, InstrumentId};
use tantra_market_store::{FilingProjection, FilingsProjectionReader};

/// Content adapter answering news/filings chat tools with explicit gap citations
/// until their data sources are bridged into this runtime.
///
/// Filings are bridged: when constructed with [`FeedContentReader::with_filings`]
/// the adapter reads the `tantra-data` filings projections
/// (`{root}/catalog/filings`) through `tantra-market-store`'s projection reader.
/// [`FeedContentReader::new`] keeps the unavailable-fallback shape for callers
/// with no data directory (so the pre-bridge semantics stay unchanged).
pub struct FeedContentReader {
    filings: Option<FilingsProjectionReader>,
}

impl FeedContentReader {
    /// Builds the content adapter without a filings projection — every content
    /// tool answers its explicit gap citation.
    pub fn new() -> Self {
        Self { filings: None }
    }

    /// Builds the content adapter over a data directory whose `catalog/filings`
    /// projections `tantra-data` maintains (closes
    /// `tantra-data/docs/gaps/0007-filings-rust-bridge.md`).
    pub fn with_filings(root: &std::path::Path) -> Self {
        Self {
            filings: Some(FilingsProjectionReader::new(root)),
        }
    }
}

impl Default for FeedContentReader {
    fn default() -> Self {
        Self::new()
    }
}

/// Parses a canonical `EXCHANGE:SYMBOL` instrument string (the projection form, e.g.
/// `NSE:RELIANCE-EQ`) into an [`InstrumentId`]. `InstrumentId` has no `FromStr` impl in
/// `tantra-core` (its `Exchange` component does), so the canonical projection form is split
/// here; rows with an unknown exchange or no symbol are rejected and skipped by the caller.
fn parse_instrument(s: &str) -> Option<InstrumentId> {
    let (exchange, symbol) = s.split_once(':')?;
    if symbol.is_empty() {
        return None;
    }
    let exchange = exchange.parse::<Exchange>().ok()?;
    Some(InstrumentId::new(exchange, symbol))
}

/// Maps one lakehouse filings projection row onto the chat tool catalog's record.
/// Rows whose instrument string does not parse as a canonical [`InstrumentId`] are
/// skipped (the projections are point-in-time data-directory rows, not validated
/// scrip references).
fn to_filing_record(projection: FilingProjection) -> Option<FilingRecord> {
    let instrument = parse_instrument(&projection.instrument)?;
    Some(FilingRecord {
        instrument,
        filing_date: Utc.from_utc_datetime(
            &projection
                .filing_date
                .and_hms_opt(0, 0, 0)
                .expect("midnight is always a valid time of day"),
        ),
        filing_type: projection.filing_type,
        title: projection.title,
        source_url: projection.source_url,
    })
}

#[async_trait]
impl ContentReader for FeedContentReader {
    async fn news_search(
        &self,
        _query: Option<&str>,
        _instrument: Option<&str>,
        _limit: Option<usize>,
    ) -> Result<Vec<NewsArticle>, PortError> {
        // No news ingestion exists anywhere yet — see
        // tantra-data/docs/gaps/0006-news-ingestion.md. An explicit error, not an
        // empty success, so the model can't mistake "not wired up" for "nothing
        // happened in the news today."
        Err(PortError::unavailable(
            "news ingestion is not wired up; see tantra-data/docs/gaps/0006-news-ingestion.md",
        ))
    }

    async fn filings(
        &self,
        instrument: Option<&InstrumentId>,
        filing_type: Option<&str>,
        limit: Option<usize>,
    ) -> Result<Vec<FilingRecord>, PortError> {
        let Some(reader) = &self.filings else {
            // No projections configured — same explicit gap citation as before the
            // bridge landed (see tantra-data/docs/gaps/0007-filings-rust-bridge.md).
            return Err(PortError::unavailable(
                "filings are ingested in tantra-data but not yet bridged to Rust consumers; see tantra-data/docs/gaps/0007-filings-rust-bridge.md",
            ));
        };
        let projections = reader
            .read()
            .map_err(|e| PortError::internal(format!("failed to read filings projections: {e}")))?;
        let mut filings: Vec<FilingRecord> = projections
            .into_iter()
            .filter_map(to_filing_record)
            .filter(|f| instrument.is_none_or(|inst| f.instrument == *inst))
            .filter(|f| filing_type.is_none_or(|ft| f.filing_type == *ft))
            .collect();
        filings.sort_by_key(|f| f.filing_date);
        filings.truncate(limit.unwrap_or(usize::MAX));
        Ok(filings)
    }
}
