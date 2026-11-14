//! Read-through `ContentReader` adapter for the chat tool catalog's news/filings tools
//! (agent plan 0006 Phase 3).
//!
//! Filings read through the `tantra-data` projection lakehouse via
//! [`tantra_market_store::FilingsProjectionReader`]; news reads the `catalog/news`
//! projections via [`tantra_market_store::NewsProjectionReader`] (closes
//! `tantra-data/docs/gaps/0007-filings-rust-bridge.md` and
//! `tantra-data/docs/gaps/0006-news-ingestion.md`). An unconfigured reader answers an
//! explicit [`PortError::unavailable`] with the gap citation the chat agent surfaces to
//! the user — never a fabricated empty success (same honest-stub shape as
//! [`crate::chat_market::FeedMarketReader`] per
//! `docs/gaps/0007-chat-market-reader-approximations-and-missing-sources.md`).

use async_trait::async_trait;
use chrono::{TimeZone, Utc};
use tantra_agent_chat_tools::ports::content::{ContentReader, FilingRecord, NewsArticle};
use tantra_agent_chat_tools::ports::error::PortError;
use tantra_core::{Exchange, InstrumentId};
use tantra_market_store::{
    FilingProjection, FilingsProjectionReader, NewsProjection, NewsProjectionReader,
};

/// Content adapter answering news/filings chat tools with real projections — or explicit
/// gap citations when a reader is not configured.
///
/// Filings are bridged: when constructed with [`FeedContentReader::with_filings`] the
/// adapter reads the `tantra-data` filings projections (`{root}/catalog/filings`) through
/// `tantra-market-store`'s projection reader. News is bridged with
/// [`FeedContentReader::with_news`] over `{root}/catalog/news`. [`FeedContentReader::new`]
/// keeps the unavailable-fallback shape for callers with no data directory (so the
/// pre-bridge semantics stay unchanged).
pub struct FeedContentReader {
    filings: Option<FilingsProjectionReader>,
    news: Option<NewsProjectionReader>,
}

impl FeedContentReader {
    /// Builds the content adapter without projections — every content tool answers its
    /// explicit gap citation.
    pub fn new() -> Self {
        Self {
            filings: None,
            news: None,
        }
    }

    /// Builds the content adapter over a data directory whose `catalog/filings`
    /// projections `tantra-data` maintains (closes
    /// `tantra-data/docs/gaps/0007-filings-rust-bridge.md`).
    pub fn with_filings(root: &std::path::Path) -> Self {
        Self {
            filings: Some(FilingsProjectionReader::new(root)),
            news: None,
        }
    }

    /// Builds the content adapter over a data directory whose `catalog/news` projections
    /// `tantra-data` maintains (closes `tantra-data/docs/gaps/0006-news-ingestion.md`).
    pub fn with_news(mut self, root: &std::path::Path) -> Self {
        self.news = Some(NewsProjectionReader::new(root));
        self
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

/// Maps one lakehouse news projection row onto the chat tool catalog's record.
fn to_news_article(projection: NewsProjection) -> NewsArticle {
    NewsArticle {
        id: projection.article_id,
        title: projection.title,
        source: projection.source,
        url: Some(projection.url),
        published_at: projection.published_at,
        summary: projection.summary,
        instruments: projection.instruments,
    }
}

#[async_trait]
impl ContentReader for FeedContentReader {
    async fn news_search(
        &self,
        query: Option<&str>,
        instrument: Option<&str>,
        limit: Option<usize>,
    ) -> Result<Vec<NewsArticle>, PortError> {
        let Some(reader) = &self.news else {
            return Err(PortError::unavailable(
                "no news projections configured; see tantra-runtime/docs/gaps/0007 item 7",
            ));
        };
        let projections = reader
            .read()
            .map_err(|e| PortError::internal(format!("failed to read news projections: {e}")))?;
        let q = query.unwrap_or("").to_lowercase();
        let mut articles: Vec<NewsArticle> = projections
            .into_iter()
            .map(to_news_article)
            .filter(|a| {
                q.is_empty()
                    || a.title.to_lowercase().contains(&q)
                    || a.summary
                        .as_deref()
                        .is_some_and(|s| s.to_lowercase().contains(&q))
            })
            .filter(|a| {
                instrument
                    .is_none_or(|inst| a.instruments.iter().any(|i| i.eq_ignore_ascii_case(inst)))
            })
            .collect();
        articles.sort_by_key(|b| std::cmp::Reverse(b.published_at)); // newest first
        articles.truncate(limit.unwrap_or(usize::MAX));
        Ok(articles)
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
