//! Read-through `CalendarReader` adapter for the chat tool catalog's economic-calendar tool
//! (agent plan 0007 Track 2 Phase 5).
//!
//! The economic calendar is ingested Python-side (`tantra-data`, Forex Factory provider) and
//! exported to `{root}/catalog/economic_calendar` projections; this adapter reads them through
//! `tantra-market-store::EconomicCalendarProjectionReader` (closes `tantra-data` gap 0018 — the
//! same projections [`crate::chat_market::FeedMarketReader::macro_series`] reads). An
//! unconfigured reader
//! answers an explicit [`PortError::unavailable`] with the gap citation the chat agent
//! surfaces to the user — never a fabricated empty success (same honest-stub shape as
//! [`crate::chat_market::FeedMarketReader`] per
//! `docs/gaps/0007-chat-market-reader-approximations-and-missing-sources.md`).

use std::path::Path;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use tantra_agent_chat_tools::ports::calendar::{
    CalendarReader, EconomicCalendarQuery, EconomicEvent,
};
use tantra_agent_chat_tools::ports::error::PortError;
use tantra_market_store::EconomicCalendarProjectionReader;

/// Calendar adapter answering the `economic_calendar` chat tool from the `tantra-data`
/// economic-calendar projections when wired via [`FeedCalendarReader::with_economic_calendar`];
/// the unwired reader answers its explicit gap citation.
pub struct FeedCalendarReader {
    economic_calendar: Option<EconomicCalendarProjectionReader>,
}

impl FeedCalendarReader {
    /// Builds the calendar adapter — every call answers its explicit gap citation until a
    /// projection reader is wired.
    pub fn new() -> Self {
        Self {
            economic_calendar: None,
        }
    }

    /// Builds a [`FeedCalendarReader`] additionally wiring the `tantra-data` economic-calendar
    /// projections (`{root}/catalog/economic_calendar`) through
    /// `tantra-market-store`'s projection reader (closes `tantra-data` gap 0018).
    pub fn with_economic_calendar(mut self, root: &Path) -> Self {
        self.economic_calendar = Some(EconomicCalendarProjectionReader::new(root));
        self
    }
}

impl Default for FeedCalendarReader {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl CalendarReader for FeedCalendarReader {
    async fn economic_calendar(
        &self,
        query: &EconomicCalendarQuery,
    ) -> Result<Vec<EconomicEvent>, PortError> {
        let Some(reader) = &self.economic_calendar else {
            // No economic-calendar projections configured — the reader is only wired by
            // [`FeedCalendarReader::with_economic_calendar`]. An explicit error, not an empty
            // success, so the model can't mistake "not wired up" for "no events are
            // scheduled" (same pattern as `indicators`/`macro_series` in
            // [`crate::chat_market::FeedMarketReader`]).
            return Err(PortError::unavailable(
                "economic-calendar reader not wired; see tantra-runtime/docs/gaps/0007 item 3b",
            ));
        };
        let projections = reader.read().map_err(|e| {
            PortError::internal(format!("failed to read economic-calendar projections: {e}"))
        })?;
        let mut events: Vec<EconomicEvent> = projections
            .into_iter()
            .filter_map(|row| {
                let event_date = DateTime::parse_from_rfc3339(&row.scheduled_at)
                    .ok()?
                    .with_timezone(&Utc);
                if let Some(start) = query.start {
                    if event_date < start {
                        return None;
                    }
                }
                if let Some(end) = query.end {
                    if event_date >= end {
                        return None;
                    }
                }
                if let Some(country) = &query.country {
                    if !row.country.eq_ignore_ascii_case(country) {
                        return None;
                    }
                }
                if let Some(impact) = &query.impact {
                    if !row.impact_level.eq_ignore_ascii_case(impact) {
                        return None;
                    }
                }
                Some(EconomicEvent {
                    id: format!("{}-{}", row.scheduled_at, row.event_name),
                    country: row.country,
                    title: row.event_name,
                    currency: Some(row.currency),
                    impact: row.impact_level.to_lowercase(),
                    forecast: row.forecast.map(|d| d.to_string()),
                    previous: row.prior.map(|d| d.to_string()),
                    actual: row.actual.map(|d| d.to_string()),
                    event_date,
                })
            })
            .collect();
        events.sort_by_key(|e| e.event_date);
        events.truncate(query.limit.unwrap_or(usize::MAX));
        Ok(events)
    }
}
