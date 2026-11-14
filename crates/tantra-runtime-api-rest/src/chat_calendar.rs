//! Read-through `CalendarReader` adapter for the chat tool catalog's economic-calendar tool
//! (agent plan 0007 Track 2 Phase 5).
//!
//! The economic calendar is ingested Python-side (`tantra-data`) but has no Rust-consumer
//! bridge yet, so every call answers an explicit [`PortError::unavailable`] with the gap
//! citation the chat agent surfaces to the user — never a fabricated empty success (same
//! honest-stub shape as [`crate::chat_market::FeedMarketReader`] per
//! `docs/gaps/0007-chat-market-reader-approximations-and-missing-sources.md`):
//! - Economic calendar: no Rust bridge exists yet — see
//!   `tantra-data/docs/gaps/0018-economic-calendar-rust-bridge.md`.

use async_trait::async_trait;
use tantra_agent_chat_tools::ports::calendar::{
    CalendarReader, EconomicCalendarQuery, EconomicEvent,
};
use tantra_agent_chat_tools::ports::error::PortError;

/// Calendar adapter answering the `economic_calendar` chat tool with an explicit gap citation
/// until the `tantra-data` economic-calendar series is bridged into this runtime.
pub struct FeedCalendarReader;

impl FeedCalendarReader {
    /// Builds the calendar adapter — every call answers its explicit gap citation.
    pub fn new() -> Self {
        Self
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
        _query: &EconomicCalendarQuery,
    ) -> Result<Vec<EconomicEvent>, PortError> {
        // The economic calendar is maintained Python-side with no Rust-consumer bridge yet —
        // see tantra-data/docs/gaps/0018-economic-calendar-rust-bridge.md. An explicit error,
        // not an empty success, so the model can't mistake "not wired up" for "no events are
        // scheduled."
        Err(PortError::unavailable(
            "economic-calendar Rust bridge not wired; see tantra-data/docs/gaps/0018-economic-calendar-rust-bridge.md",
        ))
    }
}
