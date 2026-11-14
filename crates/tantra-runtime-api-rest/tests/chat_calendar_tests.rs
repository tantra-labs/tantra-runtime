//! Unit tests for `FeedCalendarReader` — the read-through adapter answering the
//! `economic_calendar` chat tool (agent plan 0007 Track 2 Phase 5) with an explicit gap
//! citation until the `tantra-data` economic-calendar series is bridged into this runtime.

use tantra_agent_chat_tools::ports::calendar::{CalendarReader, EconomicCalendarQuery};
use tantra_agent_chat_tools::ports::error::PortErrorKind;
use tantra_runtime_api_rest::chat_calendar::FeedCalendarReader;

#[tokio::test]
async fn economic_calendar_is_explicitly_unavailable_with_gap_citation() {
    let reader = FeedCalendarReader::new();
    let err = reader
        .economic_calendar(&EconomicCalendarQuery::default())
        .await
        .unwrap_err();
    assert_eq!(err.kind, PortErrorKind::Unavailable);
    assert!(
        err.message
            .contains("0018-economic-calendar-rust-bridge.md"),
        "stub must cite the upstream gap, got: {err}"
    );
}
