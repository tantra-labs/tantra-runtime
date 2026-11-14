//! Unit tests for `FeedCalendarReader` — the read-through adapter answering the
//! `economic_calendar` chat tool (agent plan 0007 Track 2 Phase 5) from the `tantra-data`
//! economic-calendar projections (closes `tantra-data` gap 0018).

use std::sync::Arc;

use arrow::array::{Date32Array, Decimal128Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_writer::ArrowWriter;
use tantra_agent_chat_tools::ports::calendar::{CalendarReader, EconomicCalendarQuery};
use tantra_agent_chat_tools::ports::error::PortErrorKind;
use tantra_runtime_api_rest::chat_calendar::FeedCalendarReader;
use tempfile::tempdir;

#[tokio::test]
async fn economic_calendar_is_explicitly_unavailable_when_not_wired() {
    let reader = FeedCalendarReader::new();
    let err = reader
        .economic_calendar(&EconomicCalendarQuery::default())
        .await
        .unwrap_err();
    assert_eq!(err.kind, PortErrorKind::Unavailable);
    assert!(
        err.message.contains("0007 item 3b"),
        "stub must cite the runtime gap item, got: {err}"
    );
}

#[tokio::test]
async fn economic_calendar_reads_through_projection() {
    let dir = tempdir().unwrap();
    let calendar_dir = dir.path().join("catalog").join("economic_calendar");
    std::fs::create_dir_all(&calendar_dir).unwrap();

    let schema = Arc::new(Schema::new(vec![
        Field::new("scheduled_date", DataType::Date32, false),
        Field::new("scheduled_at", DataType::Utf8, false),
        Field::new("instrument", DataType::Utf8, true),
        Field::new("country", DataType::Utf8, false),
        Field::new("currency", DataType::Utf8, false),
        Field::new("category", DataType::Utf8, false),
        Field::new("event_name", DataType::Utf8, false),
        Field::new("impact_level", DataType::Utf8, false),
        Field::new("actual", DataType::Decimal128(18, 4), true),
        Field::new("forecast", DataType::Decimal128(18, 4), true),
        Field::new("prior", DataType::Decimal128(18, 4), true),
        Field::new("period_label", DataType::Utf8, true),
    ]));
    let actuals = Decimal128Array::from(vec![Some(5_500_000_i128), Some(3_250_000_i128)])
        .with_precision_and_scale(18, 4)
        .unwrap();
    let nulls = Decimal128Array::from(vec![None, None])
        .with_precision_and_scale(18, 4)
        .unwrap();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Date32Array::from(vec![0i32, 0])),
            Arc::new(StringArray::from(vec![
                "2026-01-05T06:00:00+00:00",
                "2026-01-05T07:00:00+00:00",
            ])),
            Arc::new(StringArray::from(vec![None::<&str>, None])),
            Arc::new(StringArray::from(vec!["IN", "US"])),
            Arc::new(StringArray::from(vec!["INR", "USD"])),
            Arc::new(StringArray::from(vec!["Economic", "Economic"])),
            Arc::new(StringArray::from(vec!["CPI YoY", "ISM PMI"])),
            Arc::new(StringArray::from(vec!["High", "Medium"])),
            Arc::new(actuals),
            Arc::new(nulls.clone()),
            Arc::new(nulls),
            Arc::new(StringArray::from(vec![Some("Jan"), None])),
        ],
    )
    .unwrap();

    let path = calendar_dir.join("2026-01-05.parquet");
    let file = std::fs::File::create(&path).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let reader = FeedCalendarReader::new().with_economic_calendar(dir.path());

    let all = reader
        .economic_calendar(&EconomicCalendarQuery::default())
        .await
        .unwrap();
    assert_eq!(all.len(), 2);
    assert_eq!(all[0].country, "IN");
    assert_eq!(all[0].title, "CPI YoY");
    assert_eq!(all[0].impact, "high");
    assert_eq!(all[0].actual.as_deref(), Some("550.0000"));
    assert_eq!(all[0].event_date.to_rfc3339(), "2026-01-05T06:00:00+00:00");
    assert_eq!(all[1].country, "US");
    assert_eq!(all[1].title, "ISM PMI");

    let filtered = reader
        .economic_calendar(&EconomicCalendarQuery {
            country: Some("in".to_string()),
            limit: Some(5),
            ..EconomicCalendarQuery::default()
        })
        .await
        .unwrap();
    assert_eq!(filtered.len(), 1);
    assert_eq!(filtered[0].title, "CPI YoY");

    let windowed = reader
        .economic_calendar(&EconomicCalendarQuery {
            start: Some(
                chrono::DateTime::parse_from_rfc3339("2026-01-05T06:30:00+00:00")
                    .unwrap()
                    .with_timezone(&chrono::Utc),
            ),
            ..EconomicCalendarQuery::default()
        })
        .await
        .unwrap();
    assert_eq!(windowed.len(), 1);
    assert_eq!(windowed[0].title, "ISM PMI");
}
