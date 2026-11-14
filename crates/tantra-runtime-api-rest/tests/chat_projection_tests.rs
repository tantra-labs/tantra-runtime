//! Tests for the chat adapters' read-through of the `tantra-data` projection lakehouse
//! (closes `tantra-data/docs/gaps/0007-filings-rust-bridge.md` and
//! `tantra-data/docs/gaps/0008-corporate-actions-rust-bridge.md`): filings via
//! `FeedContentReader::with_filings` and corporate actions via
//! `FeedMarketReader::with_corporate_actions`.

use std::sync::{Arc, Mutex};

use arrow::array::{Date32Array, Decimal128Array, Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_writer::ArrowWriter;
use rust_decimal::Decimal;
use tantra_agent_chat_tools::ports::content::ContentReader;
use tantra_agent_chat_tools::ports::error::PortErrorKind;
use tantra_agent_chat_tools::ports::market::MarketReader;
use tantra_core::{Currency, Exchange, InstrumentId, Money};
use tantra_risk::RiskEngine;
use tantra_runtime_api_rest::chat_content::FeedContentReader;
use tantra_runtime_api_rest::chat_market::FeedMarketReader;
use tantra_runtime_api_rest::market_feed::ports::{
    BarReader, BarRequest, InstrumentMaster, PortError as FeedPortError, QuoteReader,
};
use tantra_runtime_api_rest::market_feed::wire::Instrument;
use tempfile::tempdir;

/// Minimal in-memory feed implementing all three read ports trivially — the
/// corporate-actions test exercises the projection reader, not the feed.
#[derive(Clone, Default)]
struct FakeFeed;

#[async_trait::async_trait]
impl InstrumentMaster for FakeFeed {
    async fn get_instrument(
        &self,
        _id: &InstrumentId,
    ) -> Result<Option<Instrument>, FeedPortError> {
        Ok(None)
    }

    async fn list_instruments(&self) -> Result<Vec<Instrument>, FeedPortError> {
        Ok(Vec::new())
    }

    async fn list_chain(
        &self,
        _underlying: &InstrumentId,
        _as_of: Option<chrono::NaiveDate>,
    ) -> Result<Vec<Instrument>, FeedPortError> {
        Ok(Vec::new())
    }
}

#[async_trait::async_trait]
impl QuoteReader for FakeFeed {
    async fn read_quote(
        &self,
        _id: &InstrumentId,
        _as_of: Option<tantra_runtime_api_rest::market_feed::wire::UnixNanos>,
    ) -> Result<Option<tantra_runtime_api_rest::market_feed::wire::QuoteTick>, FeedPortError> {
        Ok(None)
    }
}

#[async_trait::async_trait]
impl BarReader for FakeFeed {
    async fn read_bars(
        &self,
        _request: &BarRequest,
    ) -> Result<Vec<tantra_runtime_api_rest::market_feed::wire::Bar>, FeedPortError> {
        Ok(Vec::new())
    }
}

#[tokio::test]
async fn filings_read_through_projection() {
    let dir = tempdir().unwrap();
    let filings_dir = dir.path().join("catalog").join("filings");
    std::fs::create_dir_all(&filings_dir).unwrap();

    let schema = Arc::new(Schema::new(vec![
        Field::new("filing_date", DataType::Date32, false),
        Field::new("instrument", DataType::Utf8, false),
        Field::new("filing_type", DataType::Utf8, false),
        Field::new("title", DataType::Utf8, false),
        Field::new("source_url", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Date32Array::from(vec![0i32, 1])),
            Arc::new(StringArray::from(vec!["NSE:RELIANCE-EQ", "NSE:TCS-EQ"])),
            Arc::new(StringArray::from(vec!["quarterly_result", "annual_report"])),
            Arc::new(StringArray::from(vec![
                "Board meeting outcome",
                "FY26 annual report",
            ])),
            Arc::new(StringArray::from(vec![
                Some("https://example.com/filing/1"),
                None,
            ])),
        ],
    )
    .unwrap();

    let path = filings_dir.join("2026-11-06.parquet");
    let file = std::fs::File::create(&path).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let reader = FeedContentReader::with_filings(dir.path());
    let filings = reader.filings(None, None, None).await.unwrap();
    assert_eq!(filings.len(), 2);
    assert_eq!(
        filings[0].instrument,
        InstrumentId::new(Exchange::Nse, "RELIANCE-EQ")
    );
    assert_eq!(filings[0].filing_type, "quarterly_result");
    assert_eq!(filings[0].title, "Board meeting outcome");
    assert_eq!(
        filings[0].source_url.as_deref(),
        Some("https://example.com/filing/1")
    );
    assert_eq!(
        filings[1].instrument,
        InstrumentId::new(Exchange::Nse, "TCS-EQ")
    );
    assert_eq!(filings[1].title, "FY26 annual report");
    assert_eq!(filings[1].source_url, None);
}

#[tokio::test]
async fn corporate_actions_read_through_projection() {
    let dir = tempdir().unwrap();
    let actions_dir = dir.path().join("catalog").join("corporate_actions");
    std::fs::create_dir_all(&actions_dir).unwrap();

    let schema = Arc::new(Schema::new(vec![
        Field::new("ex_date", DataType::Date32, false),
        Field::new("instrument", DataType::Utf8, false),
        Field::new("action_type", DataType::Utf8, false),
        Field::new("old_shares", DataType::Int32, true),
        Field::new("new_shares", DataType::Int32, true),
        Field::new("per_share", DataType::Decimal128(18, 4), true),
        Field::new("issue_price", DataType::Decimal128(18, 4), true),
        Field::new("currency", DataType::Utf8, false),
    ]));
    let per_share = Decimal128Array::from(vec![Some(12_500i128)])
        .with_precision_and_scale(18, 4)
        .unwrap();
    let issue_price = Decimal128Array::from(vec![None::<i128>])
        .with_precision_and_scale(18, 4)
        .unwrap();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Date32Array::from(vec![0i32])),
            Arc::new(StringArray::from(vec!["NSE:RELIANCE-EQ"])),
            Arc::new(StringArray::from(vec!["dividend"])),
            Arc::new(Int32Array::from(vec![None::<i32>])),
            Arc::new(Int32Array::from(vec![None::<i32>])),
            Arc::new(per_share),
            Arc::new(issue_price),
            Arc::new(StringArray::from(vec!["INR"])),
        ],
    )
    .unwrap();

    let path = actions_dir.join("2026-11-02.parquet");
    let file = std::fs::File::create(&path).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let feed = Arc::new(FakeFeed);
    let reader = FeedMarketReader::with_corporate_actions(
        dir.path(),
        feed.clone(),
        feed.clone(),
        feed.clone(),
        Arc::new(Mutex::new(RiskEngine::default())),
    );

    let actions = reader.corporate_actions(None, None, None).await.unwrap();
    assert_eq!(actions.len(), 1);
    assert_eq!(
        actions[0].instrument,
        InstrumentId::new(Exchange::Nse, "RELIANCE-EQ")
    );
    assert_eq!(actions[0].action_type, "dividend");
    assert_eq!(
        actions[0].amount,
        Some(Money::new(
            Decimal::try_from_i128_with_scale(12_500, 4).unwrap(),
            Currency::Inr
        ))
    );
    assert_eq!(actions[0].record_date, None);
    assert_eq!(actions[0].ratio_from, None);
    assert_eq!(actions[0].ratio_to, None);
}

#[tokio::test]
async fn filings_unavailable_without_projection() {
    let reader = FeedContentReader::new();
    let err = reader.filings(None, None, None).await.unwrap_err();
    assert_eq!(err.kind, PortErrorKind::Unavailable);
    assert!(
        err.message.contains("0007-filings-rust-bridge.md"),
        "unconfigured filings must cite the upstream gap, got: {err}"
    );
}

#[tokio::test]
async fn filings_empty_when_projection_dir_missing() {
    let dir = tempdir().unwrap();
    let reader = FeedContentReader::with_filings(dir.path());
    let filings = reader.filings(None, None, None).await.unwrap();
    assert!(filings.is_empty());
}
