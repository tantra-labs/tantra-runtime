//! Tests for the chat adapters' read-through of the `tantra-data` projection lakehouse
//! (closes `tantra-data/docs/gaps/0007-filings-rust-bridge.md` and
//! `tantra-data/docs/gaps/0008-corporate-actions-rust-bridge.md`): filings via
//! `FeedContentReader::with_filings` and corporate actions via
//! `FeedMarketReader::with_corporate_actions`.

use std::sync::{Arc, Mutex};

use arrow::array::{Date32Array, Decimal128Array, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use chrono::{NaiveDate, TimeZone, Utc};
use parquet::arrow::arrow_writer::ArrowWriter;
use rust_decimal::Decimal;
use tantra_agent_chat_tools::ports::content::ContentReader;
use tantra_agent_chat_tools::ports::error::PortErrorKind;
use tantra_agent_chat_tools::ports::fundamental::FundamentalReader;
use tantra_agent_chat_tools::ports::market::MarketReader;
use tantra_agent_chat_tools::ports::portfolio::DateRange;
use tantra_core::{Currency, Exchange, InstrumentId, Money, Quantity};
use tantra_risk::RiskEngine;
use tantra_runtime_api_rest::chat_content::FeedContentReader;
use tantra_runtime_api_rest::chat_fundamental::FeedFundamentalReader;
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
    let reader = FeedMarketReader::new(
        feed.clone(),
        feed.clone(),
        feed.clone(),
        Arc::new(Mutex::new(RiskEngine::default())),
    )
    .with_corporate_actions(dir.path());

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
async fn block_deals_read_through_projection() {
    let dir = tempdir().unwrap();
    let block_deals_dir = dir.path().join("catalog").join("block_deals");
    std::fs::create_dir_all(&block_deals_dir).unwrap();

    let schema = Arc::new(Schema::new(vec![
        Field::new("trade_date", DataType::Date32, false),
        Field::new("exchange", DataType::Utf8, false),
        Field::new("instrument", DataType::Utf8, false),
        Field::new("security_name", DataType::Utf8, false),
        Field::new("client_name", DataType::Utf8, false),
        Field::new("side", DataType::Utf8, false),
        Field::new("category", DataType::Utf8, false),
        Field::new("quantity", DataType::Decimal128(18, 4), false),
        Field::new("price", DataType::Decimal128(18, 4), false),
        Field::new("remarks", DataType::Utf8, true),
    ]));
    let quantities = Decimal128Array::from(vec![Some(2_500_000_000_i128), Some(100_000_000_i128)])
        .with_precision_and_scale(18, 4)
        .unwrap();
    let prices = Decimal128Array::from(vec![Some(25_005_000_i128), Some(40_000_000_i128)])
        .with_precision_and_scale(18, 4)
        .unwrap();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Date32Array::from(vec![0i32, 1])),
            Arc::new(StringArray::from(vec!["NSE", "NSE"])),
            Arc::new(StringArray::from(vec!["NSE:RELIANCE-EQ", "NSE:TCS-EQ"])),
            Arc::new(StringArray::from(vec!["RELIANCE", "TCS"])),
            Arc::new(StringArray::from(vec!["CLIENT A", "CLIENT B"])),
            Arc::new(StringArray::from(vec!["BUY", "SELL"])),
            Arc::new(StringArray::from(vec!["block", "bulk"])),
            Arc::new(quantities),
            Arc::new(prices),
            Arc::new(StringArray::from(vec![Some("Block deal window"), None])),
        ],
    )
    .unwrap();

    let path = block_deals_dir.join("2026-11-06.parquet");
    let file = std::fs::File::create(&path).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let feed = Arc::new(FakeFeed);
    let reader = FeedMarketReader::new(
        feed.clone(),
        feed.clone(),
        feed.clone(),
        Arc::new(Mutex::new(RiskEngine::default())),
    )
    .with_block_deals(dir.path());

    let deals = reader.block_deals(None, None).await.unwrap();
    assert_eq!(deals.len(), 2);
    assert_eq!(
        deals[0].instrument,
        InstrumentId::new(Exchange::Nse, "RELIANCE-EQ")
    );
    assert_eq!(deals[0].side, "buy");
    assert_eq!(deals[0].category, "block");
    assert_eq!(
        deals[0].quantity,
        tantra_core::Quantity::new(Decimal::try_from_i128_with_scale(2_500_000_000, 4).unwrap())
    );
    assert_eq!(
        deals[0].price,
        tantra_core::Price::new(Decimal::try_from_i128_with_scale(25_005_000, 4).unwrap())
    );

    let tcs = reader
        .block_deals(Some(&InstrumentId::new(Exchange::Nse, "TCS-EQ")), None)
        .await
        .unwrap();
    assert_eq!(tcs.len(), 1);
    assert_eq!(tcs[0].side, "sell");
    assert_eq!(tcs[0].category, "bulk");
}

#[tokio::test]
async fn news_search_read_through_projection() {
    let dir = tempdir().unwrap();
    let news_dir = dir.path().join("catalog").join("news");
    std::fs::create_dir_all(&news_dir).unwrap();

    let schema = Arc::new(Schema::new(vec![
        Field::new("published_date", DataType::Date32, false),
        Field::new("published_at", DataType::Utf8, false),
        Field::new("article_id", DataType::Utf8, false),
        Field::new("title", DataType::Utf8, false),
        Field::new("source", DataType::Utf8, false),
        Field::new("url", DataType::Utf8, false),
        Field::new("summary", DataType::Utf8, true),
        Field::new("instruments", DataType::Utf8, false),
    ]));
    let unix_epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    let days = i32::try_from(
        (chrono::NaiveDate::from_ymd_opt(2026, 11, 6).unwrap() - unix_epoch).num_days(),
    )
    .unwrap();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Date32Array::from(vec![days, days])),
            Arc::new(StringArray::from(vec![
                "2026-11-06T10:30:00Z",
                "2026-11-06T09:00:00Z",
            ])),
            Arc::new(StringArray::from(vec!["a1", "a2"])),
            Arc::new(StringArray::from(vec![
                "Reliance board meeting",
                "TCS results",
            ])),
            Arc::new(StringArray::from(vec!["moneycontrol", "moneycontrol"])),
            Arc::new(StringArray::from(vec![
                "https://example.com/reliance",
                "https://example.com/tcs",
            ])),
            Arc::new(StringArray::from(vec![Some("Q3 update"), None])),
            Arc::new(StringArray::from(vec!["NSE:RELIANCE-EQ", "NSE:TCS-EQ"])),
        ],
    )
    .unwrap();

    let path = news_dir.join("2026-11-06.parquet");
    let file = std::fs::File::create(&path).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let reader = FeedContentReader::new().with_news(dir.path());
    let all = reader.news_search(None, None, None).await.unwrap();
    assert_eq!(all.len(), 2);
    // Newest first: the 10:30 article sorts before the 09:00 one.
    assert_eq!(all[0].id, "a1");
    assert_eq!(all[1].id, "a2");
    assert_eq!(all[0].title, "Reliance board meeting");
    assert_eq!(all[0].source, "moneycontrol");
    assert_eq!(all[0].url.as_deref(), Some("https://example.com/reliance"));
    assert_eq!(all[0].summary.as_deref(), Some("Q3 update"));
    assert_eq!(all[0].instruments, vec!["NSE:RELIANCE-EQ"]);
    assert_eq!(
        all[0].published_at.to_rfc3339(),
        "2026-11-06T10:30:00+00:00"
    );
    assert_eq!(all[1].summary, None);

    let only_reliance = reader
        .news_search(Some("reliance"), None, None)
        .await
        .unwrap();
    assert_eq!(only_reliance.len(), 1);
    assert_eq!(only_reliance[0].id, "a1");

    let only_tcs = reader
        .news_search(None, Some("NSE:TCS-EQ"), None)
        .await
        .unwrap();
    assert_eq!(only_tcs.len(), 1);
    assert_eq!(only_tcs[0].id, "a2");

    // No reader wired: the adapter answers its explicit gap citation, not an empty
    // success, so the model can't mistake "not wired up" for "no news today".
    let unconfigured = FeedContentReader::new();
    let err = unconfigured
        .news_search(None, None, None)
        .await
        .unwrap_err();
    assert_eq!(err.kind, PortErrorKind::Unavailable);
    assert!(
        err.message.contains("no news projections configured"),
        "unconfigured news must cite the missing projections, got: {err}"
    );
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

#[tokio::test]
async fn institutional_flows_read_through_projection() {
    let dir = tempdir().unwrap();
    let flows_dir = dir.path().join("catalog").join("institutional_flows");
    std::fs::create_dir_all(&flows_dir).unwrap();

    let schema = Arc::new(Schema::new(vec![
        Field::new("trade_date", DataType::Date32, false),
        Field::new("participant", DataType::Utf8, false),
        Field::new("segment", DataType::Utf8, false),
        Field::new("gross_buy", DataType::Decimal128(18, 4), false),
        Field::new("gross_sell", DataType::Decimal128(18, 4), false),
        Field::new("net", DataType::Decimal128(18, 4), false),
        Field::new("source", DataType::Utf8, false),
    ]));
    let gross_buy = Decimal128Array::from(vec![
        10_000_000_000i128,
        8_000_000_000i128,
        5_000_000_000i128,
    ])
    .with_precision_and_scale(18, 4)
    .unwrap();
    let gross_sell = Decimal128Array::from(vec![
        6_000_000_000i128,
        7_000_000_000i128,
        3_000_000_000i128,
    ])
    .with_precision_and_scale(18, 4)
    .unwrap();
    let net = Decimal128Array::from(vec![
        4_000_000_000i128,
        1_000_000_000i128,
        2_000_000_000i128,
    ])
    .with_precision_and_scale(18, 4)
    .unwrap();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Date32Array::from(vec![0i32, 0, 0])),
            Arc::new(StringArray::from(vec!["FII", "DII", "FII"])),
            Arc::new(StringArray::from(vec!["cash", "cash", "index_futures"])),
            Arc::new(gross_buy),
            Arc::new(gross_sell),
            Arc::new(net),
            Arc::new(StringArray::from(vec!["nse", "nse", "nse"])),
        ],
    )
    .unwrap();

    let path = flows_dir.join("2026-11-06.parquet");
    let file = std::fs::File::create(&path).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let feed = Arc::new(FakeFeed);
    let reader = FeedMarketReader::new(
        feed.clone(),
        feed.clone(),
        feed.clone(),
        Arc::new(Mutex::new(RiskEngine::default())),
    )
    .with_institutional_flows(dir.path());

    let flows = reader.institutional_flows(None).await.unwrap();
    assert_eq!(flows.len(), 2, "index-futures rows must be filtered out");
    assert_eq!(flows[0].category, "fii");
    assert_eq!(
        flows[0].buy_value,
        Money::new(
            Decimal::try_from_i128_with_scale(10_000_000_000, 4).unwrap(),
            Currency::Inr
        )
    );
    assert_eq!(
        flows[0].sell_value,
        Money::new(
            Decimal::try_from_i128_with_scale(6_000_000_000, 4).unwrap(),
            Currency::Inr
        )
    );
    assert_eq!(
        flows[0].net_value,
        Money::new(
            Decimal::try_from_i128_with_scale(4_000_000_000, 4).unwrap(),
            Currency::Inr
        )
    );
    // Date32 day 0 is the Unix epoch (1970-01-01).
    assert_eq!(
        flows[0].trade_date,
        Utc.from_utc_datetime(
            &NaiveDate::from_ymd_opt(1970, 1, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap()
        )
    );
    assert_eq!(flows[1].category, "dii");
}

#[tokio::test]
async fn delivery_stats_read_through_projection() {
    let dir = tempdir().unwrap();
    let delivery_dir = dir.path().join("catalog").join("delivery");
    std::fs::create_dir_all(&delivery_dir).unwrap();

    let schema = Arc::new(Schema::new(vec![
        Field::new("trade_date", DataType::Date32, false),
        Field::new("exchange", DataType::Utf8, false),
        Field::new("instrument", DataType::Utf8, false),
        Field::new("traded_quantity", DataType::Int64, false),
        Field::new("deliverable_quantity", DataType::Int64, false),
        Field::new("delivery_pct", DataType::Decimal128(18, 4), false),
    ]));
    let delivery_pct = Decimal128Array::from(vec![50_000_000i128, 0i128])
        .with_precision_and_scale(18, 4)
        .unwrap();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Date32Array::from(vec![0i32, 0])),
            Arc::new(StringArray::from(vec!["NSE", "NSE"])),
            Arc::new(StringArray::from(vec!["NSE:RELIANCE-EQ", "NSE:TCS-EQ"])),
            Arc::new(Int64Array::from(vec![250_000i64, 100_000])),
            Arc::new(Int64Array::from(vec![125_000i64, 0])),
            Arc::new(delivery_pct),
        ],
    )
    .unwrap();

    let path = delivery_dir.join("2026-11-06.parquet");
    let file = std::fs::File::create(&path).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let feed = Arc::new(FakeFeed);
    let reader = FeedMarketReader::new(
        feed.clone(),
        feed.clone(),
        feed.clone(),
        Arc::new(Mutex::new(RiskEngine::default())),
    )
    .with_delivery(dir.path());

    let bars = reader
        .delivery_stats(
            &InstrumentId::new(Exchange::Nse, "RELIANCE-EQ"),
            DateRange::new(None, None),
            None,
        )
        .await
        .unwrap();
    assert_eq!(bars.len(), 1);
    assert_eq!(
        bars[0].instrument,
        InstrumentId::new(Exchange::Nse, "RELIANCE-EQ")
    );
    assert_eq!(bars[0].traded_qty, Quantity::new(Decimal::from(250_000)));
    assert_eq!(bars[0].delivered_qty, Quantity::new(Decimal::from(125_000)));
    assert_eq!(
        bars[0].delivery_pct,
        Decimal::try_from_i128_with_scale(50_000_000, 4).unwrap()
    );
    assert_eq!(
        bars[0].date,
        Utc.from_utc_datetime(
            &NaiveDate::from_ymd_opt(1970, 1, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap()
        )
    );
}

#[tokio::test]
async fn macro_series_reads_through_projection() {
    let dir = tempdir().unwrap();
    let cal_dir = dir.path().join("catalog").join("economic_calendar");
    std::fs::create_dir_all(&cal_dir).unwrap();

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
    let actual = Decimal128Array::from(vec![Some(5_500_000i128), None::<i128>])
        .with_precision_and_scale(18, 4)
        .unwrap();
    let forecast = Decimal128Array::from(vec![None::<i128>, None::<i128>])
        .with_precision_and_scale(18, 4)
        .unwrap();
    let prior = Decimal128Array::from(vec![None::<i128>, None::<i128>])
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
            Arc::new(StringArray::from(vec!["IN", "IN"])),
            Arc::new(StringArray::from(vec!["INR", "INR"])),
            Arc::new(StringArray::from(vec!["Economic", "IPO"])),
            Arc::new(StringArray::from(vec!["CPI YoY", "NewCo IPO"])),
            Arc::new(StringArray::from(vec!["High", "Medium"])),
            Arc::new(actual),
            Arc::new(forecast),
            Arc::new(prior),
            Arc::new(StringArray::from(vec![Some("Jan"), None::<&str>])),
        ],
    )
    .unwrap();

    let path = cal_dir.join("2026-11-06.parquet");
    let file = std::fs::File::create(&path).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let feed = Arc::new(FakeFeed);
    let reader = FeedMarketReader::new(
        feed.clone(),
        feed.clone(),
        feed.clone(),
        Arc::new(Mutex::new(RiskEngine::default())),
    )
    .with_economic_calendar(dir.path());

    let series = reader.macro_series("cpi").await.unwrap();
    assert_eq!(series.series, "cpi");
    assert_eq!(series.points.len(), 1);
    assert_eq!(
        series.points[0].1,
        Decimal::try_from_i128_with_scale(5_500_000, 4).unwrap()
    );
    assert_eq!(series.points[0].0.to_rfc3339(), "2026-01-05T06:00:00+00:00");
}

#[tokio::test]
async fn unwired_readers_still_answer_unavailable() {
    let feed = Arc::new(FakeFeed);
    let reader = FeedMarketReader::new(
        feed.clone(),
        feed.clone(),
        feed.clone(),
        Arc::new(Mutex::new(RiskEngine::default())),
    );

    let err = reader.institutional_flows(None).await.unwrap_err();
    assert_eq!(err.kind, PortErrorKind::Unavailable);
    assert!(
        err.message.contains("0007 item 11"),
        "unconfigured flows must cite the runtime gap item, got: {err}"
    );

    let err = reader
        .delivery_stats(
            &InstrumentId::new(Exchange::Nse, "RELIANCE-EQ"),
            DateRange::new(None, None),
            None,
        )
        .await
        .unwrap_err();
    assert_eq!(err.kind, PortErrorKind::Unavailable);
    assert!(
        err.message.contains("0007 item 12"),
        "unconfigured delivery must cite the runtime gap item, got: {err}"
    );

    let err = reader.macro_series("cpi").await.unwrap_err();
    assert_eq!(err.kind, PortErrorKind::Unavailable);
    assert!(
        err.message.contains("0007 item 3b"),
        "unconfigured macro series must cite the runtime gap item, got: {err}"
    );
}

#[tokio::test]
async fn fundamentals_read_through_projection() {
    let dir = tempdir().unwrap();
    let reliance = InstrumentId::new(Exchange::Nse, "RELIANCE-EQ");
    let epoch_days = |y: i32, m: u32, d: u32| -> i32 {
        (NaiveDate::from_ymd_opt(y, m, d).unwrap() - NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
            .num_days() as i32
    };

    // Fundamental-ratios fixture (one snapshot for RELIANCE).
    let ratios_dir = dir.path().join("catalog").join("fundamental_ratios");
    std::fs::create_dir_all(&ratios_dir).unwrap();
    let ratios_schema = Arc::new(Schema::new(vec![
        Field::new("as_of_date", DataType::Date32, false),
        Field::new("instrument", DataType::Utf8, false),
        Field::new("pe", DataType::Decimal128(18, 4), true),
        Field::new("pb", DataType::Decimal128(18, 4), true),
        Field::new("ev_to_ebitda", DataType::Decimal128(18, 4), true),
        Field::new("dividend_yield", DataType::Decimal128(18, 4), true),
        Field::new("roce", DataType::Decimal128(18, 4), true),
        Field::new("roe", DataType::Decimal128(18, 4), true),
        Field::new("debt_to_equity", DataType::Decimal128(18, 4), true),
        Field::new("interest_coverage", DataType::Decimal128(18, 4), true),
        Field::new("source", DataType::Utf8, false),
    ]));
    let pe = Decimal128Array::from(vec![Some(22_078_114i128)])
        .with_precision_and_scale(18, 4)
        .unwrap();
    let debt_equity = Decimal128Array::from(vec![Some(440_251i128)])
        .with_precision_and_scale(18, 4)
        .unwrap();
    let nulls = Decimal128Array::from(vec![None])
        .with_precision_and_scale(18, 4)
        .unwrap();
    let ratios_batch = RecordBatch::try_new(
        ratios_schema.clone(),
        vec![
            Arc::new(Date32Array::from(vec![epoch_days(2026, 11, 14)])),
            Arc::new(StringArray::from(vec!["NSE:RELIANCE-EQ"])),
            Arc::new(pe),
            Arc::new(nulls.clone()),
            Arc::new(nulls.clone()),
            Arc::new(nulls.clone()),
            Arc::new(nulls.clone()),
            Arc::new(nulls.clone()),
            Arc::new(debt_equity),
            Arc::new(nulls.clone()),
            Arc::new(StringArray::from(vec!["yfinance"])),
        ],
    )
    .unwrap();
    let ratios_file = std::fs::File::create(ratios_dir.join("2026-11-14.parquet")).unwrap();
    let mut ratios_writer = ArrowWriter::try_new(ratios_file, ratios_schema, None).unwrap();
    ratios_writer.write(&ratios_batch).unwrap();
    ratios_writer.close().unwrap();

    // Financial-statement fixture: three line items of the annual FY2026 income statement.
    let statements_dir = dir.path().join("catalog").join("fundamentals");
    std::fs::create_dir_all(&statements_dir).unwrap();
    let statements_schema = Arc::new(Schema::new(vec![
        Field::new("filing_date", DataType::Date32, false),
        Field::new("instrument", DataType::Utf8, false),
        Field::new("statement_type", DataType::Utf8, false),
        Field::new("period_type", DataType::Utf8, false),
        Field::new("period_end_date", DataType::Date32, false),
        Field::new("line_item", DataType::Utf8, false),
        Field::new("value", DataType::Decimal128(18, 4), false),
        Field::new("currency", DataType::Utf8, false),
    ]));
    let values = Decimal128Array::from(vec![
        1_057_219_000_000_000i128,
        80_775_000_000_000i128,
        425_000i128,
    ])
    .with_precision_and_scale(18, 4)
    .unwrap();
    let statements_batch = RecordBatch::try_new(
        statements_schema.clone(),
        vec![
            Arc::new(Date32Array::from(vec![epoch_days(2026, 11, 14); 3])),
            Arc::new(StringArray::from(vec![
                "NSE:RELIANCE-EQ",
                "NSE:RELIANCE-EQ",
                "NSE:RELIANCE-EQ",
            ])),
            Arc::new(StringArray::from(vec![
                "income_statement",
                "income_statement",
                "income_statement",
            ])),
            Arc::new(StringArray::from(vec!["annual", "annual", "annual"])),
            Arc::new(Date32Array::from(vec![epoch_days(2026, 3, 31); 3])),
            Arc::new(StringArray::from(vec![
                "total_revenue",
                "net_income",
                "diluted_eps",
            ])),
            Arc::new(values),
            Arc::new(StringArray::from(vec!["INR", "INR", "INR"])),
        ],
    )
    .unwrap();
    let statements_file = std::fs::File::create(statements_dir.join("2026-11-14.parquet")).unwrap();
    let mut statements_writer =
        ArrowWriter::try_new(statements_file, statements_schema, None).unwrap();
    statements_writer.write(&statements_batch).unwrap();
    statements_writer.close().unwrap();

    let reader = FeedFundamentalReader::new().with_fundamentals(dir.path());

    let ratios = reader.fundamental_ratios(&reliance).await.unwrap();
    assert_eq!(
        ratios.pe_ratio,
        Some(Decimal::try_from_i128_with_scale(22_078_114, 4).unwrap())
    );
    assert_eq!(
        ratios.debt_equity,
        Some(Decimal::try_from_i128_with_scale(440_251, 4).unwrap())
    );
    assert_eq!(ratios.pb_ratio, None);
    assert_eq!(
        ratios.as_of,
        Utc.with_ymd_and_hms(2026, 11, 14, 0, 0, 0).unwrap()
    );

    let statements = reader
        .financial_statements(&reliance, Some("income_statement"), None)
        .await
        .unwrap();
    assert_eq!(statements.len(), 1);
    assert_eq!(statements[0].statement_type, "income_statement");
    assert_eq!(statements[0].fiscal_quarter, "FY2026");
    assert_eq!(
        statements[0].revenue,
        Some(Money::new(
            Decimal::try_from_i128_with_scale(1_057_219_000_000_000, 4).unwrap(),
            Currency::Inr
        ))
    );
    assert_eq!(
        statements[0].net_income,
        Some(Money::new(
            Decimal::try_from_i128_with_scale(80_775_000_000_000, 4).unwrap(),
            Currency::Inr
        ))
    );
    assert_eq!(
        statements[0].eps,
        Some(Decimal::try_from_i128_with_scale(425_000, 4).unwrap())
    );

    // A different instrument with no projections answers not-found, never a fabricated empty.
    let tcs = InstrumentId::new(Exchange::Nse, "TCS");
    assert_eq!(
        reader.fundamental_ratios(&tcs).await.unwrap_err().kind,
        PortErrorKind::NotFound
    );
}
