//! E9 cross-repo parity: the paper exchange (`tantra-exec-paper`) and the
//! backtest L2 book (`tantra_runtime_backtest::sim::l2_book`) are byte-identical
//! copies by construction (exec plan 0003 slice E9, root plan 0007; runtime plan
//! 0005 D4). Driving both from the same golden tape must produce byte-identical
//! fills — at the book level (price-time priority, limit walk) and end-to-end
//! through the `PaperExchange` (fills, order states, deterministic events).
//!
//! Deviation from the plan spec (documented, engine behavior verified below):
//! the paper engine emits one `FillReport` per side per participant fill, so the
//! golden tape yields **4** fill records (sell ×2 + buy ×2), the net position is
//! **0** (the sell nets the resting buys), and cash returns to `1_000_000` with
//! `ZeroFillCost`. The spec's "3 fills / +150 / −15100" reads were buy-side-only
//! projections; the fills themselves match the book exactly.

use chrono::{TimeZone, Utc};
use rust_decimal_macros::dec;
use tantra_core::{
    DepthLevel, Exchange, InstrumentId, OrderRequest, OrderSide, OrderType, Price, Quantity,
};
use tantra_exec_paper::book::OrderBook as PaperBook;
use tantra_exec_paper::{OrderState, PaperExchange, ZeroFillCost};
use tantra_runtime_backtest::sim::l2_book::OrderBook as BacktestBook;
use uuid::Uuid;

const TS0: i64 = 1_700_000_000;

fn reliance() -> InstrumentId {
    InstrumentId::new(Exchange::Nse, "RELIANCE")
}

/// The shared golden tape: two resting buy limits (100@101, 100@100) then a
/// 150-lot market sell. Returns the exchange plus the broker order ids.
fn run_golden_tape() -> (PaperExchange, String, String, String) {
    let inst = reliance();
    let ts0 = Utc.timestamp_opt(TS0, 0).unwrap();
    let mut ex = PaperExchange::new(dec!(1_000_000)).with_cost_fn(Box::new(ZeroFillCost));
    ex.set_market_price(inst.clone(), Price::from_rupees("100").unwrap());

    let buy101 = OrderRequest::with_id(
        Uuid::from_u128(1),
        inst.clone(),
        OrderSide::Buy,
        OrderType::Limit(Price::from_rupees("101").unwrap()),
        Quantity::from_u64(100),
        "paper_parity",
    );
    let buy100 = OrderRequest::with_id(
        Uuid::from_u128(2),
        inst.clone(),
        OrderSide::Buy,
        OrderType::Limit(Price::from_rupees("100").unwrap()),
        Quantity::from_u64(100),
        "paper_parity",
    );
    let sell = OrderRequest::with_id(
        Uuid::from_u128(3),
        inst,
        OrderSide::Sell,
        OrderType::Market,
        Quantity::from_u64(150),
        "paper_parity",
    );

    let id_buy101 = ex.submit_order(buy101, ts0).unwrap();
    let id_buy100 = ex.submit_order(buy100, ts0).unwrap();
    let id_sell = ex.submit_order(sell, ts0).unwrap();
    (ex, id_buy101, id_buy100, id_sell)
}

/// Book-level parity: sell 60 against bids 100×40 + 99×20. Both books must
/// return the identical `(filled, vwap, remaining, worst)` tuple. VWAP
/// `(40*100 + 20*99)/60 = 99.6667` (rounded to 4dp inside the book).
#[test]
fn book_parity_price_time_priority() {
    let mut backtest = BacktestBook::new(0, dec!(1));
    let mut paper = PaperBook::new(0, dec!(1));
    backtest.add_bid(Price::from_rupees("100").unwrap(), 40);
    backtest.add_bid(Price::from_rupees("99").unwrap(), 20);
    paper.add_bid(Price::from_rupees("100").unwrap(), 40);
    paper.add_bid(Price::from_rupees("99").unwrap(), 20);

    let bt = backtest.execute_market_order(OrderSide::Sell, 60);
    let pp = paper.execute_market_order(OrderSide::Sell, 60);

    // Byte-identical tuple: (filled, vwap, remaining, worst).
    assert_eq!(
        (bt.0, bt.1.0, bt.2, bt.3.map(|p| p.0)),
        (pp.0, pp.1.0, pp.2, pp.3.map(|p| p.0))
    );
    assert_eq!(bt.0, 60);
    assert_eq!(bt.1.0, dec!(99.6667));
    assert_eq!(bt.2, 0);
    assert_eq!(bt.3.map(|p| p.0), Some(dec!(99)));
}

/// Book-level parity: limit buy 50 @ 101 walking two ask levels (25@100 +
/// 25@101). Both books return `(50, 100.5, 0)`.
#[test]
fn book_parity_limit_walk() {
    let asks = [
        DepthLevel {
            price: Price::from_rupees("100").unwrap(),
            quantity: Quantity::from_u64(25),
            orders_count: None,
        },
        DepthLevel {
            price: Price::from_rupees("101").unwrap(),
            quantity: Quantity::from_u64(25),
            orders_count: None,
        },
    ];
    let empty: &[DepthLevel] = &[];

    let mut backtest = BacktestBook::new(0, dec!(1));
    let mut paper = PaperBook::new(0, dec!(1));
    backtest.set_depth(empty, &asks);
    paper.set_depth(empty, &asks);

    let limit = Price::from_rupees("101").unwrap();
    let bt = backtest.execute_limit_order(OrderSide::Buy, 50, limit);
    let pp = paper.execute_limit_order(OrderSide::Buy, 50, limit);

    // Byte-identical tuple: (filled, vwap, remaining).
    assert_eq!((bt.0, bt.1.0, bt.2), (pp.0, pp.1.0, pp.2));
    assert_eq!(bt.0, 50);
    assert_eq!(bt.1.0, dec!(100.5));
    assert_eq!(bt.2, 0);
}

/// End-to-end: the golden tape through `PaperExchange` produces fills that
/// match the same tape through the backtest L2 book, with the expected
/// price-time-priority economics (100@101 + 50@100; sell VWAP 100.6667).
#[test]
fn paper_exchange_fills_match_golden_tape() {
    let (ex, id_buy101, id_buy100, id_sell) = run_golden_tape();

    // Fill records: one per side per participant fill -> 4 (sell 100@101,
    // buy@101 100@101, sell 50@100, buy@100 50@100).
    let fills = ex.fills();
    assert_eq!(fills.len(), 4);

    let sell_fills: Vec<_> = fills
        .iter()
        .filter(|f| f.client_order_id == Uuid::from_u128(3))
        .collect();
    assert_eq!(sell_fills.len(), 2);
    assert_eq!(sell_fills[0].price.0, dec!(101));
    assert_eq!(sell_fills[0].quantity.0, dec!(100));
    assert_eq!(sell_fills[1].price.0, dec!(100));
    assert_eq!(sell_fills[1].quantity.0, dec!(50));

    let buy101_fills: Vec<_> = fills
        .iter()
        .filter(|f| f.client_order_id == Uuid::from_u128(1))
        .collect();
    assert_eq!(buy101_fills.len(), 1);
    assert_eq!(buy101_fills[0].price.0, dec!(101));
    assert_eq!(buy101_fills[0].quantity.0, dec!(100));

    // Order lifecycle states.
    assert_eq!(ex.order(&id_buy101).unwrap().state, OrderState::Filled);
    assert_eq!(
        ex.order(&id_buy100).unwrap().state,
        OrderState::PartiallyFilled
    );
    let sell = ex.order(&id_sell).unwrap();
    assert_eq!(sell.state, OrderState::Filled);
    assert_eq!(sell.filled_qty.0, dec!(150));
    assert_eq!(sell.avg_fill_price.0, dec!(100.6667));

    // Buy side accumulated 150 gross (100@101 + 50@100)...
    let buy_qty: rust_decimal::Decimal = fills
        .iter()
        .filter(|f| f.side == OrderSide::Buy)
        .map(|f| f.quantity.0)
        .sum();
    assert_eq!(buy_qty, dec!(150));

    // ...but the sell nets them: net position 0, cash back to the initial
    // balance (zero fees). Deviation from the plan spec's "+150 / −15100"
    // (documented at the top of this file).
    assert_eq!(ex.positions().get(&reliance()), Some(&dec!(0)));
    assert_eq!(ex.cash(), dec!(1_000_000));

    // E9 parity: the same tape through the backtest L2 book yields the same
    // sell economics (fills 100@101 + 50@100, VWAP 100.6667, worst 100).
    let mut book = BacktestBook::new(0, dec!(1));
    book.add_bid(Price::from_rupees("101").unwrap(), 100);
    book.add_bid(Price::from_rupees("100").unwrap(), 100);
    let (filled, vwap, remaining, worst) = book.execute_market_order(OrderSide::Sell, 150);
    assert_eq!(filled, 150);
    assert_eq!(vwap.0, dec!(100.6667));
    assert_eq!(remaining, 0);
    assert_eq!(worst.map(|p| p.0), Some(dec!(100)));
    assert_eq!(sell.avg_fill_price.0, vwap.0);
}

/// Determinism: replaying the golden tape through two fresh exchanges produces
/// byte-identical `events()` and `fills()` streams (`ExchangeEvent` and
/// `FillReport` both derive `PartialEq`).
#[test]
fn paper_exchange_events_deterministic_golden() {
    let (a, ..) = run_golden_tape();
    let (b, ..) = run_golden_tape();

    assert_eq!(a.events(), b.events());
    assert_eq!(a.fills(), b.fills());

    // Golden event stream shape: 3 OrderAccepted + sell PartialFill + buy@101
    // Fill + sell Fill + buy@100 PartialFill = 7 events; 4 fill records.
    assert_eq!(a.events().len(), 7);
    assert_eq!(a.fills().len(), 4);
}
