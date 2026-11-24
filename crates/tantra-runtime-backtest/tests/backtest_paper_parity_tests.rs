//! R5 golden parity (plan 0005 D4 / T14.8): the backtest engine's L2
//! simulation now drives the shared `tantra-exec-paper` exchange as its
//! matching venue instead of the crate's inline `sim::l2_book` book.
//! Replaying one deterministic tape through (a) a `BacktestEngine` built
//! with `with_l2_simulation(...)` and (b) a
//! `tantra_runtime_strategy::PaperAdapter` (a thin driver over the same
//! paper exchange) must produce fills with identical venue economics.
//!
//! Normalization (plan 0005 R5): `fill_id`, `order_id`, `algo_id`,
//! `account_id`, and `fee` legitimately differ between the two engines —
//! the backtester mints `{client_order_id}-{seq}` fill ids and books
//! statutory fees through `tantra_ledger_journal::TaxCalculator`, while the
//! paper venue stamps `F000001`-style ids and charges its own cost model.
//! The venue economics — `(instrument, side, price, quantity, timestamp)` —
//! must match exactly.
//!
//! The tape also exercises the resting-order drain path that the inline
//! book never accounted: a non-marketable 990.00 buy limit rests in the
//! exchange and is later filled (at the resting limit price) when a market
//! sell crosses it.

use chrono::{DateTime, NaiveDate, NaiveTime, TimeZone, Utc};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::sync::Arc;
use tantra_core::{
    Exchange, FillReport, InstrumentId, OrderRequest, OrderSide, OrderType, Price, Quantity, Tick,
};
use tantra_ledger_journal::ProductType;
use tantra_risk::{OtrConfig, RiskEngine, RiskLimits};
use tantra_runtime_backtest::{
    BacktestConfig, BacktestEngine, MarketEvent, NseTradingCalendar, sim::latency::NetworkLatency,
    sim::mis::MisSquareOffManager, sim::slippage::SlippageModel,
};
use tantra_runtime_strategy::{
    BrokerAdapter, OrderIntent, PaperAdapter, Strategy, StrategyContext,
};

fn reliance() -> InstrumentId {
    InstrumentId::try_new(Exchange::Nse, "RELIANCE").unwrap()
}

fn infy() -> InstrumentId {
    InstrumentId::try_new(Exchange::Nse, "INFY").unwrap()
}

fn ts(h: u32, m: u32) -> DateTime<Utc> {
    let day = NaiveDate::from_ymd_opt(2025, 1, 15).unwrap();
    Utc.from_utc_datetime(&day.and_time(NaiveTime::from_hms_opt(h, m, 0).unwrap()))
}

fn ltp(inst: &InstrumentId, price: &str, volume: u64, t: DateTime<Utc>) -> Tick {
    Tick::Ltp {
        instrument: inst.clone(),
        price: Price::from_rupees(price).unwrap(),
        volume: Quantity::from_u64(volume),
        exchange_ts: t,
        receive_ts: t,
    }
}

/// The deterministic tape: two instruments, five ticks at fixed timestamps.
/// RELIANCE prints at 1000.00; INFY prints at 2000.00.
fn tape() -> Vec<MarketEvent> {
    vec![
        MarketEvent::Tick(ltp(&reliance(), "1000.00", 1_000_000, ts(9, 15))),
        MarketEvent::Tick(ltp(&infy(), "2000.00", 1_000_000, ts(9, 15))),
        MarketEvent::Tick(ltp(&reliance(), "1000.00", 500_000, ts(9, 20))),
        MarketEvent::Tick(ltp(&infy(), "2000.00", 800_000, ts(9, 25))),
        MarketEvent::Tick(ltp(&reliance(), "1000.00", 500_000, ts(9, 30))),
    ]
}

/// Strategy replicating the paper script: buys RELIANCE/INFY on their first
/// trade, rests a 20-share RELIANCE buy limit at 990.00 on the second
/// RELIANCE trade, then sells both positions on their respective later trades.
struct ParityTrader {
    reliance: InstrumentId,
    infy: InstrumentId,
    reliance_trades: usize,
    infy_trades: usize,
}

impl Strategy for ParityTrader {
    fn name(&self) -> &str {
        "r5-parity-trader"
    }

    fn on_trade(
        &mut self,
        ctx: &mut dyn StrategyContext,
        trade: &Tick,
    ) -> tantra_runtime_strategy::Result<()> {
        if *trade.instrument() == self.reliance {
            self.reliance_trades += 1;
            match self.reliance_trades {
                1 => ctx.submit(OrderIntent::market_buy(
                    self.reliance.clone(),
                    Quantity::from_u64(30),
                )),
                2 => ctx.submit(OrderIntent::limit_buy(
                    self.reliance.clone(),
                    Quantity::from_u64(20),
                    Price::from_rupees("990.00").unwrap(),
                )),
                3 => ctx.submit(OrderIntent::market_sell(
                    self.reliance.clone(),
                    Quantity::from_u64(20),
                )),
                _ => {}
            }
        } else {
            self.infy_trades += 1;
            match self.infy_trades {
                1 => ctx.submit(OrderIntent::market_buy(
                    self.infy.clone(),
                    Quantity::from_u64(40),
                )),
                2 => ctx.submit(OrderIntent::market_sell(
                    self.infy.clone(),
                    Quantity::from_u64(40),
                )),
                _ => {}
            }
        }
        Ok(())
    }
}

fn market_order(inst: InstrumentId, side: OrderSide, qty: u64) -> OrderRequest {
    OrderRequest::new(
        inst,
        side,
        OrderType::Market,
        Quantity::from_u64(qty),
        "r5-parity",
    )
}

/// Drives a `PaperAdapter` with the same orders at the same timestamps as
/// the strategy script and returns its fills.
fn paper_fills() -> Vec<FillReport> {
    let paper = PaperAdapter::new(dec!(1_000_000));
    paper.set_market_price(reliance(), dec!(1000.00));
    paper.set_market_price(infy(), dec!(2000.00));

    paper
        .submit_order(&market_order(reliance(), OrderSide::Buy, 30), ts(9, 15))
        .unwrap();
    paper
        .submit_order(&market_order(infy(), OrderSide::Buy, 40), ts(9, 15))
        .unwrap();
    paper
        .submit_order(
            &OrderRequest::new(
                reliance(),
                OrderSide::Buy,
                OrderType::Limit(Price::from_rupees("990.00").unwrap()),
                Quantity::from_u64(20),
                "r5-parity",
            ),
            ts(9, 20),
        )
        .unwrap();
    paper
        .submit_order(&market_order(infy(), OrderSide::Sell, 40), ts(9, 25))
        .unwrap();
    paper
        .submit_order(&market_order(reliance(), OrderSide::Sell, 20), ts(9, 30))
        .unwrap();

    paper.fills()
}

fn backtest_result() -> tantra_runtime_backtest::BacktestResult {
    let strategy = ParityTrader {
        reliance: reliance(),
        infy: infy(),
        reliance_trades: 0,
        infy_trades: 0,
    };
    let mut risk_engine = RiskEngine::new(RiskLimits::default(), OtrConfig::default());
    risk_engine.register_algo(strategy.name());

    let config = BacktestConfig::with_l2_simulation(
        dec!(1_000_000.00),
        ProductType::Delivery,
        NetworkLatency::new(0, 0, 7),
        SlippageModel::new(),
        MisSquareOffManager::new(Arc::new(NseTradingCalendar::new())),
    );
    let mut engine = BacktestEngine::new(
        strategy,
        risk_engine,
        config,
        Arc::new(NseTradingCalendar::new()),
    );
    engine.run_events(tape())
}

/// The venue economics of a fill, normalizing the ids/fees that
/// legitimately differ between the backtester and the paper venue
/// (plan 0005 R5).
#[derive(Debug, PartialEq)]
struct EconomicFill {
    instrument: InstrumentId,
    side: OrderSide,
    price: Decimal,
    quantity: Decimal,
    timestamp: DateTime<Utc>,
}

fn economic(fill: &FillReport) -> EconomicFill {
    EconomicFill {
        instrument: fill.instrument.clone(),
        side: fill.side,
        price: fill.price.value(),
        quantity: fill.quantity.value(),
        timestamp: fill.timestamp,
    }
}

/// Asserts both engines' fills are the same multiset of venue economics.
fn assert_same_economics(a: &[FillReport], b: &[FillReport]) {
    let ae: Vec<EconomicFill> = a.iter().map(economic).collect();
    let be: Vec<EconomicFill> = b.iter().map(economic).collect();
    assert_eq!(ae.len(), be.len());
    for fill in &ae {
        let in_a = ae.iter().filter(|f| *f == fill).count();
        let in_b = be.iter().filter(|f| *f == fill).count();
        assert_eq!(
            in_a, in_b,
            "economic fill mismatch: {fill:?} (backtest {in_a}x vs paper {in_b}x)"
        );
    }
}

#[test]
fn backtest_l2_fills_match_paper_adapter_on_shared_tape() {
    let result = backtest_result();

    // The L2 backtest must actually produce fills, at tape prices.
    assert!(!result.fills.is_empty(), "L2 backtest must produce fills");
    assert_eq!(result.fills.len(), 5);

    // RELIANCE: market buy 30 @ 1000.00 (tape price) then, when the market
    // sell crosses it, the resting 990.00 buy limit fills at 990.00 and the
    // seller receives the price-improved 990.00.
    let reliance_buys: Vec<_> = result
        .fills
        .iter()
        .filter(|f| f.instrument == reliance() && f.side == OrderSide::Buy)
        .collect();
    assert_eq!(reliance_buys.len(), 2);
    assert_eq!(reliance_buys[0].price.value(), dec!(1000.00));
    assert_eq!(reliance_buys[0].quantity.value(), dec!(30.00));
    assert_eq!(reliance_buys[1].price.value(), dec!(990.00));
    assert_eq!(reliance_buys[1].quantity.value(), dec!(20.00));

    let reliance_sell = result
        .fills
        .iter()
        .find(|f| f.instrument == reliance() && f.side == OrderSide::Sell)
        .expect("RELIANCE sell fill");
    assert_eq!(reliance_sell.price.value(), dec!(990.00));
    assert_eq!(reliance_sell.quantity.value(), dec!(20.00));

    // INFY: market buy 40 / sell 40 @ 2000.00 (tape price).
    let infy_sell = result
        .fills
        .iter()
        .find(|f| f.instrument == infy() && f.side == OrderSide::Sell)
        .expect("INFY sell fill");
    assert_eq!(infy_sell.price.value(), dec!(2000.00));
    assert_eq!(infy_sell.quantity.value(), dec!(40.00));

    // The paper adapter driven on the same tape books identical economics.
    let paper = paper_fills();
    assert_same_economics(&result.fills, &paper);
}
