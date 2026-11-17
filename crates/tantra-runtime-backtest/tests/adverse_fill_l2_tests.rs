//! L2 adverse-fill restore (plan 0005 D4 follow-on): with `l2_simulation` enabled, the
//! configured `sim::slippage::AdverseFillModel` is threaded into the shared
//! `tantra-exec-paper` exchange venue, so a market order that walks the book fills at a
//! WORSE price than the best touch. Without the model, the same tape fills at the best touch
//! (unchanged behavior — the R5 golden-parity and E9 parity suites run with the default
//! `adverse_fill: None` and must stay green).

use std::sync::Arc;

use chrono::{DateTime, NaiveDate, NaiveTime, TimeZone, Utc};
use rust_decimal_macros::dec;
use tantra_core::{Exchange, InstrumentId, OrderSide, Price, Quantity, Tick};
use tantra_ledger_journal::ProductType;
use tantra_risk::{OtrConfig, RiskEngine, RiskLimits};
use tantra_runtime_backtest::{
    BacktestConfig, BacktestEngine, MarketEvent, NseTradingCalendar,
    sim::latency::NetworkLatency,
    sim::mis::MisSquareOffManager,
    sim::slippage::{AdverseFillModel, SlippageModel},
};
use tantra_runtime_strategy::{OrderIntent, Strategy, StrategyContext};

fn reliance() -> InstrumentId {
    InstrumentId::new(Exchange::Nse, "RELIANCE")
}

fn ts(h: u32, m: u32) -> DateTime<Utc> {
    let day = NaiveDate::from_ymd_opt(2025, 1, 15).unwrap();
    Utc.from_utc_datetime(&day.and_time(NaiveTime::from_hms_opt(h, m, 0).unwrap()))
}

/// One LTP print at 1000.00 with a 1000-share level: a 30-share market buy's adverse-fill
/// round moves the fill by 30/1000 = 0.03, visible at the exchange's 4-dp prices.
fn print_reliance(volume: u64, t: DateTime<Utc>) -> Tick {
    Tick::Ltp {
        instrument: reliance(),
        price: Price::from_rupees("1000.00").unwrap(),
        volume: Quantity::from_u64(volume),
        exchange_ts: t,
        receive_ts: t,
    }
}

/// Buys 30 shares on the first RELIANCE trade, then does nothing.
struct AdverseBuyer {
    reliance: InstrumentId,
    trades: usize,
}

impl Strategy for AdverseBuyer {
    fn name(&self) -> &str {
        "adverse-l2-buyer"
    }

    fn on_trade(
        &mut self,
        ctx: &mut dyn StrategyContext,
        trade: &Tick,
    ) -> tantra_runtime_strategy::Result<()> {
        if *trade.instrument() == self.reliance {
            self.trades += 1;
            if self.trades == 1 {
                ctx.submit(OrderIntent::market_buy(
                    self.reliance.clone(),
                    Quantity::from_u64(30),
                ));
            }
        }
        Ok(())
    }
}

fn l2_config() -> BacktestConfig {
    BacktestConfig::with_l2_simulation(
        dec!(1_000_000.00),
        ProductType::Delivery,
        NetworkLatency::new(0, 0, 7),
        SlippageModel::new(),
        MisSquareOffManager::new(Arc::new(NseTradingCalendar::new())),
    )
}

fn run_with(adverse: bool) -> tantra_runtime_backtest::BacktestResult {
    let strategy = AdverseBuyer {
        reliance: reliance(),
        trades: 0,
    };
    let mut risk_engine = RiskEngine::new(RiskLimits::default(), OtrConfig::default());
    risk_engine.register_algo(strategy.name());

    let config = if adverse {
        l2_config().with_adverse_fill(AdverseFillModel::new(1, dec!(5.0), false))
    } else {
        l2_config()
    };
    let mut engine = BacktestEngine::new(
        strategy,
        risk_engine,
        config,
        Arc::new(NseTradingCalendar::new()),
    );
    let tape = vec![
        MarketEvent::Tick(print_reliance(1000, ts(9, 15))),
        MarketEvent::Tick(print_reliance(500, ts(9, 20))),
    ];
    engine.run_events(tape)
}

#[test]
fn l2_market_buy_fills_worse_than_best_touch_with_adverse_model() {
    let result = run_with(true);
    let buy = result
        .fills
        .iter()
        .find(|f| f.instrument == reliance() && f.side == OrderSide::Buy)
        .expect("RELIANCE buy fill");
    assert_eq!(buy.quantity.0, dec!(30.00));
    assert!(
        buy.price.0 > dec!(1000.00),
        "adverse-fill L2 buy should fill worse than the 1000.00 touch, got {}",
        buy.price.0
    );
}

#[test]
fn l2_market_buy_fills_at_best_touch_without_adverse_model() {
    let result = run_with(false);
    let buy = result
        .fills
        .iter()
        .find(|f| f.instrument == reliance() && f.side == OrderSide::Buy)
        .expect("RELIANCE buy fill");
    assert_eq!(buy.quantity.0, dec!(30.00));
    assert_eq!(buy.price.0, dec!(1000.00));
}
