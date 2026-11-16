//! Tests that `OrderBook::check_participation_cap` is actually wired into
//! execution instead of being dead code: a market order whose quantity
//! exceeds `avg_daily_volume * participation_cap / 2` is clamped, filled
//! only up to the cap, with the unfilled remainder left open (not rejected
//! outright — more realistic for a market order).

use chrono::{DateTime, NaiveDate, NaiveTime, TimeZone, Utc};
use rust_decimal_macros::dec;

use std::sync::Arc;
use tantra_core::{Bar, Exchange, InstrumentId, Price, Quantity};
use tantra_ledger_journal::ProductType;
use tantra_risk::{OtrConfig, RiskEngine, RiskLimits};
use tantra_runtime_backtest::NseTradingCalendar;
use tantra_runtime_backtest::sim::latency::NetworkLatency;
use tantra_runtime_backtest::sim::mis::MisSquareOffManager;
use tantra_runtime_backtest::sim::slippage::SlippageModel;
use tantra_runtime_backtest::{BacktestConfig, BacktestEngine, MarketEvent};
use tantra_runtime_strategy::{OrderIntent, Strategy, StrategyContext};

fn bar(inst: &InstrumentId, close: &str, volume: u64, ts: DateTime<Utc>) -> Bar {
    let price = Price::from_rupees(close).unwrap();
    Bar {
        instrument: inst.clone(),
        open: price,
        high: price,
        low: price,
        close: price,
        volume: Quantity::from_u64(volume),
        timestamp: ts,
    }
}

/// Buys a fixed quantity once, on the first bar it sees.
struct OnceBuyer {
    instrument: InstrumentId,
    quantity: Quantity,
    bought: bool,
}

impl Strategy for OnceBuyer {
    fn name(&self) -> &str {
        "once_buyer"
    }

    fn on_bar(
        &mut self,
        ctx: &mut dyn StrategyContext,
        _bar: &Bar,
    ) -> tantra_runtime_strategy::Result<()> {
        if !self.bought {
            ctx.submit(OrderIntent::market_buy(
                self.instrument.clone(),
                self.quantity,
            ));
            self.bought = true;
        }
        Ok(())
    }
}

fn run_once(
    instrument: &InstrumentId,
    order_qty: u64,
    bar_volume: u64,
) -> tantra_runtime_backtest::BacktestResult {
    let strategy = OnceBuyer {
        instrument: instrument.clone(),
        quantity: Quantity::from_u64(order_qty),
        bought: false,
    };
    let mut risk_engine = RiskEngine::new(RiskLimits::default(), OtrConfig::default());
    risk_engine.register_algo(strategy.name());

    // plan 0005 D4 / T14.8: the L2 matching venue is the shared
    // tantra-exec-paper exchange, whose feed books are built with a 100%
    // participation cap (see its `feed_bar`/`feed_tick`), so a single
    // market order is clamped to `avg_daily_volume * 1.0 / 2` = half the
    // average daily volume. The slippage model's daily-range percentage is
    // no longer threaded into the book.
    let mut slippage = SlippageModel::new();
    slippage.set_daily_range_pct(dec!(0.5));

    let config = BacktestConfig::with_l2_simulation(
        dec!(1_000_000.00),
        ProductType::Delivery,
        NetworkLatency::new(0, 0, 1),
        slippage,
        MisSquareOffManager::new(Arc::new(NseTradingCalendar::new())),
    );

    let mut engine = BacktestEngine::new(
        strategy,
        risk_engine,
        config,
        Arc::new(NseTradingCalendar::new()),
    );

    let day = NaiveDate::from_ymd_opt(2025, 1, 15).unwrap();
    let t0 = Utc.from_utc_datetime(&day.and_time(NaiveTime::from_hms_opt(9, 20, 0).unwrap()));

    engine.run_events(vec![MarketEvent::Bar(bar(
        instrument, "1500.00", bar_volume, t0,
    ))])
}

#[test]
fn test_market_order_clamped_to_participation_cap() {
    let inst = InstrumentId::new(Exchange::Nse, "INFY");
    // avg_daily_volume = 1_000, exchange cap = 1_000 * 1.0 / 2 = 500.
    // Order for 1_200 must clamp to 500 (the unfilled remainder rests in
    // the exchange until a later event crosses it).
    let result = run_once(&inst, 1_200, 1_000);

    assert_eq!(result.total_trades, 1);
    assert_eq!(result.fills.len(), 1);
    assert_eq!(result.fills[0].quantity, Quantity::from_u64(500));
}

#[test]
fn test_order_within_cap_fills_fully() {
    let inst = InstrumentId::new(Exchange::Nse, "INFY");
    // Same cap of 500, but the order (100) is within it: fills fully.
    let result = run_once(&inst, 100, 1_000);

    assert_eq!(result.total_trades, 1);
    assert_eq!(result.fills.len(), 1);
    assert_eq!(result.fills[0].quantity, Quantity::from_u64(100));
}
