//! `SharedLedger` tests (docs/gaps/0006 design decision + docs/gaps/0007 item 19): the
//! process-scoped ledger repositories are populated from real records/fills and read back
//! through the four repository trait objects, and closed (matched-away) lots accumulate into
//! the tax/statement history.

use std::sync::Arc;

use chrono::Utc;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use tantra_agent_chat_tools::ports::portfolio::PortfolioReader;
use tantra_core::{
    AccountId, AlgoId, ClientOrderId, Exchange, FillReport, InstrumentId, Money, OrderSide,
    OrderStatus, OrderType, PositionSide, Price, Quantity, TimeInForce,
};
use tantra_ledger::{
    AccountRecord, AccountRepository, LedgerRepository, OrderRecord, OrderRepository,
    PositionRepository,
};
use tantra_runtime_api::{LedgerPortfolioReader, SharedLedger};

fn acct() -> AccountId {
    AccountId::from("rest-api")
}

fn inst() -> InstrumentId {
    InstrumentId::new(Exchange::Nse, "RELIANCE")
}

fn account_record() -> AccountRecord {
    AccountRecord {
        account_id: acct(),
        algo_id: Some(AlgoId::from("rest-api")),
        cash: Money::inr(dec!(100000)),
        status: "active".to_string(),
        updated_at: Utc::now(),
    }
}

fn order_record(id: &str) -> OrderRecord {
    OrderRecord {
        client_order_id: ClientOrderId::from(id.to_string()),
        broker_order_id: None,
        algo_id: Some(AlgoId::from("rest-api")),
        account_id: acct(),
        instrument: inst(),
        side: OrderSide::Buy,
        order_type: OrderType::Market,
        time_in_force: TimeInForce::Day,
        price: None,
        quantity: Quantity::new(dec!(25)),
        filled_quantity: Quantity::new(dec!(0)),
        avg_fill_price: None,
        status: OrderStatus::PendingNew,
        created_at: Utc::now(),
        updated_at: Utc::now(),
    }
}

fn fill(order_id: &str, side: OrderSide, price: i64, qty: i64) -> FillReport {
    FillReport::new(
        format!("{order_id}-fill"),
        order_id,
        uuid::Uuid::new_v4(),
        inst(),
        side,
        Price::new(Decimal::from(price)),
        Quantity::new(Decimal::from(qty)),
        "rest-api",
        Utc::now(),
        "rest-api",
    )
}

#[test]
fn starts_empty_and_reads_back_recorded_accounts_and_orders() {
    let ledger = SharedLedger::new();
    let accounts: Arc<dyn AccountRepository> = Arc::new(ledger.clone());
    assert!(accounts.list_accounts().unwrap().is_empty());

    ledger.record_account(account_record());
    let accounts: Arc<dyn AccountRepository> = Arc::new(ledger.clone());
    let list = accounts.list_accounts().unwrap();
    assert_eq!(list.len(), 1);
    assert_eq!(list[0].account_id, acct());
    assert_eq!(list[0].cash, Money::inr(dec!(100000)));

    ledger.record_order(order_record("o1"));
    let orders: Arc<dyn OrderRepository> = Arc::new(ledger.clone());
    let active = orders
        .list_active_orders(&AlgoId::from("rest-api"))
        .unwrap();
    assert_eq!(active.len(), 1);
    assert_eq!(
        active[0].client_order_id,
        ClientOrderId::from("o1".to_string())
    );
}

#[test]
fn buy_fill_opens_a_position_and_debits_cash() {
    let ledger = SharedLedger::new();
    ledger.record_account(account_record());
    ledger.record_order(order_record("o1"));
    ledger.record_fill(&fill("o1", OrderSide::Buy, 101, 25));

    // The position read-through (the same view `LedgerPortfolioReader` serves) shows the
    // opened FIFO lot and the cash reflects the debited notional (25 × 101 = 2525).
    let positions: Arc<dyn PositionRepository> = Arc::new(ledger.clone());
    let pos = positions.get_positions(&acct()).unwrap();
    assert_eq!(pos.len(), 1);
    assert_eq!(pos[0].side, PositionSide::Long);
    assert_eq!(pos[0].quantity, Quantity::new(dec!(25)));
    assert_eq!(pos[0].avg_price, Price::new(dec!(101)));
    assert_eq!(pos[0].lots.len(), 1);

    assert_eq!(
        ledger.account(&acct()).unwrap().cash,
        Money::inr(dec!(97475))
    );
    assert_eq!(ledger.open_lots(&acct()).len(), 1);
    assert!(ledger.closed_lots(&acct()).is_empty());
}

#[test]
fn sell_fill_matches_fifo_and_accumulates_closed_lots() {
    let ledger = SharedLedger::new();
    ledger.record_account(account_record());
    // Two buys at 100 and 110 (FIFO), then a sell of 10.
    ledger.record_fill(&fill("b1", OrderSide::Buy, 100, 25));
    ledger.record_fill(&fill("b2", OrderSide::Buy, 110, 25));
    ledger.record_fill(&fill("s1", OrderSide::Sell, 120, 10));

    let closed = ledger.closed_lots(&acct());
    assert_eq!(closed.len(), 1);
    // FIFO: the oldest lot (entry 100) matched first, realized (120 - 100) × 10 = 200.
    assert_eq!(closed[0].entry_price, Price::new(dec!(100)));
    assert_eq!(closed[0].exit_price, Price::new(dec!(120)));
    assert_eq!(closed[0].realized_pnl, Money::inr(dec!(200)));
    assert_eq!(closed[0].matched_quantity, Quantity::new(dec!(10)));

    // Cash: 100000 - 25×100 - 25×110 + 10×120 = 100000 - 2500 - 2750 + 1200 = 95950.
    assert_eq!(
        ledger.account(&acct()).unwrap().cash,
        Money::inr(dec!(95950))
    );

    // The remaining open lots: 15 of the first lot + 25 of the second.
    let positions: Arc<dyn PositionRepository> = Arc::new(ledger.clone());
    let pos = positions.get_positions(&acct()).unwrap();
    assert_eq!(pos[0].quantity, Quantity::new(dec!(40)));
}

#[test]
fn update_order_fill_advances_the_order_row() {
    let ledger = SharedLedger::new();
    ledger.record_account(account_record());
    ledger.record_order(order_record("o1"));
    ledger.update_order_fill(
        "o1",
        Quantity::new(dec!(25)),
        Price::new(dec!(101)),
        OrderStatus::Filled,
        Utc::now(),
    );
    let orders: Arc<dyn OrderRepository> = Arc::new(ledger.clone());
    let row = orders
        .get_order(&ClientOrderId::from("o1".to_string()))
        .unwrap()
        .unwrap();
    assert_eq!(row.status, OrderStatus::Filled);
    assert_eq!(row.filled_quantity, Quantity::new(dec!(25)));
    assert_eq!(row.avg_fill_price, Some(Price::new(dec!(101))));
}

#[tokio::test]
async fn ledger_portfolio_reader_answers_real_state() {
    let ledger = SharedLedger::new();
    ledger.record_account(account_record());
    ledger.record_order(order_record("o1"));
    ledger.record_fill(&fill("o1", OrderSide::Buy, 101, 25));

    let reader = LedgerPortfolioReader::new(
        Arc::new(ledger.clone()),
        Arc::new(ledger.clone()),
        Arc::new(ledger.clone()),
        Arc::new(ledger.clone()),
    );

    let accounts = reader.accounts().await.unwrap();
    assert_eq!(accounts.len(), 1);
    assert_eq!(accounts[0].account_id, acct());
    assert_eq!(accounts[0].cash, Money::inr(dec!(97475)));

    let positions = reader.positions(&acct()).await.unwrap();
    assert_eq!(positions.len(), 1);
    assert_eq!(positions[0].quantity, Quantity::new(dec!(25)));

    let cash = reader.cash(&acct()).await.unwrap();
    assert_eq!(cash, vec![Money::inr(dec!(97475))]);

    let orders = reader.orders(&acct(), Default::default()).await.unwrap();
    assert_eq!(orders.len(), 1);

    let pnl = reader
        .realized_pnl(&acct(), Default::default())
        .await
        .unwrap();
    assert_eq!(pnl, Money::inr(dec!(0)));
}

#[test]
fn ledger_repository_posts_and_reads_balances() {
    use tantra_ledger::journal::AccountCode;
    use tantra_ledger::{JournalTransaction, PostingEntry, TransactionId};

    let mut ledger = SharedLedger::new();
    let tx = JournalTransaction::new(
        TransactionId::from("t1"),
        Utc::now(),
        "test",
        vec![PostingEntry {
            account: AccountCode::new(AccountCode::CASH_AT_BROKER),
            amount: Money::inr(dec!(5000)),
        }],
        vec![PostingEntry {
            account: AccountCode::new(AccountCode::HOLDINGS_EQUITY_DELIVERY),
            amount: Money::inr(dec!(5000)),
        }],
    )
    .unwrap();
    ledger.post_entry(&tx).unwrap();
    let repo: Arc<dyn LedgerRepository> = Arc::new(ledger);
    assert_eq!(
        repo.get_balance(&AccountCode::new(AccountCode::CASH_AT_BROKER))
            .unwrap(),
        Money::inr(dec!(5000))
    );
}
