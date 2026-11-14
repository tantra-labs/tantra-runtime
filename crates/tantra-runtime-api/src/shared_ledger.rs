//! Shared process-scoped ledger repositories (docs/gaps/0006 design decision).
//!
//! [`SharedLedger`] is the runtime's ledger-repository handle: the four in-memory
//! `tantra-ledger` repository implementations ([`InMemoryAccountRepository`],
//! [`InMemoryPositionRepository`], [`InMemoryOrderRepository`],
//! [`InMemoryLedgerRepository`]) plus a FIFO matched-lot history, wrapped in one
//! thread-safe, cloneable handle that `AppState` holds.
//!
//! The repository trait objects the chat adapters read through
//! ([`LedgerPortfolioReader::new`]) are built from clones of the same [`Arc`], so
//! `accounts`/`positions`/`orders`/`ledger` always agree with each other and with
//! whatever the runtime has recorded through [`SharedLedger::record_order`] /
//! [`SharedLedger::record_fill`].
//!
//! Honesty contract (docs/gaps/0006): the handle starts empty and is only ever
//! populated from the real order/fill flow, so an empty `accounts()`/`positions()`
//! answer genuinely means "no trading activity in this process" — never a
//! fabricated zero.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tantra_core::{
    AccountId, AlgoId, ClientOrderId, FillReport, InstrumentId, Money, OrderSide, PositionSide,
};
use tantra_ledger::{
    AccountCode, AccountRecord, AccountRepository, DomainError, InMemoryAccountRepository,
    InMemoryLedgerRepository, InMemoryOrderRepository, InMemoryPositionRepository,
    JournalTransaction, LedgerRepository, MatchedLot, OrderRecord, OrderRepository, PositionRecord,
    PositionRepository, TaxLot, TransactionId,
};

/// One fully closed (matched-away) tax lot as recorded by [`SharedLedger::record_fill`],
/// carrying the instrument and match timestamp so tax/statement tools can filter by account and
/// date range (docs/gaps/0007 item 19).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClosedLotEntry {
    /// Instrument the lot was traded in.
    pub instrument: InstrumentId,
    /// Tax-lot identifier.
    pub lot_id: String,
    /// Quantity matched away.
    pub matched_quantity: tantra_core::Quantity,
    /// Entry (cost-basis) price per unit.
    pub entry_price: tantra_core::Price,
    /// Exit price per unit.
    pub exit_price: tantra_core::Price,
    /// Realized P&L of the matched quantity.
    pub realized_pnl: Money,
    /// Holding period in days.
    pub holding_period_days: i64,
    /// True when the holding qualifies as long-term (equity: > 12 months).
    pub is_long_term: bool,
    /// Fill timestamp that matched the lot away.
    pub matched_at: DateTime<Utc>,
}

/// Thread-safe shared handle over the four in-memory ledger repositories plus the
/// matched-lot history the runtime maintains. Every mutation takes an internal lock, so
/// `record_*` methods are `&self` — shared through [`Arc`] clones by `AppState`, the order
/// flow, and the chat adapters (docs/gaps/0006 design decision).
#[derive(Clone)]
pub struct SharedLedger {
    accounts: Arc<Mutex<InMemoryAccountRepository>>,
    positions: Arc<Mutex<InMemoryPositionRepository>>,
    orders: Arc<Mutex<InMemoryOrderRepository>>,
    ledger: Arc<Mutex<InMemoryLedgerRepository>>,
    closed_lots: Arc<Mutex<HashMap<AccountId, Vec<ClosedLotEntry>>>>,
}

impl Default for SharedLedger {
    fn default() -> Self {
        Self::new()
    }
}

impl SharedLedger {
    /// A fresh, empty shared ledger (no accounts, positions, orders, or postings).
    pub fn new() -> Self {
        Self {
            accounts: Arc::new(Mutex::new(InMemoryAccountRepository::new())),
            positions: Arc::new(Mutex::new(InMemoryPositionRepository::new())),
            orders: Arc::new(Mutex::new(InMemoryOrderRepository::new())),
            ledger: Arc::new(Mutex::new(InMemoryLedgerRepository::new(
                tantra_core::Currency::Inr,
            ))),
            closed_lots: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Records (creates or updates) one account, e.g. the default REST account the order flow
    /// opens on first approved placement.
    pub fn record_account(&self, account: AccountRecord) {
        let _ = self
            .accounts
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .save_account(&account);
    }

    /// Records (creates or updates) one order, e.g. from an approved `POST /orders` placement.
    pub fn record_order(&self, order: OrderRecord) {
        let _ = self
            .orders
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .save_order(&order);
    }

    /// Records one execution fill: a buy opens a new FIFO tax lot (Indian equity delivery
    /// semantics), a sell matches the fill against the account's open FIFO lots and realizes
    /// P&L into the matched-lot history, and the account's cash is adjusted by the fill's
    /// notional. The account must exist (see [`SharedLedger::record_account`]).
    pub fn record_fill(&self, fill: &FillReport) {
        let Some(account_id) = fill.account_id.as_deref().map(AccountId::new) else {
            return;
        };
        let mut positions = self
            .positions
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        match fill.side {
            OrderSide::Buy => {
                let lot = TaxLot {
                    lot_id: format!("{}-lot", fill.fill_id),
                    account_id: account_id.clone(),
                    instrument: fill.instrument.clone(),
                    side: PositionSide::Long,
                    entry_price: fill.price,
                    quantity: fill.quantity,
                    remaining_quantity: fill.quantity,
                    entry_date: fill.timestamp.date_naive(),
                    created_at: fill.timestamp,
                };
                let _ = positions.save_lot(&account_id, &lot);
            }
            OrderSide::Sell => {
                let matched = positions.match_lots(fill).unwrap_or_default();
                if !matched.is_empty() {
                    let mut closed = self
                        .closed_lots
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner);
                    let history = closed.entry(account_id.clone()).or_default();
                    let ts = fill.timestamp;
                    for m in matched {
                        history.push(ClosedLotEntry {
                            instrument: fill.instrument.clone(),
                            lot_id: m.lot_id,
                            matched_quantity: m.matched_quantity,
                            entry_price: m.entry_price,
                            exit_price: m.exit_price,
                            realized_pnl: m.realized_pnl,
                            holding_period_days: m.holding_period_days,
                            is_long_term: m.is_long_term,
                            matched_at: ts,
                        });
                    }
                }
            }
        }
        drop(positions);
        // Cash follows the fill's notional: a buy debits, a sell credits. The ledger row
        // carries the authoritative account cash; a missing account is left alone.
        let notional = fill.price.value() * fill.quantity.value();
        let delta = match fill.side {
            OrderSide::Buy => -notional,
            OrderSide::Sell => notional,
        };
        let mut accounts = self.accounts.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(account) = accounts.get_account(&account_id).ok().flatten() {
            let updated = AccountRecord {
                cash: Money::new(account.cash.amount + delta, account.cash.currency),
                updated_at: fill.timestamp,
                ..account
            };
            let _ = accounts.save_account(&updated);
        }
    }

    /// The account record under `id`, when one has been opened by the order flow.
    pub fn account(&self, id: &AccountId) -> Option<AccountRecord> {
        self.accounts
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get_account(id)
            .ok()
            .flatten()
    }

    /// Every closed (matched-away) lot recorded for `account`, oldest match first.
    pub fn closed_lots(&self, account: &AccountId) -> Vec<ClosedLotEntry> {
        let closed = self
            .closed_lots
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        closed.get(account).cloned().unwrap_or_default()
    }

    /// The open FIFO tax lots of `account`, used to answer `closed_lots`-style queries that
    /// need remaining lots (e.g. statement tools).
    pub fn open_lots(&self, account: &AccountId) -> Vec<TaxLot> {
        let positions = self
            .positions
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get_positions(account)
            .unwrap_or_default();
        let mut lots: Vec<TaxLot> = positions.into_iter().flat_map(|p| p.lots).collect();
        lots.sort_by_key(|l| l.entry_date);
        lots
    }

    /// Advances a recorded order row's fill state: `filled_quantity`, `avg_fill_price`, and
    /// `status` become the fill's values (kept in lockstep with the REST `OrderLedger` row, so
    /// the chat `orders` tool sees the same fills `GET /orders` does). No-op for an unknown id.
    pub fn update_order_fill(
        &self,
        client_order_id: &str,
        filled_quantity: tantra_core::Quantity,
        avg_fill_price: tantra_core::Price,
        status: tantra_core::OrderStatus,
        updated_at: DateTime<Utc>,
    ) {
        let id = ClientOrderId::from(client_order_id.to_string());
        let mut orders = self.orders.lock().unwrap_or_else(PoisonError::into_inner);
        if let Ok(Some(mut record)) = orders.get_order(&id) {
            record.filled_quantity = filled_quantity;
            record.avg_fill_price = Some(avg_fill_price);
            record.status = status;
            record.updated_at = updated_at;
            let _ = orders.save_order(&record);
        }
    }
}

// ——————————————————————————————————————————————————————————————————————————
// Trait objects: the same `Arc<SharedLedger>` coerces to each of the four
// read ports `LedgerPortfolioReader::new` needs. The `&mut self` receivers are
// part of the trait contract; the bodies lock internally and ignore the
// mutability, so every trait call is thread-safe on a shared handle.
// ——————————————————————————————————————————————————————————————————————————

impl AccountRepository for SharedLedger {
    fn get_account(&self, id: &AccountId) -> Result<Option<AccountRecord>, DomainError> {
        self.accounts
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get_account(id)
    }

    fn save_account(&mut self, account: &AccountRecord) -> Result<(), DomainError> {
        self.accounts
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .save_account(account)
    }

    fn list_accounts(&self) -> Result<Vec<AccountRecord>, DomainError> {
        self.accounts
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .list_accounts()
    }
}

impl PositionRepository for SharedLedger {
    fn get_positions(&self, account_id: &AccountId) -> Result<Vec<PositionRecord>, DomainError> {
        self.positions
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get_positions(account_id)
    }

    fn save_lot(&mut self, account_id: &AccountId, lot: &TaxLot) -> Result<(), DomainError> {
        self.positions
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .save_lot(account_id, lot)
    }

    fn match_lots(&mut self, fill: &FillReport) -> Result<Vec<MatchedLot>, DomainError> {
        self.positions
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .match_lots(fill)
    }
}

impl OrderRepository for SharedLedger {
    fn get_order(&self, id: &ClientOrderId) -> Result<Option<OrderRecord>, DomainError> {
        self.orders
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get_order(id)
    }

    fn save_order(&mut self, record: &OrderRecord) -> Result<(), DomainError> {
        self.orders
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .save_order(record)
    }

    fn list_active_orders(&self, algo_id: &AlgoId) -> Result<Vec<OrderRecord>, DomainError> {
        self.orders
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .list_active_orders(algo_id)
    }
}

impl LedgerRepository for SharedLedger {
    fn post_entry(&mut self, tx: &JournalTransaction) -> Result<TransactionId, DomainError> {
        self.ledger
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .post_entry(tx)
    }

    fn get_balance(&self, account: &AccountCode) -> Result<Money, DomainError> {
        self.ledger
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get_balance(account)
    }
}
