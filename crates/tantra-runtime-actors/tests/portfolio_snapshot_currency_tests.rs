//! Portfolio snapshots carry the portfolio's currency instead of
//! relabelling totals INR (root gap 0014).

use rust_decimal::Decimal;
use tantra_core::{Currency, Money};
use tantra_runtime_actors::PortfolioActor;
use tantra_runtime_actors::messages::PortfolioMsg;
use tokio::sync::{mpsc, oneshot};

#[tokio::test]
async fn usd_portfolio_snapshot_is_in_usd() {
    let (tx, rx) = mpsc::channel(4);
    let initial = Money::new(Decimal::from(1000), Currency::Usd);
    let actor = tokio::spawn(PortfolioActor::new(initial, rx).run());

    let (reply_to, reply) = oneshot::channel();
    tx.send(PortfolioMsg::GetSnapshot { reply_to })
        .await
        .expect("actor accepts the request");
    let snapshot = reply
        .await
        .expect("actor replies")
        .expect("single-currency snapshot succeeds");

    assert_eq!(snapshot.cash, initial);
    assert_eq!(snapshot.total_equity, initial);
    assert_eq!(snapshot.unrealized_pnl, Money::zero(Currency::Usd));
    assert_eq!(snapshot.realized_pnl, Money::zero(Currency::Usd));

    drop(tx);
    actor.await.expect("actor stops when its channel closes");
}
