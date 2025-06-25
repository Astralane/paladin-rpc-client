use crate::{db, leader_tracker::ValidatorState, paladin_client};
use sqlx::PgPool;
use tokio::time::{interval, Duration};
use tracing::info;

pub async fn start_auction_loop(db: PgPool, validator_state: ValidatorState) {
    let mut interval = interval(Duration::from_millis(50));

    loop {
        interval.tick().await;

        // Placeholder tx pool
        let tx_pool = vec!["tx1", "tx2"];

        let leaders = validator_state.get_next_leader();

        for tx in tx_pool.iter() {
            if let Some((identity, slot)) = leaders.first() {
                paladin_client::send_paladin_transaction(tx, *slot, identity).await.ok();

                sqlx::query("INSERT INTO auction_results (tx, slot, identity) VALUES ($1, $2, $3)")
                    .bind(tx)
                    .bind(*slot as i64)
                    .bind(identity)
                    .execute(&db)
                    .await
                    .ok();

                info!(%tx, %slot, %identity, "Auction result written");
            }
        }
    }
}