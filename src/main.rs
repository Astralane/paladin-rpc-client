mod db;
mod leader_tracker;
mod auction;
mod paladin_client;
mod tip_stream;

use tokio::signal;
use tracing_subscriber;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let db = db::init_db().await?;
    let validator_state = leader_tracker::ValidatorState::new();

    tokio::spawn(leader_tracker::poll_leader_schedule(validator_state.clone()));
    tokio::spawn(tip_stream::start_tip_stream());
    tokio::spawn(auction::start_auction_loop(db.clone(), validator_state.clone()));

    signal::ctrl_c().await?;
    Ok(())
}