mod constants;
mod palidator_cache;
mod palidator_tracker;
mod quic;
mod slot_watchers;

use crate::slot_watchers::recent_slots::RecentLeaderSlots;
use crate::slot_watchers::SlotWatcher;
use anyhow::Context;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_commitment_config::CommitmentConfig;
use tokio::signal::ctrl_c;
use tokio_util::sync::CancellationToken;
use tracing::info;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let cancel = CancellationToken::new();
    tokio::spawn({
        let cancel = cancel.clone();
        async move {
            ctrl_c().await.unwrap();
            info!("Received Ctrl+C, shutting down...");
            cancel.cancel();
        }
    });

    let grpc_url = vec!["http://grpc:10000".to_string()];
    let ws_url = vec!["ws://rpc:8900".to_string()];
    let rpc_url = "http://rpc:8899";

    let rpc = RpcClient::new(rpc_url.to_owned());
    let estimated_current_slot = rpc
        .get_slot_with_commitment(CommitmentConfig::processed())
        .await?;
    let recent_slots = RecentLeaderSlots::new(estimated_current_slot);
    let mut slot_watcher_hdl =
        SlotWatcher::run_slot_watchers(ws_url, grpc_url, recent_slots.clone(), cancel.clone());
    while !cancel.is_cancelled() {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        println!("latest slot: {}", recent_slots.estimated_current_slot())
    }
    slot_watcher_hdl.join().await;
    Ok(())
}
