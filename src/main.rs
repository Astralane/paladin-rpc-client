mod constants;
mod leader_tracker;
mod quic;
mod quic_connectors;
mod slot_watchers;
mod utils;

use crate::leader_tracker::palidator_tracker::PalidatorTrackerImpl;
use crate::quic::quic_client_certificate::QuicClientCertificate;
use crate::quic::quic_networking::{create_client_config, create_client_endpoint};
use crate::slot_watchers::recent_slots::RecentLeaderSlots;
use crate::slot_watchers::SlotWatcher;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_commitment_config::CommitmentConfig;
use solana_sdk::signature::Keypair;
use solana_sdk::signer::EncodableKey;
use std::sync::Arc;
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
    let bind = "0.0.0.0:8000".parse().unwrap();
    let identity =
        Keypair::read_from_file("/identity/identity.json").expect("Failed to read identity");

    let rpc = Arc::new(RpcClient::new(rpc_url.to_owned()));
    let estimated_current_slot = rpc
        .get_slot_with_commitment(CommitmentConfig::processed())
        .await?;
    let recent_slots = RecentLeaderSlots::new(estimated_current_slot);
    // update recent slots with the latest data
    let mut slot_watcher_hdl =
        SlotWatcher::run_slot_watchers(ws_url, grpc_url, recent_slots.clone(), cancel.clone());

    let client_certificate = Arc::new(QuicClientCertificate::new(&identity));
    let client_config = create_client_config(client_certificate);
    let endpoint = Arc::new(create_client_endpoint(bind, client_config)?);
    let tracker = PalidatorTrackerImpl::new(rpc, recent_slots, endpoint, cancel.clone()).await?;
    slot_watcher_hdl.join().await;
    Ok(())
}
