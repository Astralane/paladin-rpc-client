use futures_util::future::try_join_all;
use itertools::Itertools;
use paladin_rpc_server::auction_forwarder::AuctionAndForwardStage;
use paladin_rpc_server::connectin_scheduler::ConnectionScheduler;
use paladin_rpc_server::leader_tracker::palidator_tracker::PalidatorTrackerImpl;
use paladin_rpc_server::quic::quic_networking::setup_quic_endpoint;
use paladin_rpc_server::quic_forwarder;
use paladin_rpc_server::rpc::json_rpc::spawn_paladin_json_rpc_server;
use paladin_rpc_server::slot_watchers::recent_slots::RecentLeaderSlots;
use paladin_rpc_server::slot_watchers::SlotWatcher;
use serde::Deserialize;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_commitment_config::CommitmentConfig;
use solana_sdk::signature::Keypair;
use solana_sdk::signer::EncodableKey;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::info;

#[derive(Deserialize, Debug)]
struct Config {
    leader_look_ahead: usize,
    rpc_address: SocketAddr,
    max_slot_offset: i32,
    identity_paths: Vec<String>,
    rpc_url: String,
    ws_urls: Vec<String>,
    grpc_urls: Vec<String>,
    quic_bind_address: SocketAddr,
    quic_max_reconnection_attempts: usize,
    quic_txn_max_batch_size: usize,
    quic_worker_queue_size: usize,
    auction_interval_ms: u64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config_path = std::env::args().nth(1).expect("No config path provided");
    let config_file = std::fs::read_to_string(config_path.clone())
        .expect(&format!("Failed to read config file {}", config_path));
    let config = serde_json::from_str::<Config>(&config_file).expect("Failed to parse config");

    tracing_subscriber::fmt::init();
    let cancel = CancellationToken::new();

    let paladin_keypairs: Vec<Keypair> = config
        .identity_paths
        .iter()
        .map(|path| Keypair::read_from_file(path).unwrap())
        .collect();

    let rpc = Arc::new(RpcClient::new(config.rpc_url.to_owned()));
    let estimated_current_slot = rpc
        .get_slot_with_commitment(CommitmentConfig::processed())
        .await?;

    let recent_slots = RecentLeaderSlots::new(estimated_current_slot);
    // update recent slots with the latest data
    info!("Starting slot watcher");
    let mut slot_watcher_stage = SlotWatcher::run_slot_watchers(
        config.ws_urls,
        config.grpc_urls,
        recent_slots.clone(),
        cancel.clone(),
    );

    let endpoint = Arc::new(setup_quic_endpoint(
        config.quic_bind_address,
        &paladin_keypairs[0],
    )?);
    info!("Starting leader tracker");
    let (leader_tracker, paladin_tracker_handle) =
        PalidatorTrackerImpl::new(rpc, recent_slots.clone(), endpoint, cancel.clone()).await?;
    let leader_tracker = Arc::new(leader_tracker);

    info!("Starting connection workers");
    let connection_workers = paladin_keypairs
        .iter()
        .filter_map(|identity| setup_quic_endpoint(config.quic_bind_address, identity).ok())
        .map(Arc::new)
        .map(|endpoint| {
            let (sender, receiver) = tokio::sync::mpsc::channel(512);
            let mut connection_worker = ConnectionScheduler::new(
                leader_tracker.clone(),
                recent_slots.clone(),
                endpoint.clone(),
                config.leader_look_ahead,
                receiver,
                config.quic_max_reconnection_attempts,
                config.quic_max_reconnection_attempts,
                cancel.clone(),
            );
            let task = tokio::spawn(async move { connection_worker.run().await });
            (sender, task)
        })
        .collect::<Vec<_>>();

    let (tpu_senders, worker_handles): (Vec<_>, Vec<_>) = connection_workers.into_iter().unzip();

    let (tpu_packet_sender, tpu_packet_receiver) = tokio::sync::mpsc::channel(512);
    let (verified_txns_sender, verified_txns_receiver) = crossbeam_channel::unbounded();

    let mut forward_and_auction_stage = AuctionAndForwardStage::spawn_new(
        Duration::from_millis(config.auction_interval_ms),
        verified_txns_receiver,
        tpu_packet_sender,
        None,
        cancel.clone(),
        config.quic_txn_max_batch_size,
        1,
    );

    info!("Starting quic forwarder");
    let quic_forwarder_task = quic_forwarder::forward_packets_to_tpu_load_balanced(
        tpu_senders,
        tpu_packet_receiver,
        cancel.clone(),
    );

    //run the rpc task
    let rpc_task = spawn_paladin_json_rpc_server(
        config.rpc_address.clone(),
        leader_tracker,
        verified_txns_sender,
        config.max_slot_offset,
        cancel.clone(),
    )
    .await?;

    info!("rpc server started, listening on {:}", config.rpc_address);

    paladin_tracker_handle.await?;
    try_join_all(worker_handles).await?;
    slot_watcher_stage.join().await;
    forward_and_auction_stage.join().await;
    quic_forwarder_task.await?;
    rpc_task.await?;
    Ok(())
}
