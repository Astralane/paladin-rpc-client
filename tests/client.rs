mod common;

use crate::common::init_tracing;
use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use paladin_rpc_server::leader_tracker::palidator_tracker::stub_tracker::StubPalidatorTracker;
use paladin_rpc_server::quic::quic_networking::{send_data_over_stream, setup_quic_endpoint};
use paladin_rpc_server::quic_connectors::{ConnectionScheduler, TransactionBatch, TransactionInfo};
use paladin_rpc_server::slot_watchers::recent_slots::RecentLeaderSlots;
use paladin_rpc_server::utils::PalidatorTracker;
use solana_sdk::signature::{EncodableKey, Keypair};
use std::convert::identity;
use std::net::{SocketAddr, UdpSocket};
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::info;

#[tokio::test]
pub async fn test_quic_client() {
    init_tracing();
    info!("Starting test");
    let ws_url = "ws://rpc:8900";
    let bind = "0.0.0.0:11220";
    let identity = Keypair::read_from_file("/Users/nuel/.config/solana/sig_pal.json").unwrap();
    println!("{:?}", identity.to_bytes());
    //doggo validator ip: 149.248.51.171
    let leader_tracker = StubPalidatorTracker::new("149.248.51.171".parse().unwrap());

    let recent_slots = RecentLeaderSlots::new(10);
    let (sender, mut receiver) = tokio::sync::mpsc::channel::<TransactionBatch>(10);
    let cancel = CancellationToken::new();

    let endpoint =
        setup_quic_endpoint(bind.parse().unwrap(), identity).expect("Failed to setup endpoint");
    // let [sock,] = leader_tracker.next_leaders(1)[0];
    info!("connection established");

    tokio::spawn(async move {
        let worker = ConnectionScheduler::new(
            leader_tracker,
            recent_slots,
            Arc::new(endpoint),
            1,
            receiver,
            64,
            5,
            cancel,
        )
        .await;
    });

    let encoded_tx = "AQ8u+02BWbmWoAp/l5ywboiVfqLvccf0imCVc+UBBOUzRF2n0InBPPWiPZKLuiCIm2XruFl4sjuZQX+Wf0RIsAEBAAED1C+Y6RXlWshcp9Q7xXwA76wBNxlKWPQy3zk0bTZaifYIrbZ5I8Tb2shZFMrMnlo+yQM4KGV+ex41djfeiorzggAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAXTvopTJh7ISsx99fFL/DNvqXpzmACKWoAIJy+D5pZGkCAgIAAQwCAAAAoIYBAAAAAAACAgAADAIAAABuFAAAAAAAAA==";
    let wire_transaction = BASE64_STANDARD.decode(encoded_tx).unwrap();
    let tx_batch = (0..100)
        .into_iter()
        .map(|_| TransactionInfo::new(wire_transaction.clone(), true, 0))
        .collect::<Vec<_>>();

    info!("Sending transaction batch");
    let batch = TransactionBatch::new(tx_batch);

    info!("Sending batch");
    sender.send(batch.clone()).await.unwrap();

    tokio::time::sleep(Duration::from_secs(10)).await;
}
