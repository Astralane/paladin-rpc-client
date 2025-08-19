mod common;

use crate::common::init_tracing;
use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use bytes::Bytes;
use jsonrpsee::http_client::HttpClient;
use paladin_rpc_server::leader_tracker::palidator_tracker::stub_tracker::StubPalidatorTracker;
use paladin_rpc_server::quic::quic_networking::setup_quic_endpoint;
use paladin_rpc_server::rpc::json_rpc::PaladinRpcClient;
use paladin_rpc_server::slot_watchers::recent_slots::RecentLeaderSlots;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::message::Message;
use solana_sdk::signature::{EncodableKey, Keypair, Signer};
use solana_sdk::transaction::Transaction;
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
    let (sender, receiver) = tokio::sync::mpsc::channel::<Vec<PaladinPacket>>(10);
    let cancel = CancellationToken::new();

    let endpoint =
        setup_quic_endpoint(bind.parse().unwrap(), &identity).expect("Failed to setup endpoint");
    // let [sock,] = leader_tracker.next_leaders(1)[0];
    info!("connection established");

    tokio::spawn(async move {
        let mut worker = ConnectionScheduler::new(
            leader_tracker,
            recent_slots,
            Arc::new(endpoint),
            1,
            receiver,
            64,
            5,
            cancel,
        );
        worker.run().await;
    });

    let encoded_tx = "AQ8u+02BWbmWoAp/l5ywboiVfqLvccf0imCVc+UBBOUzRF2n0InBPPWiPZKLuiCIm2XruFl4sjuZQX+Wf0RIsAEBAAED1C+Y6RXlWshcp9Q7xXwA76wBNxlKWPQy3zk0bTZaifYIrbZ5I8Tb2shZFMrMnlo+yQM4KGV+ex41djfeiorzggAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAXTvopTJh7ISsx99fFL/DNvqXpzmACKWoAIJy+D5pZGkCAgIAAQwCAAAAoIYBAAAAAAACAgAADAIAAABuFAAAAAAAAA==";
    let wire_transaction = Bytes::from(BASE64_STANDARD.decode(encoded_tx).unwrap());
    let tx_batch = (0..100)
        .into_iter()
        .map(|_| PaladinPacket::new(wire_transaction.clone(), true))
        .collect::<Vec<_>>();

    info!("Sending transaction batch");

    info!("Sending batch");
    sender.send(tx_batch.clone()).await.unwrap();

    tokio::time::sleep(Duration::from_secs(10)).await;
}

use paladin_rpc_server::connectin_scheduler::{ConnectionScheduler, PaladinPacket};
use serde::{Deserialize, Serialize};
use solana_commitment_config::CommitmentConfig;

#[derive(Serialize, Deserialize, Debug)]
struct PaladinNextLeader {
    pubkey: String,    // "pubkey": "Csd...def"
    leader_slot: u64,  // "leader_slot": 42424242
    context_slot: u64, // "context_slot": 42424242
}
#[tokio::test]
pub async fn test_rpc_send_transaction() {
    use paladin_rpc_server::rpc::json_rpc::PaladinRpcClient;
    init_tracing();
    let pal_rpc = HttpClient::builder()
        .build("http://localhost:8899")
        .unwrap();
    let keypair = Keypair::read_from_file("/Users/nuel/.config/solana/id.json").unwrap();
    let pubkey = keypair.pubkey();
    let sol_rpc = RpcClient::new("http://rpc:8899".to_owned());

    let next_palidator_slot = reqwest::get("http://paladin.astralane.io/api/next_palidator")
        .await
        .unwrap()
        .json::<PaladinNextLeader>()
        .await
        .unwrap()
        .leader_slot;

    info!("next leader {:?}", next_palidator_slot);
    let mut current_slot = sol_rpc
        .get_slot_with_commitment(CommitmentConfig::processed())
        .await
        .unwrap();

    while current_slot < next_palidator_slot {
        info!(
            "need {} slots for sending",
            next_palidator_slot.saturating_sub(current_slot)
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
        current_slot = sol_rpc
            .get_slot_with_commitment(CommitmentConfig::processed())
            .await
            .unwrap();
    }

    let instructions = vec![
        solana_compute_budget_interface::ComputeBudgetInstruction::set_compute_unit_limit(800),
        solana_system_interface::instruction::transfer(&pubkey, &pubkey, 100000),
        solana_compute_budget_interface::ComputeBudgetInstruction::set_compute_unit_price(1000),
    ];
    let message = Message::new(&instructions, Some(&pubkey));
    let block_hash = sol_rpc.get_latest_blockhash().await.unwrap();
    let transaction = Transaction::new(&[&keypair], message, block_hash);
    let wire_tx = bincode::serialize(&transaction).unwrap();
    let encoded = BASE64_STANDARD.encode(wire_tx);
    let response = pal_rpc.send_transaction(encoded, false).await;
    info!("rpc response {:?}", response);
}
