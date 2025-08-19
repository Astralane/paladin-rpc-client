use crate::leader_tracker::palidator_tracker::PalidatorTrackerImpl;
use crate::types::VerifiedTransaction;
use anyhow::Context;
use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use bincode::Options;
use jsonrpsee::core::{async_trait, RpcResult};
use jsonrpsee::proc_macros::rpc;
use jsonrpsee::server::{ServerBuilder, ServerConfig};
use jsonrpsee::types::error::{INTERNAL_ERROR_CODE, INVALID_PARAMS_CODE};
use jsonrpsee::types::ErrorObjectOwned;
use log::{error, info};
use metrics::counter;
use solana_client::rpc_client::SerializableTransaction;
use solana_sdk::transaction::VersionedTransaction;
use std::any::type_name;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tracing::warn;

const MAX_BASE58_SIZE: usize = 1683; // Golden, bump if PACKET_DATA_SIZE changes
const MAX_BASE64_SIZE: usize = 1644; // Golden, bump if PACKET_DATA_SIZE changes
const PACKET_DATA_SIZE: usize = 1280 - 40 - 8;

#[rpc(client, server)]
pub trait PaladinRpc {
    #[method(name = "getHealth")]
    async fn health(&self) -> RpcResult<String>;
    #[method(name = "sendTransaction")]
    async fn send_transaction(&self, txn: String, revert_protect: bool) -> RpcResult<String>;
}

struct PaladinRpcImpl {
    leader_tracker: Arc<PalidatorTrackerImpl>,
    auction_sender: crossbeam_channel::Sender<VerifiedTransaction>,
    max_slot_received_offset: i32,
}

impl PaladinRpcImpl {
    pub fn new(
        leader_tracker: Arc<PalidatorTrackerImpl>,
        auction_sender: crossbeam_channel::Sender<VerifiedTransaction>,
        max_slot_received_offset: i32,
    ) -> Self {
        Self {
            leader_tracker,
            auction_sender,
            max_slot_received_offset,
        }
    }
}

#[async_trait]
impl PaladinRpcServer for PaladinRpcImpl {
    async fn health(&self) -> RpcResult<String> {
        Ok("Ok".to_string())
    }

    async fn send_transaction(&self, txn: String, revert_protect: bool) -> RpcResult<String> {
        counter!("paladin_rpc_client_received_tx_count").increment(1);
        let estimated_current_slot = self.leader_tracker.recent_slots.estimated_current_slot();
        info!("recvd transaction at slot {}", estimated_current_slot);
        if !self.leader_tracker.is_paladin_slot(estimated_current_slot) {
            warn!(
                "received transaction at slot {} but not paladin slot, next paladin slot is {:?}",
                estimated_current_slot,
                self.leader_tracker.next_paladin_slot()
            );
            counter!("paladin_rpc_client_received_tx_not_paladin_slot_count").increment(1);
            return Err(invalid_request(&format!(
                "{:} not paladin slot",
                estimated_current_slot,
            )));
        }
        let (wire_output, versioned_tx) = decode_and_verify_transaction(txn)?;
        let signature = versioned_tx.get_signature().clone();
        let verified_transaction = VerifiedTransaction::from_transaction(
            wire_output,
            revert_protect,
            Some(signature),
            None,
            estimated_current_slot,
        );
        match self.auction_sender.send(verified_transaction) {
            Ok(_) => {
                counter!("paladin_rpc_client_tx_sent_to_auction_thread").increment(1);
            }
            Err(_) => {
                warn!("failed to send transaction to rpc server");
                counter!("paladin_rpc_client_tx_failed_to_send_to_auction_thread").increment(1);
                return Err(internal_error("failed to send transaction to rpc server"));
            }
        }
        Ok(signature.to_string())
    }
}

pub async fn spawn_paladin_json_rpc_server(
    address: SocketAddr,
    leader_tracker: Arc<PalidatorTrackerImpl>,
    auction_sender: crossbeam_channel::Sender<VerifiedTransaction>,
    max_slot_received_offset: i32,
    cancel: CancellationToken,
) -> anyhow::Result<tokio::task::JoinHandle<()>> {
    let rpc_server = PaladinRpcImpl::new(leader_tracker, auction_sender, max_slot_received_offset);
    let server_config = ServerConfig::builder()
        .max_connections(100_000_000)
        .http_only()
        .build();

    let server = ServerBuilder::new()
        .set_config(server_config)
        .build(address)
        .await
        .context("failed to create rpc server")?;

    let server_hdl = server.start(rpc_server.into_rpc());
    let task = tokio::spawn(async move {
        tokio::select! {
            _ = cancel.cancelled() => {},
            _ = server_hdl.stopped() => {
                error!("paladin rpc server stopped");
            }
        }
    });
    Ok(task)
}
fn invalid_request(reason: &str) -> ErrorObjectOwned {
    ErrorObjectOwned::owned(
        INVALID_PARAMS_CODE,
        format!("Invalid Request: {reason}"),
        None::<String>,
    )
}

fn internal_error(reason: &str) -> ErrorObjectOwned {
    ErrorObjectOwned::owned(
        INTERNAL_ERROR_CODE,
        format!("Internal Error: {reason}"),
        None::<String>,
    )
}

pub fn decode_and_verify_transaction(
    encoded: String,
) -> RpcResult<(Vec<u8>, VersionedTransaction)> {
    if encoded.len() > MAX_BASE64_SIZE {
        return Err(invalid_request(&format!(
            "base64 encoded {} too large: {} bytes (max: encoded/raw {}/{})",
            type_name::<VersionedTransaction>(),
            encoded.len(),
            MAX_BASE64_SIZE,
            PACKET_DATA_SIZE,
        )));
    }
    let wire_output = BASE64_STANDARD
        .decode(encoded)
        .map_err(|e| invalid_request(&format!("invalid base64 encoding: {e:?}")))?;

    if wire_output.len() > PACKET_DATA_SIZE {
        return Err(invalid_request(&format!(
            "decoded {} too large: {} bytes (max: {} bytes)",
            type_name::<VersionedTransaction>(),
            wire_output.len(),
            PACKET_DATA_SIZE
        )));
    }

    let versioned_tx = bincode::options()
        .with_limit(PACKET_DATA_SIZE as u64)
        .with_fixint_encoding()
        .allow_trailing_bytes()
        .deserialize_from::<_, VersionedTransaction>(&wire_output[..])
        .map_err(|err| {
            invalid_request(&format!(
                "failed to deserialize {}: {}",
                type_name::<VersionedTransaction>(),
                &err.to_string()
            ))
        })?;
    Ok((wire_output, versioned_tx))
}
