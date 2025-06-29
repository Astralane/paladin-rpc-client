use std::{sync::Arc, time::Instant};

use base64::prelude::*;
use jsonrpsee::server::{ServerBuilder, RpcModule};
use p3_lib::transaction_store::{TransactionData, TransactionStoreImpl};
use solana_sdk::transaction::VersionedTransaction;
use tracing::{info, error};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .init();

    let transaction_store = Arc::new(TransactionStoreImpl::new());

    let mut module = RpcModule::new(transaction_store.clone());

    // Accepts base64-encoded wire format of VersionedTransaction
    module.register_async_method("submit_tx", |params, tx_store| async move {
        let base64_tx: String = params.one()?;

        let decoded_bytes = match BASE64_STANDARD.decode(&base64_tx) {
            Ok(b) => b,
            Err(e) => {
                error!("Base64 decode error: {}", e);
                return Err(jsonrpsee::core::Error::Custom("invalid base64".into()));
            }
        };

        let versioned_tx: VersionedTransaction = match bincode::deserialize(&decoded_bytes) {
            Ok(vtx) => vtx,
            Err(e) => {
                error!("Deserialization error: {}", e);
                return Err(jsonrpsee::core::Error::Custom("invalid transaction format".into()));
            }
        };

        let transaction_data = TransactionData {
            wire_transaction: decoded_bytes,
            versioned_transaction: versioned_tx,
            sent_at: Instant::now(),
            retry_count: 0,
            max_retries: 5,
            request_metadata: None,
        };

        tx_store.add_transaction(transaction_data);
        Ok::<_, jsonrpsee::core::Error>("ok")
    })?;

    let server = ServerBuilder::default()
        .build("0.0.0.0:8080")
        .await?;

    let addr = server.local_addr()?;
    info!("JSON-RPC server listening on {addr}");

    server.start(module)?.stopped().await;
    Ok(())
}

