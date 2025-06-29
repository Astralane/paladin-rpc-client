use std::sync::Arc;

use jsonrpsee::server::{ServerBuilder, RpcModule};
use p3_lib::transaction_store::TransactionStoreImpl;
use tracing::info;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .init();

    let transaction_store = Arc::new(TransactionStoreImpl::new());

    let mut module = RpcModule::new(transaction_store.clone());
    module.register_async_method("submit_tx", |params, tx_store| async move {
        let tx: String = params.one()?;
        tx_store.push_transaction(tx);
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
