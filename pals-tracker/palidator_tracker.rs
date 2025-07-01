use crate::palidator_cache::PalidatorCache;
use crate::vendor::quic_client_certificate::QuicClientCertificate;
use crate::vendor::quic_networking::{create_client_config, create_client_endpoint};
use futures::StreamExt;
use quinn::Endpoint;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_rpc_client_api::response::SlotUpdate;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::signature::Keypair;
use std::net::SocketAddr;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, RwLock};
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

pub struct PalidatorTracker {
    pub palidator_cache: Arc<RwLock<PalidatorCache>>,
    pub slot: Arc<AtomicU64>,
    hdl: tokio::task::JoinHandle<()>,
}

impl PalidatorTracker {
    pub async fn new(
        rpc: Arc<RpcClient>,
        ws_url: String,
        identity: Keypair,
        bind: SocketAddr,
        cancel: CancellationToken,
    ) -> anyhow::Result<Self> {
        let client_certificate = Arc::new(QuicClientCertificate::new(&identity));
        let client_config = create_client_config(client_certificate);
        let endpoint = create_client_endpoint(bind, client_config)?;
        let pal_cache = PalidatorCache::load_latest(&rpc, &endpoint).await?;
        let cache = Arc::new(RwLock::new(pal_cache));
        let slot = rpc
            .get_slot_with_commitment(CommitmentConfig::processed())
            .await?;
        let slot = Arc::new(AtomicU64::new(slot));
        let hdl = tokio::spawn(Self::run(
            rpc,
            ws_url,
            endpoint,
            cache.clone(),
            slot.clone(),
            cancel,
        ));
        Ok(Self {
            palidator_cache: cache,
            slot,
            hdl,
        })
    }

    pub async fn join(self) {
        self.hdl.await.unwrap();
    }

    async fn run(
        rpc: Arc<RpcClient>,
        ws_url: String,
        endpoint: Endpoint,
        cache: Arc<RwLock<PalidatorCache>>,
        slot: Arc<AtomicU64>,
        cancel: CancellationToken,
    ) {
        let _ = tokio::join!(
            cancel.cancelled(),
            Self::run_cache_reload(cache, rpc.clone(), endpoint),
            Self::run_slot_update(slot, ws_url)
        );
    }

    async fn run_cache_reload(
        cache: Arc<RwLock<PalidatorCache>>,
        rpc: Arc<RpcClient>,
        endpoint: Endpoint,
    ) {
        loop {
            let Ok(epoch_info) = rpc
                .get_epoch_info_with_commitment(CommitmentConfig::processed())
                .await
                .inspect_err(|e| error!("Failed to get epoch info: {:?}", e))
            else {
                continue;
            };

            if epoch_info.epoch == cache.read().unwrap().epoch {
                // check every minute for epoch changes
                tokio::time::sleep(std::time::Duration::from_secs(60 *60)).await;
                continue;
            }

            let Ok(updated_cache) = PalidatorCache::load_latest(&rpc, &endpoint)
                .await
                .inspect_err(|e| error!("Failed to load latest palidator cache: {:?}", e))
            else {
                continue;
            };

            let mut cache = cache.write().unwrap();
            *cache = updated_cache;
        }
    }

    async fn run_slot_update(slot_cache: Arc<AtomicU64>, ws_url: String) {
        loop {
            let Ok(client) = solana_client::nonblocking::pubsub_client::PubsubClient::new(&ws_url)
                .await
                .inspect_err(|e| error!("Failed to create pubsub client: {:?}", e))
            else {
                continue;
            };

            let Ok((mut stream, unsub)) = client
                .slot_updates_subscribe()
                .await
                .inspect_err(|e| error!("Failed to slot subscribe: {:?}", e))
            else {
                continue;
            };
            info!("subscribed to slot updates");
            while let Some(slot_update) = stream.next().await {
                let slot = match slot_update {
                    SlotUpdate::FirstShredReceived { slot, .. } => slot,
                    SlotUpdate::Completed { slot, .. } => slot.saturating_add(1),
                    _ => continue,
                };
                slot_cache.store(slot, std::sync::atomic::Ordering::SeqCst);
            }
            //something bad happened try reconnecting now
            unsub().await;
        }
    }
}
