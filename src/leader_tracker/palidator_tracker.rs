use crate::leader_tracker::leader_schedule::PalidatorSchedule;
use crate::leader_tracker::types::PalSocketAddr;
use crate::slot_watchers::recent_slots::RecentLeaderSlots;
use quinn::Endpoint;
use solana_client::nonblocking::rpc_client::RpcClient;
use std::sync::{Arc, RwLock};
use tokio_util::sync::CancellationToken;
use tracing::error;

pub struct PalidatorTracker {
    pub recent_slots: RecentLeaderSlots,
    schedule: Arc<RwLock<PalidatorSchedule>>,
    task: tokio::task::JoinHandle<()>,
}

impl PalidatorTracker {
    pub async fn new(
        rpc: Arc<RpcClient>,
        recent_slots: RecentLeaderSlots,
        endpoint: Arc<Endpoint>,
        cancel: CancellationToken,
    ) -> anyhow::Result<Self> {
        let schedule = PalidatorSchedule::load_latest_by_quic_connect(&rpc, &endpoint).await?;
        let schedule = Arc::new(RwLock::new(schedule));
        let task = tokio::spawn(Self::run_updater(rpc, endpoint, schedule.clone(), cancel));
        Ok(Self {
            recent_slots,
            schedule,
            task,
        })
    }

    pub fn get_closest_leaders(&self, lookout_num: usize) -> Vec<Option<PalSocketAddr>> {
        let current_slot = self.recent_slots.estimated_current_slot();
        self.schedule
            .read()
            .unwrap()
            .get_next_palidator_leader(current_slot, lookout_num)
    }

    pub async fn join(self) {
        self.task.await.unwrap();
    }

    //TODO: we need to reload the schedule when epoch changes
    async fn run_updater(
        rpc: Arc<RpcClient>,
        endpoint: Arc<Endpoint>,
        cache: Arc<RwLock<PalidatorSchedule>>,
        cancel: CancellationToken,
    ) {
        const ONE_HOUR: u64 = 60 * 60;
        let mut tick = tokio::time::interval(std::time::Duration::from_secs(ONE_HOUR));
        loop {
            let _ = tokio::select! {
                _ = cancel.cancelled() => {
                    break;
                },
                _ = tick.tick() => {
                    if let Err(e) = Self::reload_schedule(cache.clone(), rpc.clone(), endpoint.clone()).await{
                        error!("Failed to reload palidator schedule: {:?}", e);
                    }
                }
            };
        }
    }

    async fn reload_schedule(
        cache: Arc<RwLock<PalidatorSchedule>>,
        rpc: Arc<RpcClient>,
        endpoint: Arc<Endpoint>,
    ) -> anyhow::Result<()> {
        let updated_cache = PalidatorSchedule::load_latest_by_quic_connect(&rpc, &endpoint)
            .await
            .inspect_err(|e| error!("Failed to load latest palidator cache: {:?}", e))?;
        {
            let mut cache = cache.write().unwrap();
            *cache = updated_cache;
        }
        Ok(())
    }
}
