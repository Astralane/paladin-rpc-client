use crate::leader_tracker::leader_schedule::PalidatorSchedule;
use crate::leader_tracker::types::{pal_socks_from_ip, PaladinSocketAddrs};
use crate::slot_watchers::recent_slots::RecentLeaderSlots;
use crate::utils::PalidatorTracker;
use quinn::Endpoint;
use solana_client::nonblocking::rpc_client::RpcClient;
use std::net::IpAddr;
use std::sync::{Arc, RwLock};
use tokio_util::sync::CancellationToken;
use tracing::error;

pub struct PalidatorTrackerImpl {
    pub recent_slots: RecentLeaderSlots,
    schedule: Arc<RwLock<PalidatorSchedule>>,
}

impl PalidatorTrackerImpl {
    pub async fn new(
        rpc: Arc<RpcClient>,
        recent_slots: RecentLeaderSlots,
        endpoint: Arc<Endpoint>,
        cancel: CancellationToken,
    ) -> anyhow::Result<(Self, tokio::task::JoinHandle<()>)> {
        let schedule = PalidatorSchedule::load_latest(&rpc, &endpoint, Default::default()).await?;
        let schedule = Arc::new(RwLock::new(schedule));
        let task = tokio::spawn(Self::run_updater(rpc, endpoint, schedule.clone(), cancel));
        Ok((
            Self {
                recent_slots,
                schedule,
            },
            task,
        ))
    }

    pub fn next_paladin_slot(&self) -> Option<u64> {
        let current_slot = self.recent_slots.estimated_current_slot();
        self.schedule
            .read()
            .unwrap()
            .slot_schedule
            .range(current_slot..)
            .next()
            .map(|(slot, _)| *slot)
    }
    pub fn is_paladin_slot(&self, slot: u64) -> bool {
        self.schedule
            .read()
            .unwrap()
            .slot_schedule
            .contains_key(&slot)
    }

    pub fn get_closest_leaders(&self, lookout_num: usize) -> Vec<Option<PaladinSocketAddrs>> {
        let current_slot = self.recent_slots.estimated_current_slot();
        self.schedule
            .read()
            .unwrap()
            .get_next_palidator_leader(current_slot, lookout_num)
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
        //run the first tick optimistically
        tick.tick().await;
        loop {
            tokio::select! {
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
        let updated_cache = PalidatorSchedule::load_latest(&rpc, &endpoint, Default::default())
            .await
            .inspect_err(|e| error!("Failed to load latest palidator cache: {:?}", e))?;
        {
            let mut cache = cache.write().unwrap();
            *cache = updated_cache;
        }
        Ok(())
    }
}

impl PalidatorTracker for PalidatorTrackerImpl {
    fn next_leaders(&self, lookahead_leaders: usize) -> Vec<PaladinSocketAddrs> {
        self.get_closest_leaders(lookahead_leaders)
            .into_iter()
            .flatten()
            .collect()
    }
}

pub mod stub_tracker {
    use super::*;
    pub struct StubPalidatorTracker(IpAddr);

    impl StubPalidatorTracker {
        pub fn new(addr: IpAddr) -> Self {
            Self(addr)
        }
    }
    impl PalidatorTracker for StubPalidatorTracker {
        fn next_leaders(&self, _lookahead_leaders: usize) -> Vec<PaladinSocketAddrs> {
            let socks = pal_socks_from_ip(self.0);
            vec![socks]
        }
    }
}
