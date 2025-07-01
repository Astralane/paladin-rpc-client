use crate::constants::{PAL_PORT_1, PAL_PORT_2};
use quinn::Endpoint;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_rpc_client_api::response::RpcContactInfo;
use solana_sdk::clock::Slot;
use solana_sdk::commitment_config::CommitmentConfig;
use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::ops::Add;
use tracing::info;

// contains all paladin
pub struct PalidatorCache {
    pub epoch: u64,
    pub epoch_start_slot: Slot,
    pub palidators: Vec<RpcContactInfo>,
    pub palidator_schedule: HashMap<String, Vec<usize>>,
    pub slot_schedule: BTreeMap<Slot, String>,
}

impl PalidatorCache {
    pub async fn load_latest(rpc: &RpcClient, endpoint: &Endpoint) -> anyhow::Result<Self> {
        let start_slot = rpc
            .get_slot_with_commitment(CommitmentConfig::processed())
            .await?;
        let epoch_info = rpc.get_epoch_info().await?;
        let epoch_start_slot = epoch_info.absolute_slot - epoch_info.slot_index;
        let leader_schedule = rpc.get_leader_schedule(Some(start_slot)).await?.unwrap();
        let leader_keys = leader_schedule.keys().cloned().collect::<Vec<_>>();
        let cluster_nodes = rpc.get_cluster_nodes().await?;

        let palidators_keys = Self::find_palidators(&endpoint, &leader_keys, &cluster_nodes).await;
        let palidators = cluster_nodes
            .iter()
            .filter(|item| palidators_keys.contains(&item.pubkey))
            .cloned()
            .collect::<Vec<_>>();

        let mut palidator_schedule = leader_schedule
            .into_iter()
            .filter(|(leader_pk, slots)| palidators_keys.contains(leader_pk))
            .collect::<HashMap<_, _>>();

        let mut slot_schedule = BTreeMap::new();

        for (key, value) in palidator_schedule.iter_mut() {
            for slot in value {
                *slot = slot.saturating_add(epoch_start_slot as usize);
                slot_schedule.insert(*slot as u64, key.clone());
            }
        }

        Ok(Self {
            epoch: epoch_info.epoch,
            epoch_start_slot,
            palidator_schedule,
            palidators,
            slot_schedule,
        })
    }

    async fn find_palidators(
        my_endpoint: &Endpoint,
        leader_keys: &[String],
        cluster_nodes: &[RpcContactInfo],
    ) -> Vec<String> {
        // creates batches of 500 keys,
        let leader_nodes = cluster_nodes
            .iter()
            .filter(|item| leader_keys.contains(&item.pubkey))
            .collect::<Vec<_>>();

        let mut results = Vec::new();
        let total = leader_nodes.len();
        let mut connected_num = 0;
        let batches = leader_nodes.chunks(500);
        for batch in batches {
            info!(
                "tried connection to {:}/{:} validators",
                connected_num, total,
            );
            let batch_fut = batch
                .iter()
                .map(|node| Box::pin(Self::try_connect_to_palidator(&my_endpoint, node)))
                .collect::<Vec<_>>();
            let result = futures::future::join_all(batch_fut).await;
            connected_num = connected_num.add(batch.len());
            results.extend(result);
        }
        results.into_iter().flatten().collect()
    }

    pub async fn try_connect_to_palidator(
        endpoint: &Endpoint,
        node: &RpcContactInfo,
    ) -> Option<String> {
        let key = node.pubkey.clone();
        let to_1 = node.tpu_quic.map(|s| SocketAddr::new(s.ip(), PAL_PORT_1))?;
        let to_2 = node.tpu_quic.map(|s| SocketAddr::new(s.ip(), PAL_PORT_2))?;
        if let Ok(connecting) = endpoint.connect(to_1, "connect") {
            if connecting.await.is_ok() {
                return Some(key);
            }
        }
        if let Ok(connecting) = endpoint.connect(to_2, "connect") {
            if connecting.await.is_ok() {
                return Some(key);
            }
        }

        None
    }

    pub fn get_all_palidator_keys(&self) -> Vec<String> {
        self.palidators
            .iter()
            .map(|item| item.pubkey.to_string())
            .collect()
    }

    pub fn get_next_palidator_with_slot(&self, curr_slot: Slot) -> Option<(Slot, String)> {
        let queue = &self.slot_schedule;
        let (slot, pk) = queue.range(curr_slot..).next()?;
        Some((*slot as Slot, pk.clone()))
    }
}
