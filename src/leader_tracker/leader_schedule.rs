use crate::constants::{PAL_PORT, PAL_PORT_MEV_PROTECT};
use crate::leader_tracker::types::{pal_socks_from_ip, PaladinSocketAddrs};
use quinn::Endpoint;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_rpc_client_api::response::RpcContactInfo;
use solana_sdk::clock::Slot;
use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::ops::Add;
use tracing::info;

// contains all paladin leaders
pub struct PalidatorSchedule {
    pub epoch: u64,
    pub epoch_start_slot: Slot,
    // contact info of all palidators
    pub palidators: Vec<RpcContactInfo>,
    // palidator slots and contact info
    pub slot_schedule: BTreeMap<Slot, RpcContactInfo>,
}

impl PalidatorSchedule {
    pub async fn load_latest_by_quic_connect(
        rpc: &RpcClient,
        endpoint: &Endpoint,
    ) -> anyhow::Result<Self> {
        let epoch_info = rpc.get_epoch_info().await?;
        let epoch_start_slot = epoch_info.absolute_slot - epoch_info.slot_index;
        let leader_schedule = rpc.get_leader_schedule(None).await?.unwrap();
        let leader_keys = leader_schedule.keys().cloned().collect::<Vec<_>>();
        let cluster_nodes = rpc.get_cluster_nodes().await?;

        let palidators_keys =
            Self::find_palidators_by_quic_connect(&endpoint, &leader_keys, &cluster_nodes).await;
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
                let contact = palidators
                    .iter()
                    .find(|item| item.pubkey == key.clone())
                    .cloned();
                slot_schedule.insert(*slot as u64, contact.unwrap());
            }
        }

        Ok(Self {
            epoch: epoch_info.epoch,
            epoch_start_slot,
            palidators,
            slot_schedule,
        })
    }

    async fn find_palidators_by_quic_connect(
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
                .map(|node| Box::pin(Self::try_connect(&my_endpoint, node)))
                .collect::<Vec<_>>();
            let result = futures::future::join_all(batch_fut).await;
            connected_num = connected_num.add(batch.len());
            results.extend(result);
        }
        results.into_iter().flatten().collect()
    }

    async fn try_connect(endpoint: &Endpoint, node: &RpcContactInfo) -> Option<String> {
        let key = node.pubkey.clone();
        let ip = node.gossip?.ip();

        for port in [PAL_PORT, PAL_PORT_MEV_PROTECT] {
            let addr = SocketAddr::new(ip, port);
            if let Ok(connecting) = endpoint.connect(addr, "connect") {
                if let Ok(connection) = connecting.await {
                    connection.close(0u32.into(), b"Closing connection");
                    return Some(key);
                }
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

    pub fn get_next_palidator_leader(
        &self,
        curr_slot: Slot,
        lookout_num: usize,
    ) -> Vec<Option<PaladinSocketAddrs>> {
        self.slot_schedule
            .range(curr_slot..)
            .take(lookout_num)
            .map(|(slot, contact)| {
                let socks = contact
                    .gossip
                    .as_ref()
                    .map(|addr| pal_socks_from_ip(addr.ip()));
                socks
            })
            .collect()
    }
}
