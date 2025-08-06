use crate::constants::{
    ASTRALANE_PALADIN_API, PALADIN_LEADERS_API, PAL_PORT, PAL_PORT_MEV_PROTECT,
};
use crate::leader_tracker::types::{pal_socks_from_ip, PaladinSocketAddrs};
use quinn::Endpoint;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_rpc_client_api::response::RpcContactInfo;
use solana_sdk::clock::Slot;
use std::collections::{BTreeMap, HashMap};
use std::net::{IpAddr, SocketAddr};
use std::ops::Add;
use std::str::FromStr;
use tracing::info;

// contains all paladin leaders
pub struct PalidatorSchedule {
    pub epoch: u64,
    pub epoch_start_slot: Slot,
    // contact info of all palidators
    pub palidators: Vec<PaladinContactInfo>,
    // palidator slots and contact info
    pub slot_schedule: BTreeMap<Slot, PaladinContactInfo>,
}

#[derive(Debug, Clone)]
pub struct PaladinContactInfo {
    pub pubkey: String,
    pub ip_address: IpAddr,
}

#[derive(Debug, Default)]
pub enum LoadMethod {
    #[default]
    AstralaneApi,
    PaladinApi,
    QuicConnect,
}

impl PalidatorSchedule {
    pub async fn load_latest(
        rpc: &RpcClient,
        endpoint: &Endpoint,
        load_method: LoadMethod,
    ) -> anyhow::Result<Self> {
        let epoch_info = rpc.get_epoch_info().await?;
        let epoch_start_slot = epoch_info.absolute_slot - epoch_info.slot_index;
        let leader_schedule = rpc.get_leader_schedule(None).await?.unwrap();
        let leader_keys = leader_schedule.keys().cloned().collect::<Vec<_>>();
        let cluster_nodes = rpc.get_cluster_nodes().await?;
        let palidators = match load_method {
            LoadMethod::AstralaneApi => {
                let palidators_result = reqwest::get(ASTRALANE_PALADIN_API).await?;
                let paladin_keys = palidators_result.json::<HashMap<String, String>>().await?;
                paladin_keys
                    .iter()
                    .map(|(key, ip_str)| PaladinContactInfo {
                        pubkey: key.to_string(),
                        ip_address: IpAddr::from_str(&ip_str).unwrap(),
                    })
                    .collect::<Vec<_>>()
            }
            LoadMethod::PaladinApi => {
                let palidators_result = reqwest::get(PALADIN_LEADERS_API).await?;
                let paladin_keys = palidators_result.json::<Vec<String>>().await?;
                Self::find_palidators_by_quic_connect(&endpoint, &paladin_keys, &cluster_nodes)
                    .await
            }
            LoadMethod::QuicConnect => {
                Self::find_palidators_by_quic_connect(&endpoint, &leader_keys, &cluster_nodes).await
            }
        };
        let palidators_keys = palidators
            .iter()
            .map(|item| item.pubkey.clone())
            .collect::<Vec<_>>();

        let mut palidator_schedule = leader_schedule
            .into_iter()
            .filter(|(leader_pk, slots)| palidators_keys.contains(leader_pk))
            .collect::<HashMap<_, _>>();

        let mut slot_schedule = BTreeMap::new();

        for (key, value) in palidator_schedule.iter_mut() {
            for slot in value {
                *slot = slot.saturating_add(epoch_start_slot as usize);
                let contact = palidators.iter().find(|item| &item.pubkey == key).cloned();
                //palidator schedule is crated from the palidators list, all elements in
                //palidator_schedule should be present in palidators list
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
    ) -> Vec<PaladinContactInfo> {
        // creates batches of 500 keys,
        let leader_nodes = cluster_nodes
            .iter()
            .filter(|item| leader_keys.contains(&item.pubkey))
            .collect::<Vec<_>>();

        let mut results = Vec::new();
        let total = leader_nodes.len();
        let mut connected_num = 0;
        let batches = leader_nodes.chunks(100);
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
            info!("connected to {:}/{:} validators", connected_num, total);
            results.extend(result);
        }
        results.into_iter().flatten().collect()
    }

    async fn try_connect(endpoint: &Endpoint, node: &RpcContactInfo) -> Option<PaladinContactInfo> {
        let key = node.pubkey.clone();
        let ip = node.gossip?.ip();

        for port in [PAL_PORT, PAL_PORT_MEV_PROTECT] {
            let addr = SocketAddr::new(ip, port);
            if let Ok(connecting) = endpoint.connect(addr, "connect") {
                if let Ok(connection) = connecting.await {
                    connection.close(0u32.into(), b"Closing connection");
                    return Some(PaladinContactInfo {
                        pubkey: key,
                        ip_address: addr.ip(),
                    });
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
            .map(|(slot, contact)| Some(pal_socks_from_ip(contact.ip_address)))
            .collect()
    }
}

#[cfg(test)]
pub mod test {
    use super::*;
    use crate::quic::quic_networking::setup_quic_endpoint;
    use solana_sdk::signature::Keypair;

    #[tokio::test]
    pub async fn load_from_paladin_list() {
        let keypair = Keypair::new();
        let bind_address = "0.0.0.0:0".parse().unwrap();
        let rpc_url = "http://rpc:8899";
        let rpc_client = RpcClient::new(rpc_url.to_owned());
        let endpoint = setup_quic_endpoint(bind_address, &keypair).unwrap();
        let list = PalidatorSchedule::load_latest(&rpc_client, &endpoint, LoadMethod::PaladinApi)
            .await
            .unwrap();
        println!("paladin list {:?}", list.get_all_palidator_keys());
    }

    #[tokio::test]
    pub async fn load_from_astralane_paladin_list() {
        let keypair = Keypair::new();
        let rpc_url = "http://rpc:8899";
        let bind_address = "0.0.0.0:0".parse().unwrap();
        let rpc_client = RpcClient::new(rpc_url.to_owned());
        let endpoint = setup_quic_endpoint(bind_address, &keypair).unwrap();
        let list = PalidatorSchedule::load_latest(&rpc_client, &endpoint, LoadMethod::AstralaneApi)
            .await
            .unwrap();
        println!("paladin list {:?}", list.get_all_palidator_keys());
    }
}
