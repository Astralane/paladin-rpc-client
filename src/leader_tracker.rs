use dashmap::DashMap;
use std::sync::Arc;
use tokio::time::{interval, Duration};
use serde::Deserialize;
use tracing::info;

#[derive(Clone)]
pub struct ValidatorState(pub Arc<DashMap<String, u64>>); // identity -> next_leader_slot

impl ValidatorState {
    pub fn new() -> Self {
        Self(Arc::new(DashMap::new()))
    }
    pub fn get_next_leader(&self) -> Vec<(String, u64)> {
        self.0.iter().map(|kv| (kv.key().clone(), *kv.value())).collect()
    }
}

#[derive(Deserialize)]
struct LeaderResponse {
    identity: String,
    next_slot: u64,
}

pub async fn poll_leader_schedule(state: ValidatorState) {
    let mut interval = interval(Duration::from_secs(1));
    loop {
        interval.tick().await;

        // Placeholder — replace with actual gRPC or JSON-RPC call
        let validators = vec![
            LeaderResponse { identity: "validator1".into(), next_slot: 123 },
            LeaderResponse { identity: "validator2".into(), next_slot: 124 },
        ];

        for val in validators {
            state.0.insert(val.identity, val.next_slot);
        }

        info!(?validators, "Updated leader schedule");
    }
}