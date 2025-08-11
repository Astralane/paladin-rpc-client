use crate::slot_watchers::grpc::GrpcSlotWatcher;
use crate::slot_watchers::recent_slots::RecentLeaderSlots;
use crate::slot_watchers::websocket::WebsocketSlotWatcher;
use tokio_util::sync::CancellationToken;

mod grpc;
pub mod recent_slots;
mod websocket;

pub struct SlotWatcher {
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl SlotWatcher {
    pub fn run_slot_watchers(
        ws_urls: Vec<String>,
        grpc_urls: Vec<String>,
        recent_slots: RecentLeaderSlots,
        cancel: CancellationToken,
    ) -> Self {
        let ws_handles = ws_urls
            .into_iter()
            .map(|url| {
                WebsocketSlotWatcher::spawn_slot_watcher(url, cancel.clone(), recent_slots.clone())
            })
            .collect::<Vec<_>>();
        let grpc_handles = grpc_urls
            .into_iter()
            .map(|url| {
                GrpcSlotWatcher::spawn_slot_watcher(url, cancel.clone(), recent_slots.clone())
            })
            .collect::<Vec<_>>();
        let tasks = ws_handles
            .into_iter()
            .chain(grpc_handles)
            .collect::<Vec<_>>();

        Self { tasks }
    }

    pub async fn join(&mut self) {
        while let Some(task) = self.tasks.pop() {
            task.await.unwrap();
        }
    }
}
