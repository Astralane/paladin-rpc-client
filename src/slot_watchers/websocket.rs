use crate::slot_watchers::recent_slots::RecentLeaderSlots;
use futures_util::StreamExt;
use solana_client::nonblocking::pubsub_client::PubsubClient;
use solana_client::rpc_response::SlotUpdate;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{error, warn};

pub struct WebsocketSlotWatcher;

impl WebsocketSlotWatcher {
    pub fn spawn_slot_watcher(
        ws_url: String,
        cancel: CancellationToken,
        recent_slots: RecentLeaderSlots,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            if let Err(e) = Self::slot_watcher_loop(ws_url, cancel, recent_slots).await {
                error!("Failed to watch slot updates: {}", e);
            }
        })
    }
    pub async fn slot_watcher_loop(
        ws_url: String,
        cancel: CancellationToken,
        recent_slots: RecentLeaderSlots,
    ) -> anyhow::Result<()> {
        const RETRY_INTERVAL: Duration = Duration::from_secs(5);
        let client = PubsubClient::new(&ws_url).await?;
        'outer: loop {
            let (mut stream, unsub) = tokio::select! {
                _ = cancel.cancelled() => {
                    break
                }
                res = client.slot_updates_subscribe() => {
                    match res{
                        Ok(sub) => sub,
                        Err(e) => {
                            error!("Failed to subscribe to slot updates: {}", e);
                            tokio::time::sleep(RETRY_INTERVAL).await;
                            continue;
                        }
                    }
                }
            };

            loop {
                let item = tokio::select! {
                    _ = cancel.cancelled() => {
                        break 'outer
                    }
                    res = stream.next() => {
                        match res {
                            Some(item) => item,
                            None => {
                                // the stream has ended
                                break;
                            }
                        }
                    }
                };

                let curr_slot = match item {
                    SlotUpdate::FirstShredReceived { slot, .. } => slot,
                    SlotUpdate::Completed {  .. } => continue,
                    _ => continue,
                };
                recent_slots.record_slot(curr_slot);
            }
            warn!("Unsubscribing from slot updates and retrying...");
            unsub().await;
            tokio::time::sleep(RETRY_INTERVAL).await;
        }

        Ok(())
    }
}
