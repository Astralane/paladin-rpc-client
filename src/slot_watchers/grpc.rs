use crate::slot_watchers::recent_slots::RecentLeaderSlots;
use anyhow::Context;
use futures_util::{SinkExt, StreamExt};
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::error;
use yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof;
use yellowstone_grpc_proto::geyser::{
    SlotStatus, SubscribeRequest, SubscribeRequestFilterSlots, SubscribeUpdateSlot,
};
use yellowstone_grpc_proto::tonic::codec::CompressionEncoding;

pub struct GrpcSlotWatcher;

impl GrpcSlotWatcher {
    pub fn spawn_slot_watcher(
        grpc_url: String,
        cancel: CancellationToken,
        recent_slots: RecentLeaderSlots,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            if let Err(e) = Self::slot_watcher_loop(grpc_url, recent_slots, cancel).await {
                error!("Failed to watch slot updates: {}", e);
            }
        })
    }

    pub async fn slot_watcher_loop(
        grpc_url: String,
        recent_slots: RecentLeaderSlots,
        cancel: CancellationToken,
    ) -> anyhow::Result<()> {
        let mut client = yellowstone_grpc_client::GeyserGrpcClient::build_from_shared(grpc_url)?
            .accept_compressed(CompressionEncoding::Gzip)
            .max_decoding_message_size(1024 * 1024 * 4)
            .tcp_keepalive(Some(Duration::from_secs(15)))
            .connect()
            .await
            .map_err(anyhow::Error::from)?;

        let slot_sub_request = vec![(
            "slot_sub".to_string(),
            SubscribeRequestFilterSlots {
                filter_by_commitment: Some(true),
                interslot_updates: Some(true),
            },
        )]
        .into_iter()
        .collect();

        let subscribe_request = SubscribeRequest {
            slots: slot_sub_request,
            ..Default::default()
        };

        let (mut sink, mut stream) = client
            .subscribe()
            .await
            .context("cannot subscribe to stream")?;

        sink.send(subscribe_request)
            .await
            .map_err(anyhow::Error::from)?;

        loop {
            let update = tokio::select! {
                _ = cancel.cancelled() => break,
                update = stream.next() => match update {
                    Some(update) => update,
                    None => {
                        error!("grpc stream ended unexpectedly");
                        break;
                    },
                },
            };
            // info!("Received update: {:?}", update);
            match update {
                Ok(update) => {
                    match update.update_oneof {
                        Some(UpdateOneof::Slot(SubscribeUpdateSlot { slot, status, .. })) => {
                            let current_slot = if status == SlotStatus::SlotCompleted as i32 {
                                slot + 1
                            } else if status == SlotStatus::SlotFirstShredReceived as i32 {
                                slot
                            } else {
                                continue;
                            };
                            recent_slots.record_slot(slot)
                        }
                        _ => continue,
                    };
                }
                Err(e) => {
                    error!("error receiving response: {:?}", e);
                    break;
                }
            }
        }
        Ok(())
    }
}
