use crate::connection_worker::spawn_new_connection_worker;
use crate::quic::quic_networking::send_data_over_stream;
use crate::slot_watchers::recent_slots::RecentLeaderSlots;
use crate::utils::PalidatorTracker;
use anyhow::Context;
use bytes::Bytes;
use lru::LruCache;
use quinn::{Connection, Endpoint};
use solana_sdk::transaction::VersionedTransaction;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::task::JoinError;
use tokio::time::{sleep, Instant};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

#[derive(Clone, Debug)]
pub struct PaladinPacket {
    //transaction data
    pub data: Bytes,
    pub revert_protect: bool,
    pub created_at: Instant,
}

impl PaladinPacket {
    pub fn new(data: Bytes, revert_protect: bool) -> Self {
        Self {
            data,
            revert_protect,
            created_at: Instant::now(),
        }
    }
}

pub struct ConnectionWorkerInfo {
    sender: tokio::sync::mpsc::Sender<Vec<PaladinPacket>>,
    handle: tokio::task::JoinHandle<()>,
    cancel: CancellationToken,
}

impl ConnectionWorkerInfo {
    pub fn new(
        sender: tokio::sync::mpsc::Sender<Vec<PaladinPacket>>,
        handle: tokio::task::JoinHandle<()>,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            sender,
            handle,
            cancel,
        }
    }

    pub fn try_send_transaction_batch(&self, batch: Vec<PaladinPacket>) -> anyhow::Result<()> {
        self.sender
            .try_send(batch)
            .context("Failed to send transaction batch")?;
        Ok(())
    }

    pub async fn shutdown(self) -> Result<(), JoinError> {
        self.cancel.cancel();
        drop(self.sender);
        self.handle.await
    }
}

pub struct WorkersCache {
    workers: LruCache<SocketAddr, ConnectionWorkerInfo>,
    cancel: CancellationToken,
}

impl WorkersCache {
    pub fn new(capacity: usize, cancel: CancellationToken) -> Self {
        Self {
            workers: LruCache::new(capacity.try_into().unwrap()),
            cancel,
        }
    }

    pub fn contains(&self, peer: &SocketAddr) -> bool {
        self.workers.contains(peer)
    }

    pub fn push(
        &mut self,
        peer: SocketAddr,
        worker: ConnectionWorkerInfo,
    ) -> Option<(SocketAddr, ConnectionWorkerInfo)> {
        self.workers.push(peer, worker)
    }

    pub fn get(&mut self, peer: &SocketAddr) -> Option<&mut ConnectionWorkerInfo> {
        self.workers.get_mut(peer)
    }
}

pub struct ConnectionScheduler<T> {
    leader_tracker: T,
    recent_slots: RecentLeaderSlots,
    endpoint: Arc<Endpoint>,
    leader_lookahead: usize,
    tx_receiver: tokio::sync::mpsc::Receiver<Vec<PaladinPacket>>,
    worker_queue_size: usize,
    max_reconnection_attempts: usize,
    cancel: CancellationToken,
}

impl<T> ConnectionScheduler<T>
where
    T: PalidatorTracker,
{
    pub fn new(
        leader_tracker: T,
        recent_slots: RecentLeaderSlots,
        endpoint: Arc<Endpoint>,
        leader_lookahead: usize,
        tx_receiver: tokio::sync::mpsc::Receiver<Vec<PaladinPacket>>,
        worker_queue_size: usize,
        max_reconnection_attempts: usize,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            leader_tracker,
            recent_slots,
            endpoint,
            leader_lookahead,
            tx_receiver,
            worker_queue_size,
            max_reconnection_attempts,
            cancel,
        }
    }

    pub async fn run(&mut self) {
        let mut workers_cache = WorkersCache::new(self.leader_lookahead * 2, self.cancel.clone());
        loop {
            let packet_batch = tokio::select! {
                _ = self.cancel.cancelled() => {
                    debug!("Cancelled: Shutting down");
                    break
                },
                batch = self.tx_receiver.recv() => match batch {
                    Some(batch) => batch,
                    None => {
                        // sender has been dropped
                        break;
                    },
                },
            };

            if packet_batch.is_empty() {
                continue;
            }

            debug!(
                "received packet batch in connection scheduler {:}",
                packet_batch.len()
            );

            let current_slot = self.recent_slots.estimated_current_slot();
            let (revert_protected_transactions, transactions): (Vec<_>, Vec<_>) =
                packet_batch.into_iter().partition(|tx| tx.revert_protect);

            let next_leaders = self.leader_tracker.next_leaders(self.leader_lookahead);

            //create connections with the leader list
            for leader in &next_leaders {
                for socks in leader.iter() {
                    if workers_cache.contains(socks) {
                        continue;
                    }
                    debug!("Creating new worker for leader: {:?}", socks);
                    let worker = spawn_new_connection_worker(
                        self.endpoint.clone(),
                        *socks,
                        self.worker_queue_size,
                        self.max_reconnection_attempts,
                    );
                    if let Some((sock, worker)) = workers_cache.push(*socks, worker) {
                        tokio::spawn(async move {
                            if let Err(e) = worker.shutdown().await {
                                error!("Failed to shutdown worker: {:?} for leader {:}", e, sock);
                            }
                        });
                    }
                }
            }

            let Some(current_leader) = next_leaders.first() else {
                continue;
            };

            // Send txns to appropriate workers
            for (leader, txns) in [
                (&current_leader[0], transactions),
                (&current_leader[1], revert_protected_transactions),
            ] {
                if txns.is_empty() {
                    continue;
                }
                match workers_cache.get(leader) {
                    Some(worker) => Self::try_send_to_worker(leader, worker, txns),
                    None => debug!("No worker for current leader: {:?}", leader),
                }
            }
        }
    }

    fn try_send_to_worker(
        peer: &SocketAddr,
        worker: &mut ConnectionWorkerInfo,
        batch: Vec<PaladinPacket>,
    ) {
        debug!("Sending transaction batch to worker: {:?}", peer);
        if let Err(e) = worker.try_send_transaction_batch(batch) {
            error!(
                "Failed to send transaction batch to worker: {:?}, error {:?}",
                peer, e
            );
        }
    }
}
