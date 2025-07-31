use crate::leader_tracker::palidator_tracker::PalidatorTracker;
use crate::leader_tracker::types::PalSocketAddr;
use lru::LruCache;
use quinn::{Connection, Endpoint};
use solana_sdk::slot_history::Slot;
use std::net::SocketAddr;
use std::sync::{mpsc, Arc};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

pub struct TransactionInfo {
    pub wire_transaction: Vec<u8>,
    pub revert_protect: bool,
    pub slot_received: Slot,
}
pub struct TransactionBatch {
    pub transactions: Vec<TransactionInfo>,
}
pub struct ConnectionWorkerInfo {
    sender: tokio::sync::mpsc::Sender<TransactionBatch>,
    handle: tokio::task::JoinHandle<()>,
    cancel: CancellationToken,
}

impl ConnectionWorkerInfo {
    pub fn new(
        sender: tokio::sync::mpsc::Sender<TransactionBatch>,
        handle: tokio::task::JoinHandle<()>,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            sender,
            handle,
            cancel,
        }
    }
}

pub struct WorkersCache {
    workers: LruCache<PalSocketAddr, ConnectionWorkerInfo>,
    cancel: CancellationToken,
}

impl WorkersCache {
    pub fn new(capacity: usize, cancel: CancellationToken) -> Self {
        Self {
            workers: LruCache::new(capacity.try_into().unwrap()),
            cancel,
        }
    }

    pub fn push(
        &mut self,
        peer: PalSocketAddr,
        worker: ConnectionWorkerInfo,
    ) -> Option<(PalSocketAddr, ConnectionWorkerInfo)> {
        self.workers.push(peer, worker)
    }

    pub fn get(&mut self, peer: &PalSocketAddr) -> Option<&mut ConnectionWorkerInfo> {
        self.workers.get_mut(peer)
    }
}

pub struct ConnectionScheduler;

impl ConnectionScheduler {
    pub async fn new(
        leader_tracker: PalidatorTracker,
        endpoint: Arc<Endpoint>,
        num_connections: usize,
        mut tx_receiver: tokio::sync::mpsc::Receiver<TransactionBatch>,
        cancel: CancellationToken,
    ) -> Self {
        let mut workers_cache = WorkersCache::new(num_connections, cancel.clone());
        loop {
            let transaction_batch = tokio::select! {
                _ = cancel.cancelled() => {
                    debug!("Cancelled: Shutting down");
                    break
                },
                batch = tx_receiver.recv() => match batch {
                    Some(batch) => batch,
                    None => {
                        // sender has been dropped
                        break;
                    },
                },
            };

            let current_slot = leader_tracker.recent_slots.estimated_current_slot();
            for transaction in transaction_batch.transactions {
                info!(
                    "slot latency between current slot {} and received slot {} is {}",
                    current_slot,
                    transaction.slot_received,
                    current_slot - transaction.slot_received
                );
            }
            let wire_transactions = transaction_batch
                .transactions
                .into_iter()
                .map(|transaction| transaction.wire_transaction)
                .collect::<Vec<_>>();

            let leaders = leader_tracker.get_closest_leaders(num_connections);
            let Some((current_leader, next_leaders)) = leaders.split_first() else {
                continue;
            };

            if let Some(peer) = current_leader {
                let worker = workers_cache.get(peer);
            }
        }
        Self {}
    }
}

enum ConnectionState {
    NotSetup,
    Active(Connection),
    Retry(usize),
    Closing,
}

pub struct ConnectionWorker {
    endpoint: Arc<Endpoint>,
    peer: PalSocketAddr,
    state: ConnectionState,
    channel_size: usize,
    cancel: CancellationToken,
    receiver: tokio::sync::mpsc::Receiver<TransactionBatch>,
    max_reconnection_attempts: usize,
}

pub fn spawn_new(
    endpoint: Arc<Endpoint>,
    peer: PalSocketAddr,
    queue_size: usize,
    max_reconnect_attempts: usize,
) -> ConnectionWorkerInfo {
    let cancel = CancellationToken::new();
    let (sender, receiver) = tokio::sync::mpsc::channel(queue_size);
    let mut connection_worker = ConnectionWorker::new(
        endpoint,
        peer,
        queue_size,
        receiver,
        max_reconnect_attempts,
        cancel.clone(),
    );
    let handle = tokio::spawn(async move {
        connection_worker.run();
    });
    ConnectionWorkerInfo::new(sender, handle, cancel)
}

impl ConnectionWorker {
    pub fn new(
        endpoint: Arc<Endpoint>,
        peer: PalSocketAddr,
        queue_size: usize,
        receiver: tokio::sync::mpsc::Receiver<TransactionBatch>,
        max_reconnection_attempts: usize,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            endpoint,
            peer,
            channel_size: queue_size,
            receiver,
            max_reconnection_attempts,
            cancel,
        }
    }
    pub fn run(&mut self) {
        loop {
            let batch = tokio::select! {
                _ = self.cancel.cancelled() => break,
                batch = self.receiver.recv() => match batch {
                    None => {
                        break;
                    }
                    Some(batch) => batch
                }
            };
        }
    }

    fn create_connection(&mut self, peer: SocketAddr) {
        let connecting = self.endpoint.connect(peer);
    }
}
