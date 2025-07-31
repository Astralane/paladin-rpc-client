use crate::leader_tracker::palidator_tracker::PalidatorTracker;
use crate::leader_tracker::types::PalSocketAddr;
use lru::LruCache;
use quinn::{Connection, Endpoint};
use solana_sdk::slot_history::Slot;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

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
    revert_protect_sender: tokio::sync::mpsc::Sender<TransactionBatch>,
    handles: Vec<tokio::task::JoinHandle<()>>,
    cancel: CancellationToken,
}

impl ConnectionWorkerInfo {
    pub fn new(
        sender: tokio::sync::mpsc::Sender<TransactionBatch>,
        revert_protect_sender: tokio::sync::mpsc::Sender<TransactionBatch>,
        handles: Vec<tokio::task::JoinHandle<()>>,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            sender,
            revert_protect_sender,
            handles,
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
            for transaction in &transaction_batch.transactions {
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
    peer: SocketAddr,
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
    let (revert_protect_sender, revert_protect_receiver) = tokio::sync::mpsc::channel(queue_size);
    let p3_worker = ConnectionWorker::new(
        endpoint.clone(),
        peer.p3_port,
        queue_size,
        receiver,
        max_reconnect_attempts,
        cancel.clone(),
    );
    let revert_protect_worker = ConnectionWorker::new(
        endpoint,
        peer.revert_protected_port,
        queue_size,
        revert_protect_receiver,
        max_reconnect_attempts,
        cancel.clone(),
    );

    let handles = [p3_worker, revert_protect_worker]
        .into_iter()
        .map(|mut worker| {
            tokio::spawn(async move {
                worker.run().await;
            })
        })
        .collect();

    ConnectionWorkerInfo::new(sender, revert_protect_sender, handles, cancel)
}

impl ConnectionWorker {
    pub fn new(
        endpoint: Arc<Endpoint>,
        peer: SocketAddr,
        queue_size: usize,
        receiver: tokio::sync::mpsc::Receiver<TransactionBatch>,
        max_reconnection_attempts: usize,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            endpoint,
            peer,
            state: ConnectionState::NotSetup,
            channel_size: queue_size,
            receiver,
            max_reconnection_attempts,
            cancel,
        }
    }

    pub async fn run(&mut self) {
        let cancel = self.cancel.clone();
        tokio::select! {
            _ = cancel.cancelled() => {
                debug!("Cancelled: Shutting down");
            },
            _ = self.handle_peer_connection() => {
                debug!("Connection worker finished");
            },
        }
    }

    async fn handle_peer_connection(&mut self) {
        const RETRY_SLEEP_INTERVAL: Duration = Duration::from_millis(100);
        loop {
            match &self.state {
                ConnectionState::NotSetup => {
                    self.create_connection(0).await;
                },
                ConnectionState::Active(conn) => {
                    let batch = match self.receiver.recv().await {
                        Some(batch) => batch,
                        None => {
                            break;
                        }
                    };
                }
                ConnectionState::Retry(num_reconnects) => {
                    if *num_reconnects > self.max_reconnection_attempts {
                        error!("Failed to establish connection: reach max reconnect attempts.");
                        self.state = ConnectionState::Closing;
                        continue;
                    }
                    sleep(RETRY_SLEEP_INTERVAL).await;
                    self.create_connection(*num_reconnects).await;
                }
                ConnectionState::Closing => {
                    break;
                }
            }
        }
    }

    async fn create_connection(&mut self, retry_attempts: usize) {
        let connect = self.endpoint.connect(self.peer, "paladin-connecting");
        let connecting = match connect {
            Ok(connecting) => connecting,
            Err(e) => {
                error!("Failed to connect to peer: {:?}", e);
                self.state = ConnectionState::Retry(retry_attempts + 1);
                return;
            }
        };

        let conn = match connecting.await {
            Ok(conn) => conn,
            Err(e) => {
                error!("Failed to connect to peer: {:?}", e);
                self.state = ConnectionState::Retry(retry_attempts + 1);
                return;
            }
        };
        self.state = ConnectionState::Active(conn);
    }
}
