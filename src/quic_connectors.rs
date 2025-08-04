use crate::quic::quic_networking::send_data_over_stream;
use crate::slot_watchers::recent_slots::RecentLeaderSlots;
use crate::utils::PalidatorTracker;
use anyhow::Context;
use lru::LruCache;
use quinn::{Connection, Endpoint};
use solana_sdk::slot_history::Slot;
use solana_sdk::transaction::VersionedTransaction;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinError;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

#[derive(Clone, Debug)]
pub struct TransactionInfo {
    pub wire_transaction: Vec<u8>,
    pub revert_protect: bool,
    pub slot_received: Slot,
}

impl TransactionInfo {
    pub fn new(wire_transaction: Vec<u8>, revert_protect: bool, slot_received: Slot) -> Self {
        Self {
            wire_transaction,
            revert_protect,
            slot_received,
        }
    }

    pub fn from_versioned_transaction(
        tx: VersionedTransaction,
        revert_protect: bool,
        slot_received: Slot,
    ) -> Self {
        let wire_transaction = bincode::serialize(&tx).unwrap();
        Self {
            wire_transaction,
            revert_protect,
            slot_received,
        }
    }
}
#[derive(Clone, Debug)]
pub struct TransactionBatch {
    pub transactions: Vec<TransactionInfo>,
}

impl TransactionBatch {
    pub fn new(transactions: Vec<TransactionInfo>) -> Self {
        Self { transactions }
    }
}

impl Into<Vec<Vec<u8>>> for TransactionBatch {
    fn into(self) -> Vec<Vec<u8>> {
        self.transactions
            .into_iter()
            .map(|item| item.wire_transaction)
            .collect::<Vec<_>>()
    }
}

pub struct ConnectionWorkerInfo {
    sender: tokio::sync::mpsc::Sender<Vec<Vec<u8>>>,
    handle: tokio::task::JoinHandle<()>,
    cancel: CancellationToken,
}

impl ConnectionWorkerInfo {
    pub fn new(
        sender: tokio::sync::mpsc::Sender<Vec<Vec<u8>>>,
        handle: tokio::task::JoinHandle<()>,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            sender,
            handle,
            cancel,
        }
    }

    pub fn try_send_transaction_batch(&self, batch: Vec<Vec<u8>>) -> anyhow::Result<()> {
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

pub struct ConnectionScheduler;

impl ConnectionScheduler {
    pub async fn new<T: PalidatorTracker>(
        leader_tracker: T,
        recent_slots: RecentLeaderSlots,
        endpoint: Arc<Endpoint>,
        leader_lookahead: usize,
        mut tx_receiver: tokio::sync::mpsc::Receiver<TransactionBatch>,
        worker_queue_size: usize,
        max_reconnection_attempts: usize,
        cancel: CancellationToken,
    ) -> Self {
        debug!("Starting connection scheduler");
        let mut workers_cache = WorkersCache::new(leader_lookahead * 2, cancel.clone());
        loop {
            let batch = tokio::select! {
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

            let current_slot = recent_slots.estimated_current_slot();
            let transactions = batch.transactions;
            for transaction in &transactions {
                // info!(
                //     "slot latency between current slot {} and received slot {} is {}",
                //     current_slot,
                //     transaction.slot_received,
                //     current_slot.saturating_sub(transaction.slot_received)
                // );
            }
            let (revert_protected_transactions, transactions): (Vec<_>, Vec<_>) =
                transactions.into_iter().partition(|tx| tx.revert_protect);

            let next_leaders = leader_tracker.next_leaders(leader_lookahead);

            //create connections with the leader list
            for leader in &next_leaders {
                for socks in leader.iter() {
                    if workers_cache.contains(socks) {
                        continue;
                    }
                    debug!("Creating new worker for leader: {:?}", socks);
                    let worker = spawn_new(
                        endpoint.clone(),
                        *socks,
                        worker_queue_size,
                        max_reconnection_attempts,
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
                match workers_cache.get(leader) {
                    Some(worker) => Self::try_send_to_worker(leader, worker, txns),
                    None => debug!("No worker for current leader: {:?}", leader),
                }
            }
        }
        Self {}
    }

    fn try_send_to_worker(
        peer: &SocketAddr,
        worker: &mut ConnectionWorkerInfo,
        batch: Vec<TransactionInfo>,
    ) {
        debug!("Sending transaction batch to worker: {:?}", peer);
        let batch = batch
            .into_iter()
            .map(|item| item.wire_transaction)
            .collect();
        if let Err(e) = worker.try_send_transaction_batch(batch) {
            error!(
                "Failed to send transaction batch to worker: {:?}, error {:?}",
                peer, e
            );
        }
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
    receiver: tokio::sync::mpsc::Receiver<Vec<Vec<u8>>>,
    max_reconnection_attempts: usize,
}

pub fn spawn_new(
    endpoint: Arc<Endpoint>,
    peer: SocketAddr,
    queue_size: usize,
    max_reconnect_attempts: usize,
) -> ConnectionWorkerInfo {
    let cancel = CancellationToken::new();
    let (sender, receiver) = tokio::sync::mpsc::channel(queue_size);
    let mut worker = ConnectionWorker::new(
        endpoint.clone(),
        peer,
        queue_size,
        receiver,
        max_reconnect_attempts,
        cancel.clone(),
    );

    let handle = tokio::spawn(async move {
        worker.run().await;
    });

    ConnectionWorkerInfo::new(sender, handle, cancel)
}

impl ConnectionWorker {
    pub fn new(
        endpoint: Arc<Endpoint>,
        peer: SocketAddr,
        queue_size: usize,
        receiver: tokio::sync::mpsc::Receiver<Vec<Vec<u8>>>,
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

    async fn send_transactions(
        &mut self,
        connection: quinn::Connection,
        transactions: Vec<Vec<u8>>,
    ) {
        let start = std::time::Instant::now();
        let tx_len = transactions.len();
        for tx in transactions {
            match send_data_over_stream(&connection, &tx).await {
                Ok(_) => {}
                Err(e) => {
                    self.state = ConnectionState::Retry(0);
                    error!("Failed to send transaction over uni-stream: {:?}", e);
                }
            }
        }
        let duration = start.elapsed();
        debug!(
            "total time to stream {} transactions: {:?} ms",
            tx_len,
            duration.as_millis()
        );
    }

    async fn handle_peer_connection(&mut self) {
        const RETRY_SLEEP_INTERVAL: Duration = Duration::from_millis(100);
        loop {
            match &self.state {
                ConnectionState::NotSetup => {
                    debug!("conn not setup yet");
                    self.create_connection(0).await;
                }
                ConnectionState::Active(conn) => {
                    debug!("conn active");
                    let batch = match self.receiver.recv().await {
                        Some(batch) => batch,
                        None => {
                            continue;
                        }
                    };
                    self.send_transactions(conn.clone(), batch.into()).await
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

#[cfg(test)]
pub mod test {
    use super::*;
}
