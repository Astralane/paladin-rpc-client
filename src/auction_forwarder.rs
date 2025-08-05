use crate::quic_connection_workers::PaladinPacket;
use crate::types::VerifiedTransaction;
use core_affinity::CoreId;
use crossbeam_channel::{Receiver, RecvTimeoutError};
use std::collections::VecDeque;
use std::thread::{Builder, JoinHandle};
use std::time::Duration;
use tokio::sync::mpsc::error::TrySendError;
use tokio_util::sync::CancellationToken;
use tracing::error;

/// Runs auction and forwards transactions to the quic clients.
/// Transactions are processed in batches and sent to TPU client
/// only after they have been held for at least one auction duration.

pub struct AuctionAndForwardStage {
    threads: Vec<JoinHandle<()>>,
}

impl AuctionAndForwardStage {
    pub fn spawn_new(
        auction_duration: Duration,
        verified_transaction_receiver: Receiver<VerifiedTransaction>,
        tpu_client_sender: tokio::sync::mpsc::Sender<Vec<PaladinPacket>>,
        affinity: Option<Vec<usize>>,
        cancel: CancellationToken,
        batch_size: usize,
        num_threads: usize,
    ) -> Self {
        const RECV_TIMEOUT: Duration = Duration::from_millis(10);
        let threads = (0..num_threads)
            .map(|id| {
                let transaction_receiver = verified_transaction_receiver.clone();
                let tpu_client_sender = tpu_client_sender.clone();
                let cancel = cancel.clone();
                let mut batch = Vec::with_capacity(batch_size);
                let mut last_auction_time = std::time::Instant::now();
                let core_id = affinity.as_ref().map(|affinity| CoreId {
                    id: affinity[id % affinity.len()],
                });
                Builder::new()
                    .name(format!("forward-and-auction-thread-{}", id))
                    .spawn(move || {
                        if let Some(core_id) = core_id {
                            core_affinity::set_for_current(core_id);
                        }
                        let mut transaction_buffer: VecDeque<VerifiedTransaction> = VecDeque::new();
                        while !cancel.is_cancelled() {
                            match transaction_receiver.recv_timeout(RECV_TIMEOUT) {
                                Ok(transactions) => {
                                    transaction_buffer.push_back(transactions);
                                }
                                Err(error) => match error {
                                    RecvTimeoutError::Timeout => {}
                                    RecvTimeoutError::Disconnected => break,
                                },
                            };
                            if last_auction_time.elapsed() > auction_duration {
                                while let Some(tx) = transaction_buffer.pop_front() {
                                    // create a batch and send to tpu client sender,
                                    // clone is cheap due to bytes
                                    batch.push(PaladinPacket::from_bytes(
                                        tx.wire_transaction.clone(),
                                        tx.revert_protect,
                                    ));

                                    if batch.len() >= batch_size {
                                        Self::try_forward_batch(&tpu_client_sender, batch.clone());
                                        batch.clear();
                                    }
                                }
                                if !batch.is_empty() {
                                    Self::try_forward_batch(&tpu_client_sender, batch.clone());
                                }
                                last_auction_time = std::time::Instant::now();
                            }
                        }
                    })
                    .unwrap()
            })
            .collect();
        Self { threads }
    }

    pub fn try_forward_batch(
        sender: &tokio::sync::mpsc::Sender<Vec<PaladinPacket>>,
        batch: Vec<PaladinPacket>,
    ) {
        match sender.try_send(batch.clone()) {
            Ok(_) => {}
            Err(e) => match e {
                TrySendError::Full(_) => {
                    error!("TPU client sender is full, dropping transactions")
                }
                TrySendError::Closed(_) => {
                    panic!("TPU client sender is closed")
                }
            },
        }
    }

    pub async fn join(&mut self) {
        for thread in self.threads.drain(..) {
            if thread.is_finished() {
                thread.join().unwrap();
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}
