use crate::connection_scheduler::PaladinPacket;
use crate::types::VerifiedTransaction;
use core_affinity::CoreId;
use crossbeam_channel::{Receiver, RecvTimeoutError};
use itertools::Itertools;
use metrics::{counter, histogram};
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
    #[allow(clippy::too_many_arguments)]
    pub fn spawn_new(
        auction_duration: Duration,
        verified_transaction_receiver: Receiver<VerifiedTransaction>,
        tpu_forwarder_sender: tokio::sync::mpsc::Sender<Vec<PaladinPacket>>,
        affinity: Option<Vec<usize>>,
        cancel: CancellationToken,
        batch_size: usize,
        num_threads: usize,
        disable_auction: bool,
    ) -> Self {
        const RECV_TIMEOUT: Duration = Duration::from_millis(10);
        let threads = (0..num_threads)
            .map(|id| {
                let transaction_receiver = verified_transaction_receiver.clone();
                let tpu_forwarder_sender = tpu_forwarder_sender.clone();
                let cancel = cancel.clone();
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
                            if last_auction_time.elapsed() > auction_duration || disable_auction {
                                histogram!("paladin_rpc_client_buffer_size")
                                    .record(transaction_buffer.len() as f64);

                                for chunks in &transaction_buffer.drain(..).chunks(batch_size) {
                                    // create a batch and send to tpu client sender,
                                    // clone is cheap due to bytes
                                    let batch: Vec<_> = chunks
                                        .map(|item| {
                                            PaladinPacket::new(
                                                item.wire_transaction,
                                                item.revert_protect,
                                            )
                                        })
                                        .collect();
                                    Self::try_forward_batch(&tpu_forwarder_sender, batch);
                                }
                                transaction_buffer.clear();

                                histogram!("paladin_rpc_client_auction_time")
                                    .record(last_auction_time.elapsed().as_millis() as f64);

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
                    error!("TPU client sender is full, dropping transactions");
                    counter!("paladin_rpc_client_tpu_client_channel_full").increment(1);
                }
                TrySendError::Closed(_) => {
                    counter!("paladin_rpc_client_tpu_client_channel_closed").increment(1);
                    panic!("TPU client sender is closed");
                }
            },
        }
    }

    pub async fn join(mut self) {
        for thread in self.threads.drain(..) {
            while !thread.is_finished() {
                tokio::time::sleep(Duration::from_millis(1000)).await;
            }
            thread.join().unwrap();
        }
    }
}
