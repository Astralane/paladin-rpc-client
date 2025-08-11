use crate::connectin_scheduler::PaladinPacket;
use tokio_util::sync::CancellationToken;
use tracing::error;

pub fn forward_packets_to_tpu_load_balanced(
    quic_client_senders: Vec<tokio::sync::mpsc::Sender<Vec<PaladinPacket>>>,
    mut packet_batch_receiver: tokio::sync::mpsc::Receiver<Vec<PaladinPacket>>,
    cancel: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    let total_sender = quic_client_senders.len();
    let mut current_sender = 0;
    tokio::spawn(async move {
        loop {
            let packet_batch = tokio::select! {
                _ = cancel.cancelled() => {
                    break;
                },
                packets = packet_batch_receiver.recv() => {
                    match packets {
                        Some(packets) => {
                            packets
                        },
                        None => {
                            error!("Packet batch receiver closed");
                            break;
                        }
                    }
                },
            };

            let sender = &quic_client_senders[current_sender];
            current_sender = (current_sender + 1) % total_sender;

            match sender.try_send(packet_batch) {
                Ok(_) => {}
                Err(e) => match e {
                    tokio::sync::mpsc::error::TrySendError::Full(_) => {
                        error!("Packet batch sender is full");
                        // increase dropped metrics
                    }
                    tokio::sync::mpsc::error::TrySendError::Closed(e) => {
                        panic!("Packet batch sender is closed");
                    }
                },
            }
        }
    })
}
