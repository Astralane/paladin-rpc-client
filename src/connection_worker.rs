use crate::connection_scheduler::{ConnectionWorkerInfo, PaladinPacket};
use crate::quic::error::QuicError;
use crate::quic::quic_networking::send_data_over_stream;
use metrics::{counter, histogram};
use quinn::{Connection, Endpoint};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

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
    cancel: CancellationToken,
    receiver: tokio::sync::mpsc::Receiver<Vec<PaladinPacket>>,
    max_reconnection_attempts: usize,
    last_connection_created_at: Option<std::time::Instant>,
}

pub fn spawn_new_connection_worker(
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
        receiver: tokio::sync::mpsc::Receiver<Vec<PaladinPacket>>,
        max_reconnection_attempts: usize,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            endpoint,
            peer,
            state: ConnectionState::NotSetup,
            receiver,
            max_reconnection_attempts,
            cancel,
            last_connection_created_at: None,
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
        packets: Vec<PaladinPacket>,
    ) {
        let start = std::time::Instant::now();
        let tx_len = packets.len();
        for packet in packets {
            match send_data_over_stream(&connection, &packet.data).await {
                Ok(_) => {
                    counter!("paladin_rpc_client_quic_worker_success", "peer"=> self.peer.to_string()).increment(1);
                }
                Err(e) => {
                    counter!("paladin_rpc_client_quic_worker_error", "peer"=> self.peer.to_string(), "error"=> format!("{:?}", e)).increment(1);
                    if matches!(e, QuicError::Connection(quinn::ConnectionError::TimedOut)) {
                        if let Some(last_connection_created_at) = self.last_connection_created_at {
                            error!(
                                "timed out, peer last connection lifetime: {:?}",
                                last_connection_created_at.elapsed().as_secs()
                            );
                        }
                    }
                    self.state = ConnectionState::Retry(0);
                    error!(
                        "Failed to send transaction over uni-stream: {:?} for peer {:?}",
                        e, self.peer
                    );
                }
            }
            info!(
                "packet lifetime {:?} milli seconds",
                packet.created_at.elapsed().as_millis()
            );
            histogram!("paladin_rpc_client_total_time_to_send_packet")
                .record(packet.created_at.elapsed().as_millis() as f64);
        }
        let duration = start.elapsed();
        debug!(
            "total time to stream {} transactions: {:?} ms",
            tx_len,
            duration.as_millis()
        );
        histogram!("paladin_rpc_client_total_time_to_stream_transactions")
            .record(duration.as_millis() as f64);
    }

    async fn handle_peer_connection(&mut self) {
        const RETRY_SLEEP_INTERVAL: Duration = Duration::from_millis(100);
        loop {
            match &self.state {
                ConnectionState::NotSetup => {
                    debug!("conn not setup yet for peer {:}", self.peer);
                    self.create_connection(0).await;
                }
                ConnectionState::Active(conn) => {
                    debug!("conn active for peer {:}", self.peer);
                    let batch = match self.receiver.recv().await {
                        Some(batch) => batch,
                        None => {
                            continue;
                        }
                    };
                    self.send_transactions(conn.clone(), batch).await
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
                counter!("paladin_rpc_client_quic_connect_error", "peer"=> self.peer.to_string(), "error"=> format!("{:?}", e)).increment(1);
                error!("Failed to connect to peer: {:?} , {:?}", self.peer, e);
                self.state = ConnectionState::Retry(retry_attempts + 1);
                return;
            }
        };

        let conn = match connecting.await {
            Ok(conn) => {
                self.last_connection_created_at = Some(std::time::Instant::now());
                conn
            }
            Err(e) => {
                counter!("paladin_rpc_client_quic_connecting_error", "peer"=> self.peer.to_string(), "error"=> format!("{:?}", e)).increment(1);
                error!("Failed to connecting to peer: {:?}, {:?}", self.peer, e);
                self.state = ConnectionState::Retry(retry_attempts + 1);
                return;
            }
        };
        self.state = ConnectionState::Active(conn);
    }
}
