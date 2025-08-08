use crate::quic::error::{IoErrorWithPartialEq, QuicError};
use crate::quic::quic_client_certificate::QuicClientCertificate;
use futures_util::TryFutureExt;
use quinn::{
    crypto::rustls::QuicClientConfig, ClientConfig, Connection, Endpoint, IdleTimeout,
    TransportConfig,
};
use solana_quic_definitions::{QUIC_KEEP_ALIVE, QUIC_MAX_TIMEOUT, QUIC_SEND_FAIRNESS};
use solana_sdk::signature::Keypair;
use solana_streamer::nonblocking::quic::ALPN_TPU_PROTOCOL_ID;
use solana_tls_utils::tls_client_config_builder;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

fn create_client_config(client_certificate: Arc<QuicClientCertificate>) -> ClientConfig {
    // adapted from QuicLazyInitializedEndpoint::create_endpoint
    let mut crypto = tls_client_config_builder()
        .with_client_auth_cert(
            vec![client_certificate.certificate.clone()],
            client_certificate.key.clone_key(),
        )
        .expect("Failed to set QUIC client certificates");
    crypto.enable_early_data = true;
    crypto.alpn_protocols = vec![ALPN_TPU_PROTOCOL_ID.to_vec()];

    let transport_config = {
        let mut res = TransportConfig::default();

        let timeout = IdleTimeout::try_from(Duration::from_secs(4)).unwrap();
        res.max_idle_timeout(Some(timeout));
        res.keep_alive_interval(Some(Duration::from_millis(500)));
        res.send_fairness(QUIC_SEND_FAIRNESS);

        res
    };

    let mut config = ClientConfig::new(Arc::new(QuicClientConfig::try_from(crypto).unwrap()));
    config.transport_config(Arc::new(transport_config));

    config
}

fn create_client_endpoint(
    bind_addr: SocketAddr,
    client_config: ClientConfig,
) -> Result<Endpoint, QuicError> {
    let mut endpoint = Endpoint::client(bind_addr).map_err(IoErrorWithPartialEq::from)?;
    endpoint.set_default_client_config(client_config);
    Ok(endpoint)
}

pub fn setup_quic_endpoint(
    bind_addr: SocketAddr,
    identity: &Keypair,
) -> Result<Endpoint, QuicError> {
    let client_certificate = Arc::new(QuicClientCertificate::new(identity));
    let client_config = create_client_config(client_certificate);
    create_client_endpoint(bind_addr, client_config)
}

pub async fn send_data_over_stream(connection: &Connection, data: &[u8]) -> Result<(), QuicError> {
    let mut send_stream = connection.open_uni().await?;
    send_stream.write_all(data).await.map_err(QuicError::from)?;
    // Stream will be finished when dropped. Finishing here explicitly is a noop.
    Ok(())
}
