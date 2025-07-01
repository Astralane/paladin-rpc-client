use crate::vendor::error::{IoErrorWithPartialEq, QuicError};
use crate::vendor::quic_client_certificate::QuicClientCertificate;
use quinn::{
    ClientConfig, Endpoint, IdleTimeout, TransportConfig, crypto::rustls::QuicClientConfig,
};
use solana_sdk::quic::{QUIC_KEEP_ALIVE, QUIC_MAX_TIMEOUT};
use solana_streamer::nonblocking::quic::ALPN_TPU_PROTOCOL_ID;
use solana_streamer::nonblocking::testing_utilities::SkipServerVerification;
use std::net::SocketAddr;
use std::sync::Arc;

pub(crate) fn create_client_config(client_certificate: Arc<QuicClientCertificate>) -> ClientConfig {
    // adapted from QuicLazyInitializedEndpoint::create_endpoint
    let mut crypto = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(SkipServerVerification::new())
        .with_client_auth_cert(
            vec![client_certificate.certificate.clone()],
            client_certificate.key.clone_key(),
        )
        .expect("Failed to set QUIC client certificates");
    crypto.enable_early_data = true;
    crypto.alpn_protocols = vec![ALPN_TPU_PROTOCOL_ID.to_vec()];

    let transport_config = {
        let mut res = TransportConfig::default();

        let timeout = IdleTimeout::try_from(QUIC_MAX_TIMEOUT).unwrap();
        res.max_idle_timeout(Some(timeout));
        res.keep_alive_interval(Some(QUIC_KEEP_ALIVE));

        res
    };

    let mut config = ClientConfig::new(Arc::new(QuicClientConfig::try_from(crypto).unwrap()));
    config.transport_config(Arc::new(transport_config));

    config
}

pub(crate) fn create_client_endpoint(
    bind_addr: SocketAddr,
    client_config: ClientConfig,
) -> Result<Endpoint, QuicError> {
    let mut endpoint = Endpoint::client(bind_addr).map_err(IoErrorWithPartialEq::from)?;
    endpoint.set_default_client_config(client_config);
    Ok(endpoint)
}
