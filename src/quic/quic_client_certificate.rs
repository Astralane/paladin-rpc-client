use solana_tls_utils::new_dummy_x509_certificate;
use {
    rustls::pki_types::{CertificateDer, PrivateKeyDer},
    solana_sdk::signature::Keypair,
};

pub struct QuicClientCertificate {
    pub certificate: CertificateDer<'static>,
    pub key: PrivateKeyDer<'static>,
}

impl QuicClientCertificate {
    pub fn new(keypair: &Keypair) -> Self {
        let (certificate, key) = new_dummy_x509_certificate(keypair);
        Self { certificate, key }
    }
}
