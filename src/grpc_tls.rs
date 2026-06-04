//! Tonic TLS configuration from [`crate::config::TlsConfig`] (`feature = "tls"`).

#![cfg(feature = "tls")]

use crate::config::TlsConfig;
use std::io;
use tonic::transport::{Certificate, ClientTlsConfig, Identity, ServerTlsConfig};

fn ensure_crypto_provider() {
    use std::sync::Once;
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    });
}

/// Server TLS for tonic (mesh registry, data plane, Paxos).
pub fn server_tls_config(tls: &TlsConfig) -> io::Result<ServerTlsConfig> {
    ensure_crypto_provider();
    let identity = Identity::from_pem(&tls.cert_pem, &tls.key_pem);
    let mut cfg = ServerTlsConfig::new().identity(identity);
    if let Some(ca) = &tls.ca_pem {
        cfg = cfg.client_ca_root(Certificate::from_pem(ca));
    }
    Ok(cfg)
}

/// Client TLS for tonic. `domain` is the HTTP/2 authority (e.g. `localhost`).
pub fn client_tls_config(tls: &TlsConfig, domain: &str) -> io::Result<ClientTlsConfig> {
    ensure_crypto_provider();
    let mut cfg = ClientTlsConfig::new().domain_name(domain);
    if let Some(ca) = &tls.ca_pem {
        cfg = cfg.ca_certificate(Certificate::from_pem(ca));
    }
    if !tls.cert_pem.is_empty() && !tls.key_pem.is_empty() {
        cfg = cfg.identity(Identity::from_pem(&tls.cert_pem, &tls.key_pem));
    }
    Ok(cfg)
}

/// Apply client TLS to a tonic endpoint when `feature = "tls"` and `tls` is `Some`.
pub fn apply_client_tls(
    endpoint: tonic::transport::Endpoint,
    tls: Option<&TlsConfig>,
    domain: &str,
) -> Result<tonic::transport::Endpoint, std::io::Error> {
    let Some(tls) = tls else {
        return Ok(endpoint);
    };
    ensure_crypto_provider();
    let tls_cfg = client_tls_config(tls, domain)?;
    endpoint
        .tls_config(tls_cfg)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}

/// Apply server TLS to a tonic builder when `feature = "tls"` and `tls` is `Some`.
pub fn apply_server_tls(
    builder: tonic::transport::Server,
    tls: Option<&TlsConfig>,
) -> tonic::transport::Server {
    #[cfg(feature = "tls")]
    if let Some(t) = tls {
        if let Ok(cfg) = server_tls_config(t) {
            return match builder.tls_config(cfg) {
                Ok(b) => b,
                Err(e) => {
                    tracing::warn!(error = %e, "gRPC server TLS config failed; serving plain");
                    tonic::transport::Server::builder()
                }
            };
        }
    }
    builder
}

/// Host portion of `host:port` for TLS SNI.
pub fn tls_domain_from_addr(addr: &str) -> &str {
    addr.rsplit_once(':')
        .map(|(host, _)| host)
        .unwrap_or(addr)
        .trim_matches(|c| c == '[' || c == ']')
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mesh::ServiceRecord;
    use crate::mesh_registry_grpc::{MeshRegistryClient, MeshRegistryHandle};

    fn rcgen_tls() -> (TlsConfig, TlsConfig) {
        let cert = rcgen::generate_simple_self_signed(vec![
            "localhost".into(),
            "127.0.0.1".into(),
        ])
        .expect("cert");
        let cert_pem = cert.cert.pem().into_bytes();
        let key_pem = cert.key_pair.serialize_pem().into_bytes();
        let server = TlsConfig {
            cert_pem: cert_pem.clone(),
            key_pem,
            ca_pem: None,
        };
        let client = TlsConfig {
            cert_pem: Vec::new(),
            key_pem: Vec::new(),
            ca_pem: Some(cert_pem),
        };
        (server, client)
    }

    #[tokio::test]
    async fn mesh_registry_tls_round_trip() {
        let (server_tls, client_tls) = rcgen_tls();
        let handle = MeshRegistryHandle::bind_with_tls("127.0.0.1:0", Some(server_tls))
            .await
            .expect("bind tls");
        let mut client = MeshRegistryClient::connect_with_tls(&handle.address, Some(&client_tls))
            .await
            .expect("connect tls");
        let record = ServiceRecord {
            service: "svc".into(),
            instance_id: "i1".into(),
            address: "127.0.0.1:1".into(),
            target: "i1".into(),
            dc: None,
            registered_at: 0,
        };
        client.register(record.clone()).await.expect("register");
        let list = client.list().await.expect("list");
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].instance_id, "i1");

        let mut plain = MeshRegistryClient::connect(&handle.address)
            .await
            .expect("lazy channel to tls port");
        assert!(
            plain.list().await.is_err(),
            "plaintext client RPC should fail against tls server"
        );
    }
}
