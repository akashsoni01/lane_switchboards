//! TLS certificate loading and rustls config builders.
//!
//! Requires the **`tls`** crate feature (`rustls`, `tokio-rustls`, etc.).
//! For connecting and framing, use [`crate::stream`].

#![cfg(feature = "tls")]

use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::{ClientConfig, RootCertStore, ServerConfig};
use rustls_pemfile::{certs, pkcs8_private_keys, rsa_private_keys};
use std::fs::File;
use std::io::{self, BufReader, ErrorKind};
use std::path::Path;
use std::sync::Arc;
use tokio_rustls::rustls;
pub use tokio_rustls::{TlsAcceptor, TlsConnector, TlsStream};
use webpki_roots;

pub use crate::stream::{accept, connect, host_from_addr, MaybeTlsStream};

fn ensure_crypto_provider() {
    use std::sync::Once;
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    });
}

/// Load PEM certificate chain from disk.
pub fn load_certs(path: impl AsRef<Path>) -> io::Result<Vec<CertificateDer<'static>>> {
    let file = File::open(path.as_ref())?;
    let mut reader = BufReader::new(file);
    certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| io::Error::new(ErrorKind::InvalidData, e))
}

/// Load first PKCS#8 or RSA private key from a PEM file.
pub fn load_private_key(path: impl AsRef<Path>) -> io::Result<PrivateKeyDer<'static>> {
    let file = File::open(path.as_ref())?;
    let mut reader = BufReader::new(file);

    if let Some(key) = pkcs8_private_keys(&mut reader)
        .next()
        .transpose()
        .map_err(|e| io::Error::new(ErrorKind::InvalidData, e))?
    {
        return Ok(PrivateKeyDer::Pkcs8(key));
    }

    let file = File::open(path.as_ref())?;
    let mut reader = BufReader::new(file);
    if let Some(key) = rsa_private_keys(&mut reader)
        .next()
        .transpose()
        .map_err(|e| io::Error::new(ErrorKind::InvalidData, e))?
    {
        return Ok(PrivateKeyDer::Pkcs1(key));
    }

    Err(io::Error::new(
        ErrorKind::InvalidData,
        "no private key found in PEM file",
    ))
}

/// Load trusted CA certificates into a root store.
pub fn load_ca_store(path: impl AsRef<Path>) -> io::Result<RootCertStore> {
    let mut roots = RootCertStore::empty();
    for cert in load_certs(path)? {
        roots.add(cert).map_err(|e| {
            io::Error::new(ErrorKind::InvalidData, format!("invalid CA cert: {e}"))
        })?;
    }
    Ok(roots)
}

fn root_store_from_optional_ca(ca_path: Option<&Path>) -> io::Result<RootCertStore> {
    let mut root_store = RootCertStore::empty();
    match ca_path {
        Some(path) => {
            for cert in load_certs(path)? {
                root_store.add(cert).map_err(|e| {
                    io::Error::new(ErrorKind::InvalidData, format!("invalid CA cert: {e}"))
                })?;
            }
        }
        None => {
            root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        }
    }
    Ok(root_store)
}

/// Build a rustls server config from PEM paths.
pub fn server_config_from_pem(
    cert_path: impl AsRef<Path>,
    key_path: impl AsRef<Path>,
    client_ca_path: Option<impl AsRef<Path>>,
) -> io::Result<ServerConfig> {
    ensure_crypto_provider();
    let cert_chain = load_certs(cert_path)?;
    let key = load_private_key(key_path)?;

    if let Some(ca_path) = client_ca_path {
        let roots = load_ca_store(ca_path)?;
        ServerConfig::builder()
            .with_client_cert_verifier(
                rustls::server::WebPkiClientVerifier::builder(Arc::new(roots))
                    .build()
                    .map_err(|e| io::Error::new(ErrorKind::InvalidData, e))?,
            )
            .with_single_cert(cert_chain, key)
            .map_err(|e| io::Error::new(ErrorKind::InvalidData, e))
    } else {
        ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(cert_chain, key)
            .map_err(|e| io::Error::new(ErrorKind::InvalidData, e))
    }
}

/// Build a rustls client config with optional client identity and custom CA roots.
pub fn client_config_from_pem(
    ca_path: Option<impl AsRef<Path>>,
    client_cert_path: Option<impl AsRef<Path>>,
    client_key_path: Option<impl AsRef<Path>>,
) -> io::Result<ClientConfig> {
    ensure_crypto_provider();
    let ca_ref = ca_path.as_ref().map(|p| p.as_ref());
    let root_store = root_store_from_optional_ca(ca_ref)?;

    if let (Some(cert_path), Some(key_path)) = (client_cert_path, client_key_path) {
        let chain = load_certs(cert_path)?;
        let key = load_private_key(key_path)?;
        ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_client_auth_cert(chain, key)
            .map_err(|e| io::Error::new(ErrorKind::InvalidData, e))
    } else {
        Ok(ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth())
    }
}

/// Wrap a [`ServerConfig`] for accepting TLS connections.
pub fn build_acceptor(config: ServerConfig) -> TlsAcceptor {
    TlsAcceptor::from(Arc::new(config))
}

/// Wrap a [`ClientConfig`] for outgoing TLS connections.
pub fn build_connector(config: ClientConfig) -> TlsConnector {
    TlsConnector::from(Arc::new(config))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rcgen::{CertificateParams, KeyPair, SanType};
    use std::io::Write;
    use std::net::{IpAddr, Ipv4Addr};
    use tempfile::TempDir;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    fn write_pem_pair(dir: &TempDir, cn: &str) -> (std::path::PathBuf, std::path::PathBuf) {
        let mut params = CertificateParams::new(vec![cn.to_string()]).unwrap();
        params.distinguished_name.push(rcgen::DnType::CommonName, cn);
        params.subject_alt_names = vec![
            SanType::DnsName(cn.try_into().unwrap()),
            SanType::IpAddress(IpAddr::V4(Ipv4Addr::LOCALHOST)),
        ];
        let key_pair = KeyPair::generate().unwrap();
        let cert = params.self_signed(&key_pair).unwrap();
        let cert_path = dir.path().join(format!("{cn}.crt"));
        let key_path = dir.path().join(format!("{cn}.key"));
        let mut cert_file = File::create(&cert_path).unwrap();
        cert_file.write_all(cert.pem().as_bytes()).unwrap();
        let mut key_file = File::create(&key_path).unwrap();
        key_file
            .write_all(key_pair.serialize_pem().as_bytes())
            .unwrap();
        (cert_path, key_path)
    }

    #[tokio::test]
    async fn tls_round_trip() {
        let dir = TempDir::new().unwrap();
        let (cert_path, key_path) = write_pem_pair(&dir, "localhost");

        let server_cfg = server_config_from_pem(&cert_path, &key_path, None::<&Path>).unwrap();
        let acceptor = build_acceptor(server_cfg);

        let client_cfg =
            client_config_from_pem(Some(&cert_path), None::<&Path>, None::<&Path>).unwrap();
        let connector = build_connector(client_cfg);

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();

        let server = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut stream = accept(tcp, Some(&acceptor)).await.unwrap();
            let len = stream.read_u32_le().await.unwrap();
            let mut buf = vec![0u8; len as usize];
            stream.read_exact(&mut buf).await.unwrap();
            assert_eq!(buf, b"hello");
            stream.write_u32_le(4).await.unwrap();
            stream.write_all(b"pong").await.unwrap();
            stream.flush().await.unwrap();
        });

        let mut client = connect(&addr, Some(&connector)).await.unwrap();
        client.write_u32_le(5).await.unwrap();
        client.write_all(b"hello").await.unwrap();
        client.flush().await.unwrap();
        let len = client.read_u32_le().await.unwrap();
        let mut buf = vec![0u8; len as usize];
        client.read_exact(&mut buf).await.unwrap();
        assert_eq!(buf, b"pong");
        server.await.unwrap();
    }
}
