//! TLS helpers for distributed and mesh TCP (rustls + tokio-rustls).

use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use rustls::{ClientConfig, RootCertStore, ServerConfig};
use rustls_pemfile::{certs, pkcs8_private_keys, rsa_private_keys};
use std::fs::File;
use std::io::{self, BufReader, ErrorKind};
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio_rustls::rustls;
pub use tokio_rustls::{TlsAcceptor, TlsConnector, TlsStream};
use webpki_roots;

fn ensure_crypto_provider() {
    use std::sync::Once;
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    });
}

/// Plain TCP or TLS-wrapped stream used by frame/control protocols.
pub enum MaybeTlsStream {
    Plain(TcpStream),
    Tls(TlsStream<TcpStream>),
}

impl AsyncRead for MaybeTlsStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            MaybeTlsStream::Plain(s) => Pin::new(s).poll_read(cx, buf),
            MaybeTlsStream::Tls(TlsStream::Client(s)) => Pin::new(s).poll_read(cx, buf),
            MaybeTlsStream::Tls(TlsStream::Server(s)) => Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for MaybeTlsStream {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            MaybeTlsStream::Plain(s) => Pin::new(s).poll_write(cx, buf),
            MaybeTlsStream::Tls(TlsStream::Client(s)) => Pin::new(s).poll_write(cx, buf),
            MaybeTlsStream::Tls(TlsStream::Server(s)) => Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            MaybeTlsStream::Plain(s) => Pin::new(s).poll_flush(cx),
            MaybeTlsStream::Tls(TlsStream::Client(s)) => Pin::new(s).poll_flush(cx),
            MaybeTlsStream::Tls(TlsStream::Server(s)) => Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            MaybeTlsStream::Plain(s) => Pin::new(s).poll_shutdown(cx),
            MaybeTlsStream::Tls(TlsStream::Client(s)) => Pin::new(s).poll_shutdown(cx),
            MaybeTlsStream::Tls(TlsStream::Server(s)) => Pin::new(s).poll_shutdown(cx),
        }
    }
}

/// Host portion of `"host:port"` for TLS SNI / server name validation.
pub fn host_from_addr(addr: &str) -> io::Result<&str> {
    addr.rsplit_once(':')
        .map(|(host, _)| host)
        .ok_or_else(|| io::Error::new(ErrorKind::InvalidInput, format!("missing port in {addr:?}")))
}

/// Connect with optional TLS (client).
pub async fn connect(
    addr: &str,
    connector: Option<&TlsConnector>,
) -> io::Result<MaybeTlsStream> {
    let tcp = TcpStream::connect(addr).await?;
    match connector {
        None => Ok(MaybeTlsStream::Plain(tcp)),
        Some(connector) => {
            let host = host_from_addr(addr)?;
            let name = ServerName::try_from(host.to_string()).map_err(|e| {
                io::Error::new(ErrorKind::InvalidInput, format!("invalid TLS server name: {e}"))
            })?;
            let tls = connector.connect(name, tcp).await?;
            Ok(MaybeTlsStream::Tls(TlsStream::Client(tls)))
        }
    }
}

/// Accept with optional TLS (server).
pub async fn accept(
    stream: TcpStream,
    acceptor: Option<&TlsAcceptor>,
) -> io::Result<MaybeTlsStream> {
    match acceptor {
        None => Ok(MaybeTlsStream::Plain(stream)),
        Some(acceptor) => {
            let tls = acceptor.accept(stream).await?;
            Ok(MaybeTlsStream::Tls(TlsStream::Server(tls)))
        }
    }
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
