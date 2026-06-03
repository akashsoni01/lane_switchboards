//! TCP stream abstraction for distributed and mesh framing.
//!
//! With the `tls` feature, connections may be wrapped in TLS via rustls.
//! Without it, only plain [`TcpStream`] is supported and TLS connectors must be `None`.

use std::io::{self, ErrorKind};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;

#[cfg(feature = "tls")]
pub use tokio_rustls::{TlsAcceptor, TlsConnector, TlsStream};

/// Placeholder when the crate is built without `feature = "tls"`.
#[cfg(not(feature = "tls"))]
#[derive(Debug, Clone)]
pub struct TlsConnector;

/// Placeholder when the crate is built without `feature = "tls"`.
#[cfg(not(feature = "tls"))]
#[derive(Debug, Clone)]
pub struct TlsAcceptor;

/// Plain TCP or, with `feature = "tls"`, a TLS-wrapped stream.
pub enum MaybeTlsStream {
    Plain(TcpStream),
    #[cfg(feature = "tls")]
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
            #[cfg(feature = "tls")]
            MaybeTlsStream::Tls(TlsStream::Client(s)) => Pin::new(s).poll_read(cx, buf),
            #[cfg(feature = "tls")]
            MaybeTlsStream::Tls(TlsStream::Server(s)) => Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for MaybeTlsStream {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            MaybeTlsStream::Plain(s) => Pin::new(s).poll_write(cx, buf),
            #[cfg(feature = "tls")]
            MaybeTlsStream::Tls(TlsStream::Client(s)) => Pin::new(s).poll_write(cx, buf),
            #[cfg(feature = "tls")]
            MaybeTlsStream::Tls(TlsStream::Server(s)) => Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            MaybeTlsStream::Plain(s) => Pin::new(s).poll_flush(cx),
            #[cfg(feature = "tls")]
            MaybeTlsStream::Tls(TlsStream::Client(s)) => Pin::new(s).poll_flush(cx),
            #[cfg(feature = "tls")]
            MaybeTlsStream::Tls(TlsStream::Server(s)) => Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            MaybeTlsStream::Plain(s) => Pin::new(s).poll_shutdown(cx),
            #[cfg(feature = "tls")]
            MaybeTlsStream::Tls(TlsStream::Client(s)) => Pin::new(s).poll_shutdown(cx),
            #[cfg(feature = "tls")]
            MaybeTlsStream::Tls(TlsStream::Server(s)) => Pin::new(s).poll_shutdown(cx),
        }
    }
}

#[cfg(not(feature = "tls"))]
const TLS_DISABLED: &str = "lane_switchboards was compiled without the `tls` feature";

/// Host portion of `"host:port"` for TLS SNI / server name validation.
pub fn host_from_addr(addr: &str) -> io::Result<&str> {
    addr.rsplit_once(':')
        .map(|(host, _)| host)
        .ok_or_else(|| io::Error::new(ErrorKind::InvalidInput, format!("missing port in {addr:?}")))
}

/// Connect with optional TLS (`connector` must be `None` without the `tls` feature).
pub async fn connect(
    addr: &str,
    connector: Option<&TlsConnector>,
) -> io::Result<MaybeTlsStream> {
    let tcp = TcpStream::connect(addr).await?;
    #[cfg(not(feature = "tls"))]
    if connector.is_some() {
        return Err(io::Error::new(ErrorKind::Unsupported, TLS_DISABLED));
    }
    #[cfg(not(feature = "tls"))]
    return Ok(MaybeTlsStream::Plain(tcp));

    #[cfg(feature = "tls")]
    match connector {
        None => Ok(MaybeTlsStream::Plain(tcp)),
        Some(connector) => {
            use rustls::pki_types::ServerName;
            let host = host_from_addr(addr)?;
            let name = ServerName::try_from(host.to_string()).map_err(|e| {
                io::Error::new(ErrorKind::InvalidInput, format!("invalid TLS server name: {e}"))
            })?;
            let tls = connector.connect(name, tcp).await?;
            Ok(MaybeTlsStream::Tls(TlsStream::Client(tls)))
        }
    }
}

/// Accept with optional TLS (`acceptor` must be `None` without the `tls` feature).
pub async fn accept(
    stream: TcpStream,
    acceptor: Option<&TlsAcceptor>,
) -> io::Result<MaybeTlsStream> {
    #[cfg(not(feature = "tls"))]
    if acceptor.is_some() {
        return Err(io::Error::new(ErrorKind::Unsupported, TLS_DISABLED));
    }
    #[cfg(not(feature = "tls"))]
    return Ok(MaybeTlsStream::Plain(stream));

    #[cfg(feature = "tls")]
    match acceptor {
        None => Ok(MaybeTlsStream::Plain(stream)),
        Some(acceptor) => {
            let tls = acceptor.accept(stream).await?;
            Ok(MaybeTlsStream::Tls(TlsStream::Server(tls)))
        }
    }
}
