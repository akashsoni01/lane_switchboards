//! TLS helpers when `feature = "tls"` is disabled.

use crate::config::TlsConfig;

/// No-op without `tls` feature.
pub fn apply_server_tls(
    builder: tonic::transport::Server,
    _tls: Option<&TlsConfig>,
) -> tonic::transport::Server {
    builder
}

/// Host portion of `host:port` for TLS SNI.
pub fn tls_domain_from_addr(addr: &str) -> &str {
    addr.rsplit_once(':')
        .map(|(host, _)| host)
        .unwrap_or(addr)
        .trim_matches(|c| c == '[' || c == ']')
}

/// No-op without `tls` feature.
pub fn apply_client_tls(
    endpoint: tonic::transport::Endpoint,
    _tls: Option<&TlsConfig>,
    _domain: &str,
) -> Result<tonic::transport::Endpoint, std::io::Error> {
    Ok(endpoint)
}
