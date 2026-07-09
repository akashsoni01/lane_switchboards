//! Stable FFI error codes (see `todo_client_ffi.md` Phase F1).

use lane_switchboards::messenger::MessengerError;
use thiserror::Error;

/// Numeric codes returned by the C API (`int32_t`).
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FfiErrorCode {
    Ok = 0,
    Io = 1,
    Protocol = 2,
    FrameTooLarge = 3,
    UnknownPacketType = 4,
    UnsupportedVersion = 5,
    Decode = 6,
    AuthFailed = 7,
    Closed = 8,
    Timeout = 9,
    Media = 10,
    E2ee = 11,
    InvalidArgument = 12,
    NotConnected = 13,
    Internal = 14,
}

impl FfiErrorCode {
    pub fn as_i32(self) -> i32 {
        self as i32
    }
}

/// Rich error for Rust callers and UniFFI-style hosts.
#[derive(Debug, Error)]
pub enum FfiError {
    #[error("{0}")]
    Messenger(#[from] MessengerError),
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error("not connected")]
    NotConnected,
    #[error("internal: {0}")]
    Internal(String),
    #[error("e2ee: {0}")]
    E2ee(String),
}

impl FfiError {
    pub fn code(&self) -> FfiErrorCode {
        match self {
            FfiError::Messenger(e) => map_messenger(e),
            FfiError::InvalidArgument(_) => FfiErrorCode::InvalidArgument,
            FfiError::NotConnected => FfiErrorCode::NotConnected,
            FfiError::Internal(_) => FfiErrorCode::Internal,
            FfiError::E2ee(_) => FfiErrorCode::E2ee,
        }
    }

    pub fn detail(&self) -> String {
        self.to_string()
    }
}

fn map_messenger(e: &MessengerError) -> FfiErrorCode {
    match e {
        MessengerError::Io(_) => FfiErrorCode::Io,
        MessengerError::Protocol(_) => FfiErrorCode::Protocol,
        MessengerError::FrameTooLarge { .. } => FfiErrorCode::FrameTooLarge,
        MessengerError::UnknownPacketType(_) => FfiErrorCode::UnknownPacketType,
        MessengerError::UnsupportedVersion(_) => FfiErrorCode::UnsupportedVersion,
        MessengerError::Decode(_) => FfiErrorCode::Decode,
        MessengerError::AuthFailed => FfiErrorCode::AuthFailed,
        MessengerError::Closed => FfiErrorCode::Closed,
        MessengerError::Timeout(_) => FfiErrorCode::Timeout,
        MessengerError::Media(_) => FfiErrorCode::Media,
        MessengerError::E2ee(_) => FfiErrorCode::E2ee,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn messenger_errors_map_once() {
        assert_eq!(
            FfiError::Messenger(MessengerError::AuthFailed).code(),
            FfiErrorCode::AuthFailed
        );
        assert_eq!(
            FfiError::Messenger(MessengerError::Closed).code(),
            FfiErrorCode::Closed
        );
        assert_eq!(
            FfiError::Messenger(MessengerError::Timeout("x")).code(),
            FfiErrorCode::Timeout
        );
        assert_eq!(
            FfiError::Messenger(MessengerError::Protocol("p".into())).code(),
            FfiErrorCode::Protocol
        );
        assert_eq!(
            FfiError::InvalidArgument("x".into()).code(),
            FfiErrorCode::InvalidArgument
        );
        assert_eq!(FfiError::NotConnected.code(), FfiErrorCode::NotConnected);
    }
}
