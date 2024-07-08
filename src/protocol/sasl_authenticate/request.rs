//! Encoding and creation for Sasl Authentication requests.
//!
//! ### Example
//! ```rust
//! let mut sasl_authentication_request = protocol::SaslAuthenticationRequest::new(
//!     correlation_id,
//!     auth_bytes
//! );
//! conn.send_request(&sasl_authentication_request).await?;
//! ```
//!
//! ### Protocol Def
//! ```text
//! SaslAuthenticate Request (Version: 1) => auth_bytes
//!   auth_bytes => BYTES
//! ```
//!
//! Note that we are using version 1 of this API

use bytes::Bytes;

use crate::{encode::ToByte, protocol::HeaderRequest};

const API_KEY_METADATA: i16 = 36;
const API_VERSION: i16 = 1;

/// The base Sasl Authentication request object.
///
/// ### Example
/// ```rust
/// let mut sasl_authentication_request = protocol::SaslAuthenticationRequest::new(
///     correlation_id,
///     client_id,
///     auth_bytes
/// );
/// conn.send_request(&sasl_authentication_request).await?;
/// ```
#[derive(Debug)]
pub struct SaslAuthenticationRequest<'a> {
    pub header: HeaderRequest<'a>,
    /// The SASL authentication bytes from the client, as defined by the SASL mechanism.
    pub auth_bytes: Bytes,
}

impl<'a> SaslAuthenticationRequest<'a> {
    /// Create a new SASL Authentication Request
    pub fn new(correlation_id: i32, client_id: &'a str, auth_bytes: Bytes) -> Self {
        let header = HeaderRequest::new(API_KEY_METADATA, API_VERSION, correlation_id, client_id);
        Self { header, auth_bytes }
    }
}

impl<'a> ToByte for SaslAuthenticationRequest<'a> {
    fn encode<T: bytes::BufMut>(&self, buffer: &mut T) -> crate::error::Result<()> {
        tracing::trace!("Encoding SaslAuthenticationRequest {:?}", self);
        self.header.encode(buffer)?;
        self.auth_bytes.encode(buffer)?;
        Ok(())
    }
}
