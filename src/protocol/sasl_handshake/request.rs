//! Encoding and creation for Create Topics requests.
//!
//! ### Example
//! ```rust
//! let mut sasl_handshake_request = protocol::SaslHandshakeRequest::new(
//!     correlation_id,
//!     mechanism
//! );
//! conn.send_request(&sasl_handshake_request).await?;
//! ```
//!
//! ### Protocol Def
//! ```text
//! SaslHandshake Request (Version: 1) => mechanism 
//!   mechanism => STRING
//! ```
//!
//! Note that we are using version 1 of this API

use crate::{encode::ToByte, error::Result, protocol::HeaderRequest};

const API_KEY_METADATA: i16 = 17;
const API_VERSION: i16 = 1;

/// The base Create Topics request object.
///
/// ### Example
/// ```rust
/// let mut sasl_handshake_request = protocol::SaslHandshakeRequest::new(
///     correlation_id,
///     client_id,
///     mechanism
/// );
/// conn.send_request(&sasl_handshake_request).await?;
/// ```
#[derive(Debug)]
pub struct SaslHandshakeRequest<'a> {
    pub header: HeaderRequest<'a>,
    /// The SASL mechanism chosen by the client.
    pub mechanism: String,
}

impl<'a> SaslHandshakeRequest<'a> {
    /// Create a new SASL Handshake Request
    pub fn new(
        correlation_id: i32,
        client_id: &'a str,
        mechanism: String,
    ) -> Result<Self> {
        let header = HeaderRequest::new(API_KEY_METADATA, API_VERSION, correlation_id, client_id);
        Ok(Self {
            header,
            mechanism,
        })
    }
}

impl<'a> ToByte for SaslHandshakeRequest<'a> {
    fn encode<T: bytes::BufMut>(&self, buffer: &mut T) -> crate::error::Result<()> {
        tracing::trace!("Encoding SaslHandshakeRequest {:?}", self);
        self.header.encode(buffer)?;
        self.mechanism.encode(buffer)?;
        Ok(())
    }
}
