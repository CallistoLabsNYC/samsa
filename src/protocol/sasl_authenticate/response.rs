//! Parsing and processing for SASL Authentication responses.
//!
//! ### Example
//! ```rust
//! let response_bytes = conn.receive_response().await?;
//! let authenticate_response = protocol::SaslAuthenticationResponse::try_from(response_bytes.freeze());
//! ```
//!
//! ### Protocol Defs
//! ```text
//! SaslAuthenticate Response (Version: 1) => error_code error_message auth_bytes session_lifetime_ms 
//!   error_code => INT16
//!   error_message => NULLABLE_STRING
//!   auth_bytes => BYTES
//!   session_lifetime_ms => INT64
//! ```
//!
//! Note we are using version 1 for the response.

use bytes::Bytes;
use nom::{IResult, number::complete::be_i64};
use nombytes::NomBytes;

use crate::{
    error::{Error, KafkaCode, Result},
    parser,
    protocol::{parse_header_response, HeaderResponse},
};

/// The base SASL Authentication response object.
///
/// ### Example
/// ```rust
/// let response_bytes = conn.receive_response().await?;
/// let authenticate_response = protocol::SaslAuthenticationResponse::try_from(response_bytes.freeze());
/// ```
#[derive(Debug, PartialEq)]
pub struct SaslAuthenticationResponse {
    pub header: HeaderResponse,
    /// The error code, or 0 if there was no error.
    pub error_code: KafkaCode,
    /// The error message, or null if there was no error.
    pub error_message: Option<Bytes>,
    /// The SASL authentication bytes from the server, as defined by the SASL mechanism.
    pub auth_bytes: Bytes,
    /// Number of milliseconds after which only re-authentication over the existing connection to create a new session can occur.
    pub session_lifetime_ms: i64,
}

impl TryFrom<Bytes> for SaslAuthenticationResponse {
    type Error = Error;

    fn try_from(s: Bytes) -> Result<Self> {
        tracing::trace!("Parsing SaslAuthenticationResponse {:?}", s);
        let (_, authenticate) =
            parse_authenticate_response(NomBytes::new(s.clone())).map_err(|err| {
                tracing::error!("ERROR: Failed parsing SaslAuthenticationResponse {:?}", err);
                tracing::error!("ERROR: SaslAuthenticationResponse Bytes {:?}", s);
                Error::ParsingError(s)
            })?;
        tracing::trace!("Parsed SaslAuthenticationResponse {:?}", authenticate);
        Ok(authenticate)
    }
}

pub fn parse_authenticate_response(s: NomBytes) -> IResult<NomBytes, SaslAuthenticationResponse> {
    let (s, header) = parse_header_response(s)?;
    let (s, error_code) = parser::parse_kafka_code(s)?;
    let (s, error_message) = parser::parse_nullable_bytes(s)?;
    let (s, auth_bytes) = parser::parse_bytes(s)?;
    let (s, session_lifetime_ms) = be_i64(s)?;

    Ok((s, SaslAuthenticationResponse { header, error_code, error_message, auth_bytes, session_lifetime_ms }))
}
