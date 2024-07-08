//! Parsing and processing for SASL Handshake responses.
//!
//! ### Example
//! ```rust
//! let response_bytes = conn.receive_response().await?;
//! let handshake_response = protocol::SaslHandshakeResponse::try_from(response_bytes.freeze());
//! ```
//!
//! ### Protocol Defs
//! ```text
//! SaslHandshake Response (Version: 1) => error_code [mechanisms]
//!   error_code => INT16
//!   mechanisms => STRING
//! ```
//!
//! Note we are using version 1 for the response.

use bytes::Bytes;
use nom::IResult;
use nombytes::NomBytes;

use crate::{
    error::{Error, KafkaCode, Result},
    parser,
    protocol::{parse_header_response, HeaderResponse},
};

/// The base SASL Handshake response object.
///
/// ### Example
/// ```rust
/// let response_bytes = conn.receive_response().await?;
/// let handshake_response = protocol::SaslHandshakeResponse::try_from(response_bytes.freeze());
/// ```
#[derive(Debug, PartialEq)]
pub struct SaslHandshakeResponse {
    pub header: HeaderResponse,
    /// The error code, or 0 if there was no error.
    pub error_code: KafkaCode,
    /// The mechanisms enabled in the server.
    pub mechanisms: Vec<Bytes>,
}

impl TryFrom<Bytes> for SaslHandshakeResponse {
    type Error = Error;

    fn try_from(s: Bytes) -> Result<Self> {
        tracing::trace!("Parsing SaslHandshakeResponse {:?}", s);
        let (_, handshake) = parse_handshake_response(NomBytes::new(s.clone())).map_err(|err| {
            tracing::error!("ERROR: Failed parsing SaslHandshakeResponse {:?}", err);
            tracing::error!("ERROR: SaslHandshakeResponse Bytes {:?}", s);
            Error::ParsingError(s)
        })?;
        tracing::trace!("Parsed SaslHandshakeResponse {:?}", handshake);
        Ok(handshake)
    }
}

pub fn parse_handshake_response(s: NomBytes) -> IResult<NomBytes, SaslHandshakeResponse> {
    let (s, header) = parse_header_response(s)?;
    let (s, error_code) = parser::parse_kafka_code(s)?;
    let (s, mechanisms) = parser::parse_array(parser::parse_string)(s)?;

    Ok((
        s,
        SaslHandshakeResponse {
            header,
            error_code,
            mechanisms,
        },
    ))
}
