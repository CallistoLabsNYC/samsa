//! Parsing and processing for Heartbeat responses.
//!
//! ### Example
//! ```rust
//! let response_bytes = conn.receive_response().await?;
//! let heartbeat_response = protocol::HeartbeatResponse::try_from(response_bytes.freeze());
//! ```
//!
//! ### Protocol Def
//! ```text
//! Heartbeat Response (Version: 0) => error_code
//!   error_code => INT16
//! ```

use bytes::Bytes;
use nom::IResult;
use nombytes::NomBytes;

use crate::{
    error::{Error, KafkaCode, Result},
    parser,
    protocol::{parse_header_response, HeaderResponse},
};

/// The base Heartbeat response object.
///
/// ### Example
/// ```rust
/// let response_bytes = conn.receive_response().await?;
/// let heartbeat_response = protocol::HeartbeatResponse::try_from(response_bytes.freeze());
/// ```
#[derive(Debug, PartialEq)]
pub struct HeartbeatResponse {
    pub header: HeaderResponse,
    /// The error code, or 0 if there was no error.
    pub error_code: KafkaCode,
}

impl TryFrom<Bytes> for HeartbeatResponse {
    type Error = Error;

    fn try_from(s: Bytes) -> Result<Self> {
        tracing::trace!("Parsing HeartbeatResponse {:?}", s);
        let (_, heartbeat) = parse_heartbeat_response(NomBytes::new(s.clone())).map_err(|err| {
            tracing::error!("ERROR: Failed parsing HeartbeatResponse {:?}", err);
            tracing::error!("ERROR: HeartbeatResponse Bytes {:?}", s);
            Error::ParsingError(s)
        })?;
        tracing::trace!("Parsed HeartbeatResponse {:?}", heartbeat);
        Ok(heartbeat)
    }
}

pub fn parse_heartbeat_response(s: NomBytes) -> IResult<NomBytes, HeartbeatResponse> {
    let (s, header) = parse_header_response(s)?;
    let (s, error_code) = parser::parse_kafka_code(s)?;

    Ok((s, HeartbeatResponse { header, error_code }))
}
