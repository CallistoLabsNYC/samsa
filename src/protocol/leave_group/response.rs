//! Parsing and processing for Leave Group responses.
//!
//! ### Example
//! ```rust
//! let response_bytes = conn.receive_response().await?;
//! let leave_response = protocol::HeartbeatResponse::try_from(response_bytes.freeze());
//! ```
//!
//! ### Protocol Defs
//! ```text
//! LeaveGroup Response (Version: 0) => error_code
//!   error_code => INT16
//! ```
//!
//! Note we are using version 0 for the response.

use bytes::Bytes;
use nom::IResult;
use nombytes::NomBytes;

use crate::{
    error::{Error, KafkaCode, Result},
    parser,
    protocol::{parse_header_response, HeaderResponse},
};

/// The base Leave Group response object.
///
/// ### Example
/// ```rust
/// let response_bytes = conn.receive_response().await?;
/// let leave_response = protocol::HeartbeatResponse::try_from(response_bytes.freeze());
/// ```
#[derive(Debug, PartialEq)]
pub struct LeaveGroupResponse {
    pub header: HeaderResponse,
    /// The error code, or 0 if there was no error.
    pub error_code: KafkaCode,
}

impl TryFrom<Bytes> for LeaveGroupResponse {
    type Error = Error;

    fn try_from(s: Bytes) -> Result<Self> {
        tracing::trace!("Parsing LeaveGroupResponse {:?}", s);
        let (_, leave_group) =
            parse_leave_group_response(NomBytes::new(s.clone())).map_err(|err| {
                tracing::error!("ERROR: Failed parsing LeaveGroupResponse {:?}", err);
                tracing::error!("ERROR: LeaveGroupResponse Bytes {:?}", s);
                Error::ParsingError(s)
            })?;
        tracing::trace!("Parsed LeaveGroupResponse {:?}", leave_group);
        Ok(leave_group)
    }
}

pub fn parse_leave_group_response(s: NomBytes) -> IResult<NomBytes, LeaveGroupResponse> {
    let (s, header) = parse_header_response(s)?;
    let (s, error_code) = parser::parse_kafka_code(s)?;

    Ok((s, LeaveGroupResponse { header, error_code }))
}
