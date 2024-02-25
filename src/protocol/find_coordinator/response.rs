//! Parsing and processing for Find Coordinator responses.
//!
//! ### Example
//! ```rust
//! let response_bytes = conn.receive_response().await?;
//! let find_coordinator_response = protocol::FindCoordinatorResponse::try_from(response_bytes.freeze());
//! ```
//!
//! ### Protocol Def
//! ```text
//! FindCoordinator Response (Version: 0) => error_code node_id host port
//!   error_code => INT16
//!   node_id => INT32
//!   host => STRING
//!   port => INT32
//! ```
//!
//! Note we are using version 0 of the response.

use bytes::Bytes;
use nom::{number::complete::be_i32, IResult};
use nombytes::NomBytes;

use crate::{
    error::{Error, KafkaCode, Result},
    parser,
    protocol::{parse_header_response, HeaderResponse},
};

/// The base Find Coordinator response object.
///
/// ### Example
/// ```rust
/// let response_bytes = conn.receive_response().await?;
/// let find_coordinator_response = protocol::FindCoordinatorResponse::try_from(response_bytes.freeze());
/// ```
#[derive(Debug, PartialEq)]
pub struct FindCoordinatorResponse {
    pub header: HeaderResponse,
    pub error_code: KafkaCode,
    pub node_id: i32,
    pub host: Bytes,
    pub port: i32,
}

// this helps us cast the server response into this type
impl TryFrom<Bytes> for FindCoordinatorResponse {
    type Error = Error;

    fn try_from(s: Bytes) -> Result<Self> {
        tracing::trace!("Parsing FindCoordinatorResponse {:?}", s);
        let (_, find_coordinator) = parse_find_coordinator_response(NomBytes::new(s.clone()))
            .map_err(|err| {
                tracing::error!("ERROR: Failed parsing FindCoordinatorResponse {:?}", err);
                tracing::error!("ERROR: FindCoordinatorResponse Bytes {:?}", s);
                Error::ParsingError(s)
            })?;
        tracing::trace!("Parsed FindCoordinatorResponse {:?}", find_coordinator);
        Ok(find_coordinator)
    }
}

pub fn parse_find_coordinator_response(s: NomBytes) -> IResult<NomBytes, FindCoordinatorResponse> {
    let (s, header) = parse_header_response(s)?;
    let (s, error_code) = parser::parse_kafka_code(s)?;
    let (s, node_id) = be_i32(s)?;
    let (s, host) = parser::parse_string(s)?;
    let (s, port) = be_i32(s)?;

    Ok((
        s,
        FindCoordinatorResponse {
            header,
            error_code,
            node_id,
            host,
            port,
        },
    ))
}
