//! Parsing and processing for Fetch Offsets responses.
//!
//! ### Example
//! ```rust
//! let response_bytes = coordinator_conn.receive_response().await?;
//! let produce_response = protocol::ProduceResponse::try_from(response_bytes.freeze());
//! ```
//!
//! ### Protocol Def
//! ```text
//! Produce Response (Version: 0) => [responses]
//!   responses => name [partition_responses]
//!     name => STRING
//!     partition_responses => index error_code base_offset
//!       index => INT32
//!       error_code => INT16
//!       base_offset => INT64
//! ```
//!
//! Note we are using version 0 for the response.

use bytes::Bytes;
use nom::{
    number::complete::{be_i32, be_i64},
    IResult,
};
use nombytes::NomBytes;

use crate::{
    error::{Error, KafkaCode, Result},
    parser,
    protocol::{parse_header_response, HeaderResponse},
};

/// The base Produce Fetch response object.
///
/// Note, the request needs to have a non-zero value for `required_acks` to receive a response.
///
/// ### Example
/// ```rust
/// let response_bytes = coordinator_conn.receive_response().await?;
/// let produce_response = protocol::ProduceFetchResponse::try_from(response_bytes.freeze());
/// ```
#[derive(Debug, PartialEq)]
pub struct ProduceResponse {
    pub header: HeaderResponse,
    /// Each produce response
    pub responses: Vec<Response>,
}

#[derive(Debug, PartialEq)]
pub struct Response {
    /// The topic name
    pub name: Bytes,
    /// Each partition that we produced to within the topic.
    pub partition_responses: Vec<PartitionResponse>,
}

#[derive(Debug, PartialEq)]
pub struct PartitionResponse {
    /// The partition index.
    pub index: i32,
    /// The error code, or 0 if there was no error.
    pub error_code: KafkaCode,
    /// The base offset.
    pub base_offset: i64,
}

impl TryFrom<Bytes> for ProduceResponse {
    type Error = Error;

    fn try_from(s: Bytes) -> Result<Self> {
        tracing::trace!("Parsing ProduceResponse {:?}", s);
        let (_, produce_fetch) =
            parse_produce_fetch_response(NomBytes::new(s.clone())).map_err(|err| {
                tracing::error!("ERROR: Failed parsing ProduceResponse {:?}", err);
                tracing::error!("ERROR: ProduceResponse Bytes {:?}", s);
                Error::ParsingError(s)
            })?;
        tracing::trace!("Parsed ProduceResponse {:?}", produce_fetch);
        Ok(produce_fetch)
    }
}

pub fn parse_produce_fetch_response(s: NomBytes) -> IResult<NomBytes, ProduceResponse> {
    let (s, header) = parse_header_response(s)?;
    let (s, responses) = parser::parse_array(parse_response)(s)?;

    Ok((s, ProduceResponse { header, responses }))
}

pub fn parse_response(s: NomBytes) -> IResult<NomBytes, Response> {
    let (s, name) = parser::parse_string(s)?;
    let (s, partition_responses) = parser::parse_array(parse_partition_response)(s)?;

    Ok((
        s,
        Response {
            name,
            partition_responses,
        },
    ))
}

pub fn parse_partition_response(s: NomBytes) -> IResult<NomBytes, PartitionResponse> {
    let (s, index) = be_i32(s)?;
    let (s, error_code) = parser::parse_kafka_code(s)?;
    let (s, base_offset) = be_i64(s)?;

    Ok((
        s,
        PartitionResponse {
            index,
            error_code,
            base_offset,
        },
    ))
}
