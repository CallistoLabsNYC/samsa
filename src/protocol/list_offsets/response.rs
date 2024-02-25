//! Parsing and processing for List Offsets responses.
//!
//! ### Example
//! ```rust
//! let response_bytes = broker_conn.receive_response().await?;
//! let list_offsets_response = protocol::ListOffsetsResponse::try_from(response_bytes.freeze())
//! ```
//!
//! ### Protocol Def
//! ```text
//! ListOffsets Response (Version: 1) => [topics]
//! topics => name [partitions]
//! name => STRING
//! partitions => partition_index error_code timestamp offset
//!     partition_index => INT32
//!     error_code => INT16
//!     timestamp => INT64
//!     offset => INT64
//! ```
//!
//! Note we are using version 1 of the response.

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

/// The base List Offsets response object.
///
/// ### Example
/// ```rust
/// let response_bytes = broker_conn.receive_response().await?;
/// let list_offsets_response = protocol::ListOffsetsResponse::try_from(response_bytes.freeze())
/// ```
#[derive(Debug, PartialEq)]
pub struct ListOffsetsResponse {
    pub header: HeaderResponse,
    /// Each topic in the response.
    pub topics: Vec<Topic>,
}

/// Each topic in the response.
#[derive(Debug, PartialEq)]
pub struct Topic {
    /// The topic name.
    pub name: Bytes,
    /// Each partition in the response.
    pub partitions: Vec<Partition>,
}

/// Each partition in the response.
#[derive(Debug, PartialEq)]
pub struct Partition {
    /// The partition index.
    pub partition_index: i32,
    /// The partition error code, or 0 if there was no error.
    pub error_code: KafkaCode,
    /// The timestamp associated with the returned offset.
    pub timestamp: i64,
    /// The returned offset.
    pub offset: i64,
}

// this helps us cast the server response into this type
impl TryFrom<Bytes> for ListOffsetsResponse {
    type Error = Error;

    fn try_from(s: Bytes) -> Result<Self> {
        tracing::trace!("Parsing ListOffsetsResponse {:?}", s);
        let (_, list_offsets) =
            parse_list_offsets_response(NomBytes::new(s.clone())).map_err(|err| {
                tracing::error!("ERROR: Failed parsing ListOffsetsResponse {:?}", err);
                tracing::error!("ERROR: ListOffsetsResponse Bytes {:?}", s);
                Error::ParsingError(s)
            })?;
        tracing::trace!("Parsed ListOffsetsResponse {:?}", list_offsets);
        Ok(list_offsets)
    }
}

impl ListOffsetsResponse {
    pub fn into_box_iter(self) -> Box<impl Iterator<Item = (Bytes, Partition)>> {
        Box::new(self.topics.into_iter().flat_map(|topic| {
            topic
                .partitions
                .into_iter()
                .map(move |partition| (topic.name.clone(), partition))
        }))
    }
}

pub fn parse_list_offsets_response(s: NomBytes) -> IResult<NomBytes, ListOffsetsResponse> {
    let (s, header) = parse_header_response(s)?;
    let (s, topics) = parser::parse_array(parse_topic)(s)?;

    Ok((s, ListOffsetsResponse { header, topics }))
}

fn parse_topic(s: NomBytes) -> IResult<NomBytes, Topic> {
    let (s, name) = parser::parse_string(s)?;
    let (s, partitions) = parser::parse_array(parse_partition)(s)?;

    Ok((s, Topic { name, partitions }))
}

fn parse_partition(s: NomBytes) -> IResult<NomBytes, Partition> {
    let (s, partition_index) = be_i32(s)?;
    let (s, error_code) = parser::parse_kafka_code(s)?;
    let (s, timestamp) = be_i64(s)?;
    let (s, offset) = be_i64(s)?;

    Ok((
        s,
        Partition {
            partition_index,
            error_code,
            timestamp,
            offset,
        },
    ))
}
