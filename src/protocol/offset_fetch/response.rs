//! Parsing and processing for Fetch Offsets responses.
//!
//! ### Example
//! ```rust
//! let response_bytes = coordinator_conn.receive_response().await?;
//! let offset_response = protocol::OffsetFetchResponse::try_from(response_bytes.freeze());
//! ```
//!
//! ### Protocol Def
//! ```text
//! OffsetFetch Response (Version: 2) => [topics] error_code
//!   topics => name [partitions]
//!   name => STRING
//!   partitions => partition_index committed_offset metadata error_code
//!     partition_index => INT32
//!     committed_offset => INT64
//!     metadata => NULLABLE_STRING
//!     error_code => INT16
//! error_code => INT16
//! ```
//!
//! Note we are using version 2 for the response.

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

/// The base Offset Fetch response object.
///
/// Note that if there is no offset associated with a topic-partition under that consumer group the broker does not set an error code (since it is not really an error), but returns empty metadata and sets the offset field to -1.
///
/// ### Example
/// ```rust
/// let response_bytes = coordinator_conn.receive_response().await?;
/// let offset_response = protocol::OffsetFetchResponse::try_from(response_bytes.freeze());
/// ```
#[derive(Debug, PartialEq)]
pub struct OffsetFetchResponse {
    pub header: HeaderResponse,
    /// The responses per topic.
    pub topics: Vec<Topic>,
    /// The top-level error code, or 0 if there was no error.
    pub error_code: KafkaCode,
}

/// The responses per topic.
#[derive(Debug, PartialEq)]
pub struct Topic {
    /// The topic name.
    pub name: Bytes,
    /// The responses per partition.
    pub partitions: Vec<Partition>,
}

/// The responses per partition.
#[derive(Debug, PartialEq)]
pub struct Partition {
    /// The partition index.
    pub partition_index: i32,
    /// The committed message offset.
    pub committed_offset: i64,
    /// The partition metadata.
    pub metadata: Option<Bytes>,
    /// The error code, or 0 if there was no error.
    pub error_code: KafkaCode,
}

impl TryFrom<Bytes> for OffsetFetchResponse {
    type Error = Error;

    fn try_from(s: Bytes) -> Result<Self> {
        tracing::trace!("Parsing OffsetFetchResponse {:?}", s);
        let (_, offset_fetch) =
            parse_offset_fetch_response(NomBytes::new(s.clone())).map_err(|err| {
                tracing::error!("ERROR: Failed parsing OffsetFetchResponse {:?}", err);
                tracing::error!("ERROR: OffsetFetchResponse Bytes {:?}", s);
                Error::ParsingError(s)
            })?;
        tracing::trace!("Parsed OffsetFetchResponse {:?}", offset_fetch);
        Ok(offset_fetch)
    }
}

impl OffsetFetchResponse {
    pub fn into_box_iter(self) -> Box<impl Iterator<Item = (Bytes, Partition)>> {
        Box::new(self.topics.into_iter().flat_map(|topic| {
            topic
                .partitions
                .into_iter()
                .map(move |partition| (topic.name.clone(), partition))
        }))
    }
}

pub fn parse_offset_fetch_response(s: NomBytes) -> IResult<NomBytes, OffsetFetchResponse> {
    let (s, header) = parse_header_response(s)?;
    let (s, topics) = parser::parse_array(parse_topic)(s)?;
    let (s, error_code) = parser::parse_kafka_code(s)?;

    Ok((
        s,
        OffsetFetchResponse {
            header,
            topics,
            error_code,
        },
    ))
}

fn parse_topic(s: NomBytes) -> IResult<NomBytes, Topic> {
    let (s, name) = parser::parse_string(s)?;
    let (s, partitions) = parser::parse_array(parse_partition)(s)?;

    Ok((s, Topic { name, partitions }))
}

fn parse_partition(s: NomBytes) -> IResult<NomBytes, Partition> {
    let (s, partition_index) = be_i32(s)?;
    let (s, committed_offset) = be_i64(s)?;
    let (s, metadata) = parser::parse_nullable_string(s)?;
    let (s, error_code) = parser::parse_kafka_code(s)?;

    Ok((
        s,
        Partition {
            partition_index,
            committed_offset,
            metadata,
            error_code,
        },
    ))
}
