//! Parsing and processing for Offset Commit responses.
//!
//! ### Example
//! ```rust
//! let response_bytes = coordinator_conn.receive_response().await?;
//! let offset_response = protocol::OffsetCommitResponse::try_from(response_bytes.freeze())?;
//! ```
//!
//! ### Protocol Def
//! ```text
//! OffsetCommit Response (Version: 2) => [topics]
//!   topics => name [partitions]
//!     name => STRING
//!     partitions => partition_index error_code
//!       partition_index => INT32
//!       error_code => INT16
//! ```
//! Note we are using version 2 of this response

use bytes::Bytes;
use nom::{number::complete::be_i32, IResult};
use nombytes::NomBytes;

use crate::{
    error::{Error, KafkaCode, Result},
    parser::{self, parse_array},
    protocol::{parse_header_response, HeaderResponse},
};

/// The base Offset Commit response object.
///
/// ### Example
/// ```rust
/// let response_bytes = coordinator_conn.receive_response().await?;
/// let offset_response = protocol::OffsetCommitResponse::try_from(response_bytes.freeze())?;
/// ```
#[derive(Debug, PartialEq)]
pub struct OffsetCommitResponse {
    pub header: HeaderResponse,
    pub topics: Vec<Topic>,
}

#[derive(Debug, PartialEq)]
pub struct Topic {
    pub name: Bytes,
    pub partitions: Vec<Partition>,
}

#[derive(Debug, PartialEq)]
pub struct Partition {
    pub partition_index: i32,
    pub error_code: KafkaCode,
}

// this helps us cast the server response into this type
impl TryFrom<Bytes> for OffsetCommitResponse {
    type Error = Error;

    fn try_from(s: Bytes) -> Result<Self> {
        tracing::trace!("Parsing OffsetCommitResponse {:?}", s);
        let (_, offset_commit) =
            parse_offset_commit_response(NomBytes::new(s.clone())).map_err(|err| {
                tracing::error!("ERROR: Failed parsing OffsetCommitResponse {:?}", err);
                tracing::error!("ERROR: OffsetCommitResponse Bytes {:?}", s);
                Error::ParsingError(s)
            })?;
        tracing::trace!("Parsed OffsetCommitResponse {:?}", offset_commit);
        Ok(offset_commit)
    }
}

impl OffsetCommitResponse {
    pub fn is_error(&self) -> Result<()> {
        self.topics
            .iter()
            .map(|topic| topic.is_error())
            .collect::<Result<Vec<()>>>()?;

        Ok(())
    }
}

impl Topic {
    /// Surface a KafkaError.
    pub fn is_error(&self) -> Result<()> {
        self.partitions
            .iter()
            .map(|partition| partition.is_error(self.name.clone()))
            .collect::<Result<Vec<()>>>()?;

        Ok(())
    }
}

impl Partition {
    /// Surface a KafkaError.
    pub fn is_error(&self, topic_name: Bytes) -> Result<()> {
        if self.error_code != KafkaCode::None {
            tracing::error!(
                "ERROR: Kafka Error {:?} in topic {:?} partition {}",
                self.error_code,
                topic_name,
                self.partition_index
            );
            Err(Error::KafkaError(self.error_code))
        } else {
            Ok(())
        }
    }
}

pub fn parse_offset_commit_response(s: NomBytes) -> IResult<NomBytes, OffsetCommitResponse> {
    let (s, header) = parse_header_response(s)?;
    let (s, topics) = parse_array(parse_topic)(s)?;

    Ok((s, OffsetCommitResponse { header, topics }))
}

fn parse_topic(s: NomBytes) -> IResult<NomBytes, Topic> {
    let (s, name) = parser::parse_string(s)?;
    let (s, partitions) = parser::parse_array(parse_partition)(s)?;

    Ok((s, Topic { name, partitions }))
}

fn parse_partition(s: NomBytes) -> IResult<NomBytes, Partition> {
    let (s, partition_index) = be_i32(s)?;
    let (s, error_code) = parser::parse_kafka_code(s)?;

    Ok((
        s,
        Partition {
            partition_index,
            error_code,
        },
    ))
}
