//! Encoding and creation for Offset Commit requests.
//!
//! Note that when this API is used for a "simple consumer," which is not part of a consumer group, then the generationId must be set to -1 and the memberId must be empty (not null). Additionally, if there is an active consumer group with the same groupId, then the commit will be rejected (typically with an UNKNOWN_MEMBER_ID or ILLEGAL_GENERATION error).
//!
//! ### Example
//! ```rust
//! let mut offset_request = protocol::OffsetCommitRequest::new(
//!     correlation_id,
//!     client_id,
//!     group_id,
//!     generation_id,
//!     member_id,
//!     retention_time_ms,
//! );
//! coordinator_conn.send_request(&offset_request).await?;
//! ```
//!
//! ### Protocol Def
//! ```text
//! OffsetCommit Request (Version: 2) => group_id generation_id_or_member_epoch member_id retention_time_ms [topics]
//!   group_id => STRING
//!   generation_id_or_member_epoch => INT32
//!   member_id => STRING
//!   retention_time_ms => INT64
//!   topics => name [partitions]
//!     name => STRING
//!     partitions => partition_index committed_offset committed_metadata
//!       partition_index => INT32
//!       committed_offset => INT64
//!       committed_metadata => NULLABLE_STRING
//! ```
//!
//! Note that we are using version 2 of this API

use crate::{
    encode::ToByte,
    error::{Error, Result},
    protocol::HeaderRequest,
};

use bytes::Bytes;
use nom::AsBytes;

const API_KEY_METADATA: i16 = 8;
const API_VERSION: i16 = 2;

/// The base Offset Commit request object.
///
/// Note that when this API is used for a "simple consumer," which is not part of a consumer group, then the generationId must be set to -1 and the memberId must be empty (not null).
///
/// ### Example
/// ```rust
/// let mut offset_request = protocol::OffsetCommitRequest::new(
///     correlation_id,
///     client_id,
///     group_id,
///     generation_id,
///     member_id,
///     retention_time_ms,
/// );
/// coordinator_conn.send_request(&offset_request).await?;
/// ```
#[derive(Debug)]
pub struct OffsetCommitRequest<'a> {
    pub header: HeaderRequest<'a>,
    /// The unique group identifier.
    pub group_id: &'a str,
    /// The generation of the group if using the generic group protocol or the member epoch if using the consumer protocol.
    pub generation_id_or_member_epoch: i32,
    /// The member ID assigned by the group coordinator.
    pub member_id: String,
    /// The time period in ms to retain the offset.
    pub retention_time_ms: i64,
    /// The topics to commit offsets for.
    pub topics: Vec<Topic<'a>>,
}

/// The topics to commit offsets for.
#[derive(Debug)]
pub struct Topic<'a> {
    /// The topic name.
    pub name: &'a str,
    /// Each partition to commit offsets for.
    pub partitions: Vec<Partition<'a>>,
}

/// Each partition to commit offsets for.
///
#[derive(Debug)]
pub struct Partition<'a> {
    /// The partition index.
    pub partition_index: i32,
    /// The message offset to be committed.
    pub committed_offset: i64,
    /// Any associated metadata the client wants to keep.
    pub committed_metadata: Option<&'a str>,
}

impl<'a> OffsetCommitRequest<'a> {
    /// Create a new Offset Commit Request
    ///
    /// This request needs to be given commits for a topic and partition
    /// before being sent to the broker. You can do this by using the `add` method.
    pub fn new(
        correlation_id: i32,
        client_id: &'a str,
        group_id: &'a str,
        generation_id_or_member_epoch: i32,
        member_id: Bytes,
        retention_time_ms: i64,
    ) -> Result<Self> {
        let header = HeaderRequest::new(API_KEY_METADATA, API_VERSION, correlation_id, client_id);
        Ok(Self {
            header,
            group_id,
            generation_id_or_member_epoch,
            member_id: String::from_utf8(member_id.as_bytes().to_vec())
                .map_err(|_| Error::DecodingUtf8Error)?,
            retention_time_ms,
            topics: vec![],
        })
    }

    /// Add an offset to be committed for a given topic and partition
    ///
    /// This will stage a new commit for a given topic and partition
    /// that will be sent along with this request.
    ///
    /// If the same topic and partition is used twice, the offset will be
    /// overwritten.
    ///
    /// ### Example
    /// ```rust
    /// offset_request.add(
    ///     topic_name,
    ///     partition_index,
    ///     committed_offset,
    ///     Some("metadata"),
    /// );
    /// ```
    pub fn add(
        &mut self,
        topic_name: &'a str,
        partition_index: i32,
        committed_offset: i64,
        committed_metadata: Option<&'a str>,
    ) {
        match self
            .topics
            .iter_mut()
            .find(|topic| topic.name == topic_name)
        {
            None => self.topics.push(Topic {
                name: topic_name,
                partitions: vec![Partition {
                    partition_index,
                    committed_offset,
                    committed_metadata,
                }],
            }),
            Some(topic) => {
                match topic
                    .partitions
                    .iter_mut()
                    .find(|partition| partition.partition_index == partition_index)
                {
                    None => topic.partitions.push(Partition {
                        partition_index,
                        committed_offset,
                        committed_metadata,
                    }),
                    Some(partition) => {
                        tracing::warn!(
                            "Overwriting commit offset for {} {}",
                            topic_name,
                            partition_index
                        );
                        partition.committed_offset = committed_offset;
                    }
                }
            }
        }
    }
}

impl<'a> ToByte for OffsetCommitRequest<'a> {
    fn encode<T: bytes::BufMut>(&self, buffer: &mut T) -> crate::error::Result<()> {
        tracing::trace!("Encoding OffsetCommitRequest {:?}", self);
        self.header.encode(buffer)?;
        self.group_id.encode(buffer)?;
        self.generation_id_or_member_epoch.encode(buffer)?;
        self.member_id.encode(buffer)?;
        self.retention_time_ms.encode(buffer)?;
        self.topics.encode(buffer)?;
        Ok(())
    }
}

impl<'a> ToByte for Topic<'a> {
    fn encode<T: bytes::BufMut>(&self, buffer: &mut T) -> crate::error::Result<()> {
        self.name.encode(buffer)?;
        self.partitions.encode(buffer)?;
        Ok(())
    }
}

impl<'a> ToByte for Partition<'a> {
    fn encode<T: bytes::BufMut>(&self, buffer: &mut T) -> crate::error::Result<()> {
        self.partition_index.encode(buffer)?;
        self.committed_offset.encode(buffer)?;
        self.committed_metadata.encode(buffer)?;
        Ok(())
    }
}
