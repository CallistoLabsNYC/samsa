//! Encoding and creation for List Offsets requests.
//!
//! Used to ask for all messages before a certain time (ms). There are two special values. Specify -1 to receive the latest offset (i.e. the offset of the next coming message) and -2 to receive the earliest available offset. This applies to all versions of the API. Note that because offsets are pulled in descending order, asking for the earliest offset will always return you a single element.
//!
//! ### Example
//! ```rust
//! let mut list_offsets_request = protocol::ListOffsetsRequest::new(correlation_id, client_id, -1);
//! broker_conn.send_request(&list_offsets_request).await?;
//! ```
//!
//! ### Protocol Def
//! ```text
//! ListOffsets Request (Version: 1) => replica_id [topics]
//!   replica_id => INT32
//!   topics => name [partitions]
//!     name => STRING
//!     partitions => partition_index timestamp
//!       partition_index => INT32
//!       timestamp => INT64
//! ```
//!
//! Note we are using version 1 of the request.

use crate::{encode::ToByte, protocol::HeaderRequest};

const API_KEY_METADATA: i16 = 2;
const API_VERSION: i16 = 1;

/// The base List Offsets request object.
///
/// ### Example
/// ```rust
/// let mut list_offsets_request = protocol::ListOffsetsRequest::new(correlation_id, client_id, -1);
/// broker_conn.send_request(&list_offsets_request).await?;
/// ```
#[derive(Debug)]
pub struct ListOffsetsRequest<'a> {
    pub header: HeaderRequest<'a>,
    /// The broker ID of the requester.
    pub replica_id: ReplicaId,
    /// Each topic in the request.
    pub topics: Vec<Topic<'a>>,
}

/// Each topic in the request.
#[derive(Debug)]
pub struct Topic<'a> {
    /// The topic name.
    pub name: &'a str,
    /// Each partition in the request.
    pub partitions: Vec<Partition>,
}

/// Each partition in the request.
#[derive(Debug)]
pub struct Partition {
    /// The partition index.
    pub partition_index: i32,
    /// The timestamp to query offsets for.
    pub timestamp: TimestampQuery,
}

#[derive(Debug, Clone, Copy)]
pub enum ReplicaId {
    /// Normal consumer request (-1)
    NormalConsumer,
    /// Broker id when used by a broker (>= 0)
    Broker(i32),
}

impl ToByte for ReplicaId {
    fn encode<T: bytes::BufMut>(&self, buffer: &mut T) -> crate::error::Result<()> {
        match *self {
            ReplicaId::NormalConsumer => (-1i32).encode(buffer),
            ReplicaId::Broker(id) => id.encode(buffer),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum TimestampQuery {
    /// Latest offset (-1)
    Latest,
    /// Earliest available offset (-2)
    Earliest,
    /// Specific timestamp in ms
    At(i64),
}

impl ToByte for TimestampQuery {
    fn encode<T: bytes::BufMut>(&self, buffer: &mut T) -> crate::error::Result<()> {
        match *self {
            TimestampQuery::Latest => (-1i64).encode(buffer),
            TimestampQuery::Earliest => (-2i64).encode(buffer),
            TimestampQuery::At(ts) => ts.encode(buffer),
        }
    }
}

impl<'a> ListOffsetsRequest<'a> {
    pub fn new(correlation_id: i32, client_id: &'a str, replica_id: ReplicaId) -> Self {
        let header = HeaderRequest::new(API_KEY_METADATA, API_VERSION, correlation_id, client_id);
        Self {
            header,
            replica_id,
            topics: vec![],
        }
    }

    pub fn add(
        &mut self,
        topic_name: &'a str,
        partition_index: i32,
        timestamp: TimestampQuery,
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
                    timestamp,
                }],
            }),
            Some(topic) => {
                if !topic
                    .partitions
                    .iter_mut()
                    .any(|partition| partition.partition_index == partition_index)
                {
                    topic.partitions.push(Partition {
                        partition_index,
                        timestamp,
                    })
                }
            }
        }
    }
}

impl ToByte for ListOffsetsRequest<'_> {
    fn encode<T: bytes::BufMut>(&self, buffer: &mut T) -> crate::error::Result<()> {
        tracing::trace!("Encoding ListOffsetRequest {:?}", self);
        self.header.encode(buffer)?;
        self.replica_id.encode(buffer)?;
        self.topics.encode(buffer)?;
        Ok(())
    }
}

impl ToByte for Topic<'_> {
    fn encode<T: bytes::BufMut>(&self, buffer: &mut T) -> crate::error::Result<()> {
        self.name.encode(buffer)?;
        self.partitions.encode(buffer)?;
        Ok(())
    }
}

impl ToByte for Partition {
    fn encode<T: bytes::BufMut>(&self, buffer: &mut T) -> crate::error::Result<()> {
        self.partition_index.encode(buffer)?;
        self.timestamp.encode(buffer)?;
        Ok(())
    }
}
