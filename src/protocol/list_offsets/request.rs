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
    /// The broker ID of the requester, or -1 if this request is being made by a normal consumer.
    pub replica_id: i32,
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
    /// The current timestamp.
    pub timestamp: i64,
}

impl<'a> ListOffsetsRequest<'a> {
    pub fn new(correlation_id: i32, client_id: &'a str, replica_id: i32) -> Self {
        let header = HeaderRequest::new(API_KEY_METADATA, API_VERSION, correlation_id, client_id);
        Self {
            header,
            replica_id,
            topics: vec![],
        }
    }

    pub fn add(&mut self, topic_name: &'a str, partition_index: i32, timestamp: i64) {
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

impl<'a> ToByte for ListOffsetsRequest<'a> {
    fn encode<T: bytes::BufMut>(&self, buffer: &mut T) -> crate::error::Result<()> {
        tracing::trace!("Encoding ListOffsetRequest {:?}", self);
        self.header.encode(buffer)?;
        self.replica_id.encode(buffer)?;
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

impl ToByte for Partition {
    fn encode<T: bytes::BufMut>(&self, buffer: &mut T) -> crate::error::Result<()> {
        self.partition_index.encode(buffer)?;
        self.timestamp.encode(buffer)?;
        Ok(())
    }
}
