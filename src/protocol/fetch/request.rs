//! Encoding and creation for Fetch requests.
//!
//! ### Example
//! ```rust
//! ```
//!
//! ### Protocol Def
//! ```text
//! Fetch Request (Version: 4) => replica_id max_wait_ms min_bytes max_bytes isolation_level [topics]
//!   replica_id => INT32
//!   max_wait_ms => INT32
//!   min_bytes => INT32
//!   max_bytes => INT32
//!   isolation_level => INT8
//!   topics => topic [partitions]
//!     topic => STRING
//!     partitions => partition fetch_offset partition_max_bytes
//!       partition => INT32
//!       fetch_offset => INT64
//!       partition_max_bytes => INT32
//! ```
//!
//! Note we are using version 4 of the request.

use bytes::BufMut;

use crate::{encode::ToByte, error::Result, protocol::HeaderRequest};

const API_KEY_FETCH: i16 = 1;
const API_VERSION: i16 = 4;

#[derive(Debug, Clone)]
pub struct FetchRequest<'a> {
    pub header: HeaderRequest<'a>,
    /// The broker ID of the follower, of -1 if this request is from a consumer.
    pub replica: i32,
    /// The maximum time in milliseconds to wait for the response.
    pub max_wait_ms: i32,
    /// The minimum bytes to accumulate in the response.
    pub min_bytes: i32,
    /// The maximum bytes to fetch. See KIP-74 for cases where this limit may not be honored.
    pub max_bytes: i32,
    /// This setting controls the visibility of transactional records. Using READ_UNCOMMITTED (isolation_level = 0) makes all records visible. With READ_COMMITTED (isolation_level = 1), non-transactional and COMMITTED transactional records are visible. To be more concrete, READ_COMMITTED returns all data from offsets smaller than the current LSO (last stable offset), and enables the inclusion of the list of aborted transactions in the result, which allows consumers to discard ABORTED transactional records
    pub isolation_level: i8,
    /// The topics to fetch.
    pub topics: Vec<TopicPartition<'a>>,
}

/// The topics to fetch.
#[derive(Debug, Clone)]
pub struct TopicPartition<'a> {
    /// The name of the topic to fetch.
    pub topic_name: &'a str,
    /// The partitions to fetch.
    pub partitions: Vec<Partition>,
}

/// The partitions to fetch.
#[derive(Debug, Clone)]
pub struct Partition {
    /// The partition index.
    pub partition_index: i32,
    /// The message offset.
    pub offset: i64,
    /// The maximum bytes to fetch from this partition. See KIP-74 for cases where this limit may not be honored.
    pub max_bytes: i32,
}

impl<'a> FetchRequest<'a> {
    pub fn new(
        correlation_id: i32,
        client_id: &'a str,
        max_wait_ms: i32,
        min_bytes: i32,
        max_bytes: i32,
        isolation_level: i8,
    ) -> FetchRequest<'a> {
        FetchRequest {
            header: HeaderRequest::new(API_KEY_FETCH, API_VERSION, correlation_id, client_id),
            replica: -1,
            max_wait_ms,
            min_bytes,
            max_bytes,
            isolation_level,
            topics: vec![],
        }
    }

    pub fn add(&mut self, topic_name: &'a str, partition_index: i32, offset: i64, max_bytes: i32) {
        match self
            .topics
            .iter_mut()
            .find(|topic| topic.topic_name == topic_name)
        {
            None => self.topics.push(TopicPartition {
                topic_name,
                partitions: vec![Partition {
                    partition_index,
                    offset,
                    max_bytes,
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
                        offset,
                        max_bytes,
                    })
                }
            }
        }
    }
}

impl<'a> ToByte for FetchRequest<'a> {
    fn encode<W: BufMut>(&self, buffer: &mut W) -> Result<()> {
        tracing::trace!("Encoding FetchRequest {:?}", self);
        self.header.encode(buffer)?;
        self.replica.encode(buffer)?;
        self.max_wait_ms.encode(buffer)?;
        self.min_bytes.encode(buffer)?;
        self.max_bytes.encode(buffer)?;
        self.isolation_level.encode(buffer)?;
        self.topics.encode(buffer)?;
        Ok(())
    }
}

impl<'a> ToByte for TopicPartition<'a> {
    fn encode<W: BufMut>(&self, buffer: &mut W) -> Result<()> {
        self.topic_name.encode(buffer)?;
        self.partitions.encode(buffer)?;
        Ok(())
    }
}

impl ToByte for Partition {
    fn encode<T: BufMut>(&self, buffer: &mut T) -> Result<()> {
        self.partition_index.encode(buffer)?;
        self.offset.encode(buffer)?;
        self.max_bytes.encode(buffer)?;
        Ok(())
    }
}
