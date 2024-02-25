//! Encoding and creation for Fetch Offsets requests.
//!
//! ### Example
//! ```rust
//! let mut offset_request = protocol::OffsetFetchRequest::new(correlation_id, client_id, group_id);
//! coordinator_conn.send_request(&offset_request).await?;
//! ```
//!
//! ### Protocol Def
//! ```text
//! OffsetFetch Request (Version: 1) => group_id [topics]
//!   group_id => STRING
//!   topics => name [partition_indexes]
//!     name => STRING
//!     partition_indexes => INT32
//! ```

use crate::{encode::ToByte, protocol::HeaderRequest};

const API_KEY_METADATA: i16 = 9;
const API_VERSION: i16 = 2;

/// The base Offset Fetch request object.
///
/// ### Example
/// ```rust
/// let mut offset_request = protocol::OffsetFetchRequest::new(correlation_id, client_id, group_id);
/// coordinator_conn.send_request(&offset_request).await?;
/// ```
#[derive(Debug)]
pub struct OffsetFetchRequest<'a> {
    pub header: HeaderRequest<'a>,
    /// The group to fetch offsets for.
    pub group_id: &'a str,
    /// Each topic we would like to fetch offsets for, or null to fetch offsets for all topics.
    pub topics: Vec<Topic<'a>>,
}

/// Each topic we would like to fetch offsets for, or null to fetch offsets for all topics.
#[derive(Debug)]
pub struct Topic<'a> {
    /// The topic name.
    pub name: &'a str,
    /// The partition indexes we would like to fetch offsets for.
    pub partition_indexes: Vec<i32>,
}

impl<'a> OffsetFetchRequest<'a> {
    pub fn new(correlation_id: i32, client_id: &'a str, group_id: &'a str) -> Self {
        let header = HeaderRequest::new(API_KEY_METADATA, API_VERSION, correlation_id, client_id);
        Self {
            header,
            group_id,
            topics: vec![],
        }
    }

    pub fn add(&mut self, topic_name: &'a str, partition_index: i32) {
        match self
            .topics
            .iter_mut()
            .find(|topic| topic.name == topic_name)
        {
            None => self.topics.push(Topic {
                name: topic_name,
                partition_indexes: vec![partition_index],
            }),
            Some(topic) => {
                if !topic
                    .partition_indexes
                    .iter_mut()
                    .any(|index| *index == partition_index)
                {
                    topic.partition_indexes.push(partition_index)
                }
            }
        }
    }
}

impl<'a> ToByte for OffsetFetchRequest<'a> {
    fn encode<T: bytes::BufMut>(&self, buffer: &mut T) -> crate::error::Result<()> {
        tracing::trace!("Encoding OffsetFetchRequest {:?}", self);
        self.header.encode(buffer)?;
        self.group_id.encode(buffer)?;
        self.topics.encode(buffer)?;
        Ok(())
    }
}

impl<'a> ToByte for Topic<'a> {
    fn encode<T: bytes::BufMut>(&self, buffer: &mut T) -> crate::error::Result<()> {
        self.name.encode(buffer)?;
        self.partition_indexes.encode(buffer)?;
        Ok(())
    }
}
