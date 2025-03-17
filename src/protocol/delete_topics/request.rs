//! Encoding and creation for Delete Topics requests.
//!
//! ### Example
//! ```rust
//! let mut delete_topics_request = protocol::DeleteTopicsRequest::new(
//!     correlation_id,
//!     client_id,
//!     timeout_ms,
//! );
//! coordinator_conn.send_request(&delete_topics_request).await?;
//! ```
//!
//! ### Protocol Def
//! ```text
//! DeleteTopics Request (Version: 3) => [topic_names] timeout_ms
//!   topic_names => STRING
//!   timeout_ms => INT32
//! ```
//!
//! Note that we are using version 3 of this API

use crate::{encode::ToByte, error::Result, protocol::HeaderRequest};

const API_KEY_METADATA: i16 = 20;
const API_VERSION: i16 = 3;

/// The base Delete Topics request object.
///
/// ### Example
/// ```rust
/// let mut delete_topics_request = protocol::DeleteTopicsRequest::new(
///     correlation_id,
///     client_id,
///     timeout_ms,
/// );
/// coordinator_conn.send_request(&delete_topics_request).await?;
/// ```
#[derive(Debug)]
pub struct DeleteTopicsRequest<'a> {
    pub header: HeaderRequest<'a>,
    /// The topics to commit offsets for.
    pub topics: Vec<Topic<'a>>,
    /// How long to wait in milliseconds before timing out the request.
    pub timeout_ms: i32,
}

/// The topics to create.
#[derive(Debug)]
pub struct Topic<'a> {
    /// The topic name.
    pub name: &'a str,
}

impl<'a> DeleteTopicsRequest<'a> {
    /// Delete a new Delete Topics Request
    ///
    /// This request needs to be given topics and partitions to be deleted
    /// before being sent to the broker. You can do this by using the `add` method.
    pub fn new(correlation_id: i32, client_id: &'a str, timeout_ms: i32) -> Result<Self> {
        let header = HeaderRequest::new(API_KEY_METADATA, API_VERSION, correlation_id, client_id);
        Ok(Self {
            header,
            timeout_ms,
            topics: vec![],
        })
    }

    /// Add a topic to be deleted
    ///
    /// If the same topic is used twice, it will do nothing the second time
    ///
    /// ### Example
    /// ```rust
    /// delete_topics_request.add(
    ///     topic_name,
    /// );
    /// ```
    pub fn add(&mut self, topic_name: &'a str) {
        match self
            .topics
            .iter_mut()
            .find(|topic| topic.name == topic_name)
        {
            None => self.topics.push(Topic { name: topic_name }),
            Some(_) => {
                // do nothing
            }
        }
    }
}

impl ToByte for DeleteTopicsRequest<'_> {
    fn encode<T: bytes::BufMut>(&self, buffer: &mut T) -> crate::error::Result<()> {
        tracing::trace!("Encoding DeleteTopicsRequest {:?}", self);
        self.header.encode(buffer)?;
        self.topics.encode(buffer)?;
        self.timeout_ms.encode(buffer)?;
        Ok(())
    }
}

impl ToByte for Topic<'_> {
    fn encode<T: bytes::BufMut>(&self, buffer: &mut T) -> crate::error::Result<()> {
        self.name.encode(buffer)?;
        Ok(())
    }
}
