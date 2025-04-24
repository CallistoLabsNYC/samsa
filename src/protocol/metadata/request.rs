//! Encoding and creation for Metadata requests.
//!
//! ### Example
//! ```rust
//! let metadata_request = protocol::MetadataRequest::new(1, self.client_id, &self.topic_names);
//! conn.send_request(&metadata_request).await?;
//! ```
//!
//! ### Protocol Def
//! ```text
//! Metadata Request (Version: 1) => [topics]
//!   topics => name
//!     name => STRING
//! ```
//!
//! Note we are using version 0 of the request.

use bytes::BufMut;

use crate::{
    encode::{AsStrings, ToByte},
    error::Result,
    protocol::HeaderRequest,
};

const API_KEY_METADATA: i16 = 3;
const API_VERSION: i16 = 1;

/// The base Metadata request object.
///
/// ### Example
/// ```rust
/// let metadata_request = protocol::MetadataRequest::new(1, self.client_id, &self.topic_names);
/// conn.send_request(&metadata_request).await?;
/// ```
#[derive(Debug)]
pub struct MetadataRequest<'a, T> {
    pub header: HeaderRequest<'a>,
    /// The topics to fetch metadata for.
    /// Kafka expects the topic to be None for it to return all topics
    pub topics: Option<&'a [T]>,
}

impl<'a, T: AsRef<str>> MetadataRequest<'a, T> {
    pub fn new(correlation_id: i32, client_id: &'a str, topics: &'a [T]) -> MetadataRequest<'a, T> {
        // better to make the topic into an option here than to change the whole code.
        let topics = if topics.is_empty() {
            None
        } else {
            Some(topics)
        };

        MetadataRequest {
            header: HeaderRequest::new(API_KEY_METADATA, API_VERSION, correlation_id, client_id),
            topics,
        }
    }
}

impl<'a, T: AsRef<str> + 'a> ToByte for MetadataRequest<'a, T> {
    fn encode<W: BufMut>(&self, buffer: &mut W) -> Result<()> {
        self.header.encode(buffer)?;

        match self.topics {
            Some(topics) => AsStrings(topics).encode(buffer)?,
            None => {
                // Kafka protocol uses -1 to signal a null array
                buffer.put_i32(-1);
            }
        }
        Ok(())
    }
}
