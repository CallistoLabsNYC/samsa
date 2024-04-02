//! Encoding and creation for Create Topics requests.
//!
//! ### Example
//! ```rust
//! let mut create_topics_request = protocol::CreateTopicsRequest::new(
//!     correlation_id,
//!     client_id,
//!     timeout_ms,
//!     validate_only,
//! );
//! coordinator_conn.send_request(&create_topics_request).await?;
//! ```
//!
//! ### Protocol Def
//! ```text
//! CreateTopics Request (Version: 3) => [topics] timeout_ms validate_only
//!  topics => name num_partitions replication_factor [assignments] [configs]
//!    name => STRING
//!    num_partitions => INT32
//!    replication_factor => INT16
//!    assignments => partition_index [broker_ids]
//!      partition_index => INT32
//!      broker_ids => INT32
//!    configs => name value
//!      name => STRING
//!      value => NULLABLE_STRING
//!  timeout_ms => INT32
//!  validate_only => BOOLEAN
//! ```
//!
//! Note that we are using version 3 of this API

use crate::{encode::ToByte, error::Result, protocol::HeaderRequest};

const API_KEY_METADATA: i16 = 19;
const API_VERSION: i16 = 3;

/// The base Create Topics request object.
///
/// ### Example
/// ```rust
/// let mut create_topics_request = protocol::CreateTopicsRequest::new(
///     correlation_id,
///     client_id,
///     timeout_ms,
///     validate_only,
/// );
/// coordinator_conn.send_request(&create_topics_request).await?;
/// ```
#[derive(Debug)]
pub struct CreateTopicsRequest<'a> {
    pub header: HeaderRequest<'a>,
    /// The topics to commit offsets for.
    pub topics: Vec<Topic<'a>>,
    /// How long to wait in milliseconds before timing out the request.
    pub timeout_ms: i32,
    /// If true, check that the topics can be created as specified, but don't create anything.
    pub validate_only: bool,
}

/// The topics to create.
#[derive(Debug)]
pub struct Topic<'a> {
    /// The topic name.
    pub name: &'a str,
    /// The number of partitions to create in the topic, or -1 if we are either specifying a manual partition assignment or using the default partitions.
    pub num_partitions: i32,
    /// The number of replicas to create for each partition in the topic, or -1 if we are either specifying a manual partition assignment or using the default replication factor.
    pub replication_factor: i16,
    /// The manual partition assignment, or the empty array if we are using automatic assignment.
    pub assignments: Vec<Assignment>,
    /// The custom topic configurations to set.
    pub configs: Vec<Config>,
}

/// The manual partition assignment, or the empty array if we are using automatic assignment.
#[derive(Debug)]
pub struct Assignment {
    /// The partition index.
    partition_index: i32,
    /// The brokers to place the partition on.
    broker_ids: Vec<i32>,
}

/// The custom topic configurations to set.
#[derive(Debug)]
pub struct Config {
    /// The configuration name.
    name: String,
    /// The configuration value.
    value: Option<String>,
}

impl<'a> CreateTopicsRequest<'a> {
    /// Create a new Create Topics Request
    ///
    /// This request needs to be given topics and partitions to be created
    /// before being sent to the broker. You can do this by using the `add` method.
    pub fn new(
        correlation_id: i32,
        client_id: &'a str,
        timeout_ms: i32,
        validate_only: bool,
    ) -> Result<Self> {
        let header = HeaderRequest::new(API_KEY_METADATA, API_VERSION, correlation_id, client_id);
        Ok(Self {
            header,
            timeout_ms,
            validate_only,
            topics: vec![],
        })
    }

    /// Add a topic to be create
    ///
    /// If the same topic is used twice, it will do nothing the second time
    ///
    /// ### Example
    /// ```rust
    /// create_topics_request.add(
    ///     topic_name,
    ///     num_partitions,
    ///     replication_factor,
    /// );
    /// ```
    pub fn add(&mut self, topic_name: &'a str, num_partitions: i32, replication_factor: i16) {
        match self
            .topics
            .iter_mut()
            .find(|topic| topic.name == topic_name)
        {
            None => self.topics.push(Topic {
                name: topic_name,
                num_partitions,
                replication_factor,
                assignments: vec![],
                configs: vec![],
            }),
            Some(_) => {
                // do nothing
            }
        }
    }
}

impl<'a> ToByte for CreateTopicsRequest<'a> {
    fn encode<T: bytes::BufMut>(&self, buffer: &mut T) -> crate::error::Result<()> {
        tracing::trace!("Encoding CreateTopicsRequest {:?}", self);
        self.header.encode(buffer)?;
        self.topics.encode(buffer)?;
        self.timeout_ms.encode(buffer)?;
        self.validate_only.encode(buffer)?;
        Ok(())
    }
}

impl<'a> ToByte for Topic<'a> {
    fn encode<T: bytes::BufMut>(&self, buffer: &mut T) -> crate::error::Result<()> {
        self.name.encode(buffer)?;
        self.num_partitions.encode(buffer)?;
        self.replication_factor.encode(buffer)?;
        self.assignments.encode(buffer)?;
        self.configs.encode(buffer)?;
        Ok(())
    }
}

impl ToByte for Assignment {
    fn encode<T: bytes::BufMut>(&self, buffer: &mut T) -> crate::error::Result<()> {
        self.partition_index.encode(buffer)?;
        self.broker_ids.encode(buffer)?;
        Ok(())
    }
}

impl ToByte for Config {
    fn encode<T: bytes::BufMut>(&self, buffer: &mut T) -> crate::error::Result<()> {
        self.name.encode(buffer)?;
        self.value.encode(buffer)?;
        Ok(())
    }
}
