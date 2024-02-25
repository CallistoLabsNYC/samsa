//! Parsing and processing for Metadata responses.
//!
//! The response contains metadata for each partition, with
//! partitions grouped together by topic. This metadata
//! refers to brokers by their broker id. The brokers each
//! have a host and port.
//!
//! ### Example
//! ```rust
//! let response_bytes = conn.receive_response().await?;
//! let metadata_response = protocol::MetadataResponse::try_from(response_bytes.freeze())?;
//! ```
//!
//! ### Protocol Def
//! ```text
//! Metadata Response (Version: 0) => [brokers] [topics]
//!   brokers => node_id host port
//!   node_id => INT32
//!   host => STRING
//!   port => INT32
//! topics => error_code name [partitions]
//!   error_code => INT16
//!   name => STRING
//!   partitions => error_code partition_index leader_id [replica_nodes] [isr_nodes]
//!     error_code => INT16
//!     partition_index => INT32
//!     leader_id => INT32
//!     replica_nodes => INT32
//!     isr_nodes => INT32
//! ```

use bytes::Bytes;
use nom::{number::complete::be_i32, IResult};
use nombytes::NomBytes;

use crate::{
    error::{Error, KafkaCode, Result},
    parser, protocol,
};

/// The base Metadata response object.
///
/// ### Example
/// ```rust
/// let response_bytes = conn.receive_response().await?;
/// let metadata_response = protocol::MetadataResponse::try_from(response_bytes.freeze())?;
/// ```
#[derive(Debug, Default, PartialEq)]
pub struct MetadataResponse {
    pub header_response: protocol::HeaderResponse,
    /// Each broker in the response.
    pub brokers: Vec<Broker>,
    /// Each topic in the response.
    pub topics: Vec<Topic>,
}

impl MetadataResponse {
    pub fn is_error(&self) -> Result<()> {
        self.topics
            .iter()
            .map(|topic| topic.is_error())
            .collect::<Result<Vec<()>>>()?;

        Ok(())
    }
}

impl TryFrom<Bytes> for MetadataResponse {
    type Error = Error;

    fn try_from(s: Bytes) -> Result<Self> {
        tracing::trace!("Parsing MetadataResponse {:?}", s);
        let (_, metadata) = parse_metadata_response(NomBytes::new(s.clone())).map_err(|err| {
            tracing::error!("ERROR: Failed parsing MetadataResponse {:?}", err);
            tracing::error!("ERROR: MetadataResponse Bytes {:?}", s);
            Error::ParsingError(s)
        })?;
        tracing::trace!("Parsed MetadataResponse {:?}", metadata);
        Ok(metadata)
    }
}

pub fn parse_metadata_response(s: NomBytes) -> IResult<NomBytes, MetadataResponse> {
    let (s, header_response) = protocol::parse_header_response(s)?;
    let (s, brokers) = parser::parse_array(parse_broker)(s)?;
    let (s, topics) = parser::parse_array(parse_topic)(s)?;

    Ok((
        s,
        MetadataResponse {
            header_response,
            brokers,
            topics,
        },
    ))
}

/// Each broker in the response.
#[derive(Debug, Clone, PartialEq)]
pub struct Broker {
    /// The broker ID.
    pub node_id: i32,
    /// The broker hostname.
    pub host: Bytes,
    /// The broker port.
    pub port: i32,
}

fn parse_broker(s: NomBytes) -> IResult<NomBytes, Broker> {
    let (s, node_id) = be_i32(s)?;
    let (s, host) = parser::parse_string(s)?;
    let (s, port) = be_i32(s)?;

    Ok((
        s,
        Broker {
            node_id,
            host,
            port,
        },
    ))
}

/// Each topic in the response.
#[derive(Debug, Clone, PartialEq)]
pub struct Topic {
    /// The topic error, or 0 if there was no error.
    pub error_code: KafkaCode,
    /// The topic name.
    pub name: Bytes,
    /// Each partition in the topic.
    pub partitions: Vec<Partition>,
}

impl Topic {
    pub fn is_error(&self) -> Result<()> {
        if self.error_code != KafkaCode::None {
            tracing::error!(
                "ERROR: Kafka Error {:?} in topic {:?}",
                self.error_code,
                self.name
            );
            return Err(Error::KafkaError(self.error_code));
        }

        self.partitions
            .iter()
            .map(|partition| partition.is_error(self.name.clone()))
            .collect::<Result<Vec<()>>>()?;

        Ok(())
    }
}

fn parse_topic(s: NomBytes) -> IResult<NomBytes, Topic> {
    let (s, error_code) = parser::parse_kafka_code(s)?;
    let (s, name) = parser::parse_string(s)?;
    let (s, partitions) = parser::parse_array(parse_partition)(s)?;

    Ok((
        s,
        Topic {
            error_code,
            name,
            partitions,
        },
    ))
}

/// Each partition in the topic.
#[derive(Debug, Clone, PartialEq)]
pub struct Partition {
    /// The partition error, or 0 if there was no error.
    pub error_code: KafkaCode,
    /// The partition index.
    pub partition_index: i32,
    /// The ID of the leader broker.
    pub leader_id: i32,
    /// The set of all nodes that host this partition.
    pub replica_nodes: Vec<i32>,
    /// The set of nodes that are in sync with the leader for this partition.
    pub isr_nodes: Vec<i32>,
}

impl Partition {
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

fn parse_partition(s: NomBytes) -> IResult<NomBytes, Partition> {
    let (s, error_code) = parser::parse_kafka_code(s)?;
    let (s, partition_index) = be_i32(s)?;
    let (s, leader_id) = be_i32(s)?;
    let (s, replica_nodes) = parser::parse_array(be_i32)(s)?;
    let (s, isr_nodes) = parser::parse_array(be_i32)(s)?;

    Ok((
        s,
        Partition {
            error_code,
            partition_index,
            leader_id,
            replica_nodes,
            isr_nodes,
        },
    ))
}
