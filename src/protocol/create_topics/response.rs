//! Parsing and processing for Create Topics responses.
//!
//! ### Example
//! ```rust
//! let response_bytes = coordinator_conn.receive_response().await?;
//! let create_topics_response = protocol::CreateTopicsResponse::try_from(response_bytes.freeze())?;
//! ```
//!
//! ### Protocol Def
//! ```text
//! CreateTopics Response (Version: 3) => throttle_time_ms [topics]
//!   throttle_time_ms => INT32
//!   topics => name error_code error_message
//!     name => STRING
//!     error_code => INT16
//!     error_message => NULLABLE_STRING
//! ```
//! Note we are using version 3 of this response

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
/// let offset_response = protocol::CreateTopicsResponse::try_from(response_bytes.freeze())?;
/// ```
#[derive(Debug, PartialEq)]
pub struct CreateTopicsResponse {
    pub header: HeaderResponse,
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    pub throttle_time_ms: i32,
    /// Results for each topic we tried to create.
    pub topics: Vec<Topic>,
}

/// Results for each topic we tried to create.
#[derive(Debug, PartialEq)]
pub struct Topic {
    /// The topic name.
    pub name: Bytes,
    /// The error code, or 0 if there was no error.
    pub error_code: KafkaCode,
    /// The error message, or null if there was no error.
    pub error_message: Option<Bytes>,
}

// this helps us cast the server response into this type
impl TryFrom<Bytes> for CreateTopicsResponse {
    type Error = Error;

    fn try_from(s: Bytes) -> Result<Self> {
        tracing::trace!("Parsing CreateTopicsResponse {:?}", s);
        let (_, create_topics) =
            parse_create_topics_response(NomBytes::new(s.clone())).map_err(|err| {
                tracing::error!("ERROR: Failed parsing CreateTopicsResponse {:?}", err);
                tracing::error!("ERROR: CreateTopicsResponse Bytes {:?}", s);
                Error::ParsingError(s)
            })?;
        tracing::trace!("Parsed CreateTopicsResponse {:?}", create_topics);
        Ok(create_topics)
    }
}

impl CreateTopicsResponse {
    /// Surface a KafkaError.
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
        match self.error_code {
            KafkaCode::None => Ok(()),
            _ => {
                tracing::error!("Kafka error: {:?}", self.error_message);
                Err(Error::KafkaError(self.error_code))
            }
        }
    }
}

pub fn parse_create_topics_response(s: NomBytes) -> IResult<NomBytes, CreateTopicsResponse> {
    let (s, header) = parse_header_response(s)?;
    let (s, throttle_time_ms) = be_i32(s)?;
    let (s, topics) = parse_array(parse_topic)(s)?;

    Ok((
        s,
        CreateTopicsResponse {
            header,
            throttle_time_ms,
            topics,
        },
    ))
}

fn parse_topic(s: NomBytes) -> IResult<NomBytes, Topic> {
    let (s, name) = parser::parse_string(s)?;
    let (s, error_code) = parser::parse_kafka_code(s)?;
    let (s, error_message) = parser::parse_nullable_string(s)?;

    Ok((
        s,
        Topic {
            name,
            error_code,
            error_message,
        },
    ))
}
