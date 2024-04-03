//! Parsing and processing for Delete Topics responses.
//!
//! ### Example
//! ```rust
//! let response_bytes = coordinator_conn.receive_response().await?;
//! let delete_topics_response = protocol::DeleteTopicsResponse::try_from(response_bytes.freeze())?;
//! ```
//!
//! ### Protocol Def
//! ```text
//!  DeleteTopics Response (Version: 3) => throttle_time_ms [responses]
//!    throttle_time_ms => INT32
//!    responses => name error_code
//!      name => STRING
//!      error_code => INT16
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
/// let offset_response = protocol::DeleteTopicsResponse::try_from(response_bytes.freeze())?;
/// ```
#[derive(Debug, PartialEq)]
pub struct DeleteTopicsResponse {
    pub header: HeaderResponse,
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    pub throttle_time_ms: i32,
    /// Results for each topic we tried to delete.
    pub topics: Vec<Topic>,
}

/// Results for each topic we tried to delete.
#[derive(Debug, PartialEq)]
pub struct Topic {
    /// The topic name.
    pub name: Bytes,
    /// The error code, or 0 if there was no error.
    pub error_code: KafkaCode,
}

// this helps us cast the server response into this type
impl TryFrom<Bytes> for DeleteTopicsResponse {
    type Error = Error;

    fn try_from(s: Bytes) -> Result<Self> {
        tracing::trace!("Parsing DeleteTopicsResponse {:?}", s);
        let (_, delete_topics) =
            parse_delete_topics_response(NomBytes::new(s.clone())).map_err(|err| {
                tracing::error!("ERROR: Failed parsing DeleteTopicsResponse {:?}", err);
                tracing::error!("ERROR: DeleteTopicsResponse Bytes {:?}", s);
                Error::ParsingError(s)
            })?;
        tracing::trace!("Parsed DeleteTopicsResponse {:?}", delete_topics);
        Ok(delete_topics)
    }
}

impl DeleteTopicsResponse {
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
            _ => Err(Error::KafkaError(self.error_code)),
        }
    }
}

pub fn parse_delete_topics_response(s: NomBytes) -> IResult<NomBytes, DeleteTopicsResponse> {
    let (s, header) = parse_header_response(s)?;
    let (s, throttle_time_ms) = be_i32(s)?;
    let (s, topics) = parse_array(parse_topic)(s)?;

    Ok((
        s,
        DeleteTopicsResponse {
            header,
            throttle_time_ms,
            topics,
        },
    ))
}

fn parse_topic(s: NomBytes) -> IResult<NomBytes, Topic> {
    let (s, name) = parser::parse_string(s)?;
    let (s, error_code) = parser::parse_kafka_code(s)?;

    Ok((s, Topic { name, error_code }))
}
