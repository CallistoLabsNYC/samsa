//! Parsing and processing for Sync Group responses.
//!
//! Each member in the group will receive the assignment
//! from the leader in the sync group response.
//!
//! ### Example
//! ```rust
//! let response_bytes = conn.receive_response().await?;
//! let sync_response = protocol::SyncGroupResponse::try_from(sync_response.freeze());
//! ```
//!
//! ### Protocol Def
//! ```text
//! SyncGroup Response (Version: 2) => throttle_time_ms error_code assignment
//!   throttle_time_ms => INT32
//!   error_code => INT16
//!   assignment => BYTES
//!
//! MemberAssignment => Version PartitionAssignment
//!   Version => int16
//!   PartitionAssignment => [Topic [Partition]]
//!     Topic => string
//!     Partition => int32
//!   UserData => bytes
//! ```
//!
//! Note that we are using version 2 of this API.

use bytes::Bytes;
use nom::{
    number::complete::{be_i16, be_i32},
    IResult,
};
use nombytes::NomBytes;

use crate::{
    error::{Error, KafkaCode, Result},
    parser, protocol,
};

/// The base Sync Group response object.
/// ### Example
/// ```rust
/// let response_bytes = conn.receive_response().await?;
/// let sync_response = protocol::SyncGroupResponse::try_from(response_bytes.freeze());
/// ```
#[derive(Debug, PartialEq)]
pub struct SyncGroupResponse {
    pub header: protocol::HeaderResponse,
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    pub throttle_time_ms: i32,
    /// The error code, or 0 if there was no error.
    pub error_code: KafkaCode,
    /// The member assignment.
    pub assignment: MemberAssignment,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MemberAssignment {
    pub version: i16,
    pub partition_assignments: Vec<PartitionAssignment>,
    pub user_data: Option<Bytes>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PartitionAssignment {
    pub topic_name: Bytes,
    pub partitions: Vec<i32>,
}

impl TryFrom<Bytes> for SyncGroupResponse {
    type Error = Error;

    fn try_from(s: Bytes) -> Result<Self> {
        tracing::trace!("Parsing SyncGroupResponse {:?}", s);
        let (_, sync_group) =
            parse_sync_group_response(NomBytes::new(s.clone())).map_err(|err| {
                tracing::error!("ERROR: Failed parsing SyncGroupResponse {:?}", err);
                tracing::error!("ERROR: SyncGroupResponse Bytes {:?}", s);
                Error::ParsingError(s)
            })?;
        tracing::trace!("Parsed SyncGroupResponse {:?}", sync_group);
        Ok(sync_group)
    }
}

/// Parse bytes into a Sync Group response.
///
/// To use the response bytes from a broker, it needs to be parsed into a
/// type. This method does the parsing.
///
/// Users do not need to use this method directly, though. It is advised to
/// convert the type from the bytes by doing the following:
///
// ### Example
/// ```
/// let response_bytes = conn.receive_response().await?;
/// let sync_response = protocol::SyncGroupResponse::try_from(response_bytes.freeze());
/// ```
pub fn parse_sync_group_response(s: NomBytes) -> IResult<NomBytes, SyncGroupResponse> {
    let (s, header) = protocol::parse_header_response(s)?;
    let (s, throttle_time_ms) = be_i32(s)?;
    let (s, error_code) = parser::parse_kafka_code(s)?;
    let (s, _metadata_length) = be_i32(s)?;
    let (s, assignment) = parse_member_assignment(s)?;

    Ok((
        s,
        SyncGroupResponse {
            header,
            throttle_time_ms,
            error_code,
            assignment,
        },
    ))
}

fn parse_member_assignment(s: NomBytes) -> IResult<NomBytes, MemberAssignment> {
    let (s, version) = be_i16(s)?;
    let (s, partition_assignments) = parser::parse_array(parse_partition_assignment)(s)?;
    let (s, user_data) = parser::parse_nullable_bytes(s)?;

    Ok((
        s,
        MemberAssignment {
            version,
            partition_assignments,
            user_data,
        },
    ))
}

fn parse_partition_assignment(s: NomBytes) -> IResult<NomBytes, PartitionAssignment> {
    let (s, topic_name) = parser::parse_string(s)?;
    let (s, partitions) = parser::parse_array(be_i32)(s)?;

    Ok((
        s,
        PartitionAssignment {
            topic_name,
            partitions,
        },
    ))
}
