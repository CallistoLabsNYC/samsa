//! Parsing and processing for Join Group responses.
//!
//! After receiving join group requests from all members in the group,
//! the coordinator will select one member to be the group leader and
//! a protocol which is supported by all members. The leader will
//! receive the full list of members along with the associated metadata
//! for the protocol chosen. Other members, followers, will receive an
//! empty array of members. It is the responsibility of the leader to
//! inspect the metadata of each member and assign state using SyncGroup
//! request below.
//!
//! Upon every completion of the join group phase, the coordinator
//! increments a GenerationId for the group. This is returned as a field
//! in the response to each member, and is sent in heartbeats and offset
//! commit requests. When the coordinator rebalances a group, the
//! coordinator will send an error code indicating that the member needs
//! to rejoin. If the member does not rejoin before a rebalance completes,
//! then it will have an old generationId, which will cause
//! `ILLEGAL_GENERATION` errors when included in new requests.
//!
//! ### Example
//! ```rust
//! let response_bytes = conn.receive_response().await?;
//! let join_response = protocol::JoinGroupResponse::try_from(response_bytes.freeze())?;
//! ````
//!
//! ### Protocol Def
//! ```text
//! JoinGroup Response (Version: 2) => throttle_time_ms error_code generation_id protocol_name leader member_id [members]
//!   throttle_time_ms => INT32
//!   error_code => INT16
//!   generation_id => INT32
//!   protocol_name => STRING
//!   leader => STRING
//!   member_id => STRING
//!   members => member_id metadata
//!     member_id => STRING
//!     metadata => BYTES
//! ```
//!
//! Note we are using version 2 for the response.

use bytes::Bytes;
use nom::{number::complete::be_i32, IResult};
use nombytes::NomBytes;

use crate::{
    error::{Error, KafkaCode, Result},
    parser::{self, parse_array},
    protocol::{parse_header_response, HeaderResponse},
};

/// The base Sync Group request object.
///
/// ### Example
/// ```rust
/// let response_bytes = conn.receive_response().await?;
/// let join_response = protocol::JoinGroupResponse::try_from(response_bytes.freeze())?;
/// ````
#[derive(Debug, PartialEq)]
pub struct JoinGroupResponse {
    pub header: HeaderResponse,
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    pub throttle_time_ms: i32,
    /// The error code, or 0 if there was no error.
    pub error_code: KafkaCode,
    /// The generation ID of the group.
    pub generation_id: i32,
    /// The group protocol selected by the coordinator.
    pub protocol_name: Bytes,
    /// The leader of the group.
    pub leader: Bytes,
    /// The member ID assigned by the group coordinator.
    pub member_id: Bytes,
    /// Each member in the group. Empty if this member is not the leader.
    pub members: Vec<Member>,
}

/// Each member in the group.
#[derive(Debug, PartialEq)]
pub struct Member {
    /// The group member ID.
    pub member_id: Bytes,
    /// The group member metadata.
    pub metadata: Bytes,
}

impl TryFrom<Bytes> for JoinGroupResponse {
    type Error = Error;

    fn try_from(s: Bytes) -> Result<Self> {
        tracing::trace!("Parsing JoinGroupResponse {:?}", s);
        let (_, join_group) =
            parse_join_group_response(NomBytes::new(s.clone())).map_err(|err| {
                tracing::error!("ERROR: Failed parsing JoinGroupResponse {:?}", err);
                tracing::error!("ERROR: JoinGroupResponse Bytes {:?}", s);
                Error::ParsingError(s)
            })?;
        tracing::trace!("Parsed JoinGroupResponse {:?}", join_group);
        Ok(join_group)
    }
}

pub fn parse_join_group_response(s: NomBytes) -> IResult<NomBytes, JoinGroupResponse> {
    let (s, header) = parse_header_response(s)?;
    let (s, throttle_time_ms) = be_i32(s)?;
    let (s, error_code) = parser::parse_kafka_code(s)?;
    let (s, generation_id) = be_i32(s)?;
    let (s, protocol_name) = parser::parse_string(s)?;
    let (s, leader) = parser::parse_string(s)?;
    let (s, member_id) = parser::parse_string(s)?;
    let (s, members) = parse_array(parse_member)(s)?;

    Ok((
        s,
        JoinGroupResponse {
            header,
            throttle_time_ms,
            error_code,
            generation_id,
            protocol_name,
            leader,
            member_id,
            members,
        },
    ))
}

fn parse_member(s: NomBytes) -> IResult<NomBytes, Member> {
    let (s, member_id) = parser::parse_string(s)?;
    let (s, metadata) = parser::parse_bytes(s)?;

    Ok((
        s,
        Member {
            member_id,
            metadata,
        },
    ))
}
