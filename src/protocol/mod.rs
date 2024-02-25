//! Bytecode protocol requests & responses.
//!
//! This module aims to implement the bytecode protocol outlined in the
//! [Kafka Documentation](https://kafka.apache.org/protocol.html)
//!
//! The module is set up as a list of message pairs containing two files
//! each corresponding to the request and response.
//!
//! The request files hold the logic for creating and encoding structs that
//! will be sent to the broker. The response files hold the logic for parsing
//! and processing the messages coming from the broker.

pub mod commit_offset;
pub mod fetch;
pub mod find_coordinator;
pub mod heartbeat;
pub mod join_group;
pub mod leave_group;
pub mod list_offsets;
pub mod metadata;
pub mod offset_fetch;
pub mod produce;
pub mod sync_group;

use bytes::BufMut;
use nom::{number::complete::be_i32, IResult};
use nombytes::NomBytes;

// re exporting these for ease
pub use self::{
    commit_offset::{request::OffsetCommitRequest, response::OffsetCommitResponse},
    fetch::{request::FetchRequest, response::FetchResponse},
    find_coordinator::{request::FindCoordinatorRequest, response::FindCoordinatorResponse},
    heartbeat::{request::HeartbeatRequest, response::HeartbeatResponse},
    join_group::{request::JoinGroupRequest, response::JoinGroupResponse},
    leave_group::{request::LeaveGroupRequest, response::LeaveGroupResponse},
    list_offsets::{request::ListOffsetsRequest, response::ListOffsetsResponse},
    metadata::{request::MetadataRequest, response::MetadataResponse},
    offset_fetch::{request::OffsetFetchRequest, response::OffsetFetchResponse},
    produce::{request::ProduceRequest, response::ProduceResponse},
    sync_group::{
        request::{Assignment, MemberAssignment, PartitionAssignment, SyncGroupRequest},
        response::SyncGroupResponse,
    },
};
use crate::{encode::ToByte, error::Result};

#[derive(Debug, Clone)]
pub struct HeaderRequest<'a> {
    /// The API key of this request.
    pub api_key: i16,
    /// The API version of this request.
    pub api_version: i16,
    /// The correlation ID of this request.
    pub correlation_id: i32,
    /// The client ID string.
    pub client_id: &'a str,
}

impl<'a> HeaderRequest<'a> {
    /// Create new header request.
    ///
    /// This goes at the beginning of every single request.
    pub fn new(
        api_key: i16,
        api_version: i16,
        correlation_id: i32,
        client_id: &'a str,
    ) -> HeaderRequest {
        HeaderRequest {
            api_key,
            api_version,
            correlation_id,
            client_id,
        }
    }
}

impl<'a> ToByte for HeaderRequest<'a> {
    fn encode<W: BufMut>(&self, buffer: &mut W) -> Result<()> {
        self.api_key.encode(buffer)?;
        self.api_version.encode(buffer)?;
        self.correlation_id.encode(buffer)?;
        self.client_id.encode(buffer)?;
        Ok(())
    }
}

#[derive(Debug, Default, PartialEq)]
pub struct HeaderResponse {
    /// The correlation ID of this response.
    pub correlation_id: i32,
}

pub fn parse_header_response(s: NomBytes) -> IResult<NomBytes, HeaderResponse> {
    let (s, correlation_id) = be_i32(s)?;
    Ok((s, HeaderResponse { correlation_id }))
}
