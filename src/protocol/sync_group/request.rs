//! Encoding and creation for Sync Group requests.
//!
//! The sync group request is used by the group
//! leader to assign state (e.g. partition assignments)
//! to all members of the current generation. All members
//! send SyncGroup immediately after joining the group,
//! but only the leader provides the group's assignment.
//!
//! ### Example
//! ```rust
//! let sync_request = protocol::SyncGroupRequest::new(
//!     CORRELATION_ID,
//!     CLIENT_ID,
//!     group_id,
//!     generation_id,
//!     member_id,
//!     assignments,
//! );
//! conn.send_request(&sync_request).await?;
//! ```
//!
//! ### Protocol Def
//! ```text
//! SyncGroup Request (Version: 2) => group_id generation_id member_id [assignments]
//!   group_id => STRING
//!   generation_id => INT32
//!   member_id => STRING
//!   assignments => member_id assignment
//!     member_id => STRING
//!     assignment => BYTES
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
use nom::AsBytes;

use crate::{
    encode::ToByte,
    error::{Error, Result},
    protocol::HeaderRequest,
};

const API_KEY_METADATA: i16 = 14;
const API_VERSION: i16 = 2;

/// The base Sync Group request object.
///
/// ### Example
/// ```rust
/// let sync_request = protocol::SyncGroupRequest::new(
///     CORRELATION_ID,
///     CLIENT_ID,
///     group_id,
///     generation_id,
///     member_id,
///     assignments,
/// );
/// conn.send_request(&sync_request).await?;
/// ```
#[derive(Debug)]
pub struct SyncGroupRequest<'a> {
    pub header: HeaderRequest<'a>,
    /// The unique group identifier.
    pub group_id: &'a str,
    /// The generation of the group.
    pub generation_id: i32,
    /// The member ID assigned by the group.
    pub member_id: String,
    /// Each assignment. Empty if this member is not the leader.
    pub assignments: Vec<Assignment<'a>>,
}

#[derive(Debug, Clone)]
pub struct Assignment<'a> {
    /// The ID of the member to assign.
    pub member_id: String,
    /// The member assignment.
    pub assignment: MemberAssignment<'a>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MemberAssignment<'a> {
    pub version: i16,
    pub partition_assignments: Vec<PartitionAssignment<'a>>,
    pub user_data: Option<Bytes>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PartitionAssignment<'a> {
    pub topic_name: &'a str,
    pub partitions: Vec<i32>,
}

impl<'a> Assignment<'a> {
    pub fn new(member_id: Bytes, assignment: MemberAssignment) -> Result<Assignment> {
        Ok(Assignment {
            member_id: String::from_utf8(member_id.as_bytes().to_vec())
                .map_err(|_| Error::DecodingUtf8Error)?,
            assignment,
        })
    }
}

impl<'a> PartitionAssignment<'a> {
    pub fn new(topic_name: &'a str, partitions: Vec<i32>) -> PartitionAssignment {
        PartitionAssignment {
            topic_name,
            partitions,
        }
    }
}

impl<'a> SyncGroupRequest<'a> {
    pub fn new(
        correlation_id: i32,
        client_id: &'a str,
        group_id: &'a str,
        generation_id: i32,
        member_id: Bytes,
        assignments: Vec<Assignment<'a>>,
    ) -> Result<Self> {
        let header = HeaderRequest::new(API_KEY_METADATA, API_VERSION, correlation_id, client_id);
        Ok(Self {
            header,
            group_id,
            generation_id,
            member_id: String::from_utf8(member_id.as_bytes().to_vec())
                .map_err(|_| Error::DecodingUtf8Error)?,
            assignments,
        })
    }
}

impl<'a> ToByte for SyncGroupRequest<'a> {
    fn encode<T: bytes::BufMut>(&self, buffer: &mut T) -> crate::error::Result<()> {
        tracing::trace!("Encoding SyncGroupRequest {:?}", self);
        self.header.encode(buffer)?;
        self.group_id.encode(buffer)?;
        self.generation_id.encode(buffer)?;
        self.member_id.encode(buffer)?;
        self.assignments.encode(buffer)?;
        Ok(())
    }
}

impl<'a> ToByte for Assignment<'a> {
    fn encode<T: bytes::BufMut>(&self, buffer: &mut T) -> crate::error::Result<()> {
        self.member_id.encode(buffer)?;

        // The protocol wants a bytestring and not a defined struct
        let mut buf = Vec::with_capacity(4);
        self.assignment.encode(&mut buf)?;
        buf.encode(buffer)?;

        Ok(())
    }
}

impl<'a> ToByte for MemberAssignment<'a> {
    fn encode<T: bytes::BufMut>(&self, buffer: &mut T) -> crate::error::Result<()> {
        self.version.encode(buffer)?;
        self.partition_assignments.encode(buffer)?;
        self.user_data.encode(buffer)?;
        Ok(())
    }
}

impl<'a> ToByte for PartitionAssignment<'a> {
    fn encode<T: bytes::BufMut>(&self, buffer: &mut T) -> crate::error::Result<()> {
        self.topic_name.encode(buffer)?;
        self.partitions.encode(buffer)?;
        Ok(())
    }
}
