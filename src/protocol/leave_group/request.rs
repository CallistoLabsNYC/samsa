//! Encoding and creation for Leave Group requests.
//!
//! To explicitly leave a group, the client can send a leave group request. This is preferred over letting the session timeout expire since it allows the group to rebalance faster, which for the consumer means that less time will elapse before partitions can be reassigned to an active member.
//!
//! ### Example
//! ```rust
//! let leave = protocol::LeaveGroupRequest::new(
//!     CORRELATION_ID,
//!     CLIENT_ID,
//!     group_id,
//!     member_id,
//! );
//! conn.send_request(&leave).await?;
//! ```
//!
//! ### Protocol Def
//! ```text
//! LeaveGroup Request (Version: 0) => group_id member_id
//!   group_id => STRING
//!   member_id => STRING
//! ```
//!
//! Note we are using version 0 for this request.

use bytes::Bytes;
use nom::AsBytes;

use crate::{
    encode::ToByte,
    error::{Error, Result},
    protocol::HeaderRequest,
};

const API_KEY_METADATA: i16 = 13;
const API_VERSION: i16 = 0;

/// The base Leave Group request object.
///
/// ### Example
/// ```rust
/// let leave = protocol::LeaveGroupRequest::new(
///     CORRELATION_ID,
///     CLIENT_ID,
///     group_id,
///     member_id,
/// );
/// conn.send_request(&leave).await?;
/// ```
#[derive(Debug)]
pub struct LeaveGroupRequest<'a> {
    pub header: HeaderRequest<'a>,
    /// The ID of the group to leave.
    pub group_id: &'a str,
    /// The member ID to remove from the group.
    pub member_id: String,
}

impl<'a> LeaveGroupRequest<'a> {
    pub fn new(
        correlation_id: i32,
        client_id: &'a str,
        group_id: &'a str,
        member_id: Bytes,
    ) -> Result<Self> {
        let header = HeaderRequest::new(API_KEY_METADATA, API_VERSION, correlation_id, client_id);
        Ok(Self {
            header,
            group_id,
            member_id: String::from_utf8(member_id.as_bytes().to_vec())
                .map_err(|_| Error::DecodingUtf8Error)?,
        })
    }
}

impl<'a> ToByte for LeaveGroupRequest<'a> {
    fn encode<T: bytes::BufMut>(&self, buffer: &mut T) -> crate::error::Result<()> {
        tracing::trace!("Encoding LeaveGroupRequest {:?}", self);
        self.header.encode(buffer)?;
        self.group_id.encode(buffer)?;
        self.member_id.encode(buffer)?;
        Ok(())
    }
}
