//! Encoding and creation for Heartbeat requests.
//!
//! Once a member has joined and synced, it will begin sending periodic
//! heartbeats to keep itself in the group. If not heartbeat has been
//! received by the coordinator with the configured session timeout,
//! the member will be kicked out of the group.
//!
//! ### Example
//! ```rust
//! let heartbeat = protocol::HeartbeatRequest::new(
//!     CORRELATION_ID,
//!     CLIENT_ID,
//!     group_id,
//!     generation_id,
//!     member_id,
//! );
//! conn.send_request(&heartbeat).await?;
//! ```
//!
//! ### Protocol Def
//! ```text
//! Heartbeat Request (Version: 0) => group_id generation_id member_id
//!   group_id => STRING
//!   generation_id => INT32
//!   member_id => STRING
//! ```
//!
//! Note that we are using version 0 of this API.

use bytes::Bytes;
use nom::AsBytes;

use crate::{
    encode::ToByte,
    error::{Error, Result},
    protocol::HeaderRequest,
};

const API_KEY_METADATA: i16 = 12;
const API_VERSION: i16 = 0;

/// The base Heartbeat request object.
///
/// ### Example
/// ```rust
/// let heartbeat = protocol::HeartbeatRequest::new(
///     CORRELATION_ID,
///     CLIENT_ID,
///     group_id,
///     generation_id,
///     member_id,
/// );
/// conn.send_request(&heartbeat).await?;
/// ```
#[derive(Debug)]
pub struct HeartbeatRequest<'a> {
    pub header: HeaderRequest<'a>,
    /// The group id.
    pub group_id: &'a str,
    /// The generation of the group.
    pub generation_id: i32,
    /// The member ID.
    pub member_id: String,
}

impl<'a> HeartbeatRequest<'a> {
    pub fn new(
        correlation_id: i32,
        client_id: &'a str,
        group_id: &'a str,
        generation_id: i32,
        member_id: Bytes,
    ) -> Result<Self> {
        let header = HeaderRequest::new(API_KEY_METADATA, API_VERSION, correlation_id, client_id);
        Ok(Self {
            header,
            group_id,
            generation_id,
            member_id: String::from_utf8(member_id.as_bytes().to_vec())
                .map_err(|_| Error::DecodingUtf8Error)?,
        })
    }
}

impl<'a> ToByte for HeartbeatRequest<'a> {
    fn encode<T: bytes::BufMut>(&self, buffer: &mut T) -> crate::error::Result<()> {
        tracing::trace!("Encoding HeartbeatRequest {:?}", self);
        self.header.encode(buffer)?;
        self.group_id.encode(buffer)?;
        self.generation_id.encode(buffer)?;
        self.member_id.encode(buffer)?;
        Ok(())
    }
}
