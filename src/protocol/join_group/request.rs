//! Encoding and creation for Join Group requests.
//!
//! The join group request is used by a client to become a member
//! of a group. When new members join an existing group, all previous
//! members are required to rejoin by sending a new join group
//! request. When a member first joins the group, the memberId
//! will be empty (i.e. `""`), but a rejoining member should use
//! the same memberId from the previous generation.
//!
//! The `SessionTimeout` field is used to indicate client liveness. If the coordinator does not receive at least one heartbeat (see below) before expiration of the session timeout, then the member will be removed from the group. Prior to version 0.10.1, the session timeout was also used as the timeout to complete a needed rebalance. Once the coordinator begins rebalancing, each member in the group has up to the session timeout in order to send a new JoinGroup request. If they fail to do so, they will be removed from the group. In 0.10.1, a new version of the JoinGroup request was created with a separate RebalanceTimeout field. Once a rebalance begins, each client has up to this duration to rejoin, but note that if the session timeout is lower than the rebalance timeout, the client must still continue to send heartbeats.
//!
//! The `ProtocolType` field defines the embedded protocol that the group implements. The group coordinator ensures that all members in the group support the same protocol type. The meaning of the protocol name and metadata contained in the `GroupProtocols` field depends on the protocol type. Note that the join group request allows for multiple protocol/metadata pairs. This enables rolling upgrades without downtime. The coordinator chooses a single protocol which all members support. The upgraded member includes both the new version and the old version of the protocol. Once all members have upgraded, the coordinator will choose whichever protocol is listed first in the `GroupProtocols` array.
//!  
//! ### Example
//! ```rust
//! let join_request = protocol::JoinGroupRequest::new(
//!     CORRELATION_ID,
//!     CLIENT_ID,
//!     group_id,
//!     30000,
//!     30000,
//!     member_id,
//!     "consumer",
//!     protocols,
//! );
//! conn.send_request(&join_request).await?;
//! ```
//!
//! ### Protocol Def
//! The kafka protocol defines this request as follows:
//! ```text
//! JoinGroup Request (Version: 2) => group_id session_timeout_ms rebalance_timeout_ms member_id protocol_type [protocols]
//!   group_id => STRING
//!   session_timeout_ms => INT32
//!   rebalance_timeout_ms => INT32
//!   member_id => STRING
//!   protocol_type => STRING
//!   protocols => name metadata
//!     name => STRING
//!     metadata => BYTES
//! ```
//!
//! Note we are using version 2 of the request.

use bytes::Bytes;
use nom::AsBytes;

use crate::{
    encode::ToByte,
    error::{Error, Result},
    protocol::HeaderRequest,
};

const API_KEY_METADATA: i16 = 11;
const API_VERSION: i16 = 2;

/// The base Sync Group request object.
///
/// ### Example
/// ```rust
/// let join_request = protocol::JoinGroupRequest::new(
///     CORRELATION_ID,
///     CLIENT_ID,
///     group_id,
///     30000,
///     30000,
///     member_id,
///     "consumer",
///     protocols,
/// );
/// conn.send_request(&join_request).await?;
/// ```
#[derive(Debug)]
pub struct JoinGroupRequest<'a> {
    pub header: HeaderRequest<'a>,
    /// The group identifier.
    pub group_id: &'a str,
    /// The coordinator considers the consumer dead if it receives no heartbeat after this timeout in milliseconds.
    pub session_timeout_ms: i32,
    /// The maximum time in milliseconds that the coordinator will wait for each member to rejoin when rebalancing the group.
    pub rebalance_timeout_ms: i32,
    /// The member id assigned by the group coordinator. Empty if the member is joining for the first time.
    pub member_id: String,
    /// The unique name the for class of protocols implemented by the group we want to join.
    pub protocol_type: &'a str,
    /// The list of protocols that the member supports.
    pub protocols: Vec<Protocol<'a>>,
}

/// The list of protocols that the member supports.
#[derive(Debug)]
pub struct Protocol<'a> {
    /// The protocol name.
    pub name: &'a str,
    /// The protocol metadata.
    pub metadata: Metadata<'a>,
}

#[derive(Debug)]
pub struct Metadata<'a> {
    pub version: i16,
    pub subscription: Vec<&'a str>,
    pub user_data: Option<Bytes>,
}

impl<'a> Protocol<'a> {
    pub fn new(name: &'a str, topics: Vec<&'a str>) -> Protocol<'a> {
        let metadata = Metadata {
            version: 3,
            subscription: topics,
            user_data: None,
        };
        Protocol { name, metadata }
    }
}

impl<'a> JoinGroupRequest<'a> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        correlation_id: i32,
        client_id: &'a str,
        group_id: &'a str,
        session_timeout_ms: i32,
        rebalance_timeout_ms: i32,
        member_id: Bytes,
        protocol_type: &'a str,
        protocols: Vec<Protocol<'a>>,
    ) -> Result<Self> {
        let header = HeaderRequest::new(API_KEY_METADATA, API_VERSION, correlation_id, client_id);
        Ok(Self {
            header,
            group_id,
            session_timeout_ms,
            rebalance_timeout_ms,
            member_id: String::from_utf8(member_id.as_bytes().to_vec())
                .map_err(|_| Error::DecodingUtf8Error)?,
            protocol_type,
            protocols,
        })
    }
}

impl<'a> ToByte for JoinGroupRequest<'a> {
    fn encode<T: bytes::BufMut>(&self, buffer: &mut T) -> crate::error::Result<()> {
        tracing::trace!("Encoding JoinGroupRequest {:?}", self);
        self.header.encode(buffer)?;
        self.group_id.encode(buffer)?;
        self.session_timeout_ms.encode(buffer)?;
        self.rebalance_timeout_ms.encode(buffer)?;
        self.member_id.encode(buffer)?;
        self.protocol_type.encode(buffer)?;
        self.protocols.encode(buffer)?;
        Ok(())
    }
}

impl<'a> ToByte for Protocol<'a> {
    fn encode<T: bytes::BufMut>(&self, buffer: &mut T) -> crate::error::Result<()> {
        self.name.encode(buffer)?;

        // hack to encode the metadata as a bytestring
        let mut buf = Vec::with_capacity(4);
        self.metadata.encode(&mut buf)?;
        buf.encode(buffer)?;

        Ok(())
    }
}

impl<'a> ToByte for Metadata<'a> {
    fn encode<T: bytes::BufMut>(&self, buffer: &mut T) -> crate::error::Result<()> {
        self.version.encode(buffer)?;
        self.subscription.encode(buffer)?;
        self.user_data.encode(buffer)?;
        Ok(())
    }
}
