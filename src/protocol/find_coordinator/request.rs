//! Encoding and creation for Find Coordinator requests.
//!
//! The offsets for a given consumer group are maintained by a
//! specific broker called the group coordinator. i.e., a consumer
//! needs to issue its offset commit and fetch requests to this
//! specific broker. It can discover the current coordinator by
//! issuing a group coordinator request.
//!
//! ### Example
//! ```rust
//! let find_coordinator_request =
//!     protocol::FindCoordinatorRequest::new(CORRELATION_ID, CLIENT_ID, group_id);
//! conn.send_request(&find_coordinator_request).await?;
//! ```
//!
//! ### Protocol Def
//! ```text
//! FindCoordinator Request (Version: 0) => key
//!   key => STRING
//! ```
//!
//! Note we are using version 0 of the request.

use crate::{encode::ToByte, protocol::HeaderRequest};

const API_KEY_METADATA: i16 = 10;
const API_VERSION: i16 = 0;

/// The base Find Coordinator request object.
///
/// ### Example
/// ```rust
/// let find_coordinator_request =
///     protocol::FindCoordinatorRequest::new(CORRELATION_ID, CLIENT_ID, group_id);
/// conn.send_request(&find_coordinator_request).await?;
/// ```
#[derive(Debug)]
pub struct FindCoordinatorRequest<'a> {
    pub header: HeaderRequest<'a>,
    /// The coordinator key.
    pub key: &'a str,
}

impl<'a> FindCoordinatorRequest<'a> {
    pub fn new(correlation_id: i32, client_id: &'a str, key: &'a str) -> Self {
        let header = HeaderRequest::new(API_KEY_METADATA, API_VERSION, correlation_id, client_id);
        Self { header, key }
    }
}

impl<'a> ToByte for FindCoordinatorRequest<'a> {
    fn encode<T: bytes::BufMut>(&self, buffer: &mut T) -> crate::error::Result<()> {
        tracing::trace!("Encoding FindCoordinatorRequest {:?}", self);
        self.header.encode(buffer)?;
        self.key.encode(buffer)?;
        Ok(())
    }
}
