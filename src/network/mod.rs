//! Connection & communication with a broker.
//!
//! # Network Module
//!
//! Kafka uses a binary protocol over TCP. The protocol defines all APIs as
//! request response message pairs. All messages are size delimited and are
//! made up of the following primitive types.
//!
//! The client initiates a socket connection and then writes a sequence of
//! request messages and reads back the corresponding response message. No
//! handshake is required on connection or disconnection. TCP is happier if
//! you maintain persistent connections used for many requests to amortize
//! the cost of the TCP handshake, but beyond this penalty connecting is
//! pretty cheap.
//!
//! The client will likely need to maintain a connection to multiple brokers,
//! as data is partitioned and the clients will need to talk to the server
//! that has their data. However it should not generally be necessary to
//! maintain multiple connections to a single broker from a single client
//! instance (i.e. connection pooling).
//!
//! The server guarantees that on a single TCP connection, requests will
//! be processed in the order they are sent and responses will return in
//! that order as well. The broker's request processing allows only a
//! single in-flight request per connection in order to guarantee this
//! ordering. Note that clients can (and ideally should) use non-blocking
//! IO to implement request pipelining and achieve higher throughput.
//! i.e., clients can send requests even while awaiting responses for
//! preceding requests since the outstanding requests will be buffered
//! in the underlying OS socket buffer. All requests are initiated by
//! the client, and result in a corresponding response message from the
//! server except where noted.
//!
//! The server has a configurable maximum limit on request size and any
//! request that exceeds this limit will result in the socket being
//! disconnected.
//!
use std::fmt::Debug;

use crate::prelude::{encode::ToByte, Result};
use async_trait::async_trait;
use bytes::BytesMut;

pub mod sasl;
pub mod tcp;
pub mod tls;

#[derive(Clone, Debug, PartialEq)]
pub struct BrokerAddress {
    pub host: String,
    pub port: u16,
}

#[async_trait]
pub trait BrokerConnection {
    type ConnConfig: Clone + Debug + Send + Sync;

    async fn send_request<R: ToByte + Sync + Send>(&mut self, req: &R) -> Result<()>;
    async fn receive_response(&mut self) -> Result<BytesMut>;
    async fn new(p: Self::ConnConfig) -> Result<Self>
    where
        Self: Sized;
    async fn from_addr(p: Self::ConnConfig, addr: BrokerAddress) -> Result<Self>
    where
        Self: Sized;
}
