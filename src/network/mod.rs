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
pub mod tcp;
// #[cfg(feature = "tls")]
pub mod tls;

use crate::prelude::{encode::ToByte, Result};
use bytes::BytesMut;

pub trait BrokerConnection {
    /// Serialize a given request and send to Kafka/Redpanda broker.
    ///
    /// The Kafka protocol specifies that all requests will
    /// be processed in the order they are sent and responses will return in
    /// that order as well. Users of this method should be sure to use the receive_response method
    /// to accept Kafka responses in the order that these requests are sent.
    ///
    /// This method is only useful in practice when used in combination with
    /// a request type. To see how this would be done, visit the protocol module.
    ///
    /// ### Example
    /// ```
    /// // send an arbitrary set of bytes to kafka broker
    /// let buf = "test";
    /// conn.send_request(buf).await?;
    /// ```
    async fn send_request<R: ToByte>(&self, req: &R) -> Result<()>;

    /// Receive a response in raw bytes from a Kafka/Redpanda broker.
    ///
    /// Kafka queues up responses on the socket as requests are sent by the client.
    /// This method pulls 1 request off the socket at a time and returns the raw bytes.
    ///
    /// This method returns raw data that is not useful until parsed
    /// into a response type. To see how this would be done, visit the
    /// protocol module.
    ///
    /// ### Example
    /// ```
    /// // receive a message from a kafka broker
    /// let response_bytes = conn.receive_response().await?;
    /// ```
    async fn receive_response(&mut self) -> Result<BytesMut>;
}
