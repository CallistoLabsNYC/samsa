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
use crate::prelude::{encode::ToByte, Error, Result};
use bytes::BytesMut;

use self::tls::ConnectionOptions;

pub mod tcp;
pub mod tls;

// #[cfg(not(feature = "tls"))]
// pub type BrokerConnection = tcp::BrokerConnection;

pub trait BrokerConnection {
    async fn send_request<R: ToByte>(&mut self, req: &R) -> Result<()>;
    async fn receive_response(&mut self) -> Result<BytesMut>;
    async fn new(p: ConnectionParamsKind) -> Result<impl BrokerConnection>;
}


#[derive(Clone, Debug)]
pub enum ConnectionParamsKind {
    TcpParams(Vec<String>),
    TlsParams(tls::ConnectionOptions),
}

#[derive(Clone, Debug)]
pub struct ConnectionParams(ConnectionParamsKind);

impl ConnectionParams {
    // this is when we need to bootstrap
    pub async fn init<T: BrokerConnection>(&self) -> Result<T> {
        match self.0 {
            ConnectionParamsKind::TcpParams(bootstrap_addrs) => {
                tcp::TcpConnection::new(bootstrap_addrs)
                    .await
            }
            ConnectionParamsKind::TlsParams(options) => {
                tls::TlsConnection::new(options).await
            }
        }
    }

    // this is when we need to connect to one broker
    pub async fn from_url<T: BrokerConnection>(&self, url: String) -> Result<T> {
        match self.0 {
            ConnectionParamsKind::TcpParams(bootstrap_addrs) => {
                tcp::TcpConnection::new(vec![url]).await
            }
            ConnectionParamsKind::TlsParams(options) => {
                let cafile = options.cafile;

                let single_connection_options = options
                    .broker_options
                    .into_iter()
                    .find(|b_options| format!("{}:{}", b_options.host, b_options.port) == url);
                match single_connection_options {
                    Some(single_connection_options) => {
                        let options = ConnectionOptions {
                            broker_options: vec![single_connection_options],
                            cafile,
                        };

                        tls::TlsConnection::new(options).await
                    }
                    None => Err(Error::MissingBrokerConfigOptions),
                }
            }
        }
    }
}
