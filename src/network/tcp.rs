use std::io::ErrorKind;
use std::{io, sync::Arc};

use bytes::{Buf, BytesMut};
use tokio::net::TcpStream;
use tracing::instrument;

use crate::{
    encode::ToByte,
    error::{Error, Result},
};

/// Reference counted TCP connection to a Kafka/Redpanda broker.
///
/// This is designed to be held by a metadata structure which will
/// dispatch many of these connections at the behest of either a
/// consumer or producer.
///
/// A client would probably need more than one of these at a given time
/// to implement all of the different protocols.
///
/// Typically this would only be used directly in a low level context.
/// Otherwise the Metadata, Consumer, or Producer modules abstract out the
/// connection details for the user.

#[derive(Clone, Debug)]
pub struct TcpConnection {
    stream: Arc<TcpStream>,
}

impl TcpConnection {
    /// Connect to a Kafka/Redpanda broker
    ///
    /// ### Example
    /// ```
    /// // connect to a kafka/redpanda broker
    /// let bootstrap_addrs = vec!["localhost:9092"];
    /// let conn = samsa::prelude::TcpConnection(bootstrap_addrs).await?;
    /// ```
    pub async fn new(bootstrap_addrs: Vec<String>) -> Result<Self> {
        tracing::debug!("Connecting to {}", bootstrap_addrs.join(","));
        let mut propagated_err: Option<Error> = None;
        let mut stream: Option<TcpStream> = None;
        for bootstrap_addr in bootstrap_addrs.iter() {
            match TcpStream::connect(bootstrap_addr).await {
                Ok(s) => {
                    stream = Some(s);
                    break;
                }
                Err(e) => {
                    propagated_err = Some(Error::IoError(e.kind()));
                }
            }
        }
        if stream.is_none() {
            if let Some(e) = propagated_err {
                return Err(e);
            }
            return Err(Error::IoError(ErrorKind::NotFound));
        }
        let stream = Arc::new(stream.unwrap());
        Ok(Self { stream })
    }

    #[instrument(name = "network-read", level = "trace")]
    async fn read(&mut self, size: usize) -> Result<BytesMut> {
        loop {
            // Wait for the socket to be readable
            self.stream
                .readable()
                .await
                .map_err(|e| Error::IoError(e.kind()))?;

            let mut buf = BytesMut::zeroed(size);

            // Try to read data, this may still fail with `WouldBlock`
            // if the readiness event is a false positive.
            match self.stream.try_read(&mut buf) {
                Ok(n) => {
                    tracing::trace!("Read {} bytes", n);
                    return Ok(buf);
                }
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                    tracing::trace!("WouldBlock on read");
                    continue;
                }
                Err(e) => {
                    tracing::error!("ERROR: Reading on Socket {:?}", e);
                    return Err(Error::IoError(e.kind()));
                }
            }
        }
    }

    #[instrument(name = "network-write", level = "trace")]
    async fn write(&mut self, buf: &[u8]) -> Result<usize> {
        loop {
            // Wait for the socket to be writable
            self.stream
                .writable()
                .await
                .map_err(|e| Error::IoError(e.kind()))?;

            // Try to write data, this may still fail with `WouldBlock`
            // if the readiness event is a false positive.
            match self.stream.try_write(buf) {
                Ok(n) => {
                    tracing::trace!("Wrote {} bytes", n);
                    return Ok(n);
                }
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                    tracing::trace!("WouldBlock on read");
                    continue;
                }
                Err(e) => {
                    tracing::error!("ERROR: Writing to Socket {:?}", e);
                    return Err(Error::IoError(e.kind()));
                }
            }
        }
    }

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
    pub async fn send_request<R: ToByte>(&mut self, req: &R) -> Result<()> {
        // TODO: Does it make sense to find the capacity of the type
        // and fill it here?
        let mut buffer = Vec::with_capacity(4);

        buffer.extend_from_slice(&[0, 0, 0, 0]);
        req.encode(&mut buffer)?;

        let size = buffer.len() as i32 - 4;
        size.encode(&mut &mut buffer[..])?;

        tracing::trace!("Sending bytes {}", buffer.len());
        self.write(&buffer).await?;

        Ok(())
    }

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
    pub async fn receive_response(&mut self) -> Result<BytesMut> {
        // figure out the message size
        let mut size = self.read(4).await?;

        let length = size.get_u32();
        tracing::trace!("Reading {} bytes", length);
        self.read(length as usize).await
    }
}
