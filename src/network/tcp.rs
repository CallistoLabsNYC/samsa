use std::io::ErrorKind;
use std::net::ToSocketAddrs;
use std::{io, sync::Arc};

use async_trait::async_trait;
use bytes::{Buf, BytesMut};
use tokio::net::TcpStream;
use tracing::instrument;

use crate::{
    encode::ToByte,
    error::{Error, Result},
};

use super::sasl::{do_sasl_v2, SaslConfig};
use super::{BrokerAddress, BrokerConnection};

/// TCP connection to a Kafka/Redpanda broker.
///
/// # Example
/// ```rust
/// // set up connection options
/// let broker_option = vec![BrokerAddress {
///     host: "127.0.0.1".to_owned(),
///     port: 9092,
/// }];
/// ```
#[derive(Clone, Debug)]
pub struct TcpConnection {
    stream: Arc<TcpStream>,
}

impl TcpConnection {
    /// Connect to a Kafka/Redpanda cluster
    pub async fn new_(bootstrap_addrs: Vec<BrokerAddress>) -> Result<Self> {
        let mut propagated_err: Option<Error> = None;
        let mut stream: Option<TcpStream> = None;
        for bootstrap_addr in bootstrap_addrs.iter() {
            tracing::debug!("Connecting to {:?}", bootstrap_addr);
            let addr = (bootstrap_addr.host.clone(), bootstrap_addr.port)
                .to_socket_addrs()
                .map_err(|err| {
                    tracing::error!(
                        "Error could not create address from host {} and port {} {:?}",
                        bootstrap_addr.host,
                        bootstrap_addr.port,
                        err
                    );
                    Error::MissingBrokerConfigOptions
                })?
                .next()
                .unwrap();
            match TcpStream::connect(addr).await {
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
        let mut buf = BytesMut::zeroed(size);
        let mut index = 0_usize;
        loop {
            // Wait for the socket to be readable
            self.stream
                .readable()
                .await
                .map_err(|e| Error::IoError(e.kind()))?;

            // Try to read data, this may still fail with `WouldBlock`
            // if the readiness event is a false positive.
            match self.stream.try_read(&mut buf[index..]) {
                Ok(0) => {
                    tracing::info!("Empty read: stop trying: probably closed");
                    return Err(Error::IncorrectConnectionUsage);
                }
                Ok(n) => {
                    index += n;
                    tracing::trace!("Read {} bytes", n);
                    if index != size {
                        tracing::trace!("Going back to read more, {} bytes left", size - index);
                    } else {
                        return Ok(buf);
                    }
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
        let size = buf.len();
        let mut index = 0_usize;
        loop {
            // Wait for the socket to be writable
            self.stream
                .writable()
                .await
                .map_err(|e| Error::IoError(e.kind()))?;

            // Try to write data, this may still fail with `WouldBlock`
            // if the readiness event is a false positive.
            match self.stream.try_write(&buf[index..]) {
                Ok(n) => {
                    index += n;
                    tracing::trace!("Wrote {} bytes", n);
                    if index != size {
                        tracing::trace!("Going back to write more, {} bytes left", size - index);
                    } else {
                        return Ok(n);
                    }
                }
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                    tracing::trace!("WouldBlock on write");
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
    pub async fn send_request_<R: ToByte + Send>(&mut self, req: &R) -> Result<()> {
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
    pub async fn receive_response_(&mut self) -> Result<BytesMut> {
        // figure out the message size
        let mut size = self.read(4).await?;
        if size.is_empty() {
            return Err(Error::MissingData(String::from(
                "could no read a packet length",
            )));
        }

        let length = size.get_u32();
        tracing::trace!("Reading {} bytes", length);
        self.read(length as usize).await
    }
}

#[async_trait]
impl BrokerConnection for TcpConnection {
    type ConnConfig = Vec<BrokerAddress>;

    async fn send_request<R: ToByte + Sync + Send>(&mut self, req: &R) -> Result<()> {
        self.send_request_(req).await
    }

    async fn receive_response(&mut self) -> Result<BytesMut> {
        self.receive_response_().await
    }

    async fn new(p: Self::ConnConfig) -> Result<Self> {
        Self::new_(p).await
    }

    async fn from_addr(_: Self::ConnConfig, addr: BrokerAddress) -> Result<Self> {
        Self::new_(vec![addr]).await
    }
}

/// SASL connection options.
#[derive(Clone, Debug)]
pub struct SaslTcpConfig {
    pub tcp_config: Vec<BrokerAddress>,
    pub sasl_config: SaslConfig,
}

/// SASL connection to a Kafka/Redpanda broker.
///
/// # Example
/// ```rust
/// let tcp_config = vec![BrokerAddress {
///     host: "127.0.0.1".to_owned(),
///     port: 9092,
/// }];
/// let sasl_config = SaslConfig::new(String::from("myuser"), String::from("pass1234"), None, None);
/// ```
#[derive(Clone, Debug)]
pub struct SaslTcpConnection {
    tcp_conn: TcpConnection,
}

#[async_trait]
impl BrokerConnection for SaslTcpConnection {
    type ConnConfig = SaslTcpConfig;

    async fn send_request<R: ToByte + Sync + Send>(&mut self, req: &R) -> Result<()> {
        self.tcp_conn.send_request_(req).await
    }

    async fn receive_response(&mut self) -> Result<BytesMut> {
        self.tcp_conn.receive_response_().await
    }

    #[instrument(name = "sasl-new", level = "trace")]
    async fn new(p: Self::ConnConfig) -> Result<Self> {
        let conn = do_sasl_v2(
            async || TcpConnection::new_(p.tcp_config.clone()).await,
            p.sasl_config.correlation_id,
            &p.sasl_config.client_id,
            p.sasl_config.clone(),
        )
        .await?;

        Ok(Self { tcp_conn: conn })
    }

    async fn from_addr(p: Self::ConnConfig, addr: BrokerAddress) -> Result<Self> {
        let tc = Self::ConnConfig {
            tcp_config: vec![addr],
            ..p
        };
        return Self::new(tc).await;
    }
}
