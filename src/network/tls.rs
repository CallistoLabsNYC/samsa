use std::fs::File;
use std::io::BufReader;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::{io, sync::Arc};

use async_trait::async_trait;
use bytes::BytesMut;
use rustls_pemfile::{certs, pkcs8_private_keys};
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use std::net::ToSocketAddrs;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio_rustls::{client::TlsStream, rustls, TlsConnector};
// use tracing::instrument;

use crate::{
    encode::ToByte,
    error::{Error, Result},
};

use super::{BrokerConnection, ConnectionParams, ConnectionParamsKind};

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
pub struct TlsConnection {
    stream: Arc<Mutex<TlsStream<TcpStream>>>,
}

#[derive(Clone, Debug)]
pub struct ConnectionOptions {
    pub broker_options: Vec<TlsBrokerOptions>,
    pub cafile: Option<PathBuf>,
}

#[derive(Clone, Debug)]
pub struct TlsBrokerOptions {
    pub host: String,
    pub port: u16,
    pub key: PathBuf,
    pub cert: PathBuf,
}

impl TlsConnection {
    /// Connect to a Kafka/Redpanda broker
    ///
    /// ### Example
    /// ```
    /// // connect to a kafka/redpanda broker
    /// let addrs = vec!["localhost:9092"];
    /// let conn = samsa::prelude::BrokerConnection(addrs).await?;
    /// ```
    pub async fn new_(options: ConnectionOptions) -> Result<Self> {
        tracing::debug!(
            "Starting connection to {} brokers",
            options.broker_options.len()
        );
        let mut propagated_err: Option<Error> = None;

        // need to figure out what this is
        let mut root_cert_store = rustls::RootCertStore::empty();
        if let Some(cafile) = &options.cafile {
            let mut pem = BufReader::new(File::open(cafile).unwrap());
            for cert in rustls_pemfile::certs(&mut pem) {
                root_cert_store.add(cert.unwrap()).unwrap();
            }
        } else {
            root_cert_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        }

        for broker_option in options.broker_options.iter() {
            let addr = (broker_option.host.as_str(), broker_option.port)
                .to_socket_addrs()
                .unwrap()
                .next()
                .ok_or_else(|| Error::IoError(ErrorKind::NotFound))
                .unwrap();

            tracing::debug!("Connecting to {}", broker_option.host);
            let certs = load_certs(&broker_option.cert).map_err(|e| Error::IoError(e.kind()))?;
            let key = load_keys(&broker_option.key).map_err(|e| Error::IoError(e.kind()))?;
            tracing::debug!("keys ready");

            match TcpStream::connect(addr).await {
                Ok(s) => {
                    tracing::debug!("connected on tcp");

                    let config = rustls::ClientConfig::builder()
                        .with_root_certificates(root_cert_store)
                        .with_client_auth_cert(certs, key)
                        .unwrap();

                    let connector = TlsConnector::from(Arc::new(config));
                    tracing::debug!("tls connected");

                    let domain = rustls_pki_types::ServerName::try_from(broker_option.host.clone())
                        .map_err(|_| Error::IoError(ErrorKind::InvalidInput))?
                        .to_owned();
                    tracing::debug!("dns ready");

                    let stream = connector
                        .connect(domain, s)
                        .await
                        .map_err(|e| Error::IoError(e.kind()))?;
                    tracing::debug!("tls connected to tcp");

                    return Ok(Self {
                        stream: Arc::new(Mutex::new(stream)),
                    });
                }
                Err(e) => {
                    propagated_err = Some(Error::IoError(e.kind()));
                }
            }
        }

        // if there was an error, report it
        if let Some(e) = propagated_err {
            return Err(e);
        }
        return Err(Error::IoError(ErrorKind::NotFound));
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
    async fn send_request_<R: ToByte + Send>(&mut self, req: &R) -> Result<()> {
        // TODO: Does it make sense to find the capacity of the type
        // and fill it here?
        let mut buffer = Vec::with_capacity(4);

        buffer.extend_from_slice(&[0, 0, 0, 0]);
        req.encode(&mut buffer)?;

        let size = buffer.len() as i32 - 4;
        size.encode(&mut &mut buffer[..])?;

        tracing::trace!("Sending bytes {}", buffer.len());
        self.stream
            .lock()
            .await
            .write_all(&buffer)
            .await
            .map_err(|e| Error::IoError(e.kind()))?;

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
    async fn receive_response_(&mut self) -> Result<BytesMut> {
        // figure out the message size
        let mut stream = self.stream.lock().await;

        let length = stream
            .read_u32()
            .await
            .map_err(|e| Error::IoError(e.kind()))?;

        tracing::trace!("Reading {} bytes", length);
        let mut buffer = BytesMut::zeroed(length as usize);
        tracing::trace!("before {:?}", buffer);

        stream
            .read_exact(&mut buffer)
            .await
            .map_err(|e| Error::IoError(e.kind()))?;
        tracing::trace!("Read {:?}", buffer);

        Ok(buffer)
    }
}

fn load_certs(path: &Path) -> io::Result<Vec<CertificateDer<'static>>> {
    certs(&mut BufReader::new(File::open(path)?)).collect()
}

fn load_keys(path: &Path) -> io::Result<PrivateKeyDer<'static>> {
    pkcs8_private_keys(&mut BufReader::new(File::open(path)?))
        .next()
        .unwrap()
        .map(Into::into)
}

#[async_trait]
impl BrokerConnection for TlsConnection {
    async fn send_request<R: ToByte + Sync + Send>(&mut self, req: &R) -> Result<()> {
        self.send_request_(req).await
    }

    async fn receive_response(&mut self) -> Result<BytesMut> {
        self.receive_response_().await
    }

    async fn new(p: ConnectionParams) -> Result<Self> {
        match p.0 {
            ConnectionParamsKind::TlsParams(p) => Self::new_(p).await,
            _ => Err(Error::IncorrectConnectionUsage),
        }
    }
}
