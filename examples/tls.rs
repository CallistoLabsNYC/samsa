use std::fs::File;
use std::io;
use std::io::BufReader;
use std::net::ToSocketAddrs;
use std::path::PathBuf;
use std::sync::Arc;

use argh::FromArgs;
use samsa::prelude::encode::ToByte;
use tokio::io::{copy, split, stdin as tokio_stdin, stdout as tokio_stdout, AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::{rustls, TlsConnector};

/// Tokio Rustls client example
#[derive(FromArgs)]
struct Options {
    /// host
    #[argh(positional)]
    host: String,

    /// port
    #[argh(option, short = 'p', default = "443")]
    port: u16,

    /// domain
    #[argh(option, short = 'd')]
    domain: Option<String>,

    /// cafile
    #[argh(option, short = 'c')]
    cafile: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let options: Options = argh::from_env();

    let addr = (options.host.as_str(), options.port)
        .to_socket_addrs().unwrap()
        .next()
        .ok_or_else(|| io::Error::from(io::ErrorKind::NotFound)).unwrap();
    let domain = options.domain.unwrap_or(options.host);
    let topics = vec!["tester"];

    let mut root_cert_store = rustls::RootCertStore::empty();
    if let Some(cafile) = &options.cafile {
        let mut pem = BufReader::new(File::open(cafile).unwrap());
        for cert in rustls_pemfile::certs(&mut pem) {
            println!("adding!");
            root_cert_store.add(cert.unwrap()).unwrap();
        }
    } else {
        root_cert_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    }

    let config = rustls::ClientConfig::builder()
        .with_root_certificates(root_cert_store)
        .with_no_client_auth(); // i guess this was previously the default.unwrap()
    let connector = TlsConnector::from(Arc::new(config));

    let stream = TcpStream::connect(&addr).await.unwrap();

    let (mut stdin, mut stdout) = (tokio_stdin(), tokio_stdout());

    let domain = rustls_pki_types::ServerName::try_from(domain.as_str())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid dnsname")).unwrap()
        .to_owned();

    let mut stream = connector.connect(domain, stream).await.unwrap();
    
    let req = samsa::prelude::protocol::MetadataRequest::new(1, "rust", &topics);
    let mut buf = vec![];
    req.encode(&mut buf).unwrap();
    stream.write_all(&buf).await.unwrap();

    // let size = stream.read_buf(buf)
    Ok(())
}