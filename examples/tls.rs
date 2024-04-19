use argh::FromArgs;
use rustls_pemfile::{certs, pkcs8_private_keys};
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use samsa::prelude::encode::ToByte;
use std::fs::File;
use std::io;
use std::io::BufReader;
use std::net::ToSocketAddrs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
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
    #[argh(option, short = 'a')]
    cafile: Option<PathBuf>,

    /// cert file
    #[argh(option, short = 'c')]
    cert: PathBuf,

    /// key file
    #[argh(option, short = 'k')]
    key: PathBuf,
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

#[tokio::main]
async fn main() -> io::Result<()> {
    let options: Options = argh::from_env();

    let addr = (options.host.as_str(), options.port)
        .to_socket_addrs()
        .unwrap()
        .next()
        .ok_or_else(|| io::Error::from(io::ErrorKind::NotFound))
        .unwrap();
    let domain = options.domain.unwrap_or(options.host);

    let certs = load_certs(&options.cert)?;
    let key = load_keys(&options.key)?;

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
        .with_client_auth_cert(certs, key)
        .unwrap();
    let connector = TlsConnector::from(Arc::new(config));

    let stream = TcpStream::connect(&addr).await.unwrap();

    let domain = rustls_pki_types::ServerName::try_from(domain.as_str())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid dnsname"))
        .unwrap()
        .to_owned();

    let mut stream = connector.connect(domain, stream).await.unwrap();

    let mut req = samsa::prelude::protocol::ProduceRequest::new(0, 1000, 1, "rust");
    req.add(
        "test-1-user-signedup",
        0,
        Some(bytes::Bytes::from("new")),
        Some(bytes::Bytes::from("old")),
        vec![],
    );
    let mut buf = vec![];
    req.encode(&mut buf).unwrap();
    stream.write_all(&buf).await.unwrap();

    // let size = stream.read_buf(buf)
    Ok(())
}
