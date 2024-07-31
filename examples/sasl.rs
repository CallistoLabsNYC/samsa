//! A simple client using SASLConfig::with_credentials
//!
//! This can authenticate a client side with mechanisms that only require an authid, authzid and
//! password. Currently this means 'SCRAM-SHA-2', 'SCRAM-SHA-1', 'PLAIN', and 'LOGIN', preferred
//! in that order.

use std::time::Duration;

use samsa::prelude::{
    BrokerAddress, ProduceMessage, ProducerBuilder, SaslConfig, SaslTcpConfig, SaslTcpConnection,
};

use bytes::Bytes;
use tokio_stream::{iter, StreamExt};

#[tokio::main]
async fn main() -> Result<(), ()> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .compact()
        .with_file(true)
        .with_line_number(true)
        .with_thread_ids(true)
        .with_target(false)
        .init();

    let tcp_config = vec![BrokerAddress {
        host: "piggy.callistolabs.cloud".to_owned(),
        port: 9092,
    }];
    let sasl_config = SaslConfig::new(String::from("myuser"), String::from("pass1234"), None, None);

    let options = SaslTcpConfig {
        tcp_config,
        sasl_config,
    };

    let topic_name = "atopic";

    let stream = iter(0..1_000_000)
        .map(|_| ProduceMessage {
            topic: topic_name.to_string(),
            partition_id: 0,
            key: None,
            value: Some(Bytes::from_static(b"0123456789")),
            headers: vec![],
        })
        .chunks_timeout(2000, Duration::from_secs(1));

    tracing::info!("Connecting to cluster");
    let output_stream =
        ProducerBuilder::<SaslTcpConnection>::new(options, vec![topic_name.to_string()])
            .await
            .map_err(|err| tracing::error!("{:?}", err))?
            .clone()
            .build_from_stream(stream)
            .await;

    tracing::info!("running");
    tokio::pin!(output_stream);
    while (output_stream.next().await).is_some() {}
    tracing::info!("done");

    Ok(())
}
