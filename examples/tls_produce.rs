use bytes::Bytes;
use futures::{stream::iter, StreamExt};
use samsa::prelude::{
    BrokerAddress, Compression, ProduceMessage, ProducerBuilder, TlsConnection,
    TlsConnectionOptions,
};

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

    let options = TlsConnectionOptions {
        broker_options: vec![BrokerAddress {
            host: "piggy.callistolabs.cloud".to_owned(),
            port: 9092,
        }],
        key: "./etc/redpanda/certs/piggy.key".into(),
        cert: "./etc/redpanda/certs/piggy_callisto_labs_cloud.crt".into(),
        cafile: Some("./etc/redpanda/certs/trustedroot.crt".into()),
    };

    let topic_name = "my-tester";

    let stream = iter(vec![0].into_iter()).cycle().map(|_| {
        let partition_id = 0;
        ProduceMessage {
            topic: topic_name.to_string(),
            partition_id,
            key: Some(Bytes::from_static(b"Tester")),
            value: Some(Bytes::from_static(b"Value")),
            headers: vec![],
        }
    });

    tracing::info!("Connecting to cluster");
    let output_stream =
        ProducerBuilder::<TlsConnection>::new(options, vec![topic_name.to_string()])
            .await
            .map_err(|err| tracing::error!("{:?}", err))?
            .compression(Compression::Gzip)
            .clone()
            .build_from_stream(tokio_stream::StreamExt::chunks_timeout(
                stream,
                200,
                std::time::Duration::from_secs(3),
            ))
            .await;

    tokio::pin!(output_stream);
    while let Some(message) = output_stream.next().await {
        tracing::info!("Produced {} message", message.len());
    }

    Ok(())
}
