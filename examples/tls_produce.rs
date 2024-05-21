use bytes::Bytes;
use futures::{stream::iter, StreamExt};
use samsa::prelude::{
    Compression, ProduceMessage, ProducerBuilder, TlsBrokerOptions, TlsConnection, TlsConnectionOptions
};

#[tokio::main]
async fn main() -> Result<(), ()> {
    tracing_subscriber::fmt()
        // filter spans/events with level TRACE or higher.
        .with_max_level(tracing::Level::TRACE)
        .compact()
        // Display source code file paths
        .with_file(true)
        // Display source code line numbers
        .with_line_number(true)
        // Display the thread ID an event was recorded on
        .with_thread_ids(true)
        // Don't display the event's target (module path)
        .with_target(false)
        // Build the subscriber
        .init();

    let options = TlsConnectionOptions {
        broker_options: vec![TlsBrokerOptions {
            key: "./etc/redpanda/certs/client.key".into(),
            cert: "./etc/redpanda/certs/client.crt".into(),
            host: "piggy.callistolabs.cloud".to_owned(),
            port: 9092,
        }],
        cafile: Some("./etc/redpanda/certs/root.crt".into()),
    };

    let topic_name = "my-tester";

    let stream = tokio_stream::StreamExt::throttle(
        iter(vec![0].into_iter()).cycle().map(|_| {
            let partition_id = 0;
            ProduceMessage {
                topic: topic_name.to_string(),
                partition_id,
                key: Some(Bytes::from_static(b"Tester")),
                value: Some(Bytes::from_static(b"Value")),
                headers: vec![],
            }
        }),
        std::time::Duration::from_secs(1),
    );

    tracing::info!("Connecting to cluster");
    let output_stream = ProducerBuilder::<TlsConnection>::new(
        options,
        vec![topic_name.to_string()],
    )
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
