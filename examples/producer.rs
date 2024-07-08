use bytes::Bytes;
use futures::{stream::iter, StreamExt};
use samsa::prelude::{ProduceMessage, ProducerBuilder, TcpConnection};

#[tokio::main]
async fn main() -> Result<(), ()> {
    tracing_subscriber::fmt()
        // filter spans/events with level TRACE or higher.
        .with_max_level(tracing::Level::INFO)
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

    let bootstrap_addrs = vec![samsa::prelude::BrokerAddress {
        host: "127.0.0.1".to_owned(),
        port: 9092,
    }];
    let topic_name = "benchmark";

    let stream = iter(0..5000000).map(|_| ProduceMessage {
        topic: topic_name.to_string(),
        partition_id: 0,
        key: None,
        value: Some(Bytes::from_static(b"0123456789")),
        headers: vec![],
    });

    tracing::info!("Connecting to cluster");
    let output_stream =
        ProducerBuilder::<TcpConnection>::new(bootstrap_addrs, vec![topic_name.to_string()])
            .await
            .map_err(|err| tracing::error!("{:?}", err))?
            // .compression(Compression::Gzip)
            // .required_acks(1)
            .clone()
            .build_from_stream(stream.chunks(50000))
            .await
            // .chunks(2000)
            ;

    tracing::info!("running");
    tokio::pin!(output_stream);
    while (output_stream.next().await).is_some() {}
    tracing::info!("done");

    Ok(())
}
