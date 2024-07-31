use bytes::Bytes;
use futures::{stream::iter, StreamExt};
use samsa::prelude::{ProduceMessage, ProducerBuilder, TcpConnection};

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

    let bootstrap_addrs = vec![samsa::prelude::BrokerAddress {
        host: "127.0.0.1".to_owned(),
        port: 9092,
    }];
    let topic_name = "benchmark";

    let stream = iter(0..1_000_000)
        .map(|_| ProduceMessage {
            topic: topic_name.to_string(),
            partition_id: 0,
            key: None,
            value: Some(Bytes::from_static(b"0123456789")),
            headers: vec![],
        })
        .chunks(50_000);

    tracing::info!("Connecting to cluster");
    let output_stream =
        ProducerBuilder::<TcpConnection>::new(bootstrap_addrs, vec![topic_name.to_string()])
            .await
            .map_err(|err| tracing::error!("{:?}", err))?
            .max_batch_size(50_000)
            .batch_timeout_ms(100)
            .clone()
            .build_from_stream(stream)
            .await;

    tracing::info!("Running");
    tokio::pin!(output_stream);
    while (output_stream.next().await).is_some() {
        tracing::info!("Batch sent");
    }
    tracing::info!("Done");

    Ok(())
}
