use bytes::Bytes;
use futures::{stream::iter, StreamExt};
use samsa::prelude::{ProduceMessage, ProducerBuilder};

#[tokio::main]
async fn main() -> Result<(), ()> {
    tracing_subscriber::fmt::init();
    let bootstrap_addrs = vec!["127.0.0.1:9092".to_string()];
    let topic_name = "my-topic";

    let stream = iter(vec![0].into_iter()).cycle().enumerate().map(|(i, _)| {
        let partition_id = (i % 4) as i32;
        ProduceMessage {
            topic: topic_name.to_string(),
            partition_id,
            key: Some(Bytes::from_static(b"Tester")),
            value: Some(Bytes::from_static(b"Value")),
            headers: vec![],
        }
    });

    tracing::info!("Connecting to cluster");
    let output_stream = ProducerBuilder::new(bootstrap_addrs, vec![topic_name.to_string()])
        .await
        .map_err(|err| tracing::error!("{:?}", err))?
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
