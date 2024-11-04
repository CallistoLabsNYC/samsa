use futures::stream::StreamExt;
use samsa::prelude::{ConsumerBuilder, TcpConnection, TopicPartitionsBuilder};

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

    let topic = "benchmark";

    //
    // Test fetch (read)
    //
    tracing::info!("Connecting to cluster");
    let stream = ConsumerBuilder::new_tcp(
        bootstrap_addrs.clone(),
        TopicPartitionsBuilder::new()
            .assign(topic.to_string(), vec![0])
            .build(),
    )
    .await
    .map_err(|err| tracing::error!("{:?}", err))?
    .max_bytes(3_000_000)
    .max_partition_bytes(3_000_000)
    .build()
    .into_stream();

    let size = 1_000_000;

    let mut count = 0;
    tokio::pin!(stream);
    tracing::info!("Starting!");
    while let Some(message) = stream.next().await {
        let new = message.unwrap().count();
        count += new;
        tracing::info!("{} - read {} of {}", new, count, size);
        if count == size {
            tracing::info!("done!");
            break;
        }
    }

    Ok(())
}
