use futures::stream::StreamExt;
use samsa::prelude::{ConsumerBuilder, TcpConnection, TopicPartitionsBuilder};

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

    let src_topic = "benchmark".to_string();

    let stream = ConsumerBuilder::<TcpConnection>::new(
        bootstrap_addrs,
        TopicPartitionsBuilder::new()
            .assign(src_topic, vec![0])
            .build(),
        None,
    )
    .await
    .map_err(|err| tracing::error!("{:?}", err))?
    .max_bytes(1000000)
    .max_partition_bytes(1000000)
    .build()
    .into_stream();

    tokio::pin!(stream);
    tracing::info!("starting!");
    while let Some(message) = stream.next().await {
        if message.unwrap().0.len() == 0 {
            tracing::info!("done!");
        }
    }

    Ok(())
}
