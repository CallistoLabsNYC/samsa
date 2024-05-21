use std::time::Duration;

use samsa::prelude::{ConnectionParams, ConnectionParamsKind, ConsumerGroupBuilder, TcpConnection, TopicPartitionsBuilder};
use tokio_stream::StreamExt;

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

    let bootstrap_addrs = vec!["127.0.0.1:9092".to_string()];

    let group_id = "Squad".to_string();
    let src_topic = "my-topic".to_string();

    let stream = ConsumerGroupBuilder::new(
        ConnectionParams(ConnectionParamsKind::TcpParams(bootstrap_addrs)),
        group_id,
        TopicPartitionsBuilder::new()
            .assign(src_topic, vec![0, 1, 2, 3])
            .build(),
    )
    .await
    .map_err(|err| tracing::error!("{:?}", err))?
    .build::<TcpConnection>()
    .await
    .map_err(|err| tracing::error!("{:?}", err))?
    .into_stream()
    .throttle(Duration::from_secs(2));

    tokio::pin!(stream);

    while let Some(message) = stream.next().await {
        tracing::info!("{:?}", message);
    }
    Ok(())
}
