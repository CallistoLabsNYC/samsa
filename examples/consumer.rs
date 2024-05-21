use samsa::prelude::{
    ConnectionParams, ConnectionParamsKind, ConsumerBuilder, TcpConnection, TopicPartitionsBuilder
};
use tokio_stream::StreamExt;

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

    let bootstrap_addrs = vec!["127.0.0.1:9092".to_string()];

    let src_topic = "my-tester".to_string();

    let stream = ConsumerBuilder::<TcpConnection>::new(
        ConnectionParams(ConnectionParamsKind::TcpParams(bootstrap_addrs)),
        TopicPartitionsBuilder::new()
            .assign(src_topic, vec![0, 1, 2, 3])
            .build(),
    )
    .await
    .map_err(|err| tracing::error!("{:?}", err))?
    .build()
    .into_stream()
    .throttle(std::time::Duration::from_secs(1));

    tokio::pin!(stream);

    while let Some(message) = stream.next().await {
        tracing::info!("{:?}", message);
    }

    Ok(())
}
