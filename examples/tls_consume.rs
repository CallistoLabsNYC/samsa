use tokio_stream::StreamExt;
use samsa::prelude::{
    ConsumerBuilder, TlsBrokerOptions, TlsConnection, TlsConnectionOptions, TopicPartitionsBuilder
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

    let src_topic = "my-tester".to_owned();

    let s = ConsumerBuilder::<TlsConnection>::new(
        options,
        TopicPartitionsBuilder::new()
            .assign(src_topic, vec![0, 1, 2, 3])
            .build(),
    )
    .await
    .unwrap()
    .build().into_flat_stream();

    tokio::pin!(s);

    while let Some(m) = s.next().await {
        tracing::info!("{:?}", m);
    }

    Ok(())
}
