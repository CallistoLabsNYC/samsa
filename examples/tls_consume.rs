use samsa::prelude::{
    BrokerAddress, ConsumerBuilder, TlsConnection, TlsConnectionOptions, TopicPartitionsBuilder,
};
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

    let options = TlsConnectionOptions {
        broker_options: vec![BrokerAddress {
            host: "piggy.callistolabs.cloud".to_owned(),
            port: 9092,
        }],
        key: "./etc/redpanda/certs/piggy.key".into(),
        cert: "./etc/redpanda/certs/piggy_callisto_labs_cloud.crt".into(),
        cafile: Some("./etc/redpanda/certs/trustedroot.crt".into()),
    };

    let src_topic = "my-tester".to_owned();

    let s = ConsumerBuilder::<TlsConnection>::new(
        options,
        TopicPartitionsBuilder::new()
            .assign(src_topic, vec![0])
            .build(),
    )
    .await
    .unwrap()
    .build()
    .into_stream();

    tokio::pin!(s);

    while let Some(m) = s.next().await {
        tracing::info!("{:?} read", m.unwrap().count());
    }

    Ok(())
}
