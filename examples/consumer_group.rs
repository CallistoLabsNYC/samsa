use std::time::Duration;

use samsa::prelude::{ConsumeMessage, ConsumerGroupBuilder, TcpConnection, TopicPartitionsBuilder};
use tokio_stream::StreamExt;

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

    let group_id = "Squad".to_string();
    let src_topic = "my-topic".to_string();

    let stream = ConsumerGroupBuilder::<TcpConnection>::new(
        bootstrap_addrs,
        group_id,
        TopicPartitionsBuilder::new()
            .assign(src_topic, vec![0, 1, 2, 3])
            .build(),
    )
    .await
    .map_err(|err| tracing::error!("{:?}", err))?
    .build()
    .await
    .map_err(|err| tracing::error!("{:?}", err))?
    .into_stream()
    .throttle(Duration::from_secs(2));

    tokio::pin!(stream);

    while let Some(message) = stream.next().await {
        let messages: Vec<ConsumeMessage> = message.unwrap().collect();
        tracing::info!("{:?}", messages);
    }
    Ok(())
}
