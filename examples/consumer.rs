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

    let topic = "benchmark";

    //
    // Test fetch (read)
    //
    let stream = ConsumerBuilder::<TcpConnection>::new(
        bootstrap_addrs.clone(),
        TopicPartitionsBuilder::new()
            .assign(topic.to_string(), vec![0])
            .build(),
    )
    .await
    .map_err(|err| tracing::error!("{:?}", err))?
    .max_bytes(3000000)
    .max_partition_bytes(3000000)
    .max_wait_ms(200)
    .build()
    .into_stream();

    let size = 1000000;

    let mut count = 0;
    tokio::pin!(stream);
    tracing::info!("starting!");
    while let Some(message) = stream.next().await {
        // let new = message.unwrap().count();
        // count += new;
        tracing::info!("reading");
        // tracing::info!("{} - read {} of {}", new, count, size);
        // if count == size {
        //     tracing::info!("done!");
        //     break;
        // }
    }

    Ok(())
}
