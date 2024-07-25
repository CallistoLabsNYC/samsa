use futures::stream::{iter, StreamExt};
use samsa::prelude::{
    ConsumerBuilder, ProduceMessage, ProducerBuilder, TcpConnection, TopicPartitionsBuilder,
};

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

    let stream = iter(0..100)
        .map(move |_| ProduceMessage {
            topic: topic.to_string(),
            partition_id: 0,
            key: None,
            value: Some(bytes::Bytes::from_static(b"0123456789")),
            headers: vec![],
        })
        .chunks(50);

    let output_stream =
        ProducerBuilder::<TcpConnection>::new(bootstrap_addrs.clone(), vec![topic.to_string()])
            .await
            .unwrap()
            .max_batch_size(1)
            .required_acks(1)
            .clone()
            .build_from_stream(stream)
            .await;

    tokio::pin!(output_stream);
    // producing
    while let Some(message) = output_stream.next().await {
        let res = message[0].as_ref();
        tracing::info!("{:?}", res);
    }
    // done

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
    .max_bytes(1000000)
    .max_partition_bytes(500000)
    .build()
    .into_stream();

    // let mut counter = 0;
    tokio::pin!(stream);
    tracing::info!("starting!");
    while let Some(message) = stream.next().await {
        if message.unwrap().0.count() == 0 {
            tracing::info!("done!");
        }
    }

    Ok(())
}
