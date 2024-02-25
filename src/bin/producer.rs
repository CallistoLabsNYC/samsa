use bytes::Bytes;
use samsa::prelude::{ProduceMessage, ProducerBuilder};

#[tokio::main]
async fn main() -> Result<(), ()> {
    tracing_subscriber::fmt::init();
    let bootstrap_addrs = vec!["127.0.0.1:9092".to_string()];
    let topics = vec!["purchases".to_string()];

    tracing::info!("Connecting to cluster");
    let producer_client = ProducerBuilder::new(bootstrap_addrs, topics)
        .await
        .map_err(|err| tracing::error!("{:?}", err))?
        .build()
        .await;

    let topic_name = "purchases";
    let mut partition_id = 0;

    loop {
        if partition_id <= 2 {
            partition_id += 1
        } else {
            partition_id = 0
        };

        tracing::info!(
            "Producing 3 records to topic {} partition {}",
            topic_name.to_string(),
            partition_id
        );

        producer_client
            .produce(ProduceMessage {
                topic: topic_name.to_string(),
                partition_id,
                key: Some(Bytes::from_static(b"Tester")),
                value: Some(Bytes::from_static(b"Value 1")),
            })
            .await;
        producer_client
            .produce(ProduceMessage {
                topic: topic_name.to_string(),
                partition_id,
                key: Some(Bytes::from_static(b"Tester")),
                value: Some(Bytes::from_static(b"Value 2")),
            })
            .await;
        producer_client
            .produce(ProduceMessage {
                topic: topic_name.to_string(),
                partition_id,
                key: Some(Bytes::from_static(b"Tester")),
                value: Some(Bytes::from_static(b"Value 3")),
            })
            .await;
    }
}
