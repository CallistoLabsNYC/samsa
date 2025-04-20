use std::time::Duration;
use samsa::prelude::{BrokerAddress, ConsumeMessage, ConsumerGroupBuilder, TcpConnection, TopicPartitionsBuilder};
use tokio_stream::StreamExt;
use tracing::{error, info};

async fn start_consumer() {
    let bootstrap_address = vec![BrokerAddress {
        host: "localhost".to_owned(),
        port: 9092
    }];

    let partitions = vec![0];
    let topic_name = "benchmark".to_string();
    let assignment = TopicPartitionsBuilder::new()
        .assign(topic_name, partitions)
        .build();

    let consumer = ConsumerGroupBuilder::<TcpConnection>::new(
        bootstrap_address,
        "ism".to_string(),
        assignment,
    ).await
        .expect("Could not create consumer.")
        .client_id("ism-1".parse().unwrap()) //SETTING MY CLIENT ID
        .build()
        .await
        .expect("Could not create consumer.");

    let stream = consumer.into_stream().throttle(Duration::from_secs(5));
    // have to pin streams before iterating
    tokio::pin!(stream);

    // Stream will do nothing unless consumed.
    while let Some(msg) = stream.next().await {
        match msg {
            Ok(msg) => {
                msg.for_each(|notification| {
                    match String::from_utf8(notification.value.to_vec()) {
                        Ok(str) => info!("{}", str),
                        Err(err) => error!("{}", err),
                    }
                    info!("Received message: {}", notification.offset);
                });
            },
            Err(e) => {
                error!("Error: {e}");
            }
        }
    }
}


#[tokio::main]
pub async fn main() -> Result<(), ()> {
    start_consumer().await;
    Ok(())
}
