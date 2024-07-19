use std::collections::HashMap;

use futures::stream::iter;
use futures::StreamExt;

use samsa::prelude;
use samsa::prelude::{
    BrokerConnection, ConsumerBuilder, Error, KafkaCode, ProduceMessage, ProducerBuilder,
    TcpConnection, TopicPartitionsBuilder,
};

mod testsupport;

const CLIENT_ID: &str = "multi partition read and write test";
const CORRELATION_ID: i32 = 1;
const NUMBER_OF_PARTITIONS: i32 = 10;
const CHUNK_SIZE: usize = 10;

#[tokio::test]
async fn multi_partition_writing_and_reading() -> Result<(), Box<Error>> {
    let (skip, brokers) = testsupport::get_brokers()?;
    if skip {
        return Ok(());
    }
    let conn = TcpConnection::new(brokers.clone()).await?;
    let topic_name = testsupport::create_topic_from_file_path(file!())?;

    //
    // Create topic with 10 partitions
    //
    let create_res = prelude::create_topics(
        conn.clone(),
        CORRELATION_ID,
        CLIENT_ID,
        HashMap::from([(topic_name.as_str(), NUMBER_OF_PARTITIONS)]),
    )
    .await?;

    // TopicAlreadyExists is an acceptable error in the instance of this test
    // aborting prematurely
    if create_res.topics[0].error_code != KafkaCode::TopicAlreadyExists {
        assert_eq!(create_res.topics[0].error_code, KafkaCode::None);
    }

    //
    // Test producing (writing)
    //
    let inner_topic = topic_name.clone();
    let stream = iter(0..NUMBER_OF_PARTITIONS).map(move |i| ProduceMessage {
        topic: inner_topic.clone(),
        partition_id: i,
        key: None,
        value: Some(bytes::Bytes::from_static(b"0123456789")),
        headers: vec![],
    });

    let output_stream =
        ProducerBuilder::<TcpConnection>::new(brokers.clone(), vec![topic_name.clone()])
            .await?
            .required_acks(1)
            .clone()
            .build_from_stream(stream.chunks(CHUNK_SIZE))
            .await;
    tokio::pin!(output_stream);
    // producing
    while let Some(message) = output_stream.next().await {
        let res = message[0].as_ref().unwrap();
        assert_eq!(res.responses.len(), 1);
        assert_eq!(res.responses[0].name, bytes::Bytes::from(topic_name.clone()));
        assert_eq!(
            res.responses[0].partition_responses[0].error_code,
            KafkaCode::None
        );
    }
    // done

    //
    // Test fetch
    //
    let partitions: Vec<i32> = (0..NUMBER_OF_PARTITIONS).collect();
    let stream = ConsumerBuilder::<TcpConnection>::new(
        brokers.clone(),
        TopicPartitionsBuilder::new()
            .assign(topic_name.to_string(), partitions)
            .build(),
    )
    .await?
    .build()
    .into_stream();

    tokio::pin!(stream);
    while let Some(message) = stream.next().await {
        // assert topic name
        let res = message.unwrap().0;
        if !res.is_empty() {
            assert_eq!(res[0].topic_name, bytes::Bytes::from(topic_name));
            break;
        }
    }

    Ok(())
}
