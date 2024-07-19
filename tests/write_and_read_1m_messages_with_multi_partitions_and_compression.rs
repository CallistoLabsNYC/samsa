use futures::stream::iter;
use futures::StreamExt;
use samsa::prelude;
use samsa::prelude::{
    BrokerConnection, Compression, ConsumerBuilder, Error, KafkaCode, ProduceMessage,
    ProducerBuilder, TcpConnection, TopicPartitionsBuilder,
};
use std::collections::HashMap;

mod testsupport;

const CLIENT_ID: &str = "all together";
const CORRELATION_ID: i32 = 1;
const NUMBER_OF_PARTITIONS: i32 = 10;
const CHUNK_SIZE: usize = 50_000;

#[tokio::test]
async fn write_and_read_1m_messages_with_multi_partitions_and_compression() -> Result<(), Box<Error>>
{
    let (skip, brokers) = testsupport::get_brokers()?;
    if skip {
        return Ok(());
    }
    let conn = TcpConnection::new(brokers.clone()).await?;
    let topic_name = testsupport::create_topic_from_file_path(file!())?;

    //
    // Create topic with 1O partitions
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
    // Test producing (writing) for 1 million topics
    //
    let inner_topic = topic_name.clone();
    let stream = iter(0..1_000_000).map(move |i| ProduceMessage {
        topic: inner_topic.clone(),
        partition_id: i % 10, // should give a number between 0 and 10 each time
        key: None,
        value: Some(bytes::Bytes::from_static(b"0123456789")),
        headers: vec![],
    });

    let output_stream =
        ProducerBuilder::<TcpConnection>::new(brokers.clone(), vec![topic_name.to_string()])
            .await?
            .compression(Compression::Gzip)
            .required_acks(1)
            .clone()
            .build_from_stream(stream.chunks(CHUNK_SIZE))
            .await;
    tokio::pin!(output_stream);
    while output_stream.next().await.is_some() {}

    //
    // Test fetch (read)
    //
    let stream = ConsumerBuilder::<TcpConnection>::new(
        brokers.clone(),
        TopicPartitionsBuilder::new()
            .assign(topic_name.to_string(), vec![0])
            .build(),
    )
    .await?
    .build()
    .into_stream();

    let mut counter = 0;
    tokio::pin!(stream);
    while let Some(_message) = stream.next().await {
        counter += 1;
        if counter == 1_000_000 {
            break;
        }
    }

    assert_eq!(counter, 1_000_000);

    //
    // Delete topic
    //
    let delete_res = prelude::delete_topics(
        conn.clone(),
        CORRELATION_ID,
        CLIENT_ID,
        vec![topic_name.as_str()],
    )
    .await?;
    assert_eq!(delete_res.topics[0].error_code, KafkaCode::None);

    Ok(())
}
