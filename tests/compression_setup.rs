use futures::stream::iter;
use futures::StreamExt;
use samsa::prelude::{self, ClusterMetadata};

use samsa::prelude::{
    Compression, ConsumerBuilder, Error, KafkaCode, ProduceMessage, ProducerBuilder, TcpConnection,
    TopicPartitionsBuilder,
};

mod testsupport;

const CLIENT_ID: &str = "writing and reading using compression setup";
const CORRELATION_ID: i32 = 1;
const PARTITION_ID: i32 = 0;

#[tokio::test]
async fn writing_and_reading_using_compression_setup() -> Result<(), Box<Error>> {
    let (skip, brokers) = testsupport::get_brokers()?;
    if skip {
        return Ok(());
    }
    let topic = testsupport::create_topic_from_file_path(file!())?;

    // set up tcp connection options
    let mut metadata = ClusterMetadata::new(brokers.clone(), CLIENT_ID.to_owned(), vec![]).await?;
    let conn: &mut TcpConnection = metadata
        .broker_connections
        .get_mut(&metadata.controller_id)
        .unwrap();

    testsupport::ensure_topic_creation(conn.clone(), topic.as_str(), CORRELATION_ID, CLIENT_ID)
        .await?;

    //
    // Test producing
    //
    let inner_topic = topic.clone();
    let stream = iter(0..5).map(move |_| ProduceMessage {
        topic: inner_topic.clone(),
        partition_id: PARTITION_ID,
        key: None,
        value: Some(bytes::Bytes::from_static(b"0123456789")),
        headers: vec![],
    });

    let output_stream = ProducerBuilder::<TcpConnection>::new(brokers.clone(), vec![topic.clone()])
        .await?
        .required_acks(1)
        .compression(Compression::Gzip)
        .clone()
        .build_from_stream(stream.chunks(1))
        .await;
    tokio::pin!(output_stream);
    // producing
    while let Some(message) = output_stream.next().await {
        let res = message[0].as_ref().unwrap();
        assert_eq!(res.responses.len(), 1);
        assert_eq!(res.responses[0].name, bytes::Bytes::from(topic.to_string()));
        assert_eq!(
            res.responses[0].partition_responses[0].error_code,
            KafkaCode::None
        );
    }
    // done

    //
    // Test fetch
    //
    let stream = ConsumerBuilder::<TcpConnection>::new(
        brokers.clone(),
        TopicPartitionsBuilder::new()
            .assign(topic.to_string(), vec![0])
            .build(),
    )
    .await?
    .build()
    .into_processed_stream();

    tokio::pin!(stream);
    while let Some(message) = stream.next().await {
        // assert topic name
        let res = message.unwrap().0;
        if !res.is_empty() {
            assert_eq!(res[0].topic_name, bytes::Bytes::from(topic.to_string()));
            assert_eq!(res[0].value, bytes::Bytes::from_static(b"0123456789"));
            break;
        }
    }

    //
    // Delete topic
    //
    prelude::delete_topics(
        conn.clone(),
        CORRELATION_ID,
        CLIENT_ID,
        vec![topic.as_str()],
    )
    .await?;

    Ok(())
}
