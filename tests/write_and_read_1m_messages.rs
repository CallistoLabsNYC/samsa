use futures::stream::iter;
use futures::StreamExt;
use samsa::prelude::{self, ClusterMetadata};
use samsa::prelude::{
    Compression, ConsumerBuilder, Error, ProduceMessage, ProducerBuilder, TcpConnection,
    TopicPartitionsBuilder,
};

mod testsupport;
const CLIENT_ID: &str = "write and read 1M messages";
const CORRELATION_ID: i32 = 1;
const PARTITION_ID: i32 = 0;
const CHUNK_SIZE: usize = 50_000;

#[tokio::test]
async fn write_and_read_1m_messages() -> Result<(), Box<Error>> {
    let (skip, brokers) = testsupport::get_brokers()?;
    if skip {
        return Ok(());
    }
    let topic = testsupport::create_topic_from_file_path(file!())?;
    let mut metadata = ClusterMetadata::new(
        brokers.clone(),
        CORRELATION_ID,
        CLIENT_ID.to_owned(),
        vec![],
    )
    .await?;
    let conn: &mut TcpConnection = metadata
        .broker_connections
        .get_mut(&metadata.controller_id)
        .unwrap();
    testsupport::ensure_topic_creation(conn.clone(), &topic, CORRELATION_ID, CLIENT_ID).await?;

    //
    // Test writing
    //
    let inner_topic = topic.clone();
    let stream = iter(0..1_000_000).map(move |_| ProduceMessage {
        topic: inner_topic.clone(),
        partition_id: PARTITION_ID,
        key: None,
        value: Some(bytes::Bytes::from_static(b"0123456789")),
        headers: vec![],
    });

    let output_stream = ProducerBuilder::<TcpConnection>::new(brokers.clone(), vec![topic.clone()])
        .await?
        .compression(Compression::Gzip)
        .required_acks(1)
        .max_batch_size(50000)
        .clone()
        .build_from_stream(stream.chunks(CHUNK_SIZE))
        .await;
    tokio::pin!(output_stream);
    // producing
    while let Some(message) = output_stream.next().await {
        let res = message[0].as_ref().unwrap();
        assert_eq!(res.responses.len(), 1);
        assert_eq!(res.responses[0].name, bytes::Bytes::from(topic.clone()));
    }
    // done

    //
    // Test fetch (read)
    //
    let stream = ConsumerBuilder::<TcpConnection>::new(
        brokers.clone(),
        TopicPartitionsBuilder::new()
            .assign(topic.to_string(), vec![0])
            .build(),
    )
    .await?
    .max_bytes(2000000)
    .max_partition_bytes(200000)
    .build()
    .into_stream();

    let mut counter = 0;
    tokio::pin!(stream);
    while let Some(message) = stream.next().await {
        let new_count: usize = message.unwrap().count();
        counter += new_count;
        if counter >= 1_000_000 {
            break;
        }
    }

    assert_eq!(counter, 1_000_000);

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
