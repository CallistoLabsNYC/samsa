use samsa::prelude;
use samsa::prelude::{BrokerConnection, Compression, Error, KafkaCode, protocol, TcpConnection};
use std::collections::HashMap;
use futures::StreamExt;
use tokio_stream::iter;
use samsa::prelude::protocol::produce::request::Attributes;

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
    let mut conn = TcpConnection::new(brokers.clone()).await?;
    let topic_name = "tester-topic";

    //
    // Create topic with 1O partitions
    //
    let create_res = prelude::create_topics(
        conn.clone(),
        CORRELATION_ID,
        CLIENT_ID,
        HashMap::from([(topic_name, NUMBER_OF_PARTITIONS)]),
    )
    .await?;

    // TopicAlreadyExists is an acceptable error in the instance of this test
    // aborting prematurely
    if create_res.topics[0].error_code != KafkaCode::TopicAlreadyExists {
        assert_eq!(create_res.topics[0].error_code, KafkaCode::None);
    }

    //
    // Create request to this topic
    //
    let mut cluster_metadata = prelude::ClusterMetadata::<TcpConnection>::new(
        brokers.clone(),
        CLIENT_ID.to_string(),
        vec![topic_name.to_string()],
    )
    .await?;

    //
    // Test producing (writing) for 1 million topics
    //
    let stream = iter(0..1000000);
    let mut stream = stream.ready_chunks(CHUNK_SIZE);

    let key = bytes::Bytes::from("testing testing...");
    let value = bytes::Bytes::from("123!");
    let header = protocol::Header::new(
        String::from("Header key"),
        bytes::Bytes::from("Header value"),
    );

    while let Some(chunk) = stream.next().await {
        let mut produce_request = protocol::ProduceRequest::new(
            1,
            1000,
            CORRELATION_ID,
            CLIENT_ID,
            Attributes::new(Some(Compression::Gzip)),
        );

        for _ in chunk {
            // spread it among all the partitions
            for i in 0..NUMBER_OF_PARTITIONS {
                let partition_id = i;
                produce_request.add(
                    topic_name,
                    partition_id,
                    Some(key.clone()),
                    Some(value.clone()),
                    vec![header.clone()],
                );
            }
        }

        cluster_metadata.sync().await?;
        conn.send_request(&produce_request).await?;

        let bytes = conn.receive_response().await?.freeze();
        let produce_response = protocol::ProduceResponse::try_from(bytes)?;

        assert_eq!(produce_response.responses.len(), 1);
        assert_eq!(
            produce_response.responses[0].name,
            bytes::Bytes::from(topic_name)
        );
        assert_eq!(
            produce_response.responses[0].partition_responses[0].error_code,
            KafkaCode::None
        );
    }

    //
    // Test fetch (read)
    //
    let mut fetch_req = protocol::FetchRequest::new(CORRELATION_ID, CLIENT_ID, 1000, 10, 1000, 0);
    fetch_req.add(topic_name, 0, 0, 1000);

    conn.send_request(&fetch_req).await?;
    let bytes = conn.receive_response().await?.freeze();
    let fetch_response = protocol::FetchResponse::try_from(bytes)?;

    assert_eq!(fetch_response.topics.len(), 1);
    assert_eq!(fetch_response.topics[0].partitions.len(), 1);
    assert_eq!(
        fetch_response.topics[0].partitions[0].error_code,
        KafkaCode::None
    );

    let mut records = fetch_response.topics[0].partitions[0]
        .clone()
        .into_box_iter();

    let (partition, err_code, _base_offset, _base_timestamp, record) = records.next().unwrap();

    assert_eq!(partition, 0);
    assert_eq!(err_code, KafkaCode::None);
    assert_eq!(record.key, key);
    assert_eq!(record.value, value);

    Ok(())
}
