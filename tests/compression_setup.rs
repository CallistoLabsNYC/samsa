mod testsupport;

use samsa::prelude::{
    protocol::{self, produce::request::Attributes},
    BrokerConnection, Compression, Error, KafkaCode, TcpConnection,
};
use std::collections::HashMap;

const CLIENT_ID: &str = "writing and reading using compression setup";
const CORRELATION_ID: i32 = 1;
const PARTITION_ID: i32 = 0;

#[tokio::test]
async fn writing_and_reading_using_compression_setup() -> Result<(), Box<Error>> {
    let (skip, brokers, topic) = testsupport::get_brokers_and_topic()?;
    if skip {
        return Ok(());
    }

    // set up tcp connection options
    let conn = TcpConnection::new(brokers.clone()).await?;
    testsupport::ensure_topic_creation(conn, &topic, CORRELATION_ID, CLIENT_ID).await?;

    let cluster_metadata = samsa::prelude::ClusterMetadata::<TcpConnection>::new(
        brokers.clone(),
        CLIENT_ID.to_string(),
        vec![topic.clone()],
    )
    .await?;
    let topic_partition = HashMap::from([(topic.to_string(), vec![PARTITION_ID])]);
    let (mut conn, _) =
        cluster_metadata.get_connections_for_topic_partitions(&topic_partition)?[0].to_owned();

    let key = bytes::Bytes::from("testing testing...");
    let value = bytes::Bytes::from("123!");

    //
    // Test producing
    //
    let mut produce_request = protocol::ProduceRequest::new(
        1,
        1000,
        CORRELATION_ID,
        CLIENT_ID,
        Attributes::new(Some(Compression::Gzip)),
    );
    let header = protocol::Header::new(
        String::from("Header key"),
        bytes::Bytes::from("Header value"),
    );
    produce_request.add(
        &topic,
        PARTITION_ID,
        Some(key.clone()),
        Some(value.clone()),
        vec![header],
    );

    conn.send_request(&produce_request).await?;
    let bytes = conn.receive_response().await?.freeze();

    let produce_response = protocol::ProduceResponse::try_from(bytes)?;

    assert_eq!(produce_response.responses.len(), 1);
    assert_eq!(
        produce_response.responses[0].name,
        bytes::Bytes::from(topic.clone())
    );
    assert_eq!(
        produce_response.responses[0].partition_responses[0].error_code,
        KafkaCode::None
    );

    //
    // Test fetch
    //
    let mut fetch_req = protocol::FetchRequest::new(CORRELATION_ID, CLIENT_ID, 1000, 10, 1000, 0);
    fetch_req.add(&topic, PARTITION_ID, 0, 10000);
    conn.send_request(&fetch_req).await?;
    let fetch_response =
        protocol::FetchResponse::try_from(conn.receive_response().await?.freeze())?;

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