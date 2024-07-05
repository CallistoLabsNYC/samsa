mod testsupport;

use samsa::prelude::protocol::produce::request::Attributes;
use samsa::prelude::{protocol, BrokerConnection, Compression, Error, TcpConnection, KafkaCode};
use std::collections::HashMap;

const CLIENT_ID: &str = "writing and reading using compression setup";
const CORRELATION_ID: i32 = 1;
const PARTITION_ID: i32 = 0;

#[tokio::test]
async fn writing_using_compression_setup() -> Result<(), Box<Error>> {
    let (skip, brokers, topic) = testsupport::get_brokers_and_topic()?;
    if skip {
        return Ok(());
    }

    // set up tls connection options
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
    let value = bytes::Bytes::from("123");

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
    assert_eq!(produce_response.responses[0].name, bytes::Bytes::from(topic.clone()));
    assert_eq!(produce_response.responses[0].partition_responses[0].error_code, KafkaCode::None);

    Ok(())
}
