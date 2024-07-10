use samsa::prelude;
use samsa::prelude::protocol::produce::request::Attributes;
use samsa::prelude::{protocol, BrokerConnection, Error, KafkaCode, TcpConnection};
use std::collections::HashMap;

mod testsupport;

const CLIENT_ID: &str = "multi partition read and write test";
const CORRELATION_ID: i32 = 1;
const NUMBER_OF_PARTITIONS: i32 = 10;

#[tokio::test]
async fn multi_partition_writing_and_reading() -> Result<(), Box<Error>> {
    let (skip, brokers) = testsupport::get_brokers()?;
    if skip {
        return Ok(());
    }
    let mut conn = TcpConnection::new(brokers.clone()).await?;
    let topic_name = "tester-creation-partition";

    //
    // Create topic with 10 partitions
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
    // Test producing (writing)
    //
    let mut produce_request =
        protocol::ProduceRequest::new(1, 1000, CORRELATION_ID, CLIENT_ID, Attributes::new(None));

    let key = bytes::Bytes::from("testing testing...");
    let value = bytes::Bytes::from("123!");

    let header = protocol::Header::new(
        String::from("Header key"),
        bytes::Bytes::from("Header value"),
    );

    // send a message to the 8 partitions
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
        produce_response.responses[0].partition_responses.len() as i32,
        NUMBER_OF_PARTITIONS
    );

    //
    // Test fetch
    //
    let mut fetch_req = protocol::FetchRequest::new(CORRELATION_ID, CLIENT_ID, 1000, 10, 1000, 0);
    fetch_req.add(topic_name, 0, 0, 10000);

    conn.send_request(&fetch_req).await?;
    let bytes = conn.receive_response().await?.freeze();
    let fetch_response = protocol::FetchResponse::try_from(bytes)?;

    assert_eq!(fetch_response.topics.len(), 1);
    assert_eq!(fetch_response.topics[0].partitions.len() as i32, 1);

    //
    // Delete topic
    //
    let delete_res =
        prelude::delete_topics(conn.clone(), CORRELATION_ID, CLIENT_ID, vec![topic_name]).await?;
    assert_eq!(delete_res.topics[0].error_code, KafkaCode::None);

    Ok(())
}