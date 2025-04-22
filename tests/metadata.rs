mod testsupport;

use samsa::prelude::{self, ClusterMetadata};
use samsa::prelude::{protocol, Error, TcpConnection};

const CLIENT_ID: &str = "metadata protocol integration test";
const CORRELATION_ID: i32 = 1;

#[tokio::test]
async fn it_can_get_metadata() -> Result<(), Box<Error>> {
    let (skip, brokers, topic) = testsupport::get_brokers_and_topic()?;
    if skip {
        return Ok(());
    }
    let mut conn = TcpConnection::new_(brokers).await?;

    let topics = vec![topic.clone()];
    let metadata_request = protocol::MetadataRequest::new(CORRELATION_ID, CLIENT_ID, &topics);
    conn.send_request_(&metadata_request).await?;

    let metadata = conn.receive_response_().await?;
    let metadata = protocol::MetadataResponse::try_from(metadata.freeze())?;

    assert_eq!(metadata.brokers.len(), 2);
    assert_eq!(metadata.topics.len(), 1);

    assert_eq!(metadata.topics[0].name, bytes::Bytes::from(topic.clone()));

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

#[tokio::test]
async fn it_can_return_all_the_broker_topics() -> Result<(), Box<Error>> {
    // create topics
    let (_, topic_1) = testsupport::get_topic(file!())?;
    let (_, topic_2) = testsupport::get_topic_2(file!())?;
    let (skip, brokers) = testsupport::get_brokers()?;
    if skip {
        return Ok(());
    }

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
    testsupport::ensure_topic_creation(conn.clone(), &topic_1, CORRELATION_ID, CLIENT_ID).await?;
    testsupport::ensure_topic_creation(conn.clone(), &topic_2, CORRELATION_ID, CLIENT_ID).await?;

    //
    // Test all topic retrevial
    //
    let addrs = vec![samsa::prelude::BrokerAddress {
        host: "localhost".to_owned(),
        port: 9092,
    }];

    let metadata_response =
        ClusterMetadata::<TcpConnection>::new(addrs, CORRELATION_ID, CLIENT_ID.to_owned(), vec![])
            .await?;

    // topics should not be empty
    assert!(!metadata_response.topics.is_empty());
    assert!(metadata_response.topic_names.contains(&topic_1));
    assert!(metadata_response.topic_names.contains(&topic_2));

    Ok(())
}
