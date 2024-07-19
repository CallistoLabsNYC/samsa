mod testsupport;

use samsa::prelude;
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
