mod testsupport;

use samsa::prelude::{protocol, Error};

const CLIENT_ID: &str = "metadata protocol integration test";
const CORRELATION_ID: i32 = 1;

#[tokio::test]
async fn it_can_get_metadata() -> Result<(), Box<Error>> {
    let (skip, brokers, topic) = testsupport::get_brokers_and_topic()?;
    if skip {
        return Ok(());
    }
    let conn = samsa::prelude::BrokerConnection::new(brokers).await?;

    let topics = vec![topic.clone()];
    let metadata_request = protocol::MetadataRequest::new(CORRELATION_ID, CLIENT_ID, &topics);
    conn.send_request(&metadata_request).await?;

    let metadata = conn.receive_response().await?;
    let metadata = protocol::MetadataResponse::try_from(metadata.freeze())?;

    assert_eq!(metadata.brokers.len(), 2);
    assert_eq!(metadata.topics.len(), 1);

    assert_eq!(metadata.topics[0].name, bytes::Bytes::from(topic));
    Ok(())
}
