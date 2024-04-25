mod testsupport;

use samsa::prelude::{self, protocol, Error, KafkaCode, TcpBrokerConnection};
use std::collections::HashMap;

const CLIENT_ID: &str = "create delete topic integration test";
const CORRELATION_ID: i32 = 1;

#[tokio::test]
async fn it_can_create_and_delete_topics() -> Result<(), Box<Error>> {
    let (skip, brokers) = testsupport::get_brokers()?;
    if skip {
        return Ok(());
    }
    let conn = TcpBrokerConnection::new(brokers).await?;

    //
    // Create topic
    //
    let mut create_req =
        protocol::CreateTopicsRequest::new(CORRELATION_ID, CLIENT_ID, 4000, false)?;

    create_req.add("tester-creation", 2, 1);

    conn.send_request_(&create_req).await?;
    let res = conn.receive_response_().await?.freeze();

    let create_res = protocol::CreateTopicsResponse::try_from(res)?;

    assert_eq!(create_res.topics[0].error_code, KafkaCode::None);

    //
    // Delete topic
    //
    let mut delete_req = protocol::DeleteTopicsRequest::new(CORRELATION_ID, CLIENT_ID, 4000)?;

    delete_req.add("tester-creation");

    conn.send_request_(&delete_req).await?;
    let res = conn.receive_response_().await?.freeze();

    let delete_res = protocol::DeleteTopicsResponse::try_from(res)?;

    assert_eq!(delete_res.topics[0].error_code, KafkaCode::None);

    Ok(())
}

#[tokio::test]
async fn it_can_create_and_delete_topics_with_functions() -> Result<(), Box<Error>> {
    let (skip, brokers) = testsupport::get_brokers()?;
    if skip {
        return Ok(());
    }
    let conn = TcpBrokerConnection::new(brokers).await?;

    //
    // Create topic
    //
    let create_res = prelude::create_topics(
        conn.clone(),
        CORRELATION_ID,
        CLIENT_ID,
        HashMap::from([("function-topic", 2)]),
    )
    .await?;
    assert_eq!(create_res.topics[0].error_code, KafkaCode::None);

    //
    // Delete topic
    //
    let delete_res = prelude::delete_topics(
        conn.clone(),
        CORRELATION_ID,
        CLIENT_ID,
        vec!["function-topic"],
    )
    .await?;
    assert_eq!(delete_res.topics[0].error_code, KafkaCode::None);

    Ok(())
}
