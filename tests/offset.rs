mod testsupport;

use nom::AsBytes;
use samsa::prelude::{self, ClusterMetadata};
use samsa::prelude::{protocol, BrokerAddress, BrokerConnection, Error, KafkaCode, TcpConnection};
use std::collections::HashMap;

const CLIENT_ID: &str = "offset protocol integration test";
const CORRELATION_ID: i32 = 1;
const GROUP_ID: &str = "offset integration test";
const PARTITION_ID: i32 = 0;
const OFFSET: i64 = 42;

#[tokio::test]
async fn it_can_commit_and_fetch_offsets() -> Result<(), Box<Error>> {
    let (skip, brokers, topic) = testsupport::get_brokers_and_topic()?;
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
    testsupport::ensure_topic_creation(conn.clone(), &topic, CORRELATION_ID, CLIENT_ID).await?;

    //
    // Get coordinator for this group
    //
    let coordinator_req =
        protocol::FindCoordinatorRequest::new(CORRELATION_ID, CLIENT_ID, GROUP_ID);
    conn.send_request(&coordinator_req).await?;
    let coordinator_res =
        protocol::FindCoordinatorResponse::try_from(conn.receive_response().await?.freeze())?;
    assert_eq!(coordinator_res.error_code, KafkaCode::None);
    let host = std::str::from_utf8(coordinator_res.host.as_bytes()).unwrap();
    let port = coordinator_res.port;
    let mut coordinator_conn = TcpConnection::new(vec![BrokerAddress {
        host: host.to_owned(),
        port: port.try_into().map_err(|err| {
            tracing::error!(
                "Error decoding Broker connection port from metadata {:?}",
                err
            );
            Error::MetadataNeedsSync
        })?,
    }])
    .await?;

    // idk why this helps... maybe redpanda needs a second to accept for the coordinator
    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

    //
    // Test offset commit
    //
    let mut offset_commit_request = protocol::OffsetCommitRequest::new(
        CORRELATION_ID,
        CLIENT_ID,
        GROUP_ID,
        -1,
        bytes::Bytes::from(""),
        1000,
    )?;

    offset_commit_request.add(&topic, PARTITION_ID, OFFSET, Some("metadata"));

    coordinator_conn
        .send_request(&offset_commit_request)
        .await?;
    let offset_commit_response = protocol::OffsetCommitResponse::try_from(
        coordinator_conn.receive_response().await?.freeze(),
    )?;

    assert_eq!(offset_commit_response.topics.len(), 1);
    assert_eq!(offset_commit_response.topics[0].partitions.len(), 1);
    assert_eq!(
        offset_commit_response.topics[0].partitions[0].error_code,
        KafkaCode::None
    );
    assert!(offset_commit_response.is_error().is_ok());

    //
    // Test offset fetch
    //
    let mut offset_fetch_req =
        protocol::OffsetFetchRequest::new(CORRELATION_ID, CLIENT_ID, GROUP_ID);
    offset_fetch_req.add(&topic, PARTITION_ID);
    coordinator_conn.send_request(&offset_fetch_req).await?;
    let offset_fetch_response = protocol::OffsetFetchResponse::try_from(
        coordinator_conn.receive_response().await?.freeze(),
    )?;

    assert_eq!(offset_fetch_response.error_code, KafkaCode::None);
    assert_eq!(offset_fetch_response.topics.len(), 1);
    assert_eq!(offset_fetch_response.topics[0].partitions.len(), 1);
    assert_eq!(
        offset_fetch_response.topics[0].partitions[0].error_code,
        KafkaCode::None
    );
    assert_eq!(
        offset_fetch_response.topics[0].partitions[0].committed_offset,
        OFFSET
    );

    //
    // Delete topic
    //
    let delete_res = prelude::delete_topics(
        conn.clone(),
        CORRELATION_ID,
        CLIENT_ID,
        vec![topic.as_str()],
    )
    .await?;
    assert_eq!(delete_res.topics[0].error_code, KafkaCode::None);

    Ok(())
}

#[tokio::test]
async fn it_can_commit_and_fetch_offsets_with_functions() -> Result<(), Box<Error>> {
    let (skip, brokers, topic) = testsupport::get_brokers_and_topic()?;
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
    testsupport::ensure_topic_creation(conn.clone(), &topic, CORRELATION_ID, CLIENT_ID).await?;

    //
    // Get coordinator for this group
    //
    let coordinator_res =
        samsa::prelude::find_coordinator(conn.clone(), CORRELATION_ID, CLIENT_ID, GROUP_ID).await?;
    assert_eq!(coordinator_res.error_code, KafkaCode::None);
    let host = std::str::from_utf8(coordinator_res.host.as_bytes()).unwrap();
    let port = coordinator_res.port;

    let coordinator_conn = TcpConnection::new(vec![BrokerAddress {
        host: host.to_owned(),
        port: port.try_into().map_err(|err| {
            tracing::error!(
                "Error decoding Broker connection port from metadata {:?}",
                err
            );
            Error::MetadataNeedsSync
        })?,
    }])
    .await?;

    // idk why this helps... maybe redpanda needs a second to accept for the coordinator
    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

    //
    // Test offset commit
    //
    let offsets = HashMap::from([((topic.clone(), PARTITION_ID), OFFSET)]);
    let offset_commit_response = samsa::prelude::commit_offset(
        CORRELATION_ID,
        CLIENT_ID,
        GROUP_ID,
        coordinator_conn.clone(),
        -1,
        bytes::Bytes::from(""),
        offsets,
        1000,
    )
    .await?;

    assert_eq!(offset_commit_response.topics.len(), 1);
    assert_eq!(offset_commit_response.topics[0].partitions.len(), 1);
    assert_eq!(
        offset_commit_response.topics[0].partitions[0].error_code,
        KafkaCode::None
    );
    assert!(offset_commit_response.is_error().is_ok());

    //
    // Test offset fetch
    //
    let topic_partitions = HashMap::from([(topic.clone(), vec![PARTITION_ID])]);
    let offset_fetch_response = samsa::prelude::fetch_offset(
        CORRELATION_ID,
        CLIENT_ID,
        GROUP_ID,
        coordinator_conn.clone(),
        &topic_partitions,
    )
    .await?;

    assert_eq!(offset_fetch_response.error_code, KafkaCode::None);
    assert_eq!(offset_fetch_response.topics.len(), 1);
    assert_eq!(offset_fetch_response.topics[0].partitions.len(), 1);
    assert_eq!(
        offset_fetch_response.topics[0].partitions[0].error_code,
        KafkaCode::None
    );
    assert_eq!(
        offset_fetch_response.topics[0].partitions[0].committed_offset,
        OFFSET
    );

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
