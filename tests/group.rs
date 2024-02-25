mod testsupport;

use nom::AsBytes;
use samsa::prelude::{protocol, BrokerConnection, Error, KafkaCode, ROUND_ROBIN_PROTOCOL};

const CLIENT_ID: &str = "group protocol integration test";
const CORRELATION_ID: i32 = 1;
const GROUP_ID: &str = "group integration test";
const GROUP_ID2: &str = "group integration test 2";
const PARTITION_ID: i32 = 0;

#[tokio::test]
async fn it_can_join_and_sync_groups() -> Result<(), Box<Error>> {
    let (skip, brokers, topic) = testsupport::get_brokers_and_topic()?;
    if skip {
        return Ok(());
    }
    let conn = BrokerConnection::new(brokers).await?;
    testsupport::ensure_topic_creation(&conn, &topic, CORRELATION_ID, CLIENT_ID).await?;

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
    let coordinator_addr = format!("{}:{}", host, port);
    let coordinator_conn = BrokerConnection::new(vec![coordinator_addr]).await?;

    // idk why this helps... maybe redpanda needs a second to accept for the coordinator
    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

    //
    // Test join group
    //
    let protocols = [ROUND_ROBIN_PROTOCOL]
        .iter()
        .map(|protocol| protocol::join_group::request::Protocol {
            name: protocol,
            metadata: protocol::join_group::request::Metadata {
                version: 3,
                subscription: vec![&topic],
                user_data: None,
            },
        })
        .collect();
    let join_group_request = protocol::JoinGroupRequest::new(
        CORRELATION_ID,
        CLIENT_ID,
        GROUP_ID,
        100000,
        10000,
        bytes::Bytes::from(""),
        "consumer",
        protocols,
    )?;

    coordinator_conn.send_request(&join_group_request).await?;
    let join_group_response =
        protocol::JoinGroupResponse::try_from(coordinator_conn.receive_response().await?.freeze())?;

    assert_eq!(join_group_response.members.len(), 1);
    assert_eq!(join_group_response.leader, join_group_response.member_id);
    assert_eq!(join_group_response.error_code, KafkaCode::None);

    //
    // Test sync group
    //
    let assignments = protocol::sync_group::request::Assignment::new(
        join_group_response.member_id.clone(),
        protocol::sync_group::request::MemberAssignment {
            version: 3,
            partition_assignments: vec![protocol::sync_group::request::PartitionAssignment {
                topic_name: &topic,
                partitions: vec![PARTITION_ID],
            }],
            user_data: None,
        },
    )?;

    let sync_req = protocol::SyncGroupRequest::new(
        CORRELATION_ID,
        CLIENT_ID,
        GROUP_ID,
        join_group_response.generation_id,
        join_group_response.member_id.clone(),
        vec![assignments],
    )?;

    coordinator_conn.send_request(&sync_req).await?;
    let sync_response =
        protocol::SyncGroupResponse::try_from(coordinator_conn.receive_response().await?.freeze())?;

    assert_eq!(sync_response.error_code, KafkaCode::None);
    assert_eq!(sync_response.assignment.partition_assignments.len(), 1);
    assert_eq!(
        sync_response.assignment.partition_assignments[0],
        protocol::sync_group::response::PartitionAssignment {
            topic_name: bytes::Bytes::from(topic),
            partitions: vec![PARTITION_ID]
        }
    );
    assert_eq!(sync_response.error_code, KafkaCode::None);

    //
    // Test heartbeat
    //
    let heartbeat_request = protocol::HeartbeatRequest::new(
        CORRELATION_ID,
        CLIENT_ID,
        GROUP_ID,
        join_group_response.generation_id,
        join_group_response.member_id.clone(),
    )?;
    coordinator_conn.send_request(&heartbeat_request).await?;
    let heartbeat_response =
        protocol::HeartbeatResponse::try_from(coordinator_conn.receive_response().await?.freeze())?;

    assert_eq!(heartbeat_response.error_code, KafkaCode::None);

    //
    // Test leave group
    //
    let leave_group_request = protocol::LeaveGroupRequest::new(
        CORRELATION_ID,
        CLIENT_ID,
        GROUP_ID,
        join_group_response.member_id.clone(),
    )?;
    coordinator_conn.send_request(&leave_group_request).await?;
    let leave_group_response = protocol::LeaveGroupResponse::try_from(
        coordinator_conn.receive_response().await?.freeze(),
    )?;

    assert_eq!(leave_group_response.error_code, KafkaCode::None);

    Ok(())
}

#[tokio::test]
async fn it_can_join_and_sync_groups_with_functions() -> Result<(), Box<Error>> {
    let (skip, brokers, topic) = testsupport::get_brokers_and_topic()?;
    if skip {
        return Ok(());
    }
    let conn = BrokerConnection::new(brokers).await?;
    testsupport::ensure_topic_creation(&conn, &topic, CORRELATION_ID, CLIENT_ID).await?;

    //
    // Get coordinator for this group
    //
    let coordinator_res =
        samsa::prelude::find_coordinator(conn, CORRELATION_ID, CLIENT_ID, GROUP_ID2).await?;
    assert_eq!(coordinator_res.error_code, KafkaCode::None);
    let host = std::str::from_utf8(coordinator_res.host.as_bytes()).unwrap();
    let port = coordinator_res.port;
    let coordinator_addr = format!("{}:{}", host, port);
    let coordinator_conn = BrokerConnection::new(vec![coordinator_addr]).await?;

    // idk why this helps... maybe redpanda needs a second to accept for the coordinator
    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

    //
    // Test join group
    //
    let protocols = [ROUND_ROBIN_PROTOCOL]
        .iter()
        .map(|protocol| protocol::join_group::request::Protocol {
            name: protocol,
            metadata: protocol::join_group::request::Metadata {
                version: 3,
                subscription: vec![&topic],
                user_data: None,
            },
        })
        .collect();

    let join_group_response = samsa::prelude::join_group(
        coordinator_conn.clone(),
        CORRELATION_ID,
        CLIENT_ID,
        GROUP_ID2,
        100000,
        10000,
        bytes::Bytes::from(""),
        "consumer",
        protocols,
    )
    .await?;

    assert_eq!(join_group_response.members.len(), 1);
    assert_eq!(join_group_response.leader, join_group_response.member_id);
    assert_eq!(join_group_response.error_code, KafkaCode::None);

    //
    // Test sync group
    //
    let assignments = protocol::sync_group::request::Assignment::new(
        join_group_response.member_id.clone(),
        protocol::sync_group::request::MemberAssignment {
            version: 3,
            partition_assignments: vec![protocol::sync_group::request::PartitionAssignment {
                topic_name: &topic,
                partitions: vec![PARTITION_ID],
            }],
            user_data: None,
        },
    )?;

    let sync_response = samsa::prelude::sync_group(
        coordinator_conn.clone(),
        CORRELATION_ID,
        CLIENT_ID,
        GROUP_ID2,
        join_group_response.generation_id,
        join_group_response.member_id.clone(),
        vec![assignments],
    )
    .await?;

    assert_eq!(sync_response.error_code, KafkaCode::None);
    assert_eq!(sync_response.assignment.partition_assignments.len(), 1);
    assert_eq!(
        sync_response.assignment.partition_assignments[0],
        protocol::sync_group::response::PartitionAssignment {
            topic_name: bytes::Bytes::from(topic),
            partitions: vec![PARTITION_ID]
        }
    );
    assert_eq!(sync_response.error_code, KafkaCode::None);

    //
    // Test heartbeat
    //
    let heartbeat_response = samsa::prelude::heartbeat(
        coordinator_conn.clone(),
        CORRELATION_ID,
        CLIENT_ID,
        GROUP_ID2,
        join_group_response.generation_id,
        join_group_response.member_id.clone(),
    )
    .await?;
    assert_eq!(heartbeat_response.error_code, KafkaCode::None);

    //
    // Test leave group
    //
    let leave_group_response = samsa::prelude::leave_group(
        coordinator_conn,
        CORRELATION_ID,
        CLIENT_ID,
        GROUP_ID2,
        join_group_response.member_id.clone(),
    )
    .await?;
    assert_eq!(leave_group_response.error_code, KafkaCode::None);

    Ok(())
}
