mod testsupport;

use samsa::prelude::BrokerConnection;
use samsa::prelude::ClusterMetadata;
use samsa::prelude::KafkaCode;
use samsa::prelude::TcpConnection;
use samsa::prelude::protocol;
use samsa::prelude::{self};

const CLIENT_ID: &str = "describe-configs integration test";
const CORRELATION_ID: i32 = 1;
const TOPIC_NAME: &str = "describe-configs-topic";

#[tokio::test]
async fn it_can_describe_topic_configs() -> Result<(), Box<prelude::Error>> {
    let (skip, brokers) = testsupport::get_brokers()?;
    if skip {
        dbg!("skipping test");
        return Ok(());
    }

    let mut metadata =
        ClusterMetadata::<TcpConnection>::new(brokers, 1, "rust".to_string(), vec![]).await?;

    let conn: &mut TcpConnection = metadata
        .broker_connections
        .get_mut(&metadata.controller_id)
        .unwrap();

    // Ensure topic exists (ignore TopicAlreadyExists)
    let mut create_req =
        protocol::CreateTopicsRequest::new(CORRELATION_ID, CLIENT_ID, 4000, false)?;
    create_req.add(TOPIC_NAME, 1, 1);
    conn.send_request(&create_req).await?;
    let create_res =
        protocol::CreateTopicsResponse::try_from(conn.receive_response().await?.freeze())?;
    let code = create_res.topics[0].error_code;
    if !(code == KafkaCode::None || code == KafkaCode::TopicAlreadyExists) {
        return Err(Box::new(prelude::Error::KafkaError(code)));
    }

    // Describe topic configs
    let mut req =
        protocol::describe_configs::request::DescribeConfigsRequest::new(CORRELATION_ID, CLIENT_ID);
    req.add_resource(protocol::describe_configs::request::ConfigResource::Topic {
        resource_name: TOPIC_NAME,
        config_names: vec![],
    });

    conn.send_request(&req).await?;
    let resp_bytes = conn.receive_response().await?.freeze();
    let resp = protocol::DescribeConfigsResponse::try_from(resp_bytes)?;

    assert!(resp.resources.len() >= 1);
    assert_eq!(resp.resources[0].error_code, KafkaCode::None);
    // Should return at least some configs for a topic
    assert!(!resp.resources[0].configs.is_empty());

    Ok(())
}

#[tokio::test]
async fn it_can_describe_broker_configs() -> Result<(), Box<prelude::Error>> {
    let (skip, brokers) = testsupport::get_brokers()?;
    if skip {
        dbg!("skipping test");
        return Ok(());
    }

    let mut metadata =
        ClusterMetadata::<TcpConnection>::new(brokers, 1, "rust".to_string(), vec![]).await?;

    let conn: &mut TcpConnection = metadata
        .broker_connections
        .get_mut(&metadata.controller_id)
        .unwrap();

    // Describe broker configs for the controller broker
    let broker_id_str = metadata.controller_id.to_string();
    let mut req =
        protocol::describe_configs::request::DescribeConfigsRequest::new(CORRELATION_ID, CLIENT_ID);
    req.add_resource(
        protocol::describe_configs::request::ConfigResource::Broker {
            resource_name: broker_id_str.as_str(),
            config_names: vec![],
        },
    );

    conn.send_request(&req).await?;
    let resp_bytes = conn.receive_response().await?.freeze();
    let resp = protocol::DescribeConfigsResponse::try_from(resp_bytes)?;

    dbg!(&resp);

    assert!(resp.resources.len() >= 1);
    assert_eq!(resp.resources[0].error_code, KafkaCode::None);
    assert_eq!(resp.resources[0].resource_type, 4); // Broker
    // Ensure num.partitions is present among broker configs
    let has_num_partitions = resp.resources[0]
        .configs
        .iter()
        .any(|c| c.name.as_ref() == b"num.partitions");
    assert!(has_num_partitions);

    Ok(())
}

#[cfg(feature = "serde")]
#[test]
fn it_parses_values_with_get_value() {
    use samsa::prelude::bytes::Bytes;

    let resp = protocol::describe_configs::response::DescribeConfigsResponse {
        header: protocol::HeaderResponse { correlation_id: 1 },
        throttle_time_ms: 0,
        resources: vec![protocol::describe_configs::response::Resource {
            error_code: KafkaCode::None,
            error_message: None,
            resource_type: 4,
            resource_name: Bytes::from("broker-1"),
            configs: vec![
                protocol::describe_configs::response::Config {
                    name: Bytes::from("zookeeper.ssl.endpoint.identification.algorithm"),
                    value: Some(Bytes::from("HTTPS")),
                    read_only: true,
                    is_default: true,
                    is_sensitive: false,
                    synonyms: vec![],
                },
                protocol::describe_configs::response::Config {
                    name: Bytes::from("replication.quota.window.num"),
                    value: Some(Bytes::from("11")),
                    read_only: true,
                    is_default: true,
                    is_sensitive: false,
                    synonyms: vec![],
                },
                protocol::describe_configs::response::Config {
                    name: Bytes::from("advertised.listeners"),
                    value: None,
                    read_only: false,
                    is_default: true,
                    is_sensitive: false,
                    synonyms: vec![],
                },
                protocol::describe_configs::response::Config {
                    name: Bytes::from("queued.max.request.bytes"),
                    value: Some(Bytes::from("-1")),
                    read_only: true,
                    is_default: true,
                    is_sensitive: false,
                    synonyms: vec![],
                },
            ],
        }],
    };

    let algo: Option<String> = resp.get_value(
        "broker-1",
        "zookeeper.ssl.endpoint.identification.algorithm",
    );
    assert_eq!(algo.as_deref(), Some("HTTPS"));

    let win_num: Option<u32> = resp.get_value("broker-1", "replication.quota.window.num");
    assert_eq!(win_num, Some(11));

    let adv: Option<String> = resp.get_value("broker-1", "advertised.listeners");
    assert!(adv.is_none());

    let queued: Option<i64> = resp.get_value("broker-1", "queued.max.request.bytes");
    assert_eq!(queued, Some(-1));
}
