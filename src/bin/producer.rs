
use samsa::prelude::{protocol, BrokerConnection, Error, KafkaCode};
use std::collections::HashMap;

const CLIENT_ID: &str = "produce & fetch protocol integration test";
const CORRELATION_ID: i32 = 1;
const PARTITION_ID: i32 = 0;
use std::env;

const KAFKA_BROKERS: &str = "KAFKA_BROKERS";
const KAFKA_TOPIC: &str = "KAFKA_TOPIC";

#[allow(dead_code)]
pub async fn ensure_topic_creation(
    conn: &BrokerConnection,
    topic: &str,
    correlation_id: i32,
    client_id: &str,
) -> Result<(), Error> {
    let topics = vec![topic];
    let metadata_request =
        samsa::prelude::protocol::MetadataRequest::new(correlation_id, client_id, &topics);
    conn.send_request(&metadata_request).await?;
    let _metadata = conn.receive_response().await?;
    Ok(())
}

pub fn get_brokers() -> Result<(bool, Vec<String>), Error> {
    let brokers: Vec<String> = match env::var(KAFKA_BROKERS) {
        Ok(brokers) => brokers.split(',').map(str::to_string).collect(),
        Err(_) => {
            tracing::warn!("Skipping test because no {} is set", KAFKA_BROKERS);
            return Ok((true, vec![]));
        }
    };
    Ok((false, brokers))
}

pub fn get_brokers_and_topic() -> Result<(bool, Vec<String>, String), Error> {
    let (skip, brokers) = get_brokers()?;
    if skip {
        return Ok((skip, vec![], "".to_string()));
    }
    let topic = match env::var(KAFKA_TOPIC) {
        Ok(topic) => topic,
        Err(_) => {
            tracing::warn!("Skipping test because no {} is set", KAFKA_TOPIC);
            return Ok((true, vec![], "".to_string()));
        }
    };
    Ok((false, brokers, topic))
}


#[tokio::main]
async fn main() -> Result<(), Box<Error>> {
    let (skip, brokers, topic) = get_brokers_and_topic()?;
    if skip {
        return Ok(());
    }
    let conn = BrokerConnection::new(brokers).await?;
    ensure_topic_creation(&conn, &topic, CORRELATION_ID, CLIENT_ID).await?;

    let key = bytes::Bytes::from("testing testing...");
    let value = bytes::Bytes::from("123!");

    //
    // Test producing
    //
    // let mut produce_request = protocol::ProduceRequest::new(1, 1000, CORRELATION_ID, CLIENT_ID);
    // produce_request.add(
    //     &topic,
    //     PARTITION_ID,
    //     protocol::Message {
    //         key: Some(key.clone()),
    //         value: Some(value.clone()),
    //         headers: vec![],
    //     },
    // );
    // println!("{:?}", produce_request);

    // conn.send_request(&produce_request).await?;
    // let bytess = conn.receive_response().await?.freeze();

    // let produce_response = protocol::ProduceResponse::try_from(bytess)?;

    // assert_eq!(produce_response.responses.len(), 1);
    // assert_eq!(
    //     produce_response.responses[0].name,
    //     bytes::Bytes::from(topic.clone())
    // );
    // assert_eq!(produce_response.responses[0].partition_responses.len(), 1);
    // assert_eq!(
    //     produce_response.responses[0].partition_responses[0].error_code,
    //     KafkaCode::None
    // );

    //
    // Test fetch
    //
    let mut fetch_req = protocol::FetchRequest::new(CORRELATION_ID, CLIENT_ID, 10000, 10, 1000, 0);
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
