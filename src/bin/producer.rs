
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
    let mut produce_request = protocol::ProduceRequest::new(1, 1000, CORRELATION_ID, CLIENT_ID);
    produce_request.add(
        &topic,
        PARTITION_ID,
        protocol::Message {
            key: Some(key.clone()),
            value: Some(value.clone()),
            headers: vec![],
        },
    );
    println!("produce request {:?}", produce_request);

    conn.send_request(&produce_request).await?;
    let bytess = conn.receive_response().await?.freeze();

    let produce_response = protocol::ProduceResponse::try_from(bytess)?;
    println!("produce response {:?}", produce_response);

    assert_eq!(produce_response.responses.len(), 1);
    assert_eq!(
        produce_response.responses[0].name,
        bytes::Bytes::from(topic.clone())
    );
    assert_eq!(produce_response.responses[0].partition_responses.len(), 1);
    assert_eq!(
        produce_response.responses[0].partition_responses[0].error_code,
        KafkaCode::None
    );

    //
    // Test fetch
    //
    let mut fetch_req = protocol::FetchRequest::new(CORRELATION_ID, CLIENT_ID, 10000, 10, 1000, 0);
    fetch_req.add(&topic, PARTITION_ID, 0, 10000);
    println!("{:?}", fetch_req);
    conn.send_request(&fetch_req).await?;
    let b = conn.receive_response().await?.freeze();
    println!("fetch bytes {:?}", b.as_ref());
    let fetch_response =
        protocol::FetchResponse::try_from(b)?;
        println!(" fetch response {:?}", fetch_response);

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


/*


[
0, 0, 0, 1, // correlation_id
0, 0, 0, 0, // throttle time
0, 0, 0, 1, // one topic
0, 9, 112, 117, 114, 99, 104, 97, 115, 101, 115, // topic name
0, 0, 0, 1, // one partition
0, 0, 0, 0, // partition id
0, 0, // error code
0, 0, 0, 0, 0, 3, 118, 209, // high water mark
0, 0, 0, 0, 0, 3, 118, 209, // last stable offset
255, 255, 255, 255, // aborted transactions

0, 0, 4, 58, // size of incoming message
0, 0, 0, 0, 0, 0, 0, 0, // base offset
0, 0, 2, 17, // record batch length
0, 0, 0, 4, // partition leader epoch
2, // magic
223, 174, 68, 36, // crc
0, 0, // attributes
0, 0, 0, 23, // last offset
0, 0, 1, 142, 15, 146, 119, 225, // base timestamp
0, 0, 1, 142, 15, 146, 119, 225, // max timestamp
255, 255, 255, 255, 255, 255, 255, 255, // producer id
255, 255, // producer epoch
255, 255, 255, 255, // base sequence
0, 0, 0, 24, // record count
38, // record length
0, // attributes
0, // timestampDelta
0, // offset delta
12, 84, 101, 115, 116, 101, 114, 
14, 86, 97, 108, 117, 101, 32, 49, 
0, 

38, 0, 0, 2, 12, 84, 101, 115, 116, 101, 114, 14, 86, 97, 108, 117, 101, 32, 50, 0, 38, 0, 0, 4, 12, 84, 101, 115, 116, 101, 114, 14, 86, 97, 108, 117, 101, 32, 51, 0, 38, 0, 0, 6, 12, 84, 101, 115, 116, 101, 114, 14, 86, 97, 108, 117, 101, 32, 49, 0, 38, 0, 0, 8, 12, 84, 101, 115, 116, 101, 114, 14, 86, 97, 108, 117, 101, 32, 50, 0, 38, 0, 0, 10, 12, 84, 101, 115, 116, 101, 114, 14, 86, 97, 108, 117, 101, 32, 51, 0, 38, 0, 0, 12, 12, 84, 101, 115, 116, 101, 114, 14, 86, 97, 108, 117, 101, 32, 49, 0, 38, 0, 0, 14, 12, 84, 101, 115, 116, 101, 114, 14, 86, 97, 108, 117, 101, 32, 50, 0, 38, 0, 0, 16, 12, 84, 101, 115, 116, 101, 114, 14, 86, 97, 108, 117, 101, 32, 51, 0, 38, 0, 0, 18, 12, 84, 101, 115, 116, 101, 114, 14, 86, 97, 108, 117, 101, 32, 49, 0, 38, 0, 0, 20, 12, 84, 101, 115, 116, 101, 114, 14, 86, 97, 108, 117, 101, 32, 50, 0, 38, 0, 0, 22, 12, 84, 101, 115, 116, 101, 114, 14, 86, 97, 108, 117, 101, 32, 51, 0, 38, 0, 0, 24, 12, 84, 101, 115, 116, 101, 114, 14, 86, 97, 108, 117, 101, 32, 49, 0, 38, 0, 0, 26, 12, 84, 101, 115, 116, 101, 114, 14, 86, 97, 108, 117, 101, 32, 50, 0, 38, 0, 0, 28, 12, 84, 101, 115, 116, 101, 114, 14, 86, 97, 108, 117, 101, 32, 51, 0, 38, 0, 0, 30, 12, 84, 101, 115, 116, 101, 114, 14, 86, 97, 108, 117, 101, 32, 49, 0, 38, 0, 0, 32, 12, 84, 101, 115, 116, 101, 114, 14, 86, 97, 108, 117, 101, 32, 50, 0, 38, 0, 0, 34, 12, 84, 101, 115, 116, 101, 114, 14, 86, 97, 108, 117, 101, 32, 51, 0, 38, 0, 0, 36, 12, 84, 101, 115, 116, 101, 114, 14, 86, 97, 108, 117, 101, 32, 49, 0, 38, 0, 0, 38, 12, 84, 101, 115, 116, 101, 114, 14, 86, 97, 108, 117, 101, 32, 50, 0, 38, 0, 0, 40, 12, 84, 101, 115, 116, 101, 114, 14, 86, 97, 108, 117, 101, 32, 51, 0, 38, 0, 0, 42, 12, 84, 101, 115, 116, 101, 114, 14, 86, 97, 108, 117, 101, 32, 49, 0, 38, 0, 0, 44, 12, 84, 101, 115, 116, 101, 114, 14, 86, 97, 108, 117, 101, 32, 50, 0, 38, 0, 0, 46, 12, 84, 101, 115, 116, 101, 114, 14, 86, 97, 108, 117, 101, 32, 51, 0, 0, 0, 0, 0, 0, 0, 0, 24, 0, 0, 2, 17, 0, 0, 0, 4, 2, 241, 245, 203, 172, 0, 0, 0, 0, 0, 23, 0, 0, 1, 142, 15, 146, 119, 244, 0, 0, 1, 142, 15, 146, 119, 244, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 0, 0, 0, 24, 38, 0, 0, 0, 12, 84, 101, 115, 116, 101, 114, 14, 86, 97, 108, 117, 101, 32, 49, 0, 38, 0, 0, 2, 12, 84, 101, 115, 116, 101, 114, 14, 86, 97, 108, 117, 101, 32, 50, 0, 38, 0, 0, 4, 12, 84, 101, 115, 116, 101, 114, 14, 86, 97, 108, 117, 101, 32, 51, 0, 38, 0, 0, 6, 12, 84, 101, 115, 116, 101, 114, 14, 86, 97, 108, 117, 101, 32, 49, 0, 38, 0, 0, 8, 12, 84, 101, 115, 116, 101, 114, 14, 86, 97, 108, 117, 101, 32, 50, 0, 38, 0, 0, 10, 12, 84, 101, 115, 116, 101, 114, 14, 86, 97, 108, 117, 101, 32, 51, 0, 38, 0, 0, 12, 12, 84, 101, 115, 116, 101, 114, 14, 86, 97, 108, 117, 101, 32, 49, 0, 38, 0, 0, 14, 12, 84, 101, 115, 116, 101, 114, 14, 86, 97, 108, 117, 101, 32, 50, 0, 38, 0, 0, 16, 12, 84, 101, 115, 116, 101, 114, 14, 86, 97, 108, 117, 101, 32, 51, 0, 38, 0, 0, 18, 12, 84, 101, 115, 116, 101, 114, 14, 86, 97, 108, 117, 101, 32, 49, 0, 38, 0, 0, 20, 12, 84, 101, 115, 116, 101, 114, 14, 86, 97, 108, 117, 101, 32, 50, 0, 38, 0, 0, 22, 12, 84, 101, 115, 116, 101, 114, 14, 86, 97, 108, 117, 101, 32, 51, 0, 38, 0, 0, 24, 12, 84, 101, 115, 116, 101, 114, 14, 86, 97, 108, 117, 101, 32, 49, 0, 38, 0, 0, 26, 12, 84, 101, 115, 116, 101, 114, 14, 86, 97, 108, 117, 101, 32, 50, 0, 38, 0, 0, 28, 12, 84, 101, 115, 116, 101, 114, 14, 86, 97, 108, 117, 101, 32, 51, 0, 38, 0, 0, 30, 12, 84, 101, 115, 116, 101, 114, 14, 86, 97, 108, 117, 101, 32, 49, 0, 38, 0, 0, 32, 12, 84, 101, 115, 116, 101, 114, 14, 86, 97, 108, 117, 101, 32, 50, 0, 38, 0, 0, 34, 12, 84, 101, 115, 116, 101, 114, 14, 86, 97, 108, 117, 101, 32, 51, 0, 38, 0, 0, 36, 12, 84, 101, 115, 116, 101, 114, 14, 86, 97, 108, 117, 101, 32, 49, 0, 38, 0, 0, 38, 12, 84, 101, 115, 116, 101, 114, 14, 86, 97, 108, 117, 101, 32, 50, 0, 38, 0, 0, 40, 12, 84, 101, 115, 116, 101, 114, 14, 86, 97, 108, 117, 101, 32, 51, 0, 38, 0, 0, 42, 12, 84, 101, 115, 116, 101, 114, 14, 86, 97, 108, 117, 101, 32, 49, 0, 38, 0, 0, 44, 12, 84, 101, 115, 116, 101, 114, 14, 86, 97, 108, 117, 101, 32, 50, 0, 38, 0, 0, 46, 12, 84, 101, 115, 116, 101, 114, 14, 86, 97, 108, 117, 101, 32, 51, 0]
FetchResponse { header_response: HeaderResponse { correlation_id: 1 }, trottle_time: 0, topics: [Topic { name: b"purchases", partitions: [Partition { id: 0, error_code: None, high_water_mark: 227025, last_stable_offset: 227025, aborted_transactions: [], record_batch: [RecordBatch { base_offset: 0, batch_length: 529, partition_leader_epoch: 4, magic: 2, crc: -542227420, attributes: 0, last_offset_delta: 23, base_timestamp: 1709658240993, max_timestamp: 1709658240993, producer_id: -1, producer_epoch: -1, base_sequence: -1, records: [Record { length: 38, attributes: 0, timestamp_delta: 0, offset_delta: 0, key_length: 12, key: b"Tester", value_len: 14, value: b"Value 1", headers: [] }, Record { length: 38, attributes: 0, timestamp_delta: 0, offset_delta: 2, key_length: 12, key: b"Tester", value_len: 14, value: b"Value 2", headers: [] }, Record { length: 38, attributes: 0, timestamp_delta: 0, offset_delta: 4, key_length: 12, key: b"Tester", value_len: 14, value: b"Value 3", headers: [] }, Record { length: 38, attributes: 0, timestamp_delta: 0, offset_delta: 6, key_length: 12, key: b"Tester", value_len: 14, value: b"Value 1", headers: [] }, Record { length: 38, attributes: 0, timestamp_delta: 0, offset_delta: 8, key_length: 12, key: b"Tester", value_len: 14, value: b"Value 2", headers: [] }, Record { length: 38, attributes: 0, timestamp_delta: 0, offset_delta: 10, key_length: 12, key: b"Tester", value_len: 14, value: b"Value 3", headers: [] }, Record { length: 38, attributes: 0, timestamp_delta: 0, offset_delta: 12, key_length: 12, key: b"Tester", value_len: 14, value: b"Value 1", headers: [] }, Record { length: 38, attributes: 0, timestamp_delta: 0, offset_delta: 14, key_length: 12, key: b"Tester", value_len: 14, value: b"Value 2", headers: [] }, Record { length: 38, attributes: 0, timestamp_delta: 0, offset_delta: 16, key_length: 12, key: b"Tester", value_len: 14, value: b"Value 3", headers: [] }, Record { length: 38, attributes: 0, timestamp_delta: 0, offset_delta: 18, key_length: 12, key: b"Tester", value_len: 14, value: b"Value 1", headers: [] }, Record { length: 38, attributes: 0, timestamp_delta: 0, offset_delta: 20, key_length: 12, key: b"Tester", value_len: 14, value: b"Value 2", headers: [] }, Record { length: 38, attributes: 0, timestamp_delta: 0, offset_delta: 22, key_length: 12, key: b"Tester", value_len: 14, value: b"Value 3", headers: [] }, Record { length: 38, attributes: 0, timestamp_delta: 0, offset_delta: 24, key_length: 12, key: b"Tester", value_len: 14, value: b"Value 1", headers: [] }, Record { length: 38, attributes: 0, timestamp_delta: 0, offset_delta: 26, key_length: 12, key: b"Tester", value_len: 14, value: b"Value 2", headers: [] }, Record { length: 38, attributes: 0, timestamp_delta: 0, offset_delta: 28, key_length: 12, key: b"Tester", value_len: 14, value: b"Value 3", headers: [] }, Record { length: 38, attributes: 0, timestamp_delta: 0, offset_delta: 30, key_length: 12, key: b"Tester", value_len: 14, value: b"Value 1", headers: [] }, Record { length: 38, attributes: 0, timestamp_delta: 0, offset_delta: 32, key_length: 12, key: b"Tester", value_len: 14, value: b"Value 2", headers: [] }, Record { length: 38, attributes: 0, timestamp_delta: 0, offset_delta: 34, key_length: 12, key: b"Tester", value_len: 14, value: b"Value 3", headers: [] }, Record { length: 38, attributes: 0, timestamp_delta: 0, offset_delta: 36, key_length: 12, key: b"Tester", value_len: 14, value: b"Value 1", headers: [] }, Record { length: 38, attributes: 0, timestamp_delta: 0, offset_delta: 38, key_length: 12, key: b"Tester", value_len: 14, value: b"Value 2", headers: [] }, Record { length: 38, attributes: 0, timestamp_delta: 0, offset_delta: 40, key_length: 12, key: b"Tester", value_len: 14, value: b"Value 3", headers: [] }, Record { length: 38, attributes: 0, timestamp_delta: 0, offset_delta: 42, key_length: 12, key: b"Tester", value_len: 14, value: b"Value 1", headers: [] }, Record { length: 38, attributes: 0, timestamp_delta: 0, offset_delta: 44, key_length: 12, key: b"Tester", value_len: 14, value: b"Value 2", headers: [] }, Record { length: 38, attributes: 0, timestamp_delta: 0, offset_delta: 46, key_length: 12, key: b"Tester", value_len: 14, value: b"Value 3", headers: [] }] }, RecordBatch { base_offset: 24, batch_length: 529, partition_leader_epoch: 4, magic: 2, crc: -235549780, attributes: 0, last_offset_delta: 23, base_timestamp: 1709658241012, max_timestamp: 1709658241012, producer_id: -1, producer_epoch: -1, base_sequence: -1, records: [Record { length: 38, attributes: 0, timestamp_delta: 0, offset_delta: 0, key_length: 12, key: b"Tester", value_len: 14, value: b"Value 1", headers: [] }, Record { length: 38, attributes: 0, timestamp_delta: 0, offset_delta: 2, key_length: 12, key: b"Tester", value_len: 14, value: b"Value 2", headers: [] }, Record { length: 38, attributes: 0, timestamp_delta: 0, offset_delta: 4, key_length: 12, key: b"Tester", value_len: 14, value: b"Value 3", headers: [] }, Record { length: 38, attributes: 0, timestamp_delta: 0, offset_delta: 6, key_length: 12, key: b"Tester", value_len: 14, value: b"Value 1", headers: [] }, Record { length: 38, attributes: 0, timestamp_delta: 0, offset_delta: 8, key_length: 12, key: b"Tester", value_len: 14, value: b"Value 2", headers: [] }, Record { length: 38, attributes: 0, timestamp_delta: 0, offset_delta: 10, key_length: 12, key: b"Tester", value_len: 14, value: b"Value 3", headers: [] }, Record { length: 38, attributes: 0, timestamp_delta: 0, offset_delta: 12, key_length: 12, key: b"Tester", value_len: 14, value: b"Value 1", headers: [] }, Record { length: 38, attributes: 0, timestamp_delta: 0, offset_delta: 14, key_length: 12, key: b"Tester", value_len: 14, value: b"Value 2", headers: [] }, Record { length: 38, attributes: 0, timestamp_delta: 0, offset_delta: 16, key_length: 12, key: b"Tester", value_len: 14, value: b"Value 3", headers: [] }, Record { length: 38, attributes: 0, timestamp_delta: 0, offset_delta: 18, key_length: 12, key: b"Tester", value_len: 14, value: b"Value 1", headers: [] }, Record { length: 38, attributes: 0, timestamp_delta: 0, offset_delta: 20, key_length: 12, key: b"Tester", value_len: 14, value: b"Value 2", headers: [] }, Record { length: 38, attributes: 0, timestamp_delta: 0, offset_delta: 22, key_length: 12, key: b"Tester", value_len: 14, value: b"Value 3", headers: [] }, Record { length: 38, attributes: 0, timestamp_delta: 0, offset_delta: 24, key_length: 12, key: b"Tester", value_len: 14, value: b"Value 1", headers: [] }, Record { length: 38, attributes: 0, timestamp_delta: 0, offset_delta: 26, key_length: 12, key: b"Tester", value_len: 14, value: b"Value 2", headers: [] }, Record { length: 38, attributes: 0, timestamp_delta: 0, offset_delta: 28, key_length: 12, key: b"Tester", value_len: 14, value: b"Value 3", headers: [] }, Record { length: 38, attributes: 0, timestamp_delta: 0, offset_delta: 30, key_length: 12, key: b"Tester", value_len: 14, value: b"Value 1", headers: [] }, Record { length: 38, attributes: 0, timestamp_delta: 0, offset_delta: 32, key_length: 12, key: b"Tester", value_len: 14, value: b"Value 2", headers: [] }, Record { length: 38, attributes: 0, timestamp_delta: 0, offset_delta: 34, key_length: 12, key: b"Tester", value_len: 14, value: b"Value 3", headers: [] }, Record { length: 38, attributes: 0, timestamp_delta: 0, offset_delta: 36, key_length: 12, key: b"Tester", value_len: 14, value: b"Value 1", headers: [] }, Record { length: 38, attributes: 0, timestamp_delta: 0, offset_delta: 38, key_length: 12, key: b"Tester", value_len: 14, value: b"Value 2", headers: [] }, Record { length: 38, attributes: 0, timestamp_delta: 0, offset_delta: 40, key_length: 12, key: b"Tester", value_len: 14, value: b"Value 3", headers: [] }, Record { length: 38, attributes: 0, timestamp_delta: 0, offset_delta: 42, key_length: 12, key: b"Tester", value_len: 14, value: b"Value 1", headers: [] }, Record { length: 38, attributes: 0, timestamp_delta: 0, offset_delta: 44, key_length: 12, key: b"Tester", value_len: 14, value: b"Value 2", headers: [] }, Record { length: 38, attributes: 0, timestamp_delta: 0, offset_delta: 46, key_length: 12, key: b"Tester", value_len: 14, value: b"Value 3", headers: [] }] }] }] }] }
*/