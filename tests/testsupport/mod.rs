use samsa::prelude::{BrokerConnection, Error};
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
