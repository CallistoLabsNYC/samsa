use samsa::prelude::{create_topics, BrokerConnection, Error};
use std::{collections::HashMap, env};

const KAFKA_BROKERS: &str = "KAFKA_BROKERS";
#[allow(dead_code)]
const KAFKA_BROKER_URLS: &str = "KAFKA_BROKER_URLS";
#[allow(dead_code)]
const KAFKA_TOPIC: &str = "KAFKA_TOPIC";

#[allow(dead_code)]
pub async fn ensure_topic_creation(
    conn: &BrokerConnection,
    topic: &str,
    correlation_id: i32,
    client_id: &str,
) -> Result<(), Error> {
    let create_topic_response = create_topics(
        conn.clone(),
        correlation_id,
        client_id,
        HashMap::from([(topic, 1)]),
    )
    .await?;

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

#[allow(dead_code)]
pub fn get_broker_urls() -> Result<(bool, Vec<String>), Error> {
    let urls: Vec<String> = match env::var(KAFKA_BROKER_URLS) {
        Ok(brokers) => brokers.split(',').map(str::to_string).collect(),
        Err(_) => {
            tracing::warn!("Skipping test because no {} is set", KAFKA_BROKER_URLS);
            return Ok((true, vec![]));
        }
    };
    Ok((false, urls))
}

#[allow(dead_code)]
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
