use samsa::prelude::{create_topics, BrokerConnection, Error};
use std::{collections::HashMap, env};

const KAFKA_BROKERS: &str = "KAFKA_BROKERS";
#[allow(dead_code)]
const REDPANDA_ADMIN_URLS: &str = "REDPANDA_ADMIN_URLS";
#[allow(dead_code)]
const KAFKA_TOPIC: &str = "KAFKA_TOPIC";
#[allow(dead_code)]
const KAFKA_TOPIC_2: &str = "KAFKA_TOPIC_2";

#[allow(dead_code)]
pub async fn ensure_topic_creation(
    conn: impl BrokerConnection,
    topic: &str,
    correlation_id: i32,
    client_id: &str,
) -> Result<(), Error> {
    create_topics(conn, correlation_id, client_id, HashMap::from([(topic, 1)])).await?;

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
pub fn get_brokers_and_topic() -> Result<(bool, Vec<String>, String), Error> {
    let (skip, brokers) = get_brokers()?;
    if skip {
        return Ok((skip, vec![], "".to_string()));
    }
    let (skip, topic) = get_topic()?;
    if skip {
        return Ok((skip, vec![], "".to_string()));
    }
    Ok((false, brokers, topic))
}

#[allow(dead_code)]
pub fn get_redpanda_admin_urls() -> Result<(bool, Vec<String>), Error> {
    let urls: Vec<String> = match env::var(REDPANDA_ADMIN_URLS) {
        Ok(brokers) => brokers.split(',').map(str::to_string).collect(),
        Err(_) => {
            tracing::warn!("Skipping test because no {} is set", REDPANDA_ADMIN_URLS);
            return Ok((true, vec![]));
        }
    };
    Ok((false, urls))
}

#[allow(dead_code)]
pub fn get_topic() -> Result<(bool, String), Error> {
    let topic = match env::var(KAFKA_TOPIC) {
        Ok(topic) => topic,
        Err(_) => {
            tracing::warn!("Skipping test because no {} is set", KAFKA_TOPIC);
            return Ok((true, "".to_string()));
        }
    };
    Ok((false, topic))
}

#[allow(dead_code)]
pub fn get_topic_2() -> Result<(bool, String), Error> {
    let topic = match env::var(KAFKA_TOPIC_2) {
        Ok(topic) => topic,
        Err(_) => {
            tracing::warn!("Skipping test because no {} is set", KAFKA_TOPIC_2);
            return Ok((true, "".to_string()));
        }
    };
    Ok((false, topic))
}
