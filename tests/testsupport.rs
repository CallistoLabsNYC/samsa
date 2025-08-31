use samsa::prelude::{create_topics, BrokerAddress, BrokerConnection, Error};
use std::panic::Location;
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

pub fn get_brokers() -> Result<(bool, Vec<BrokerAddress>), Error> {
    let brokers = match env::var(KAFKA_BROKERS) {
        Ok(brokers) => brokers
            .split(',')
            .map(|addr| {
                let addr = str::to_string(addr);
                let strings: Vec<&str> = addr.split(':').collect();

                if strings.len() > 2 {
                    panic!("The broker connection is not well formed");
                }

                let host = strings[0].to_owned();
                let port: u16 = strings[1].parse().unwrap();

                BrokerAddress { host, port }
            })
            .collect(),
        Err(_) => {
            tracing::warn!("Skipping test because no {} is set", KAFKA_BROKERS);
            return Ok((true, vec![]));
        }
    };
    Ok((false, brokers))
}

#[allow(dead_code)]
#[track_caller]
pub fn get_brokers_and_topic() -> Result<(bool, Vec<BrokerAddress>, String), Error> {
    let caller_path = Location::caller().file();

    let (skip, brokers) = get_brokers()?;
    if skip {
        return Ok((skip, vec![], "".to_string()));
    }
    let (skip, topic) = get_topic(caller_path)?;
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
pub fn get_topic(caller_path: &str) -> Result<(bool, String), Error> {
    let topic = match create_topic_from_file_path(caller_path) {
        Ok(topic) => topic,
        Err(_) => {
            tracing::warn!("Skipping test because no {} is set", KAFKA_TOPIC);
            return Ok((true, "".to_string()));
        }
    };
    Ok((false, topic))
}

#[allow(dead_code)]
pub fn get_topic_2(caller_path: &str) -> Result<(bool, String), Error> {
    let topic = match create_topic_from_file_path(caller_path) {
        Ok(topic) => format!("{topic}-2"),
        Err(_) => {
            tracing::warn!("Skipping test because no {} is set", KAFKA_TOPIC_2);
            return Ok((true, "".to_string()));
        }
    };
    Ok((false, topic))
}

#[allow(dead_code)]
pub fn create_topic_from_file_path(caller_path: &str) -> Result<String, Error> {
    let file_name = match std::path::Path::new(caller_path)
        .file_stem() // using `file_stem` to remove any extension
        .and_then(std::ffi::OsStr::to_str)
    {
        Some(topic) => topic,
        None => {
            tracing::warn!("Skipping test because could not read file directory");
            return Ok("".to_string());
        }
    };

    Ok(format!("{file_name}-integration"))
}
