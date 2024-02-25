//! Client that sends records to a cluster.
//!
//! # Producer Module
//!
//! We provide a Producer struct that takes care of the inner details
//! relating to all things Kafka.
//!
//! We also provide low level methods that allow users to interface with
//!  the Kafka API directly if they so choose. For those looking to get
//! their hands even dirtier and handle the specific requests and
//! responses that make up the Kafka protocol, visit the [protocol module].
//!
//! ## Producer
//! The [`Producer`] struct is useful for easily sending messages to brokers.
//! The producer is represented as a background worker containing a queue of messages to be
//! sent upon meeting either of two conditions:
//! - The maximum number of messages is filled
//! - The wait time has ran out
//! When either of these two are met, the record queue is flushed and sent to the appropriate
//! brokers.
//!
//! To produce, simply provide the initial bootstrap broker and the working topics
//! to the [`ProducerBuilder`]. This you can use to configure the producing parameters as
//! needed.
//! ### Example
//! ```rust
//! let producer_client = producer::ProducerBuilder::new(bootstrap_url, topics)
//!     .await?
//!     .build()
//!     .await;
//!
//! producer_client
//!     .produce(ProduceMessage {
//!         topic: topic_name,
//!         partition_id,
//!         key: Some(Bytes::from_static(b"Tester")),
//!         value: Some(Bytes::from_static(b"Value 1")),
//!     })
//!     .await;
//! ```
//!
//! ## Protocol functions
//! We provide a set of protocol primitives for users to build their own clients.
//! They are presented as the building blocks that we use to build the higher level
//! abstractions.
//!
//! ### Produce
//! [`produce`] sends messages to a broker.
//! #### Example
//! ```rust
//! produce(
//!     broker_conn,
//!     correlation_id,
//!     client_id,
//!     required_acks,
//!     timeout_ms,
//!     messages,
//! ).await?;
//! ```
//! [protocol module]: protocol
use std::collections::HashMap;

use bytes::Bytes;
use tokio::sync::mpsc::Sender;
use tracing::instrument;

use crate::{
    error::{Error, Result},
    metadata::ClusterMetadata,
    network::BrokerConnection,
    protocol::{ProduceRequest, ProduceResponse},
    DEFAULT_CLIENT_ID, DEFAULT_CORRELATION_ID,
};

const DEFAULT_REQUIRED_ACKS: i16 = 0;
const DEFAULT_TIMEOUT_MS: i32 = 1000;

#[derive(Clone)]
pub(crate) struct ProduceParams {
    pub correlation_id: i32,
    pub client_id: String,
    pub required_acks: i16,
    pub timeout_ms: i32,
}

impl ProduceParams {
    pub fn new() -> Self {
        Self {
            correlation_id: DEFAULT_CORRELATION_ID,
            client_id: DEFAULT_CLIENT_ID.to_owned(),
            required_acks: DEFAULT_REQUIRED_ACKS,
            timeout_ms: DEFAULT_TIMEOUT_MS,
        }
    }
}

/// Kafka/Redpanda Producer.
///
/// This struct is a broker to a background worker that
/// does the actual producing. The background worker's job is to
/// collect incoming messages in a queue. When the queue fills up,
/// the messages are flushed. If the queue takes longer than a given
/// time to fill up, the messages are flushed. These two configurable
/// parameters found in the [`ProducerBuilder`] help dial in latency and throughput.
///
/// ### Example
/// ```rust
/// let producer_client = producer::ProducerBuilder::new(bootstrap_url, topics)
///     .await?
///     .build()
///     .await;
///
/// producer_client
///     .produce(ProduceMessage {
///         topic: topic_name,
///         partition_id,
///         key: Some(Bytes::from_static(b"Tester")),
///         value: Some(Bytes::from_static(b"Value 1")),
///     })
///     .await;
/// ```
pub struct Producer {
    /// Direct connection to the background worker.
    pub sender: Sender<ProduceMessage>,
}

pub struct ProducerSink;

/// Common produce message format.
#[derive(Clone)]
pub struct ProduceMessage {
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
    pub topic: String,
    pub partition_id: i32,
}

impl Producer {
    pub async fn produce(&self, message: ProduceMessage) {
        if self.sender.send(message).await.is_err() {
            tracing::warn!("Producer has hung up channel");
        }
    }
}

#[instrument(skip(messages, produce_params, cluster_metadata))]
pub(crate) async fn flush_producer(
    cluster_metadata: &ClusterMetadata,
    produce_params: &ProduceParams,
    messages: Vec<ProduceMessage>,
) -> Result<()> {
    let mut brokers_and_messages = HashMap::new();
    tracing::info!("Producing {} messages", messages.len());
    for message in messages {
        let broker_id = cluster_metadata
            .get_leader_for_topic_partition(&message.topic, message.partition_id)
            .ok_or(Error::NoLeaderForTopicPartition(
                message.topic.clone(),
                message.partition_id,
            ))?;

        match brokers_and_messages.get_mut(&broker_id) {
            None => {
                brokers_and_messages.insert(
                    broker_id,
                    vec![ProduceMessage {
                        key: message.key,
                        value: message.value,
                        topic: message.topic,
                        partition_id: message.partition_id,
                    }],
                );
            }
            Some(messages) => messages.push(message),
        };
    }

    for (broker, messages) in brokers_and_messages.iter() {
        let broker_conn = cluster_metadata.broker_connections.get(broker).unwrap();
        produce(
            broker_conn.to_owned(),
            produce_params.correlation_id,
            &produce_params.client_id,
            produce_params.required_acks,
            produce_params.timeout_ms,
            messages,
        )
        .await?;
    }

    Ok(())
}

/// Produce messages to a broker.
///
/// See this [protocol spec] for more information.
///
/// [protocol spec]: protocol::produce
pub async fn produce(
    broker_conn: BrokerConnection,
    correlation_id: i32,
    client_id: &str,
    required_acks: i16,
    timeout_ms: i32,
    messages: &Vec<ProduceMessage>,
) -> Result<Option<ProduceResponse>> {
    tracing::debug!("Producing {} messages", messages.len());

    let mut produce_request =
        ProduceRequest::new(required_acks, timeout_ms, correlation_id, client_id);

    for message in messages {
        produce_request.add(
            &message.topic,
            message.partition_id,
            message.key.clone(),
            message.value.clone(),
        );
    }

    broker_conn.send_request(&produce_request).await?;
    if required_acks > 0 {
        let response = ProduceResponse::try_from(broker_conn.receive_response().await?.freeze())?;
        Ok(Some(response))
    } else {
        Ok(None)
    }
}
