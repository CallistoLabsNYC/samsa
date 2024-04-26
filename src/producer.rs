//! Client that sends records to a cluster.

use std::collections::HashMap;

use bytes::Bytes;
use tokio::sync::mpsc::{Sender, UnboundedReceiver};
use tracing::instrument;

use crate::{
    error::{Error, Result},
    metadata::ClusterMetadata,
    network::tcp::TcpBrokerConnection,
    protocol::{Header, ProduceRequest, ProduceResponse},
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
/// parameters found in the [`ProducerBuilder`](crate::prelude::ProducerBuilder) help dial in latency and throughput.
///
/// ### Example
/// ```rust
/// let bootstrap_addrs = vec!["127.0.0.1:9092".to_string()];
/// let topic_name = "my-topic";
/// let partition_id = 0;
///
/// let message = samsa::prelude::ProduceMessage {
///         topic: topic_name.to_string(),
///         partition_id,
///         key: Some(bytes::Bytes::from_static(b"Tester")),
///         value: Some(bytes::Bytes::from_static(b"Value")),
///         headers: vec![String::from("Key"), bytes::Bytes::from("Value")]
///     };
///
/// let producer_client = samsa::prelude::ProducerBuilder::new(bootstrap_addrs, vec![topic_name.to_string()])
///     .await?
///     .batch_timeout_ms(1)
///     .max_batch_size(2)
///     .clone()
///     .build()
///     .await;
///
/// producer_client
///     .produce(message)
///     .await;
/// ```
pub struct Producer {
    /// Direct connection to the background worker.
    pub sender: Sender<ProduceMessage>,
    /// Responses of the
    pub receiver: UnboundedReceiver<Vec<Option<ProduceResponse>>>,
}

/// Common produce message format.
#[derive(Clone)]
pub struct ProduceMessage {
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
    pub headers: Vec<Header>,
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

// vector for the results from each broker
#[instrument(skip(messages, produce_params, cluster_metadata))]
pub(crate) async fn flush_producer(
    cluster_metadata: ClusterMetadata,
    produce_params: &ProduceParams,
    messages: Vec<ProduceMessage>,
) -> Result<Vec<Option<ProduceResponse>>> {
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
                brokers_and_messages.insert(broker_id, vec![message]);
            }
            Some(messages) => messages.push(message),
        };
    }

    let mut responses = vec![];
    for (broker, messages) in brokers_and_messages.into_iter() {
        let broker_conn = cluster_metadata
            .broker_connections
            .get(&broker)
            .ok_or(Error::NoConnectionForBroker(broker))?
            .clone();
        let p = produce_params.clone();
        let conn = broker_conn.lock().await;
        let res = produce(
            &conn,
            p.correlation_id,
            &p.client_id,
            p.required_acks,
            p.timeout_ms,
            &messages,
        )
        .await?;
        responses.push(res);
    }

    Ok(responses)
}

/// Produce messages to a broker.
///
/// See this [protocol spec](crate::prelude::protocol::produce) for more information.
pub async fn produce(
    broker_conn: &TcpBrokerConnection,
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
            message.headers.clone(),
        );
    }

    broker_conn.send_request_(&produce_request).await?;
    if required_acks > 0 {
        let response = ProduceResponse::try_from(broker_conn.receive_response_().await?.freeze())?;
        Ok(Some(response))
    } else {
        Ok(None)
    }
}
