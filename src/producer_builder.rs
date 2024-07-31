use std::fmt::Debug;
use std::time::Duration;

use tokio::sync::mpsc::{channel, unbounded_channel, Receiver, UnboundedSender};
use tokio_stream::{Stream, StreamExt};

use crate::network::BrokerConnection;
use crate::prelude::Compression;
use crate::producer::{flush_producer, ProduceMessage, ProduceParams, Producer};
use crate::protocol::produce::request::Attributes;
use crate::protocol::ProduceResponse;
use crate::DEFAULT_CORRELATION_ID;
use crate::{error::Result, metadata::ClusterMetadata, DEFAULT_CLIENT_ID};

const DEFAULT_MAX_BATCH_SIZE: usize = 100;
const DEFAULT_BATCH_TIMEOUT_MS: u64 = 1000;

/// Configure a [`Producer`].
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
#[derive(Clone)]
pub struct ProducerBuilder<T: BrokerConnection> {
    cluster_metadata: ClusterMetadata<T>,
    produce_params: ProduceParams,
    max_batch_size: usize,
    batch_timeout_ms: u64,
    attributes: Attributes,
}

impl<'a, T> ProducerBuilder<T>
where
    T: BrokerConnection + Clone + Debug + Send + Sync + 'static,
{
    /// Start a producer builder. To complete, use the [`build`](Self::build) method.
    pub async fn new(connection_params: T::ConnConfig, topics: Vec<String>) -> Result<Self> {
        let cluster_metadata = ClusterMetadata::new(
            connection_params,
            DEFAULT_CORRELATION_ID,
            DEFAULT_CLIENT_ID.to_owned(),
            topics,
        )
        .await?;

        Ok(Self {
            cluster_metadata,
            produce_params: ProduceParams::new(),
            max_batch_size: DEFAULT_MAX_BATCH_SIZE,
            batch_timeout_ms: DEFAULT_BATCH_TIMEOUT_MS,
            attributes: Attributes::new(None),
        })
    }

    /// The max number of messages that will sit in queue to be produced.
    ///
    /// When the queue size surpasses this number, the queue will be flushed and
    /// all records produced. Unless the [`batch_timeout_ms`](Self::batch_timeout_ms) has passed, then the
    /// queue will be flushed regardless of its size.
    ///
    /// Increasing this number will increase latency, but also increase throughput.
    pub fn max_batch_size(&mut self, max_batch_size: usize) -> &mut Self {
        self.max_batch_size = max_batch_size;
        self
    }

    /// The maximum time a message will sit in the queue to be produced.
    ///
    /// Each batch will wait a maximum of this time, and then be flushed.
    /// If the batch fills up with [`max_batch_size`](Self::max_batch_size) then it will be flushed
    /// before this time runs out.
    ///
    /// Decreasing this number will lower latency, but also lower throughput.
    pub fn batch_timeout_ms(&mut self, batch_timeout_ms: u64) -> &mut Self {
        self.batch_timeout_ms = batch_timeout_ms;
        self
    }

    pub fn correlation_id(&mut self, correlation_id: i32) -> &mut Self {
        self.produce_params.correlation_id = correlation_id;
        self
    }

    pub fn client_id(mut self, client_id: String) -> Self {
        self.produce_params.client_id = client_id;
        self
    }

    /// The number of acknowledgments the producer requires the leader to have received before considering a request complete. Allowed values: 0 for no acknowledgments, 1 for only the leader and -1 for the full ISR.
    pub fn required_acks(&mut self, required_acks: i16) -> &mut Self {
        self.produce_params.required_acks = required_acks;
        self
    }

    /// The timeout to await a response in milliseconds.
    pub fn timeout_ms(&mut self, timeout_ms: i32) -> &mut Self {
        self.produce_params.timeout_ms = timeout_ms;
        self
    }

    pub fn compression(&mut self, algo: Compression) -> &mut Self {
        self.attributes.compression = Some(algo);
        self
    }

    pub async fn build(self) -> Producer {
        let (input_sender, input_receiver) = channel(self.max_batch_size);
        // unbounded because you don't want to force the reading.
        let (output_sender, output_receiver) = unbounded_channel();

        // we should make this chunks timeout optional
        let produce_stream = into_produce_stream(input_receiver).chunks_timeout(
            self.max_batch_size,
            Duration::from_millis(self.batch_timeout_ms),
        );

        tokio::spawn(producer(
            produce_stream,
            output_sender,
            self.cluster_metadata,
            self.produce_params,
            self.attributes,
        ));

        Producer {
            sender: input_sender,
            receiver: output_receiver,
        }
    }

    pub async fn build_from_stream(
        self,
        stream: impl Stream<Item = Vec<ProduceMessage>> + std::marker::Send + 'static,
    ) -> impl Stream<Item = Vec<Option<ProduceResponse>>> {
        // unbounded because you don't want to force the reading.
        let (output_sender, mut output_receiver) = unbounded_channel();

        tokio::spawn(producer(
            stream,
            output_sender,
            self.cluster_metadata,
            self.produce_params,
            self.attributes,
        ));

        async_stream::stream! {
            while let Some(message) = output_receiver.recv().await {
                yield message;
            }
        }
    }
}

fn into_produce_stream(
    mut receiver: Receiver<ProduceMessage>,
) -> impl Stream<Item = ProduceMessage> {
    async_stream::stream! {
        while let Some(message) = receiver.recv().await {
            yield message;
        }
    }
}

async fn producer<T: BrokerConnection + Clone + Debug + Send + 'static>(
    stream: impl Stream<Item = Vec<ProduceMessage>> + Send + 'static,
    output_sender: UnboundedSender<Vec<Option<ProduceResponse>>>,
    cluster_metadata: ClusterMetadata<T>,
    produce_params: ProduceParams,
    attributes: Attributes,
) {
    tokio::pin!(stream);
    while let Some(messages) = stream.next().await {
        match flush_producer(
            &cluster_metadata,
            &produce_params,
            messages,
            attributes.clone(),
        )
        .await
        {
            Err(err) => {
                tracing::error!("Error in producer agent {:?}", err);
            }
            Ok(r) => {
                if let Err(err) = output_sender.send(r) {
                    tracing::error!("Error sending results from producer agent {:?}", err);
                }
            }
        }
    }
}
