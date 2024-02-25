use std::time::Duration;

use tokio::sync::broadcast;
use tokio::sync::mpsc::channel;
use tokio::sync::mpsc::Receiver;
use tokio_stream::{Stream, StreamExt};

use crate::producer::{flush_producer, ProduceMessage, ProduceParams, Producer, ProducerSink};
use crate::{error::Result, metadata::ClusterMetadata, DEFAULT_CLIENT_ID};

const DEFAULT_MAX_BATCH_SIZE: usize = 100;
const DEFAULT_BATCH_TIMEOUT_MS: u64 = 1000;

/// Configure a [`Producer`].
#[derive(Clone)]
pub struct ProducerBuilder {
    cluster_metadata: ClusterMetadata,
    produce_params: ProduceParams,
    max_batch_size: usize,
    batch_timeout_ms: u64,
}

impl<'a> ProducerBuilder {
    /// Start a producer builder. To complete, use the [`build`] method.
    pub async fn new(bootstrap_addrs: Vec<String>, topics: Vec<String>) -> Result<Self> {
        let cluster_metadata =
            ClusterMetadata::new(bootstrap_addrs, DEFAULT_CLIENT_ID.to_owned(), topics).await?;

        Ok(Self {
            cluster_metadata,
            produce_params: ProduceParams::new(),
            max_batch_size: DEFAULT_MAX_BATCH_SIZE,
            batch_timeout_ms: DEFAULT_BATCH_TIMEOUT_MS,
        })
    }

    /// The max number of messages that will sit in queue to be produced.
    ///
    /// When the queue size surpasses this number, the queue will be flushed and
    /// all records produced. Unless the [`batch_timeout_ms`] has passed, then the
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
    /// If the batch fills up with [`max_batch_size`] then it will be flushed
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

    pub async fn build(self) -> Producer {
        let (sender, receiver) = channel(self.max_batch_size);

        let produce_stream = into_produce_stream(receiver).chunks_timeout(
            self.max_batch_size,
            Duration::from_millis(self.batch_timeout_ms),
        );

        tokio::spawn(async move {
            tokio::pin!(produce_stream);
            while let Some(messages) = produce_stream.next().await {
                if let Err(err) =
                    flush_producer(&self.cluster_metadata, &self.produce_params, messages).await
                {
                    tracing::error!("Error in producer agent {:?}", err);
                }
            }
        });

        Producer { sender }
    }

    pub async fn build_from_channel(
        self,
        receiver: broadcast::Receiver<ProduceMessage>,
    ) -> ProducerSink {
        let produce_stream = into_produce_stream_broadcast(receiver).chunks_timeout(
            self.max_batch_size,
            Duration::from_millis(self.batch_timeout_ms),
        );

        tokio::spawn(async move {
            tokio::pin!(produce_stream);
            while let Some(messages) = produce_stream.next().await {
                if let Err(err) =
                    flush_producer(&self.cluster_metadata, &self.produce_params, messages).await
                {
                    tracing::error!("Error in producer agent {:?}", err);
                }
            }
        });

        ProducerSink
    }

    pub async fn build_from_stream(
        self,
        stream: impl Stream<Item = ProduceMessage> + std::marker::Send + 'static,
    ) -> ProducerSink {
        tokio::spawn(async move {
            let produce_stream = stream.chunks_timeout(
                self.max_batch_size,
                Duration::from_millis(self.batch_timeout_ms),
            );
            tokio::pin!(produce_stream);
            while let Some(messages) = produce_stream.next().await {
                if let Err(err) =
                    flush_producer(&self.cluster_metadata, &self.produce_params, messages).await
                {
                    tracing::error!("Error in producer agent {:?}", err);
                }
            }
        });

        ProducerSink
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

fn into_produce_stream_broadcast(
    mut receiver: broadcast::Receiver<ProduceMessage>,
) -> impl Stream<Item = ProduceMessage> {
    async_stream::stream! {
        while let Ok(message) = receiver.recv().await {
            yield message;
        }
    }
}
