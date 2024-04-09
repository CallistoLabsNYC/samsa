//! Client that consumes records from a cluster.

use std::collections::HashMap;

use async_stream::try_stream;
use bytes::Bytes;
use nom::AsBytes;
use tokio_stream::{Stream, StreamExt};
use tracing::instrument;

use crate::{
    error::{Error, Result},
    metadata::ClusterMetadata,
    network::{self, BrokerConnection},
    protocol, DEFAULT_CLIENT_ID, DEFAULT_CORRELATION_ID,
};

const DEFAULT_MAX_WAIT_MS: i32 = 200;
const DEFAULT_MIN_BYTES: i32 = 100;
const DEFAULT_MAX_BYTES: i32 = 30000;
const DEFAULT_MAX_PARTITION_BYTES: i32 = 20000;
const DEFAULT_ISOLATION_LEVEL: i8 = 0;

/// Common consumed message format.
#[derive(Clone, Debug, PartialEq)]
pub struct ConsumeMessage {
    pub key: Bytes,
    pub value: Bytes,
    pub offset: usize,
    pub timestamp: usize,
    pub topic_name: Bytes,
    pub partition_index: i32,
}

#[derive(Clone, Debug)]
pub struct FetchParams {
    pub correlation_id: i32,
    pub client_id: String,
    pub max_wait_ms: i32,
    pub min_bytes: i32,
    pub max_bytes: i32,
    pub max_partition_bytes: i32,
    pub isolation_level: i8,
}

impl Default for FetchParams {
    fn default() -> Self {
        Self::new()
    }
}

impl FetchParams {
    pub fn new() -> Self {
        Self {
            correlation_id: DEFAULT_CORRELATION_ID,
            client_id: DEFAULT_CLIENT_ID.to_owned(),
            max_wait_ms: DEFAULT_MAX_WAIT_MS,
            min_bytes: DEFAULT_MIN_BYTES,
            max_bytes: DEFAULT_MAX_BYTES,
            max_partition_bytes: DEFAULT_MAX_PARTITION_BYTES,
            isolation_level: DEFAULT_ISOLATION_LEVEL,
        }
    }
}

type TopicPartitionKey = (String, i32);

/// Used to represent topic-partition assignments.
///
/// Consumers need to be assigned to consume from topics and their partitions.
/// The [TopicPartitionsBuilder] is an ease of use type to build these assignments
pub type TopicPartitions = HashMap<String, Vec<i32>>;

/// Build a topic-partition assignment for Consumers.
///
/// # Example
/// ```rust
/// let topic_partitions = TopicPartitionsBuilder::new()
///     .assign("topic1", vec![0,1,2])
///     .assign("topic1", vec![3,4,5])
///     .build();
/// ```
pub struct TopicPartitionsBuilder {
    data: TopicPartitions,
}

impl TopicPartitionsBuilder {
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
        }
    }

    /// Add assignment for a topic and its partitions.
    pub fn assign(mut self, topic: String, partitions: Vec<i32>) -> Self {
        self.data.insert(topic, partitions);

        self
    }

    pub fn build(self) -> TopicPartitions {
        self.data
    }
}

/// Used to represent topic partition offsets.
pub type PartitionOffsets = HashMap<TopicPartitionKey, i64>;

/// Kafka/Redpanda Consumer.
///
/// This structure holds an [`TopicPartitions`] representing the topic partitions to read from.
/// It also has [`PartitionOffsets`] offsets that correspond to the current read-state for the topic-partitions.
///
/// Represented as various types of [`Streams`](https://docs.rs/futures/latest/futures/stream/trait.Stream.html).
/// These can be transformed, aggregated, and composed into newer streams
/// to enable flexible stream processing.
///
/// To consume, simply provide the initial bootstrap broker and the assignments
/// to the [`ConsumerBuilder`](crate::prelude::ConsumerBuilder). This you can use to configure the fetching parameters as
/// needed.
///
/// *Note:* The streams are lazy, so without anything to execute them, they will do nothing.
///
/// ### Example
/// ```rust
/// let bootstrap_addrs = vec!["127.0.0.1:9092".to_string()];
/// let partitions = vec![0];
/// let topic_name = "my-topic";
/// let assignment = samsa::prelude::TopicPartitionsBuilder::new()
///     .assign(topic_name, partitions)
///     .build();
///
/// let consumer = samsa::prelude::ConsumerBuilder::new(
///     bootstrap_addrs,
///     assignment,
/// )
/// .await?
/// .build();
///
/// let stream = consumer.into_stream();
/// // have to pin streams before iterating
/// tokio::pin!(stream);
///
/// // Stream will do nothing unless consumed.
/// while let Some(Ok((batch, offsets))) = stream.next().await {
///     println!("{:?}", batch);
/// }
/// ```
#[derive(Clone, Debug, Default)]
pub struct Consumer {
    /// Keeps track of the brokers and the topic partition info for the cluster.
    pub(crate) cluster_metadata: ClusterMetadata,
    /// Parameters for fetching.
    pub(crate) fetch_params: FetchParams,
    /// Assignment of topic partitions.
    pub(crate) assigned_topic_partitions: TopicPartitions,
    /// Offsets to read from for each assigned topic partition.
    pub(crate) offsets: PartitionOffsets,
}

impl<'a> Consumer {
    #[instrument]
    async fn consume(&self) -> Result<Vec<protocol::FetchResponse>> {
        // TODO: Push this into the metadata
        let brokers_and_their_topic_partitions = self
            .cluster_metadata
            .get_connections_for_topic_partitions(&self.assigned_topic_partitions)?;
        let mut responses = vec![];

        // TODO: Make these all calls run async
        // try this https://docs.rs/tokio/latest/tokio/task/join_set/struct.JoinSet.html#examples
        for (broker_conn, topic_partitions) in brokers_and_their_topic_partitions.into_iter() {
            let response = fetch(
                broker_conn.clone(),
                self.fetch_params.correlation_id,
                &self.fetch_params.client_id,
                self.fetch_params.max_wait_ms,
                self.fetch_params.min_bytes,
                self.fetch_params.max_bytes,
                self.fetch_params.max_partition_bytes,
                self.fetch_params.isolation_level,
                &topic_partitions,
                &self.offsets,
            )
            .await?;

            responses.push(response);
        }

        Ok(responses)
    }

    pub async fn next_batch(&mut self) -> Result<(Vec<ConsumeMessage>, PartitionOffsets)> {
        let responses = self.consume().await?;
        let mut records = vec![];

        // for each group of broker reponses
        for response in responses {
            for topic in response.topics {
                // is this really the best way to do this?
                // is it efficient? maybe need to tweak types
                // Bytes is common so there will be loads of examples somewhere
                let topic_name = std::str::from_utf8(topic.name.as_bytes()).map_err(|err| {
                    tracing::error!("Error converting from UTF8 {:?}", err);
                    Error::DecodingUtf8Error
                })?;

                // this is a sneaky way to use data that we own :)
                let topic_name = self
                    .cluster_metadata
                    .topic_names
                    .iter()
                    .find(|my_topic| **my_topic == topic_name)
                    .unwrap();
                for partition in topic.partitions {
                    // TODO: handle kafka error code here
                    /*
                     * OFFSET_OUT_OF_RANGE (1)
                     * UNKNOWN_TOPIC_OR_PARTITION (3)
                     * NOT_LEADER_FOR_PARTITION (6)
                     * REPLICA_NOT_AVAILABLE (9)
                     * UNKNOWN (-1)
                     */
                    for record_batch in partition.record_batch {
                        let base_offset = record_batch.base_offset;
                        let base_timestamp = record_batch.base_timestamp;

                        for record in record_batch.records {
                            let new_offset = (record.offset_delta / 2) + (base_offset as usize);

                            self.offsets.insert(
                                (topic_name.to_owned(), partition.id),
                                record.offset_delta as i64 + base_offset + 1,
                            );

                            records.push(ConsumeMessage {
                                key: record.key,
                                value: record.value,
                                offset: new_offset,
                                timestamp: base_timestamp as usize + record.timestamp_delta,
                                topic_name: topic.name.clone(),
                                partition_index: partition.id,
                            });
                        }
                    }
                }
            }
        }

        tracing::info!(
            "Read {} records, newest offset {:?}",
            records.len(),
            self.offsets
        );

        // gotta clone here, this will be difficult
        // because it is big, but we can't do a ref
        // because we will mutate it underneath and
        // rust will definitely get mad at us
        Ok((records, self.offsets.clone()))
    }

    /// Convert consumer into an asynchronous iterator.
    ///
    /// Returns a tuple of a RecordBatch and the max offsets
    /// for the topic-partitions. Useful for manual commiting.
    #[must_use = "stream does nothingby itself"]
    pub fn into_stream(
        mut self,
    ) -> impl Stream<Item = Result<(Vec<ConsumeMessage>, PartitionOffsets)>> {
        async_stream::stream! {
            loop {
                yield self.next_batch().await;
            }
        }
    }

    /// Apply auto-commit to the consumer.
    ///
    /// Each time a message is pulled from this stream, the highest offsets
    /// are committed to the provided coordinator for the given group.
    ///
    /// To learn more about offset committing, see the protocol module.
    pub fn into_autocommit_stream(
        self,
        coordinator_conn: network::BrokerConnection,
        group_id: &'a str,
        generation_id: i32,
        member_id: Bytes,
        retention_time_ms: i64,
    ) -> impl Stream<Item = Result<Vec<ConsumeMessage>>> + 'a {
        let fetch_params = self.fetch_params.clone();
        try_stream! {
            for await stream_message in self.into_stream() {
                let (messages, offsets) = stream_message?;
                yield messages;
                commit_offset_wrapper(
                    fetch_params.correlation_id,
                    &fetch_params.client_id,
                    group_id,
                    coordinator_conn.clone(),
                    generation_id,
                    member_id.clone(),
                    offsets,
                    retention_time_ms
                ).await?;
            }
        }
    }

    /// Break the batched messages into individual elements.
    pub fn into_flat_stream(self) -> impl Stream<Item = ConsumeMessage> {
        into_flat_stream(self.into_stream())
    }
}

pub fn into_flat_stream(
    stream: impl Stream<Item = Result<(Vec<ConsumeMessage>, PartitionOffsets)>>,
) -> impl Stream<Item = ConsumeMessage> {
    futures::StreamExt::flat_map(
        stream
            .filter(|batch| batch.is_ok())
            .map(|batch| batch.unwrap())
            .map(|(batch, _)| batch),
        futures::stream::iter,
    )
}

/// Commit a set of offsets for a consumer group.
///
/// See this [protocol spec] for more information.
///
/// [protocol spec]: protocol::commit_offset
#[instrument(level = "debug")]
#[allow(clippy::too_many_arguments)]
pub async fn commit_offset(
    correlation_id: i32,
    client_id: &str,
    group_id: &str,
    coordinator_conn: network::BrokerConnection,
    generation_id: i32,
    member_id: Bytes,
    offsets: PartitionOffsets,
    retention_time_ms: i64,
) -> Result<protocol::OffsetCommitResponse> {
    let mut offset_request = protocol::OffsetCommitRequest::new(
        correlation_id,
        client_id,
        group_id,
        generation_id,
        member_id.clone(),
        retention_time_ms,
    )?;

    tracing::info!("Member {:?} - Committing offsets {:?}", member_id, offsets);

    for ((topic_name, partition_index), committed_offset) in offsets.iter() {
        offset_request.add(
            topic_name,
            *partition_index,
            *committed_offset,
            // TODO: find out why using None or Some("") causes an error in broker
            Some("metadata"),
        );
    }

    coordinator_conn.send_request(&offset_request).await?;

    let offset_response = coordinator_conn.receive_response().await?;

    let response = protocol::OffsetCommitResponse::try_from(offset_response.freeze())?;

    /*
     * OFFSET_METADATA_TOO_LARGE (12)
     * GROUP_LOAD_IN_PROGRESS (14)
     * GROUP_COORDINATOR_NOT_AVAILABLE (15)
     * NOT_COORDINATOR_FOR_GROUP (16)
     * ILLEGAL_GENERATION (22)
     * UNKNOWN_MEMBER_ID (25)
     * REBALANCE_IN_PROGRESS (27)
     * INVALID_COMMIT_OFFSET_SIZE (28)
     * TOPIC_AUTHORIZATION_FAILED (29)
     * GROUP_AUTHORIZATION_FAILED (30)
     */
    response.is_error()?;

    Ok(response)
}

#[allow(clippy::too_many_arguments)]
async fn commit_offset_wrapper(
    correlation_id: i32,
    client_id: &str,
    group_id: &str,
    coordinator_conn: network::BrokerConnection,
    generation_id: i32,
    member_id: Bytes,
    offsets: PartitionOffsets,
    retention_time_ms: i64,
) -> Result<()> {
    commit_offset(
        correlation_id,
        client_id,
        group_id,
        coordinator_conn,
        generation_id,
        member_id,
        offsets,
        retention_time_ms,
    )
    .await?;
    Ok(())
}

/// Fetch messages from a broker.
///
/// See this [protocol spec] for more information.
///
/// [protocol spec]: protocol::fetch
#[instrument(level = "debug")]
#[allow(clippy::too_many_arguments)]
pub async fn fetch(
    broker_conn: BrokerConnection,
    correlation_id: i32,
    client_id: &str,
    max_wait_ms: i32,
    min_bytes: i32,
    max_bytes: i32,
    max_partition_bytes: i32,
    isolation_level: i8,
    topic_partitions: &TopicPartitions,
    offsets: &PartitionOffsets,
) -> Result<protocol::FetchResponse> {
    tracing::debug!(
        "Consuming {:?} with offsets {:?}",
        topic_partitions,
        offsets
    );
    let mut request = protocol::FetchRequest::new(
        correlation_id,
        client_id,
        max_wait_ms,
        min_bytes,
        max_bytes,
        isolation_level,
    );

    // tracing::info!("Reading with offset {:?}", offsets);

    for (topic_name, partitions) in topic_partitions.iter() {
        for partition_index in partitions.iter() {
            // Default missing offsets to 0
            let offset = offsets
                .get(&(topic_name.to_owned(), *partition_index))
                .unwrap_or(&0);
            request.add(topic_name, *partition_index, *offset, max_partition_bytes);
        }
    }

    broker_conn.send_request(&request).await?;
    let response =
        protocol::FetchResponse::try_from(broker_conn.receive_response().await?.freeze())?;

    Ok(response)
}

#[cfg(test)]
mod test {
    use super::Consumer;

    struct ConsumerWrapper {
        consumer: Consumer,
    }

    #[tokio::test]
    async fn it_can_stream_via_ref_to_wrapper() {
        let consumer = Consumer {
            ..Default::default()
        };
        let wrapper = &ConsumerWrapper { consumer };
        let _stream = wrapper.consumer.clone().into_stream();
    }
}
