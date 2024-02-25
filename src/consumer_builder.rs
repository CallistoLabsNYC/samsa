use crate::consumer::{Consumer, FetchParams, PartitionOffsets, TopicPartitions};
use crate::utils::fetch_offset;
use crate::{
    error::{Error, KafkaCode, Result},
    metadata::{self},
    network::{self, BrokerConnection},
    protocol, DEFAULT_CLIENT_ID,
};
use nom::AsBytes;
use std::collections::HashMap;
use tracing::instrument;

/// Configure a [`Consumer`].
pub struct ConsumerBuilder {
    consumer: Consumer,
}

impl<'a> ConsumerBuilder {
    /// Start a consumer builder. To complete, use the [`build`] method.
    pub async fn new(
        bootstrap_addrs: Vec<String>,
        assigned_topic_partitions: TopicPartitions,
    ) -> Result<Self> {
        let topics = assigned_topic_partitions
            .keys()
            .map(|topic_name| topic_name.to_owned())
            .collect();

        let cluster_metadata =
            metadata::ClusterMetadata::new(bootstrap_addrs, DEFAULT_CLIENT_ID.to_owned(), topics)
                .await?;

        Ok(Self {
            consumer: Consumer {
                cluster_metadata,
                assigned_topic_partitions,
                fetch_params: FetchParams::new(),
                offsets: HashMap::new(),
            },
        })
    }

    /// Seek offsets to a given timestamp.
    ///
    /// Given a timestamp, move the offsets for each assigned topic partition
    /// to the point in the log at that time.
    ///
    /// Note: This method overwrites the entire offsets object.
    pub async fn seek_to_timestamp(mut self, timestamp: i64) -> Result<Self> {
        tracing::debug!("Seeking offsets to timestamp {}", timestamp);
        // TODO: Push this into the metadata
        let brokers_and_their_topic_partitions = self
            .consumer
            .cluster_metadata
            .get_connections_for_topic_partitions(&self.consumer.assigned_topic_partitions)?;
        self.consumer.offsets = HashMap::new();

        // TODO: Make these all calls run async
        // try this https://docs.rs/tokio/latest/tokio/task/join_set/struct.JoinSet.html#examples
        for (broker_conn, topic_partitions) in brokers_and_their_topic_partitions.into_iter() {
            let offsets_list = list_offsets(
                broker_conn,
                self.consumer.fetch_params.correlation_id,
                &self.consumer.fetch_params.client_id,
                &topic_partitions,
                timestamp,
            )
            .await?;

            let partition_offsets = offsets_list.into_box_iter();
            for (topic_name, partition) in partition_offsets {
                if partition.error_code != KafkaCode::None {
                    return Err(Error::KafkaError(partition.error_code));
                }

                let topic_name = std::str::from_utf8(topic_name.as_bytes()).map_err(|err| {
                    tracing::error!("Error converting from UTF8 {:?}", err);
                    Error::DecodingUtf8Error
                })?;

                // this is a sneaky way to use data that we own :)
                let topic_name = self
                    .consumer
                    .cluster_metadata
                    .topic_names
                    .iter()
                    .find(|my_topic| **my_topic == topic_name)
                    .unwrap();

                self.consumer.offsets.insert(
                    (topic_name.to_owned(), partition.partition_index),
                    partition.offset,
                );
            }
        }
        tracing::trace!("Offsets set to {:?}", self.consumer.offsets);

        Ok(self)
    }

    /// Seek offsets to a given group id.
    ///
    /// Given a group id, move the offsets for each assigned topic partitin to
    /// sync up with the group id.
    ///
    /// Note: If the group does not have an offset for a topic partition, the
    /// offset is intialized to 0.
    pub async fn seek_to_group(
        mut self,
        coordinator_conn: network::BrokerConnection,
        group_id: &str,
    ) -> Result<Self> {
        tracing::debug!("Seeking offsets to group {}", group_id);
        let fetch_params = &self.consumer.fetch_params;

        let offset_response = fetch_offset(
            fetch_params.correlation_id,
            &fetch_params.client_id,
            group_id,
            coordinator_conn,
            &self.consumer.assigned_topic_partitions,
        )
        .await?;

        if offset_response.error_code != KafkaCode::None {
            return Err(Error::KafkaError(offset_response.error_code));
        }

        let partition_offsets = offset_response.into_box_iter();
        for (topic_name, partition) in partition_offsets {
            if partition.error_code != KafkaCode::None {
                return Err(Error::KafkaError(partition.error_code));
            }

            // this is a sneaky way to use data that we own :)
            let topic_name = std::str::from_utf8(topic_name.as_bytes()).map_err(|err| {
                tracing::error!("Error converting from UTF8 {:?}", err);
                Error::DecodingUtf8Error
            })?;

            let topic_name = self
                .consumer
                .cluster_metadata
                .topic_names
                .iter()
                .find(|my_topic| **my_topic == topic_name)
                .unwrap();

            // starting from zero!
            let offset = if partition.committed_offset == -1 {
                tracing::debug!(
                    "No offset found for topic {} partition {}, initializing to 0",
                    topic_name,
                    partition.partition_index
                );
                0
            } else {
                partition.committed_offset
            };

            self.consumer
                .offsets
                .insert((topic_name.to_owned(), partition.partition_index), offset);
        }
        tracing::trace!("Offsets set to {:?}", self.consumer.offsets);

        Ok(self)
    }

    /// Seek offsets to a given set of partition offsets.
    ///
    /// Overwrites the current offsets with the given offsets.
    pub fn seek(mut self, offsets: &PartitionOffsets) -> Self {
        tracing::debug!("Seeking offsets to given values");
        self.consumer.offsets = offsets.clone();
        tracing::trace!("Offsets set to {:?}", self.consumer.offsets);

        self
    }

    pub fn correlation_id(mut self, correlation_id: i32) -> Self {
        self.consumer.fetch_params.correlation_id = correlation_id;
        self
    }

    pub fn client_id(mut self, client_id: String) -> Self {
        self.consumer.fetch_params.client_id = client_id;
        self
    }

    /// The maximum time in milliseconds to wait for the response.
    pub fn max_wait_ms(mut self, max_wait_ms: i32) -> Self {
        self.consumer.fetch_params.max_wait_ms = max_wait_ms;
        self
    }

    /// The minimum bytes to accumulate in the response.
    pub fn min_bytes(mut self, min_bytes: i32) -> Self {
        self.consumer.fetch_params.min_bytes = min_bytes;
        self
    }

    /// The maximum bytes to fetch. See KIP-74 for cases where this limit may not be honored.
    pub fn max_bytes(mut self, max_bytes: i32) -> Self {
        self.consumer.fetch_params.max_bytes = max_bytes;
        self
    }

    /// The maximum bytes to fetch from the partitions. See KIP-74 for cases where this limit may not be honored.
    pub fn max_partition_bytes(mut self, max_partition_bytes: i32) -> Self {
        self.consumer.fetch_params.max_partition_bytes = max_partition_bytes;
        self
    }

    /// This setting controls the visibility of transactional records. Using READ_UNCOMMITTED (isolation_level = 0) makes all records visible. With READ_COMMITTED (isolation_level = 1), non-transactional and COMMITTED transactional records are visible. To be more concrete, READ_COMMITTED returns all data from offsets smaller than the current LSO (last stable offset), and enables the inclusion of the list of aborted transactions in the result, which allows consumers to discard ABORTED transactional records
    pub fn isolation_level(mut self, isolation_level: i8) -> Self {
        self.consumer.fetch_params.isolation_level = isolation_level;
        self
    }

    pub fn build(self) -> Consumer {
        self.consumer
    }
}

/// Get information about the available offsets for a given topic partition.
///
/// Used to ask for all messages before a certain time (ms). There are two special values. Specify -1 to receive the latest offset (i.e. the offset of the next coming message) and -2 to receive the earliest available offset. This applies to all versions of the API. Note that because offsets are pulled in descending order, asking for the earliest offset will always return you a single element.
///
/// See this [protocol spec] for more information.
///
/// [protocol spec]: protocol::list_offsets
#[instrument(level = "debug")]
async fn list_offsets(
    broker_conn: &BrokerConnection,
    correlation_id: i32,
    client_id: &str,
    topic_partitions: &TopicPartitions,
    timestamp: i64,
) -> Result<protocol::ListOffsetsResponse> {
    tracing::debug!(
        "Listing offset for time {} for {:?}",
        timestamp,
        topic_partitions
    );
    let mut list_offsets_request = protocol::ListOffsetsRequest::new(correlation_id, client_id, -1);
    for (topic_name, partitions) in topic_partitions.iter() {
        for partition_index in partitions.iter() {
            list_offsets_request.add(topic_name, *partition_index, timestamp);
        }
    }

    broker_conn.send_request(&list_offsets_request).await?;
    let list_offsets_response = broker_conn.receive_response().await?;
    protocol::ListOffsetsResponse::try_from(list_offsets_response.freeze())
}
