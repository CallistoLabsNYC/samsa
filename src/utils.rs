use crate::consumer::{StreamMessage, TopicPartitions};
use crate::error::Result;
use crate::network::BrokerConnection;
use crate::prelude::PartitionOffsets;
use crate::protocol::{OffsetFetchRequest, OffsetFetchResponse};
use crc::Crc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio_stream::{Stream, StreamExt};
use tracing::instrument;

/// Fetch a set of offsets for a consumer group.
///
/// See this [protocol spec] for more information.
///
/// [protocol spec]: protocol::offset_fetch
#[instrument(level = "debug")]
pub async fn fetch_offset(
    correlation_id: i32,
    client_id: &str,
    group_id: &str,
    coordinator_conn: BrokerConnection,
    topic_partitions: &TopicPartitions,
) -> Result<OffsetFetchResponse> {
    tracing::debug!(
        "Fetching offset for group {} for {:?}",
        group_id,
        topic_partitions
    );
    let mut offset_request = OffsetFetchRequest::new(correlation_id, client_id, group_id);
    for (topic_name, partitions) in topic_partitions.iter() {
        for partition_index in partitions.iter() {
            offset_request.add(topic_name, *partition_index);
        }
    }
    coordinator_conn.send_request(&offset_request).await?;

    let offset_response = coordinator_conn.receive_response().await?;
    OffsetFetchResponse::try_from(offset_response.freeze())
}

pub fn into_flat_stream(
    stream: impl Stream<Item = Result<(Vec<StreamMessage>, PartitionOffsets)>>,
) -> impl Stream<Item = StreamMessage> {
    futures::StreamExt::flat_map(
        stream
            .filter(|batch| batch.is_ok())
            .map(|batch| batch.unwrap())
            .map(|(batch, _)| batch),
        futures::stream::iter,
    )
}

#[allow(dead_code)]
pub fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_millis() as i64
}

pub fn to_crc(data: &[u8]) -> u32 {
    Crc::<u32>::new(&crc::CRC_32_ISO_HDLC).checksum(data)
}
