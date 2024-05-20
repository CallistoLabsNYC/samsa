use std::fmt::Debug;

use bytes::Bytes;
use nom::AsBytes;

use crate::{
    consumer::{FetchParams, TopicPartitions},
    consumer_group::ConsumerGroup,
    error::{Error, KafkaCode, Result},
    network::{BrokerConnection, ConnectionParams},
    protocol, DEFAULT_CLIENT_ID, DEFAULT_CORRELATION_ID,
};

const DEFAULT_RETENTION_TIME_MS: i64 = 100000;
const DEFAULT_SESSION_TIMEOUT_MS: i32 = 10000;
const DEFAULT_REBALANCE_TIMEOUT_MS: i32 = 10000;

#[derive(Clone)]
pub struct ConsumerGroupBuilder {
    pub connection_params: ConnectionParams,
    pub correlation_id: i32,
    pub client_id: String,
    pub session_timeout_ms: i32,
    pub rebalance_timeout_ms: i32,
    pub group_id: String,
    pub retention_time_ms: i64,
    pub group_topic_partitions: TopicPartitions,
    pub fetch_params: FetchParams,
}

impl<'a> ConsumerGroupBuilder {
    /// Start a consumer group builder. To complete, use the [`build`](Self::build) method.
    pub async fn new(
        connection_params: ConnectionParams,
        group_id: String,
        group_topic_partitions: TopicPartitions,
    ) -> Result<Self> {
        Ok(Self {
            connection_params,
            correlation_id: DEFAULT_CORRELATION_ID,
            client_id: DEFAULT_CLIENT_ID.to_owned(),
            session_timeout_ms: DEFAULT_SESSION_TIMEOUT_MS,
            rebalance_timeout_ms: DEFAULT_REBALANCE_TIMEOUT_MS,
            group_id,
            retention_time_ms: DEFAULT_RETENTION_TIME_MS,
            group_topic_partitions,
            fetch_params: FetchParams::new(),
        })
    }

    pub fn correlation_id(mut self, correlation_id: i32) -> Self {
        self.fetch_params.correlation_id = correlation_id;
        self
    }

    pub fn client_id(mut self, client_id: String) -> Self {
        self.fetch_params.client_id = client_id;
        self
    }

    pub fn retention_time_ms(mut self, retention_time_ms: i64) -> Self {
        self.retention_time_ms = retention_time_ms;
        self
    }
    pub fn session_timeout_ms(mut self, session_timeout_ms: i32) -> Self {
        self.session_timeout_ms = session_timeout_ms;
        self
    }
    pub fn rebalance_timeout_ms(mut self, rebalance_timeout_ms: i32) -> Self {
        self.rebalance_timeout_ms = rebalance_timeout_ms;
        self
    }

    /// The maximum time in milliseconds to wait for the response.
    pub fn max_wait_ms(mut self, max_wait_ms: i32) -> Self {
        self.fetch_params.max_wait_ms = max_wait_ms;
        self
    }

    /// The minimum bytes to accumulate in the response.
    pub fn min_bytes(mut self, min_bytes: i32) -> Self {
        self.fetch_params.min_bytes = min_bytes;
        self
    }

    /// The maximum bytes to fetch. See KIP-74 for cases where this limit may not be honored.
    pub fn max_bytes(mut self, max_bytes: i32) -> Self {
        self.fetch_params.max_bytes = max_bytes;
        self
    }

    /// The maximum bytes to fetch from the partitions. See KIP-74 for cases where this limit may not be honored.
    pub fn max_partition_bytes(mut self, max_partition_bytes: i32) -> Self {
        self.fetch_params.max_partition_bytes = max_partition_bytes;
        self
    }

    /// This setting controls the visibility of transactional records. Using READ_UNCOMMITTED (isolation_level = 0) makes all records visible. With READ_COMMITTED (isolation_level = 1), non-transactional and COMMITTED transactional records are visible. To be more concrete, READ_COMMITTED returns all data from offsets smaller than the current LSO (last stable offset), and enables the inclusion of the list of aborted transactions in the result, which allowss to discard ABORTED transactional records
    pub fn isolation_level(mut self, isolation_level: i8) -> Self {
        self.fetch_params.isolation_level = isolation_level;
        self
    }

    pub async fn build<T>(self) -> Result<ConsumerGroup<T>> {
        let conn = self.connection_params.init::<T>().await?;
        let coordinator =
            find_coordinator(conn, self.correlation_id, &self.client_id, &self.group_id).await?;

        if coordinator.error_code != KafkaCode::None {
            return Err(Error::KafkaError(coordinator.error_code));
        }

        let host = std::str::from_utf8(coordinator.host.as_bytes()).map_err(|err| {
            tracing::error!("Error converting from UTF8 {:?}", err);
            Error::DecodingUtf8Error
        })?;
        let port = coordinator.port;
        let coordinator_addr = format!("{}:{}", host, port);
        let coordinator_conn = self.connection_params.from_url(coordinator_addr).await?;

        Ok(ConsumerGroup {
            connection_params: self.connection_params,
            coordinator_conn,
            correlation_id: self.correlation_id,
            client_id: self.client_id,
            session_timeout_ms: self.session_timeout_ms,
            rebalance_timeout_ms: self.rebalance_timeout_ms,
            group_id: self.group_id,
            retention_time_ms: self.retention_time_ms,
            group_topic_partitions: self.group_topic_partitions,
            fetch_params: self.fetch_params,
            member_id: Bytes::from_static(b""),
            generation_id: 0,
            assignment: None,
        })
    }
}

/// Locate the current coordinator of a group.
///
/// See this [protocol spec] for more information.
///
/// [protocol spec]: protocol::find_coordinator
pub async fn find_coordinator<T: BrokerConnection>(
    mut conn: T,
    correlation_id: i32,
    client_id: &str,
    group_id: &str,
) -> Result<protocol::FindCoordinatorResponse> {
    let find_coordinator_request =
        protocol::FindCoordinatorRequest::new(correlation_id, client_id, group_id);
    conn.send_request(&find_coordinator_request).await?;

    let find_coordinator_response = conn.receive_response().await?;

    protocol::FindCoordinatorResponse::try_from(find_coordinator_response.freeze())
}
