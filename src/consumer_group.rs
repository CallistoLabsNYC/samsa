//! Consumer which cooperates with others to consume data.
//!
//! # Consumer Group Module
//!
//! We provide a Consumer Group struct that takes care of the inner details relating
//! to all things Kafka.
//!
//! We also provide low level methods that allow users to interface with the
//! Kafka API directly if they so choose. For those looking to get their hands
//! even dirtier and handle the specific requests and responses that make up
//! the Kafka protocol, visit the [protocol module].
//!
//! ## ConsumerGroup
//! The [`ConsumerGroup`] struct is an abstraction over the typical Kafka Consumer Groups.
//! This struct represents one member. It is used similarly to the [`Consumer`] in that it
//! is based on streams.
//!
//! To use, simply provide the initial bootstrap broker, the group id, and the assignments
//! to the [`ConsumerGroupBuilder`]. This you can use to configure the fetching parameters as needed.
//!
//! ### Example
//! ```rust
//! let consumer_group_member = consumer_group::ConsumerGroupBuilder::new(
//!     bootstrap_url,
//!     "Squad",
//!     HashMap::from([(src_topic, vec![0, 1, 2, 3])]),
//! ).await?
//! .build().await?
//!
//! let stream = consumer_group_member.into_stream();
//!
//! // have to pin streams before iterating
//! tokio::pin!(stream);
//!
//! // Stream will do nothing unless consumed.
//! while let Some(batch) = stream.next().await {
//!     println!("{:?}", batch);
//! }
//! ```
//!
//! ## Protocol functions
//! We provide a set of protocol primitives for users to build their own clients.
//! They are presented as the building blocks that we use to build the higher level
//! abstractions.
//!
//! ### Join Group
//! [`join_group`] Become a member of a group, creating it if there are no active members..
//! #### Example
//! ```rust
//! let join_response = join_group(
//!     correlation_id,
//!     client_id,
//!     group_id,
//!     session_timeout_ms,
//!     rebalance_timeout_ms,
//!     member_id,
//!     protocol_type,
//!     protocols,
//! ).await?;
//! ```
//!  ### Sync Group
//! [`sync_group`] Synchronize state for all members of a group.
//! #### Example
//! ```rust
//! let sync_response = sync_group(
//!     correlation_id,
//!     client_id,
//!     group_id,
//!     generation_id,
//!     member_id,
//!     assignments,
//! ).await?;
//! ```
//!
//! ### Heartbeat
//! [`heartbeat`] Keep a member alive in the group.
//! #### Example
//! ```rust
//! let heartbeat_response = heartbeat(
//!     correlation_id,
//!     client_id,
//!     group_id,
//!     generation_id,
//!     member_id,
//! ).await?;
//! ```
//!
//! ### Leave Group
//! [`leave_group`] Directly depart a group.
//! #### Example
//! ```rust
//! let leave_response = leave_group(
//!     correlation_id, client_id, group_id, member_id
//! ).await?;
//! ```
//!
//! [protocol module]: protocol
use std::collections::HashMap;

use bytes::Bytes;
use nom::AsBytes;
use tokio_stream::{Stream, StreamExt};

use crate::{
    assignor::{assign, ROUND_ROBIN_PROTOCOL},
    consumer::{FetchParams, StreamMessage, TopicPartitions},
    consumer_builder::ConsumerBuilder,
    error::{Error, KafkaCode, Result},
    network::BrokerConnection,
    protocol::{
        self,
        join_group::request::{Metadata, Protocol},
        Assignment,
    },
    DEFAULT_CLIENT_ID, DEFAULT_CORRELATION_ID,
};

const DEFAULT_PROTOCOL_TYPE: &str = "consumer";
const DEFAULT_RETENTION_TIME_MS: i64 = 100000;
const DEFAULT_SESSION_TIMEOUT_MS: i32 = 10000;
const DEFAUTL_REBALANCE_TIMEOUT_MS: i32 = 10000;

pub struct ConsumerGroupBuilder {
    pub bootstrap_addrs: Vec<String>,
    pub correlation_id: i32,
    pub client_id: String,
    pub session_timeout_ms: i32,
    pub rebalance_timeout_ms: i32,
    pub group_id: String,
    pub retention_time_ms: i64,
    pub group_topic_partitions: TopicPartitions,
    pub fetch_params: FetchParams,
}

pub struct ConsumerGroup {
    pub bootstrap_addrs: Vec<String>,
    pub coordinator_conn: BrokerConnection,
    pub correlation_id: i32,
    pub client_id: String,
    pub session_timeout_ms: i32,
    pub rebalance_timeout_ms: i32,
    pub group_id: String,
    pub member_id: Bytes,
    pub generation_id: i32,
    pub assignment: Option<protocol::sync_group::response::MemberAssignment>,
    pub retention_time_ms: i64,
    pub group_topic_partitions: TopicPartitions,
    pub fetch_params: FetchParams,
}

impl<'a> ConsumerGroupBuilder {
    /// Start a consumer group builder. To complete, use the [`build`] method.
    pub async fn new(
        bootstrap_addrs: Vec<String>,
        group_id: String,
        group_topic_partitions: TopicPartitions,
    ) -> Result<Self> {
        Ok(Self {
            bootstrap_addrs,
            correlation_id: DEFAULT_CORRELATION_ID,
            client_id: DEFAULT_CLIENT_ID.to_owned(),
            session_timeout_ms: DEFAULT_SESSION_TIMEOUT_MS,
            rebalance_timeout_ms: DEFAUTL_REBALANCE_TIMEOUT_MS,
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

    pub async fn build(self) -> Result<ConsumerGroup> {
        let conn = BrokerConnection::new(self.bootstrap_addrs.clone()).await?;
        let coordinator =
            find_coordinator(conn, self.correlation_id, &self.client_id, &self.group_id).await?;

        if coordinator.error_code != KafkaCode::None {
            return Err(Error::KafkaError(coordinator.error_code));
        }

        let host = std::str::from_utf8(coordinator.host.as_bytes()).unwrap();
        let port = coordinator.port;
        let coordinator_addr = format!("{}:{}", host, port);
        let coordinator_conn = BrokerConnection::new(vec![coordinator_addr]).await?;

        Ok(ConsumerGroup {
            bootstrap_addrs: self.bootstrap_addrs,
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

impl ConsumerGroup {
    pub fn into_stream(mut self) -> impl Stream<Item = Result<Vec<StreamMessage>>> {
        async_stream::stream! {
            loop {
                tracing::info!(
                    "Member {:?} | Joining group {} for generation {}",
                    self.member_id,
                    self.group_id,
                    self.generation_id
                );
                let protocols = [ROUND_ROBIN_PROTOCOL]
                    .iter()
                    .map(|protocol| Protocol {
                        name: protocol,
                        metadata: Metadata {
                            version: 3,
                            subscription: self
                                .group_topic_partitions.keys().map(|k| k.as_ref())
                                .collect::<Vec<&str>>(),
                            user_data: None,
                        },
                    })
                    .collect();

                let join = join_group(
                    self.coordinator_conn.clone(),
                    self.correlation_id,
                    &self.client_id,
                    &self.group_id,
                    self.session_timeout_ms,
                    self.rebalance_timeout_ms,
                    self.member_id.clone(),
                    DEFAULT_PROTOCOL_TYPE,
                    protocols,
                )
                .await?;

                /*
                * GROUP_LOAD_IN_PROGRESS (14)
                * GROUP_COORDINATOR_NOT_AVAILABLE (15)
                * NOT_COORDINATOR_FOR_GROUP (16)
                * INCONSISTENT_GROUP_PROTOCOL (23)
                * UNKNOWN_MEMBER_ID (25)
                * INVALID_SESSION_TIMEOUT (26)
                * GROUP_AUTHORIZATION_FAILED (30)
                */
                // if join.error_code != KafkaCode::None {
                //     return Err(Error::KafkaError(join.error_code));
                // }

                self.member_id = join.member_id;
                self.generation_id = join.generation_id;

                tracing::info!(
                    "Member {:?} | group info: {} members, {:?} protocol, {:?} leader",
                    self.member_id,
                    join.members.len(),
                    join.protocol_name,
                    join.leader
                );

                let assignments = if self.member_id == join.leader {
                    //ToDo:: make partitions configurable
                    let number_of_consumers = join.members.len();

                    // hmm is this leaky?? I gotta do this because of memory and lifetimes
                    let assigned_topic_partitions: Vec<(&str, &Vec<i32>)> = self.group_topic_partitions
                        .iter()
                        .map(|(a, b)| (a.as_ref(), b))
                        .collect();

                    let partition_assignments = assign(
                        std::str::from_utf8(join.protocol_name.as_bytes()).unwrap(),
                        assigned_topic_partitions,
                        number_of_consumers,
                    )?;

                    join.members
                        .iter()
                        .enumerate()
                        .map(|(i, member)| {
                            protocol::Assignment::new(
                                member.member_id.clone(),
                                partition_assignments[i].clone(),
                            )
                        })
                        .collect::<Result<Vec<Assignment>>>()?
                } else {
                    vec![]
                };

                tracing::info!(
                    "Member {:?} | making assignments {:?}",
                    self.member_id,
                    assignments
                );
                let sync = sync_group(
                    self.coordinator_conn.clone(),
                    self.correlation_id,
                    &self.client_id,
                    &self.group_id,
                    self.generation_id,
                    self.member_id.clone(),
                    assignments,
                )
                .await?;

                /*
                * GROUP_COORDINATOR_NOT_AVAILABLE (15)
                * NOT_COORDINATOR_FOR_GROUP (16)
                * ILLEGAL_GENERATION (22)
                * UNKNOWN_MEMBER_ID (25)
                * REBALANCE_IN_PROGRESS (27)
                * GROUP_AUTHORIZATION_FAILED (30)
                */
                // if sync.error_code != KafkaCode::None {
                //     return Err(Error::KafkaError(sync.error_code));
                // }

                tracing::info!(
                    "Member {:?} | Assigned to {:?}",
                    self.member_id,
                    sync.assignment
                );

                self.assignment = Some(sync.assignment);

                tracing::info!(
                    "Member {:?} | Assigned to {:?}",
                    self.member_id,
                    self.assignment
                );

                let assigned_topic_partitions: TopicPartitions =
                    self.assignment
                        .iter()
                        .fold(HashMap::new(), |mut acc, assignment| {
                            for assignment in assignment.partition_assignments.clone() {
                                let topic_name =
                                    std::str::from_utf8(assignment.topic_name.as_bytes()).unwrap();
                                let topic = self
                                    .group_topic_partitions
                                    .keys()
                                    .find(|topic| *topic == topic_name)
                                    .unwrap();
                                acc.insert(topic.to_owned(), assignment.partitions);
                            }
                            acc
                        });

                let consumer = ConsumerBuilder::new(self.bootstrap_addrs.clone(), assigned_topic_partitions)
                    .await?
                    .seek_to_group(self.coordinator_conn.clone(), &self.group_id)
                    .await?
                    .build()
                    .into_autocommit_stream(
                        self.coordinator_conn.clone(),
                        &self.group_id,
                        self.generation_id,
                        self.member_id.clone(),
                        self.retention_time_ms,
                    );

                tokio::pin!(consumer);

                loop {
                    // Ugly unwrap here...
                    yield consumer.next().await.unwrap();

                    tracing::info!("Member {:?} | Heartbeat", self.member_id);
                    let hb = heartbeat(
                        self.coordinator_conn.clone(),
                        self.correlation_id,
                        &self.client_id,
                        &self.group_id,
                        self.generation_id,
                        self.member_id.clone(),
                    )
                    .await?;

                    /*
                    * GROUP_COORDINATOR_NOT_AVAILABLE (15)
                    * NOT_COORDINATOR_FOR_GROUP (16)
                    * ILLEGAL_GENERATION (22)
                    * UNKNOWN_MEMBER_ID (25)
                    * REBALANCE_IN_PROGRESS (27)
                    * GROUP_AUTHORIZATION_FAILED (30)
                    */
                    if hb.error_code == KafkaCode::RebalanceInProgress {
                        // TODO: Include a state here that symbols a need to rebalance
                        break;
                    }
                }
            }
        }
    }
}

/// Locate the current coordinator of a group.
///
/// See this [protocol spec] for more information.
///
/// [protocol spec]: protocol::find_coordinator
pub async fn find_coordinator(
    conn: BrokerConnection,
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

/// Synchronize state for all members of a group (e.g. distribute partition assignments to consumers).
///
/// See this [protocol spec] for more information.
///
/// [protocol spec]: protocol::sync_group
pub async fn sync_group<'a>(
    conn: BrokerConnection,
    correlation_id: i32,
    client_id: &str,
    group_id: &str,
    generation_id: i32,
    member_id: Bytes,
    assignments: Vec<protocol::Assignment<'a>>,
) -> Result<protocol::SyncGroupResponse> {
    let sync_request = protocol::SyncGroupRequest::new(
        correlation_id,
        client_id,
        group_id,
        generation_id,
        member_id,
        assignments,
    )?;
    conn.send_request(&sync_request).await?;

    let sync_response = conn.receive_response().await?;

    protocol::SyncGroupResponse::try_from(sync_response.freeze())
}

/// Become a member of a group, creating it if there are no active members.
///
/// See this [protocol spec] for more information.
///
/// [protocol spec]: protocol::join_group
#[allow(clippy::too_many_arguments)]
pub async fn join_group<'a>(
    conn: BrokerConnection,
    correlation_id: i32,
    client_id: &str,
    group_id: &str,
    session_timeout_ms: i32,
    rebalance_timeout_ms: i32,
    member_id: Bytes,
    protocol_type: &str,
    protocols: Vec<Protocol<'a>>,
) -> Result<protocol::JoinGroupResponse> {
    let join_request = protocol::JoinGroupRequest::new(
        correlation_id,
        client_id,
        group_id,
        session_timeout_ms,
        rebalance_timeout_ms,
        member_id,
        protocol_type,
        protocols,
    )?;
    conn.send_request(&join_request).await?;
    let join_response = conn.receive_response().await?;

    protocol::JoinGroupResponse::try_from(join_response.freeze())
}

/// Keep a member alive in the group.
///
/// See this [protocol spec] for more information.
///
/// [protocol spec]: protocol::heartbeat
pub async fn heartbeat(
    conn: BrokerConnection,
    correlation_id: i32,
    client_id: &str,
    group_id: &str,
    generation_id: i32,
    member_id: Bytes,
) -> Result<protocol::HeartbeatResponse> {
    let heartbeat = protocol::HeartbeatRequest::new(
        correlation_id,
        client_id,
        group_id,
        generation_id,
        member_id,
    )?;
    conn.send_request(&heartbeat).await?;

    let heartbeat_response = conn.receive_response().await?;

    protocol::HeartbeatResponse::try_from(heartbeat_response.freeze())
}

/// Directly depart a group.
///
/// See this [protocol spec] for more information.
///
/// [protocol spec]: protocol::leave_group
pub async fn leave_group(
    conn: BrokerConnection,
    correlation_id: i32,
    client_id: &str,
    group_id: &str,
    member_id: Bytes,
) -> Result<protocol::LeaveGroupResponse> {
    let leave = protocol::LeaveGroupRequest::new(correlation_id, client_id, group_id, member_id)?;
    conn.send_request(&leave).await?;

    let leave_response = conn.receive_response().await?;

    protocol::LeaveGroupResponse::try_from(leave_response.freeze())
}
