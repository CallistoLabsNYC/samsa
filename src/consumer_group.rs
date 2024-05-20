//! Consumer which cooperates with others to consume data.

use std::{collections::HashMap, fmt::Debug};

use bytes::Bytes;
use nom::AsBytes;
use tokio_stream::{Stream, StreamExt};

use crate::{
    assignor::{assign, ROUND_ROBIN_PROTOCOL},
    consumer::{ConsumeMessage, FetchParams, TopicPartitions},
    consumer_builder::ConsumerBuilder,
    error::{Error, KafkaCode, Result},
    network::{BrokerConnection, ConnectionParams},
    protocol::{
        self,
        join_group::request::{Metadata, Protocol},
        Assignment,
    },
};

const DEFAULT_PROTOCOL_TYPE: &str = "consumer";

#[derive(Clone, Debug)]
pub struct ConsumerGroup<T: BrokerConnection> {
    pub connection_params: ConnectionParams,
    pub coordinator_conn: T,
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

impl<T: BrokerConnection + Debug + Copy> ConsumerGroup<T> {
    pub fn into_stream(mut self) -> impl Stream<Item = Result<Vec<ConsumeMessage>>> {
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
                    self.coordinator_conn,
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
                        std::str::from_utf8(join.protocol_name.as_bytes()).map_err(|err| {
                            tracing::error!("Error converting from UTF8 {:?}", err);
                            Error::DecodingUtf8Error
                        })?,
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
                    self.coordinator_conn,
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

                let consumer = ConsumerBuilder::new(self.connection_params.clone(), assigned_topic_partitions)
                    .await?
                    .seek_to_group(self.coordinator_conn, &self.group_id)
                    .await?
                    .build()
                    .into_autocommit_stream(
                        self.coordinator_conn,
                        &self.group_id,
                        self.generation_id,
                        self.member_id.clone(),
                        self.retention_time_ms,
                    );

                tokio::pin!(consumer);

                loop {
                    // If the consumer stream yields None...
                    // We need to decide what to do,
                    // For now we just ignore Nones and keep beating
                    // (the consumer would yield None if it was done?
                    // not really a thing in kafka, and ours doesn't ever do a None)
                    if let Some(v) = consumer.next().await {
                        yield v;
                    }

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

/// Synchronize state for all members of a group (e.g. distribute partition assignments to consumers).
///
/// See this [protocol spec] for more information.
///
/// [protocol spec]: protocol::sync_group
pub async fn sync_group<'a, T: BrokerConnection>(
    mut conn: T,
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
pub async fn join_group<'a, T: BrokerConnection>(
    mut conn: T,
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
pub async fn heartbeat<T: BrokerConnection>(
    mut conn: T,
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
pub async fn leave_group<T: BrokerConnection>(
    mut conn: T,
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
