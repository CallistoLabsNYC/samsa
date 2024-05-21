//! Cluster metadata & operations.
use std::collections::HashMap;

use nom::AsBytes;

use crate::{
    error::{Error, Result},
    network::{BrokerConnection, ConnectionParams},
    protocol::{self, metadata::response::*},
};

#[derive(Clone, Default, Debug)]
pub struct ClusterMetadata<T: BrokerConnection> {
    pub connection_params: ConnectionParams,
    pub broker_connections: HashMap<i32, T>,
    pub brokers: Vec<Broker>,
    pub topics: Vec<Topic>,
    pub client_id: String,
    pub topic_names: Vec<String>,
}

type TopicPartition = HashMap<String, Vec<i32>>;

impl<'a, T: BrokerConnection + Clone> ClusterMetadata<T> {
    pub async fn new(
        connection_params: ConnectionParams,
        client_id: String,
        topics: Vec<String>,
    ) -> Result<ClusterMetadata<T>> {
        // tracing::info!("Conencting to cluster at {}", bootstrap_addrs.join(","));
        let mut metadata = ClusterMetadata {
            connection_params: connection_params.clone(),
            broker_connections: HashMap::new(),
            brokers: vec![],
            topics: vec![],
            client_id,
            topic_names: topics,
        };
        let bootstrap_connection = T::new(connection_params).await?;

        metadata.fetch(bootstrap_connection).await?;
        metadata.sync().await?;

        Ok(metadata)
    }

    pub fn get_broker_by_id(&self, id: i32) -> Option<&Broker> {
        self.brokers.iter().find(|b| b.node_id == id)
    }

    pub fn get_topic_partition_by_id(
        &self,
        topic_name: &'a str,
        partition_id: i32,
    ) -> Option<&Partition> {
        let topic = self.topics.iter().find(|t| t.name == topic_name)?;
        topic
            .partitions
            .iter()
            .find(|b| b.partition_index == partition_id)
    }

    pub fn get_leader_for_topic_partition(
        &self,
        topic_name: &'a str,
        partition_id: i32,
    ) -> Option<i32> {
        let partition = self.get_topic_partition_by_id(topic_name, partition_id)?;
        let leader = self.get_broker_by_id(partition.leader_id)?;
        tracing::debug!(
            "Leader is {:?} for topic {} and partition {}",
            leader,
            topic_name,
            partition_id
        );
        Some(leader.node_id)
    }

    pub async fn sync(&mut self) -> Result<()> {
        tracing::debug!("Syncing metadata");
        // let mut set = JoinSet::new();

        for broker in self.brokers.iter() {
            let id: i32 = broker.node_id;
            let addr = broker.addr()?;
            let url = self.connection_params.from_url(addr)?;
            let conn = T::new(url).await?;
            self.broker_connections.insert(id, conn);
        }

        Ok(())
    }

    // brokers: [
    //     Broker { node_id: 2, host: "localhost", port: 9093 },
    //     Broker { node_id: 1, host: "localhost", port: 9092 }],
    // topics: [Topic { error_code: KafkaCode::None, name: "purchases", partitions: [
    //         Partition { error_code: KafkaCode::None, partition_index: 0, leader_id: 1, replica_nodes: [1], isr_nodes: [1] },
    //         Partition { error_code: KafkaCode::None, partition_index: 1, leader_id: 2, replica_nodes: [2], isr_nodes: [2] },
    //         Partition { error_code: KafkaCode::None, partition_index: 2, leader_id: 1, replica_nodes: [1], isr_nodes: [1] },
    //         Partition { error_code: KafkaCode::None, partition_index: 3, leader_id: 2, replica_nodes: [2], isr_nodes: [2] }] }] }
    // #[instrument(name = "metadata-fetch")]
    pub async fn fetch(&mut self, mut conn: T) -> Result<()> {
        tracing::debug!("Fetching metadata");
        let metadata_request =
            protocol::MetadataRequest::new(1, &self.client_id, &self.topic_names);
        conn.send_request(&metadata_request).await?;

        let response_bytes = conn.receive_response().await?;
        let metadata_response = protocol::MetadataResponse::try_from(response_bytes.freeze())?;

        metadata_response.is_error()?;

        self.topics = metadata_response.topics;
        self.brokers = metadata_response.brokers;

        Ok(())
    }

    pub fn get_connections_for_topic_partitions(
        &'a self,
        topic_partitions: &TopicPartition,
    ) -> Result<Vec<(T, TopicPartition)>> {
        let leaders = self.get_leaders_for_topic_partitions(topic_partitions)?;
        let mut connections = vec![];
        for (broker_id, assignments) in leaders.iter() {
            let broker_conn = self
                .broker_connections
                .get(broker_id)
                .ok_or(Error::MetadataNeedsSync)
                .map(|c| (*c).clone());
            if let Err(err) = broker_conn {
                tracing::error!("No broker connection for assignment {:?}", assignments);
                return Err(err);
            };

            tracing::debug!("Broker {} is in charge of {:?}", broker_id, assignments);

            connections.push((broker_conn.unwrap(), assignments.to_owned()));
        }

        Ok(connections)
    }

    // Given topics and partitions
    // get back a map where
    // broker connection is the key
    // and value is a list of tuples of (topic, partitions)
    pub fn get_leaders_for_topic_partitions(
        &'a self,
        topic_partitions: &TopicPartition,
    ) -> Result<HashMap<i32, TopicPartition>> {
        let mut broker_to_partition_map: HashMap<i32, HashMap<String, Vec<i32>>> = HashMap::new();

        let flattened_partition_brokers = topic_partitions
            .iter()
            // Flatten out the topic with nested partitions
            .flat_map(|(new_topic_name, partitions)| {
                partitions
                    .iter()
                    .map(|partition| (new_topic_name.to_owned(), partition))
                    .collect::<Vec<(String, &i32)>>()
            })
            // Attach each partition with its appropriate broker
            .map(|(new_topic_name, new_partition)| {
                match self.get_leader_for_topic_partition(&new_topic_name, *new_partition) {
                    Some(broker_id) => Ok((new_topic_name, new_partition, broker_id)),
                    None => Err(Error::MetadataNeedsSync),
                }
            })
            .collect::<Result<Vec<(String, &i32, i32)>>>()?;

        // Build up the Broker -> TopicPartition map
        for (new_topic_name, new_partition, broker_id) in flattened_partition_brokers {
            // Do we have this broker already?
            if let Some(broker_ownership) = broker_to_partition_map.get_mut(&broker_id) {
                // Do we have this topic already?
                if let Some(existing_partitions) = broker_ownership.get_mut(&new_topic_name) {
                    // Don't push the partition on more than once
                    if !existing_partitions
                        .iter()
                        .any(|existing_partition| *existing_partition == *new_partition)
                    {
                        existing_partitions.push(*new_partition);
                    }
                } else {
                    broker_ownership.insert(new_topic_name.to_owned(), vec![*new_partition]);
                }
            } else {
                let mut new_topic_partitions = HashMap::new();
                new_topic_partitions.insert(new_topic_name, vec![*new_partition]);
                broker_to_partition_map.insert(broker_id, new_topic_partitions);
            }
        }

        Ok(broker_to_partition_map)
    }
}

impl Broker {
    pub fn addr(&self) -> Result<String> {
        let host = std::str::from_utf8(self.host.as_bytes()).map_err(|err| {
            tracing::error!("Error converting from UTF8 {:?}", err);
            Error::DecodingUtf8Error
        })?;
        Ok(format!("{}:{}", host, self.port))
    }
}

#[cfg(test)]
mod test {
    use bytes::Bytes;

    use super::*;
    use crate::{
        error::KafkaCode,
        network::{tcp::TcpConnection, ConnectionParams, ConnectionParamsKind},
    };

    macro_rules! test_metadata {
        () => {
            ClusterMetadata {
                connection_params: ConnectionParams(ConnectionParamsKind::TcpParams(vec![
                    "localhost:9092".to_owned(),
                ])),
                broker_connections: HashMap::new(),
                topic_names: vec![String::from("purchases")],
                client_id: String::from("client_id"),
                brokers: vec![
                    Broker {
                        node_id: 1,
                        host: Bytes::from("localhost"),
                        port: 9092,
                    },
                    Broker {
                        node_id: 2,
                        host: Bytes::from("localhost"),
                        port: 9093,
                    },
                ],
                topics: vec![Topic {
                    error_code: KafkaCode::None,
                    name: Bytes::from("purchases"),
                    partitions: vec![
                        Partition {
                            error_code: KafkaCode::None,
                            partition_index: 0,
                            leader_id: 2,
                            replica_nodes: vec![2],
                            isr_nodes: vec![2],
                        },
                        Partition {
                            error_code: KafkaCode::None,
                            partition_index: 1,
                            leader_id: 1,
                            replica_nodes: vec![1],
                            isr_nodes: vec![1],
                        },
                        Partition {
                            error_code: KafkaCode::None,
                            partition_index: 2,
                            leader_id: 2,
                            replica_nodes: vec![2],
                            isr_nodes: vec![2],
                        },
                        Partition {
                            error_code: KafkaCode::None,
                            partition_index: 3,
                            leader_id: 1,
                            replica_nodes: vec![1],
                            isr_nodes: vec![1],
                        },
                    ],
                }],
            }
        };
    }

    #[test]
    fn test_broker_by_id() {
        let cluster: ClusterMetadata<TcpConnection> = test_metadata!();
        let id = 1;

        let broker = cluster.get_broker_by_id(id);

        assert!(broker.is_some());
    }

    #[test]
    fn test_partition_by_id() {
        let cluster: ClusterMetadata<TcpConnection> = test_metadata!();
        let id = 1;
        let partition = cluster.get_topic_partition_by_id("purchases", id);

        assert!(partition.is_some());
        assert_eq!(partition.unwrap().partition_index, id);
    }

    #[test]
    fn test_broker_url() {
        let broker = Broker {
            node_id: 2,
            host: Bytes::from("localhost"),
            port: 9093,
        };
        assert_eq!(broker.addr().unwrap(), String::from("localhost:9093"));
    }

    #[test]
    fn test_partition_leader() {
        let cluster: ClusterMetadata<TcpConnection> = test_metadata!();

        let leader = cluster.get_leader_for_topic_partition("purchases", 1);

        assert!(leader.is_some());
        assert_eq!(leader.unwrap(), 1);

        let leader = cluster.get_leader_for_topic_partition("purchases", 0);

        assert!(leader.is_some());
        assert_eq!(leader.unwrap(), 2);
    }

    #[test]
    fn test_get_leaders_for_topic_partitions() {
        let cluster: ClusterMetadata<TcpConnection> = test_metadata!();
        let mut topic_partitions = HashMap::new();
        topic_partitions.insert(String::from("purchases"), vec![0, 1, 2, 3]);
        let leaders = cluster.get_leaders_for_topic_partitions(&topic_partitions);

        assert!(leaders.is_ok());
        let leaders = leaders.unwrap();
        assert_eq!(leaders.keys().len(), 2);

        let broker1 = leaders.get(&1);
        assert!(broker1.is_some());
        let broker2 = leaders.get(&2);
        assert!(broker2.is_some());

        assert_eq!(
            broker1.unwrap(),
            &HashMap::from([(String::from("purchases"), vec![1, 3])])
        );
        assert_eq!(
            broker2.unwrap(),
            &HashMap::from([(String::from("purchases"), vec![0, 2])])
        );
    }
}
