//! Describes the currently available brokers, their host and port
//! information, and gives information about which broker hosts
//! which partitions.
//!
//! This API answers the following questions:
//! - What topics exist?
//! - How many partitions does each topic have?
//! - Which broker is currently the leader for each partition?
//! - What is the host and port for each of these brokers?
//! This is the only request that can be addressed to any broker
//! in the cluster.
//!
//! Since there may be many topics the client can give an
//! optional list of topic names in order to only return metadata
//! for a subset of topics.
//!
//! The metadata returned is at the partition level, but grouped
//!  together by topic for convenience and to avoid redundancy.
//! For each partition the metadata contains the information for
//! the leader as well as for all the replicas and the list of
//! replicas that are currently in-sync.

pub mod request;
pub mod response;

#[cfg(test)]
mod test {
    use bytes::Bytes;
    use nombytes::NomBytes;

    use super::*;
    use crate::{encode::ToByte, error::KafkaCode, protocol};

    #[test]
    fn encode() {
        let b = [
            0, 3, 0, 0, 0, 0, 0, 1, 0, 4, 114, 117, 115, 116, 0, 0, 0, 1, 0, 9, 112, 117, 114, 99,
            104, 97, 115, 101, 115,
        ];
        let correlation_id = 1;
        let client_id = "rust";
        let topics = vec!["purchases"];

        let req = request::MetadataRequest::new(correlation_id, client_id, &topics);

        let mut buffer: Vec<u8> = vec![];

        req.encode(&mut buffer).unwrap();

        assert_eq!(buffer, b);
    }

    #[test]
    fn parse() {
        let buf = [
            0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 1, 0, 9, 108, 111, 99, 97, 108, 104, 111, 115, 116, 0,
            0, 35, 132, 0, 0, 0, 2, 0, 9, 108, 111, 99, 97, 108, 104, 111, 115, 116, 0, 0, 35, 133,
            0, 0, 0, 1, 0, 0, 0, 9, 112, 117, 114, 99, 104, 97, 115, 101, 115, 0, 0, 0, 4, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 0, 0, 1,
            0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 2, 0, 0, 0,
            2, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 0, 0, 3, 0, 0, 0, 1, 0, 0,
            0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1,
        ];
        let res = test_metadata();

        let (_, parsed) =
            response::parse_metadata_response(NomBytes::from(buf.as_slice())).unwrap();
        assert_eq!(parsed, res);
    }

    fn test_metadata() -> response::MetadataResponse {
        response::MetadataResponse {
            header_response: protocol::HeaderResponse { correlation_id: 1 },
            brokers: vec![
                response::Broker {
                    node_id: 1,
                    host: Bytes::from("localhost"),
                    port: 9092,
                },
                response::Broker {
                    node_id: 2,
                    host: Bytes::from("localhost"),
                    port: 9093,
                },
            ],
            topics: vec![response::Topic {
                error_code: KafkaCode::None,
                name: Bytes::from("purchases"),
                partitions: vec![
                    response::Partition {
                        error_code: KafkaCode::None,
                        partition_index: 0,
                        leader_id: 2,
                        replica_nodes: vec![2],
                        isr_nodes: vec![2],
                    },
                    response::Partition {
                        error_code: KafkaCode::None,
                        partition_index: 1,
                        leader_id: 1,
                        replica_nodes: vec![1],
                        isr_nodes: vec![1],
                    },
                    response::Partition {
                        error_code: KafkaCode::None,
                        partition_index: 2,
                        leader_id: 2,
                        replica_nodes: vec![2],
                        isr_nodes: vec![2],
                    },
                    response::Partition {
                        error_code: KafkaCode::None,
                        partition_index: 3,
                        leader_id: 1,
                        replica_nodes: vec![1],
                        isr_nodes: vec![1],
                    },
                ],
            }],
        }
    }
}
