//! Become a member of a group, creating it if there are no active members.
//!
//! These requests are used by clients to participate in a client group
//! managed by Kafka. From a high level, each group in the cluster is
//! assigned one the brokers (its group coordinator) to facilitate group
//!  management. Once the coordinator has been located (using the group
//! coordinator request from above), group members can join the group
//! and synchronize state, and then use heartbeats to stay active in the
//! group. When the client shuts down, it uses a leave group request to
//! deregister from the group. More detail on protocol semantics is
//! outlined in Kafka Client-side Assignment Proposal.
//!
//! The primary use case for the membership API is consumer groups, but
//! the requests are intentionally generic to support other cases
//! (e.g. Kafka Connect groups). The cost of this generality is that
//! specific group semantics are pushed into the client. For example,
//! the JoinGroup/SyncGroup requests defined below have no explicit
//! fields supporting partition assignment for consumer groups. Instead,
//! they contain generic byte arrays in which assignments can be embedded
//! by the consumer client implementation.
//!
//! But although this allows each client implementation to define its own
//! embedded schema, compatibility with Kafka tooling requires clients to
//! use the standard embedded schema used by the client shipped with Kafka.
//! The consumer-groups.sh utility, for example, assumes this format to
//! display partition assignments. We therefore recommend that clients
//! should follow the same schema so that these tools will work for all
//! client implementations.

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
            0, 11, 0, 2, 0, 0, 0, 1, 0, 4, 114, 117, 115, 116, 0, 8, 66, 105, 103, 32, 68, 111,
            103, 115, 0, 0, 39, 16, 0, 0, 39, 16, 0, 0, 0, 14, 99, 111, 110, 115, 117, 109, 101,
            114, 45, 114, 97, 110, 103, 101, 0, 0, 0, 1, 0, 8, 99, 111, 110, 115, 117, 109, 101,
            114, 0, 0, 0, 21, 0, 3, 0, 0, 0, 1, 0, 9, 112, 117, 114, 99, 104, 97, 115, 101, 115,
            255, 255, 255, 255,
        ];

        let correlation_id = 1;
        let client_id = "rust";
        let consumer_group_key = "Big Dogs";
        let member_id = "";
        let session_timeout_ms = 10000;
        let rebalance_timeout_ms = 10000;
        let protocol_type = "consumer-range";
        let topics = vec!["purchases"];
        let protocol = request::Protocol::new("consumer", topics);
        let protocols = vec![protocol];

        let req = request::JoinGroupRequest::new(
            correlation_id,
            client_id,
            consumer_group_key,
            session_timeout_ms,
            rebalance_timeout_ms,
            member_id.into(),
            protocol_type,
            protocols,
        )
        .unwrap();

        let mut buffer: Vec<u8> = vec![];

        req.encode(&mut buffer).unwrap();

        assert_eq!(buffer, b);
    }

    #[test]
    fn parse() {
        let b = b"\0\0\0\x01\0\0\0\0\0\0\0\0\0\x02\0\x08consumer\0;group integration test-1fdacda0-218b-4c93-aa1d-bfe1ee48e9c9\0;group integration test-1fdacda0-218b-4c93-aa1d-bfe1ee48e9c9\0\0\0\x02\0;group integration test-1fdacda0-218b-4c93-aa1d-bfe1ee48e9c9\0\0\0\x15\0\x03\0\0\0\x01\0\tpurchases\xff\xff\xff\xff\0;group integration test-f92a30c7-3927-4817-8a13-7949b4688680\0\0\0\x15\0\x03\0\0\0\x01\0\tpurchases\xff\xff\xff\xff";

        let res = response::JoinGroupResponse {
            header: protocol::HeaderResponse { correlation_id: 1 },
            throttle_time_ms: 0,
            error_code: KafkaCode::None,
            generation_id: 2,
            protocol_name: Bytes::from_static(b"consumer"),
            leader: Bytes::from_static(
                b"group integration test-1fdacda0-218b-4c93-aa1d-bfe1ee48e9c9",
            ),
            member_id: Bytes::from_static(
                b"group integration test-1fdacda0-218b-4c93-aa1d-bfe1ee48e9c9",
            ),
            members: vec![
                response::Member {
                    member_id: Bytes::from_static(
                        b"group integration test-1fdacda0-218b-4c93-aa1d-bfe1ee48e9c9",
                    ),
                    metadata: Bytes::from_static(b"\0\x03\0\0\0\x01\0\tpurchases\xff\xff\xff\xff"),
                },
                response::Member {
                    member_id: Bytes::from_static(
                        b"group integration test-f92a30c7-3927-4817-8a13-7949b4688680",
                    ),
                    metadata: Bytes::from_static(b"\0\x03\0\0\0\x01\0\tpurchases\xff\xff\xff\xff"),
                },
            ],
        };

        let x = response::parse_join_group_response(NomBytes::new(Bytes::from_static(b)))
            .unwrap()
            .1;

        assert_eq!(res, x);
    }
}
