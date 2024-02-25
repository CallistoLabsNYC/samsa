//! Synchronize state for all members of a group (e.g. distribute partition assignments to consumers).

pub mod request;
pub mod response;

#[cfg(test)]
mod test {
    use nombytes::NomBytes;

    use super::*;
    use crate::{encode::ToByte, error::KafkaCode, protocol};

    #[test]
    fn encode() {
        let b = [
            0, 14, 0, 2, 0, 0, 0, 1, 0, 4, 114, 117, 115, 116, 0, 8, 66, 105, 103, 32, 68, 111,
            103, 115, 0, 0, 0, 0, 0, 4, 106, 111, 101, 121, 0, 0, 0, 1, 0, 4, 106, 111, 101, 121,
            0, 0, 0, 29, 0, 3, 0, 0, 0, 1, 0, 9, 112, 117, 114, 99, 104, 97, 115, 101, 115, 0, 0,
            0, 1, 0, 0, 0, 1, 255, 255, 255, 255,
        ];

        let correlation_id = 1;
        let client_id = "rust";
        let group_id = "Big Dogs";
        let topic_name = "purchases";
        let partition_index = 1;

        let assignments = vec![request::Assignment::new(
            "joey".into(),
            request::MemberAssignment {
                version: 3,
                partition_assignments: vec![request::PartitionAssignment::new(
                    topic_name,
                    vec![partition_index],
                )],
                user_data: None,
            },
        )
        .unwrap()];

        let req = request::SyncGroupRequest::new(
            correlation_id,
            client_id,
            group_id,
            0,
            "joey".into(),
            assignments,
        )
        .unwrap();
        let mut buffer: Vec<u8> = vec![];

        req.encode(&mut buffer).unwrap();

        assert_eq!(buffer, b);
    }

    #[test]
    fn parse() {
        let buf = b"\0\0\0\x01\0\0\0\0\0\0\0\0\0\x1d\0\x03\0\0\0\x01\0\tpurchases\0\0\0\x01\0\0\0\0\xff\xff\xff\xff";
        let res = response::SyncGroupResponse {
            header: protocol::HeaderResponse { correlation_id: 1 },
            throttle_time_ms: 0,
            error_code: KafkaCode::None,
            assignment: response::MemberAssignment {
                version: 3,
                partition_assignments: vec![response::PartitionAssignment {
                    topic_name: "purchases".into(),
                    partitions: vec![0],
                }],
                user_data: None,
            },
        };

        let (_, parsed) =
            response::parse_sync_group_response(NomBytes::from(buf.as_slice())).unwrap();
        assert_eq!(parsed, res);
    }
}
