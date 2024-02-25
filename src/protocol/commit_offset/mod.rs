//! Commit a set of offsets for a consumer group.

pub mod request;
pub mod response;

#[cfg(test)]
mod test {
    use bytes::Bytes;
    use nombytes::NomBytes;

    use super::*;
    use crate::{encode::ToByte, protocol};

    #[test]
    fn encode() {
        let b = [
            0, 8, 0, 2, 0, 0, 0, 1, 0, 4, 114, 117, 115, 116, 0, 8, 66, 105, 103, 32, 68, 111, 103,
            115, 0, 0, 0, 1, 0, 7, 68, 97, 32, 66, 111, 115, 115, 0, 0, 0, 0, 0, 0, 7, 208, 0, 0,
            0, 1, 0, 9, 112, 117, 114, 99, 104, 97, 115, 101, 115, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 1, 44, 255, 255, 255, 255,
        ];

        let correlation_id = 1;
        let client_id = "rust";
        let consumer_group_key = "Big Dogs";
        let generation_or_member_epic = 1;
        let member_id = "Da Boss";
        let topic_name = "purchases";
        let partition_index = 0;
        let committed_offset = 300;
        let retention_time_ms = 2000;

        let mut req = request::OffsetCommitRequest::new(
            correlation_id,
            client_id,
            consumer_group_key,
            generation_or_member_epic,
            member_id.into(),
            retention_time_ms,
        )
        .unwrap();

        req.add(topic_name, partition_index, committed_offset, None);

        let mut buffer: Vec<u8> = vec![];

        req.encode(&mut buffer).unwrap();

        assert_eq!(buffer, b);
    }

    #[test]
    fn parse() {
        let b = b"\0\0\0\x01\0\0\0\x01\0\0\0\0\0\0";

        let a_topic: response::Topic = response::Topic {
            name: Bytes::from_static(b""),
            partitions: vec![],
        };

        let res = response::OffsetCommitResponse {
            header: protocol::HeaderResponse { correlation_id: 1 },
            topics: vec![a_topic],
        };

        let x = response::parse_offset_commit_response(NomBytes::new(Bytes::from_static(b)))
            .unwrap()
            .1;

        assert_eq!(res, x);
    }

    #[test]
    fn add_to_req() {
        let correlation_id = 1;
        let client_id = "rust";
        let consumer_group_key = "Big Dogs";
        let generation_or_member_epic = 1;
        let member_id = "Da Boss";
        let topic_name = "purchases";
        let partitions = vec![0, 1, 2, 3];
        let committed_offset = 300;
        let retention_time_ms = 2000;
        let committed_metadata = Some("Testing This");

        let mut request = request::OffsetCommitRequest::new(
            correlation_id,
            client_id,
            consumer_group_key,
            generation_or_member_epic,
            member_id.into(),
            retention_time_ms,
        )
        .unwrap();

        for partition in partitions.clone() {
            request.add(topic_name, partition, committed_offset, committed_metadata);
        }

        // wont register duplicates
        for partition in partitions.clone() {
            request.add(topic_name, partition, committed_offset, committed_metadata);
        }

        // will allow
        for partition in partitions.clone() {
            request.add(
                "second topic",
                partition,
                committed_offset,
                committed_metadata,
            );
        }

        assert_eq!(request.topics.len(), 2);
        assert_eq!(request.topics[0].name, topic_name);
        assert_eq!(request.topics[0].partitions.len(), 4);

        assert_eq!(request.topics[1].name, "second topic");
        assert_eq!(request.topics[1].partitions.len(), 4);

        for partition in request.topics[0].partitions.iter() {
            assert_eq!(partition.committed_offset, committed_offset);
            assert_eq!(partition.committed_metadata, committed_metadata);
        }
    }
}
