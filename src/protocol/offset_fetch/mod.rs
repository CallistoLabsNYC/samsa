//! Fetch a set of offsets for a consumer group.

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
            0, 9, 0, 2, 0, 0, 0, 1, 0, 4, 114, 117, 115, 116, 0, 8, 66, 105, 103, 32, 68, 111, 103,
            115, 0, 0, 0, 1, 0, 9, 112, 117, 114, 99, 104, 97, 115, 101, 115, 0, 0, 0, 1, 0, 0, 0,
            1,
        ];

        let correlation_id = 1;
        let client_id = "rust";
        let consumer_group_key = "Big Dogs";
        let topic_name = "purchases";
        let partition_index = 1;

        let mut req =
            request::OffsetFetchRequest::new(correlation_id, client_id, consumer_group_key);

        req.add(topic_name, partition_index);

        let mut buffer: Vec<u8> = vec![];

        req.encode(&mut buffer).unwrap();

        assert_eq!(buffer, b);
    }

    #[test]
    fn parse() {
        let buf = b"\0\0\0\x01\0\0\0\x01\0\tpurchases\0\0\0\x01\0\0\0\0\0\0\0\0\0\0\0\n\0\x0bplease work\0\0\0\0";
        let res = example_res();

        let (_, parsed) =
            response::parse_offset_fetch_response(NomBytes::from(buf.as_slice())).unwrap();
        assert_eq!(parsed, res);
    }

    #[test]
    fn add_to_req() {
        let correlation_id = 1;
        let client_id = "rust";
        let group_id = "Big Dogs";
        let topic_name = "purchases";
        let partitions = vec![1, 2, 3];

        let mut request = request::OffsetFetchRequest::new(correlation_id, client_id, group_id);

        for partition in partitions.clone() {
            request.add(topic_name, partition);
        }

        assert_eq!(request.topics.len(), 1);
        assert_eq!(request.topics[0].name, topic_name);
        assert_eq!(request.topics[0].partition_indexes, partitions);
    }

    #[test]
    fn read_from_res() {
        let res = example_res();

        let mut partition_offsets = res.into_box_iter();

        if let Some((topic_name, partition)) = partition_offsets.next() {
            assert_eq!(topic_name, Bytes::from_static(b"purchases"));
            assert_eq!(partition.committed_offset, 10);
            assert_eq!(partition.partition_index, 0);
        } else {
            panic!();
        }
    }

    fn example_res() -> response::OffsetFetchResponse {
        response::OffsetFetchResponse {
            header: protocol::HeaderResponse { correlation_id: 1 },
            topics: vec![response::Topic {
                name: Bytes::from_static(b"purchases"),
                partitions: vec![response::Partition {
                    partition_index: 0,
                    committed_offset: 10,
                    metadata: Some(Bytes::from_static(b"please work")),
                    error_code: KafkaCode::None,
                }],
            }],
            error_code: KafkaCode::None,
        }
    }
}
