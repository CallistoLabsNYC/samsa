//! Get information about the available offsets for a given topic partition.

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
            0, 2, 0, 1, 0, 0, 0, 1, 0, 4, 114, 117, 115, 116, 0, 0, 0, 0, 0, 0, 0, 1, 0, 9, 112,
            117, 114, 99, 104, 97, 115, 101, 115, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 1, 226,
            64,
        ];
        let correlation_id = 1;
        let client_id = "rust";
        let topic_name = "purchases";
        let partition_index = 1;

        let mut req = request::ListOffsetsRequest::new(correlation_id, client_id, 0);

        req.add(topic_name, partition_index, 123456);

        let mut buffer: Vec<u8> = vec![];

        req.encode(&mut buffer).unwrap();

        assert_eq!(buffer, b);
    }

    #[test]
    fn parse() {
        let b = b"\0\0\0\x01\0\0\0\x01\0\tpurchases\0\0\0\x01\0\0\0\0\0\x03\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff";

        let res = example_res();

        let x = response::parse_list_offsets_response(NomBytes::new(Bytes::from_static(b)))
            .unwrap()
            .1;

        assert_eq!(res, x);
    }

    #[test]
    fn add_to_req() {
        let correlation_id = 1;
        let client_id = "rust";
        let replica_id = 0;
        let topic_name = "purchases";
        let partitions = vec![1, 2, 3];

        let mut request = request::ListOffsetsRequest::new(correlation_id, client_id, replica_id);

        for partition in partitions.clone() {
            request.add(topic_name, partition, 12000);
        }

        // wont allow duplicates
        for partition in partitions.clone() {
            request.add(topic_name, partition, 12000);
        }

        for partition in partitions.clone() {
            request.add("second topic", partition, 12000);
        }

        assert_eq!(request.topics.len(), 2);
        assert_eq!(request.topics[0].name, topic_name);
        assert_eq!(request.topics[0].partitions.len(), 3);

        for (i, partition) in request.topics[0].partitions.iter().enumerate() {
            assert_eq!(partition.timestamp, 12000);
            assert_eq!(partition.partition_index, (i + 1) as i32);
        }
    }

    #[test]
    fn read_from_res() {
        let res = example_res();

        let mut partition_offsets = res.into_box_iter();

        if let Some((topic_name, partition)) = partition_offsets.next() {
            assert_eq!(topic_name, Bytes::from_static(b"purchases"));
            assert_eq!(partition.offset, -1);
            assert_eq!(partition.partition_index, 0);
        } else {
            panic!();
        }
    }

    fn example_res() -> response::ListOffsetsResponse {
        response::ListOffsetsResponse {
            header: protocol::HeaderResponse { correlation_id: 1 },
            topics: vec![response::Topic {
                name: Bytes::from_static(b"purchases"),
                partitions: vec![response::Partition {
                    partition_index: 0,
                    error_code: KafkaCode::UnknownTopicOrPartition,
                    timestamp: -1,
                    offset: -1,
                }],
            }],
        }
    }
}
