//! Send messages to a broker.

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
        let correlation_id = 2;
        let client_id = "rust";
        let topic_name = "purchases";
        let partition_id = 3;

        let encoded_buf = [
            0, 0, 0, 0, 0, 0, 0, 2, 0, 4, 114, 117, 115, 116, 0, 0, 0, 0, 3, 232, 0, 0, 0, 1, 0, 9,
            112, 117, 114, 99, 104, 97, 115, 101, 115, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, 234, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 27, 58, 153, 7, 97, 0, 0, 0, 0, 0, 6, 84, 101, 115, 116,
            101, 114, 0, 0, 0, 7, 86, 97, 108, 117, 101, 32, 49, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            27, 163, 144, 86, 219, 0, 0, 0, 0, 0, 6, 84, 101, 115, 116, 101, 114, 0, 0, 0, 7, 86,
            97, 108, 117, 101, 32, 50, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 27, 212, 151, 102, 77, 0,
            0, 0, 0, 0, 6, 84, 101, 115, 116, 101, 114, 0, 0, 0, 7, 86, 97, 108, 117, 101, 32, 51,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 27, 212, 151, 102, 77, 0, 0, 0, 0, 0, 6, 84, 101, 115,
            116, 101, 114, 0, 0, 0, 7, 86, 97, 108, 117, 101, 32, 51, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 27, 212, 151, 102, 77, 0, 0, 0, 0, 0, 6, 84, 101, 115, 116, 101, 114, 0, 0, 0, 7,
            86, 97, 108, 117, 101, 32, 51, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 27, 212, 151, 102, 77,
            0, 0, 0, 0, 0, 6, 84, 101, 115, 116, 101, 114, 0, 0, 0, 7, 86, 97, 108, 117, 101, 32,
            51, 0, 0, 0, 4, 0, 0, 2, 151, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 27, 212, 151, 102, 77,
            0, 0, 0, 0, 0, 6, 84, 101, 115, 116, 101, 114, 0, 0, 0, 7, 86, 97, 108, 117, 101, 32,
            51, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 27, 212, 151, 102, 77, 0, 0, 0, 0, 0, 6, 84, 101,
            115, 116, 101, 114, 0, 0, 0, 7, 86, 97, 108, 117, 101, 32, 51, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 27, 212, 151, 102, 77, 0, 0, 0, 0, 0, 6, 84, 101, 115, 116, 101, 114, 0, 0, 0,
            7, 86, 97, 108, 117, 101, 32, 51, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 27, 212, 151, 102,
            77, 0, 0, 0, 0, 0, 6, 84, 101, 115, 116, 101, 114, 0, 0, 0, 7, 86, 97, 108, 117, 101,
            32, 51, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 27, 212, 151, 102, 77, 0, 0, 0, 0, 0, 6, 84,
            101, 115, 116, 101, 114, 0, 0, 0, 7, 86, 97, 108, 117, 101, 32, 51, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 27, 212, 151, 102, 77, 0, 0, 0, 0, 0, 6, 84, 101, 115, 116, 101, 114, 0,
            0, 0, 7, 86, 97, 108, 117, 101, 32, 51, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 27, 212, 151,
            102, 77, 0, 0, 0, 0, 0, 6, 84, 101, 115, 116, 101, 114, 0, 0, 0, 7, 86, 97, 108, 117,
            101, 32, 51, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 27, 212, 151, 102, 77, 0, 0, 0, 0, 0, 6,
            84, 101, 115, 116, 101, 114, 0, 0, 0, 7, 86, 97, 108, 117, 101, 32, 51, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 27, 212, 151, 102, 77, 0, 0, 0, 0, 0, 6, 84, 101, 115, 116, 101, 114,
            0, 0, 0, 7, 86, 97, 108, 117, 101, 32, 51, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 27, 212,
            151, 102, 77, 0, 0, 0, 0, 0, 6, 84, 101, 115, 116, 101, 114, 0, 0, 0, 7, 86, 97, 108,
            117, 101, 32, 51, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 27, 212, 151, 102, 77, 0, 0, 0, 0,
            0, 6, 84, 101, 115, 116, 101, 114, 0, 0, 0, 7, 86, 97, 108, 117, 101, 32, 51, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 27, 212, 151, 102, 77, 0, 0, 0, 0, 0, 6, 84, 101, 115, 116,
            101, 114, 0, 0, 0, 7, 86, 97, 108, 117, 101, 32, 51, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            27, 212, 151, 102, 77, 0, 0, 0, 0, 0, 6, 84, 101, 115, 116, 101, 114, 0, 0, 0, 7, 86,
            97, 108, 117, 101, 32, 51, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 27, 212, 151, 102, 77, 0,
            0, 0, 0, 0, 6, 84, 101, 115, 116, 101, 114, 0, 0, 0, 7, 86, 97, 108, 117, 101, 32, 51,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 27, 212, 151, 102, 77, 0, 0, 0, 0, 0, 6, 84, 101, 115,
            116, 101, 114, 0, 0, 0, 7, 86, 97, 108, 117, 101, 32, 51, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 27, 212, 151, 102, 77, 0, 0, 0, 0, 0, 6, 84, 101, 115, 116, 101, 114, 0, 0, 0, 7,
            86, 97, 108, 117, 101, 32, 51, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 27, 212, 151, 102, 77,
            0, 0, 0, 0, 0, 6, 84, 101, 115, 116, 101, 114, 0, 0, 0, 7, 86, 97, 108, 117, 101, 32,
            51,
        ];

        let mut produce_req = request::ProduceRequest::new(0, 1000, correlation_id, client_id);
        produce_req.add(
            topic_name,
            partition_id,
            Some(Bytes::from_static(b"Tester")),
            Some(Bytes::from_static(b"Value 1")),
        );
        produce_req.add(
            topic_name,
            partition_id,
            Some(Bytes::from_static(b"Tester")),
            Some(Bytes::from_static(b"Value 2")),
        );
        produce_req.add(
            topic_name,
            partition_id,
            Some(Bytes::from_static(b"Tester")),
            Some(Bytes::from_static(b"Value 3")),
        );
        produce_req.add(
            topic_name,
            partition_id,
            Some(Bytes::from_static(b"Tester")),
            Some(Bytes::from_static(b"Value 3")),
        );
        produce_req.add(
            topic_name,
            partition_id,
            Some(Bytes::from_static(b"Tester")),
            Some(Bytes::from_static(b"Value 3")),
        );
        produce_req.add(
            topic_name,
            partition_id,
            Some(Bytes::from_static(b"Tester")),
            Some(Bytes::from_static(b"Value 3")),
        );
        produce_req.add(
            topic_name,
            partition_id + 1,
            Some(Bytes::from_static(b"Tester")),
            Some(Bytes::from_static(b"Value 3")),
        );
        produce_req.add(
            topic_name,
            partition_id + 1,
            Some(Bytes::from_static(b"Tester")),
            Some(Bytes::from_static(b"Value 3")),
        );
        produce_req.add(
            topic_name,
            partition_id + 1,
            Some(Bytes::from_static(b"Tester")),
            Some(Bytes::from_static(b"Value 3")),
        );
        produce_req.add(
            topic_name,
            partition_id + 1,
            Some(Bytes::from_static(b"Tester")),
            Some(Bytes::from_static(b"Value 3")),
        );
        produce_req.add(
            topic_name,
            partition_id + 1,
            Some(Bytes::from_static(b"Tester")),
            Some(Bytes::from_static(b"Value 3")),
        );
        produce_req.add(
            topic_name,
            partition_id + 1,
            Some(Bytes::from_static(b"Tester")),
            Some(Bytes::from_static(b"Value 3")),
        );
        produce_req.add(
            topic_name,
            partition_id + 1,
            Some(Bytes::from_static(b"Tester")),
            Some(Bytes::from_static(b"Value 3")),
        );
        produce_req.add(
            topic_name,
            partition_id + 1,
            Some(Bytes::from_static(b"Tester")),
            Some(Bytes::from_static(b"Value 3")),
        );
        produce_req.add(
            topic_name,
            partition_id + 1,
            Some(Bytes::from_static(b"Tester")),
            Some(Bytes::from_static(b"Value 3")),
        );
        produce_req.add(
            topic_name,
            partition_id + 1,
            Some(Bytes::from_static(b"Tester")),
            Some(Bytes::from_static(b"Value 3")),
        );
        produce_req.add(
            topic_name,
            partition_id + 1,
            Some(Bytes::from_static(b"Tester")),
            Some(Bytes::from_static(b"Value 3")),
        );
        produce_req.add(
            topic_name,
            partition_id + 1,
            Some(Bytes::from_static(b"Tester")),
            Some(Bytes::from_static(b"Value 3")),
        );
        produce_req.add(
            topic_name,
            partition_id + 1,
            Some(Bytes::from_static(b"Tester")),
            Some(Bytes::from_static(b"Value 3")),
        );
        produce_req.add(
            topic_name,
            partition_id + 1,
            Some(Bytes::from_static(b"Tester")),
            Some(Bytes::from_static(b"Value 3")),
        );
        produce_req.add(
            topic_name,
            partition_id + 1,
            Some(Bytes::from_static(b"Tester")),
            Some(Bytes::from_static(b"Value 3")),
        );
        produce_req.add(
            topic_name,
            partition_id + 1,
            Some(Bytes::from_static(b"Tester")),
            Some(Bytes::from_static(b"Value 3")),
        );
        produce_req.add(
            topic_name,
            partition_id + 1,
            Some(Bytes::from_static(b"Tester")),
            Some(Bytes::from_static(b"Value 3")),
        );
        let mut buffer = Vec::with_capacity(4);
        produce_req.encode(&mut buffer).unwrap();

        assert_eq!(buffer.len(), buffer.len());
        assert_eq!(buffer, encoded_buf);
    }

    #[test]
    fn parse() {
        let buf = b"\0\0\0\x01\0\0\0\x01\0\x06tester\0\0\0\x01\0\0\0\0\0\0\0\0\0\0\0\0\0\x04";
        let res = response::ProduceResponse {
            header: protocol::HeaderResponse { correlation_id: 1 },
            responses: vec![response::Response {
                name: Bytes::from("tester"),
                partition_responses: vec![response::PartitionResponse {
                    index: 0,
                    error_code: KafkaCode::None,
                    base_offset: 4,
                }],
            }],
        };

        let (_, parsed) =
            response::parse_produce_fetch_response(NomBytes::from(buf.as_slice())).unwrap();
        assert_eq!(parsed, res);
    }
}
