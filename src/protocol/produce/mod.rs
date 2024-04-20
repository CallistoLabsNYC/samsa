//! Send messages to a broker.

pub mod request;
pub mod response;

#[cfg(test)]
mod test {
    use bytes::Bytes;
    use nombytes::NomBytes;

    use self::request::Attributes;

    use super::*;
    use crate::{encode::ToByte, error::KafkaCode, prelude::Compression, protocol::{self, fetch::response::parse_record_batch}, utils::{compress, uncompress}};

    #[test]
    fn encode() {
        let correlation_id = 2;
        let client_id = "rust";
        let topic_name = "purchases";
        let partition_id = 3;

        let encoded_buf = [
            0, 0, 0, 3, 0, 0, 0, 2, 0, 4, 114, 117, 115, 116, 255, 255, 0, 0, 0, 0, 3, 232, 0, 0,
            0, 1, 0, 9, 112, 117, 114, 99, 104, 97, 115, 101, 115, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0,
            181, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 169, 255, 255, 255, 255, 2, 189, 191, 202, 234,
            0, 0, 0, 0, 0, 5, 0, 0, 1, 142, 16, 208, 185, 246, 0, 0, 1, 142, 16, 208, 185, 246,
            255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 0, 0, 0, 6, 38,
            0, 0, 0, 12, 84, 101, 115, 116, 101, 114, 14, 86, 97, 108, 117, 101, 32, 49, 0, 38, 0,
            0, 2, 12, 84, 101, 115, 116, 101, 114, 14, 86, 97, 108, 117, 101, 32, 50, 0, 38, 0, 0,
            4, 12, 84, 101, 115, 116, 101, 114, 14, 86, 97, 108, 117, 101, 32, 49, 0, 38, 0, 0, 6,
            12, 84, 101, 115, 116, 101, 114, 14, 86, 97, 108, 117, 101, 32, 51, 0, 38, 0, 0, 8, 12,
            84, 101, 115, 116, 101, 114, 14, 86, 97, 108, 117, 101, 32, 51, 0, 38, 0, 0, 10, 12,
            84, 101, 115, 116, 101, 114, 14, 86, 97, 108, 117, 101, 32, 51, 0, 0, 0, 0, 4, 0, 0, 1,
            145, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 133, 255, 255, 255, 255, 2, 76, 6, 84, 254, 0, 0,
            0, 0, 0, 16, 0, 0, 1, 142, 16, 208, 185, 246, 0, 0, 1, 142, 16, 208, 185, 246, 255,
            255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 0, 0, 0, 17, 38, 0, 0,
            0, 12, 84, 101, 115, 116, 101, 114, 14, 86, 97, 108, 117, 101, 32, 51, 0, 38, 0, 0, 2,
            12, 84, 101, 115, 116, 101, 114, 14, 86, 97, 108, 117, 101, 32, 51, 0, 38, 0, 0, 4, 12,
            84, 101, 115, 116, 101, 114, 14, 86, 97, 108, 117, 101, 32, 51, 0, 38, 0, 0, 6, 12, 84,
            101, 115, 116, 101, 114, 14, 86, 97, 108, 117, 101, 32, 51, 0, 38, 0, 0, 8, 12, 84,
            101, 115, 116, 101, 114, 14, 86, 97, 108, 117, 101, 32, 51, 0, 38, 0, 0, 10, 12, 84,
            101, 115, 116, 101, 114, 14, 86, 97, 108, 117, 101, 32, 51, 0, 38, 0, 0, 12, 12, 84,
            101, 115, 116, 101, 114, 14, 86, 97, 108, 117, 101, 32, 51, 0, 38, 0, 0, 14, 12, 84,
            101, 115, 116, 101, 114, 14, 86, 97, 108, 117, 101, 32, 51, 0, 38, 0, 0, 16, 12, 84,
            101, 115, 116, 101, 114, 14, 86, 97, 108, 117, 101, 32, 51, 0, 38, 0, 0, 18, 12, 84,
            101, 115, 116, 101, 114, 14, 86, 97, 108, 117, 101, 32, 51, 0, 38, 0, 0, 20, 12, 84,
            101, 115, 116, 101, 114, 14, 86, 97, 108, 117, 101, 32, 51, 0, 38, 0, 0, 22, 12, 84,
            101, 115, 116, 101, 114, 14, 86, 97, 108, 117, 101, 32, 51, 0, 38, 0, 0, 24, 12, 84,
            101, 115, 116, 101, 114, 14, 86, 97, 108, 117, 101, 32, 51, 0, 38, 0, 0, 26, 12, 84,
            101, 115, 116, 101, 114, 14, 86, 97, 108, 117, 101, 32, 51, 0, 38, 0, 0, 28, 12, 84,
            101, 115, 116, 101, 114, 14, 86, 97, 108, 117, 101, 32, 51, 0, 38, 0, 0, 30, 12, 84,
            101, 115, 116, 101, 114, 14, 86, 97, 108, 117, 101, 32, 51, 0, 38, 0, 0, 32, 12, 84,
            101, 115, 116, 101, 114, 14, 86, 97, 108, 117, 101, 32, 51, 0,
        ];

        let mut produce_req = request::ProduceRequest::new(
            0,
            1000,
            correlation_id,
            client_id,
            request::Attributes::new(Some(Compression::Gzip)),
        );
        produce_req.add(
            topic_name,
            partition_id,
            Some(Bytes::from_static(b"Tester")),
            Some(Bytes::from_static(b"Value 1")),
            vec![],
        );
        produce_req.add(
            topic_name,
            partition_id,
            Some(Bytes::from_static(b"Tester")),
            Some(Bytes::from_static(b"Value 2")),
            vec![],
        );
        produce_req.add(
            topic_name,
            partition_id,
            Some(Bytes::from_static(b"Tester")),
            Some(Bytes::from_static(b"Value 1")),
            vec![],
        );
        produce_req.add(
            topic_name,
            partition_id,
            Some(Bytes::from_static(b"Tester")),
            Some(Bytes::from_static(b"Value 3")),
            vec![],
        );
        produce_req.add(
            topic_name,
            partition_id,
            Some(Bytes::from_static(b"Tester")),
            Some(Bytes::from_static(b"Value 3")),
            vec![],
        );
        produce_req.add(
            topic_name,
            partition_id,
            Some(Bytes::from_static(b"Tester")),
            Some(Bytes::from_static(b"Value 3")),
            vec![],
        );
        produce_req.add(
            topic_name,
            partition_id + 1,
            Some(Bytes::from_static(b"Tester")),
            Some(Bytes::from_static(b"Value 3")),
            vec![],
        );
        produce_req.add(
            topic_name,
            partition_id + 1,
            Some(Bytes::from_static(b"Tester")),
            Some(Bytes::from_static(b"Value 3")),
            vec![],
        );
        produce_req.add(
            topic_name,
            partition_id + 1,
            Some(Bytes::from_static(b"Tester")),
            Some(Bytes::from_static(b"Value 3")),
            vec![],
        );
        produce_req.add(
            topic_name,
            partition_id + 1,
            Some(Bytes::from_static(b"Tester")),
            Some(Bytes::from_static(b"Value 3")),
            vec![],
        );
        produce_req.add(
            topic_name,
            partition_id + 1,
            Some(Bytes::from_static(b"Tester")),
            Some(Bytes::from_static(b"Value 3")),
            vec![],
        );
        produce_req.add(
            topic_name,
            partition_id + 1,
            Some(Bytes::from_static(b"Tester")),
            Some(Bytes::from_static(b"Value 3")),
            vec![],
        );
        produce_req.add(
            topic_name,
            partition_id + 1,
            Some(Bytes::from_static(b"Tester")),
            Some(Bytes::from_static(b"Value 3")),
            vec![],
        );
        produce_req.add(
            topic_name,
            partition_id + 1,
            Some(Bytes::from_static(b"Tester")),
            Some(Bytes::from_static(b"Value 3")),
            vec![],
        );
        produce_req.add(
            topic_name,
            partition_id + 1,
            Some(Bytes::from_static(b"Tester")),
            Some(Bytes::from_static(b"Value 3")),
            vec![],
        );
        produce_req.add(
            topic_name,
            partition_id + 1,
            Some(Bytes::from_static(b"Tester")),
            Some(Bytes::from_static(b"Value 3")),
            vec![],
        );
        produce_req.add(
            topic_name,
            partition_id + 1,
            Some(Bytes::from_static(b"Tester")),
            Some(Bytes::from_static(b"Value 3")),
            vec![],
        );
        produce_req.add(
            topic_name,
            partition_id + 1,
            Some(Bytes::from_static(b"Tester")),
            Some(Bytes::from_static(b"Value 3")),
            vec![],
        );
        produce_req.add(
            topic_name,
            partition_id + 1,
            Some(Bytes::from_static(b"Tester")),
            Some(Bytes::from_static(b"Value 3")),
            vec![],
        );
        produce_req.add(
            topic_name,
            partition_id + 1,
            Some(Bytes::from_static(b"Tester")),
            Some(Bytes::from_static(b"Value 3")),
            vec![],
        );
        produce_req.add(
            topic_name,
            partition_id + 1,
            Some(Bytes::from_static(b"Tester")),
            Some(Bytes::from_static(b"Value 3")),
            vec![],
        );
        produce_req.add(
            topic_name,
            partition_id + 1,
            Some(Bytes::from_static(b"Tester")),
            Some(Bytes::from_static(b"Value 3")),
            vec![],
        );
        produce_req.add(
            topic_name,
            partition_id + 1,
            Some(Bytes::from_static(b"Tester")),
            Some(Bytes::from_static(b"Value 3")),
            vec![],
        );
        let mut buffer = Vec::with_capacity(4);
        produce_req.encode(&mut buffer).unwrap();

        assert_eq!(buffer.len(), buffer.len());
        assert_eq!(buffer, encoded_buf);
    }

    #[test]
    fn parse() {
        let buf = b"\0\0\0\x01\0\0\0\x01\0\x06tester\0\0\0\x01\0\0\0\0\0\0\0\0\0\0\0\0\0\x02\xff\xff\xff\xff\xff\xff\xff\xff\0\0\0\0";
        let res = response::ProduceResponse {
            header: protocol::HeaderResponse { correlation_id: 1 },
            responses: vec![response::Response {
                name: bytes::Bytes::from_static(b"tester"),
                partition_responses: vec![response::PartitionResponse {
                    index: 0,
                    error_code: KafkaCode::None,
                    base_offset: 2,
                    log_append_time: -1,
                }],
            }],
        };

        let (_, parsed) =
            response::parse_produce_fetch_response(NomBytes::from(buf.as_slice())).unwrap();
        assert_eq!(parsed, res);
    }

    #[test]
    fn it_compresses_a_record_correctly() {
        let record = request::Record::new(request::Message { key: Some(Bytes::from("key")), value: Some(Bytes::from("value")), headers: vec![] }, 100, 100);

        let mut buf = Vec::with_capacity(10);

        record.encode(&mut buf).unwrap();
        println!("buf {:?}", buf);

        let compressed = compress(&buf).unwrap();
        println!("compressed {:?}", compressed);

        let uncompressed = uncompress(Bytes::from(compressed).as_ref()).unwrap();
        println!("uncompressed {:?}", uncompressed);

        assert_eq!(buf, uncompressed);
    }

    #[test]
    fn it_compresses_many_records_correctly() {
        let mut record_batch = request::RecordBatch::new(Attributes::new(Some(Compression::Gzip)));
        record_batch.add(request::Message { key: Some(Bytes::from("key")), value: Some(Bytes::from("1")), headers: vec![] });
        // record_batch.add(request::Message { key: Some(Bytes::from("key")), value: Some(Bytes::from("2")), headers: vec![] });
        // record_batch.add(request::Message { key: Some(Bytes::from("key")), value: Some(Bytes::from("3")), headers: vec![] });

        let mut buf = Vec::with_capacity(10);
        record_batch._encode_to_buf(&mut buf).unwrap();

        println!("{:?}", buf);
        let (s, unparsed_batch) = parse_record_batch(nombytes::NomBytes::new(Bytes::from(buf))).unwrap();
        assert_eq!(unparsed_batch.batch_length, 3);
        // assert_eq!(record_batch, unparsed_batch);
    }

}
