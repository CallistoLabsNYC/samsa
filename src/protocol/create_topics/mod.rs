pub mod request;
pub mod response;

/// Description of a topic to create.
#[derive(Clone, Debug)]
pub struct NewTopic {
    pub topic_name: String,
    pub num_partitions: request::NumPartitions,
    pub replication_factor: request::ReplicationFactor,
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::encode::ToByte;
    use crate::protocol;
    use bytes::Bytes;
    use nombytes::NomBytes;

    #[test]
    fn encode() {
        let b = [
            0, 19, 0, 3, 0, 0, 0, 1, 0, 4, 114, 117, 115, 116, 0, 0, 0, 1, 0, 15, 116, 101, 115,
            116, 101, 114, 45, 99, 114, 101, 97, 116, 105, 111, 110, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 7, 208, 0,
        ];

        let correlation_id = 1;
        let client_id = "rust";
        let topic_name = "tester-creation";
        let timeout_ms = 2000;
        let num_partitions = 1;

        let mut req =
            request::CreateTopicsRequest::new(correlation_id, client_id, timeout_ms, false)
                .unwrap();

        req.add(
            topic_name,
            request::NumPartitions::Count(num_partitions),
            request::ReplicationFactor::Factor(0),
        );

        let mut buffer: Vec<u8> = vec![];

        req.encode(&mut buffer).unwrap();

        assert_eq!(buffer, b);
    }

    #[test]
    fn parse() {
        let b = b"\0\0\0\x01\0\0\0\0\0\0\0\x01\0\x0ftester-creation\0\0\xff\xff";

        let res = response::CreateTopicsResponse {
            header: protocol::HeaderResponse { correlation_id: 1 },
            throttle_time_ms: 0,
            topics: vec![response::Topic {
                name: Bytes::from("tester-creation"),
                error_code: crate::prelude::KafkaCode::None,
                error_message: None,
            }],
        };

        let x = response::parse_create_topics_response(NomBytes::new(Bytes::from_static(b)))
            .unwrap()
            .1;

        assert_eq!(res, x);
    }
}
