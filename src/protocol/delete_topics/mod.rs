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
            0, 20, 0, 3, 0, 0, 0, 1, 0, 4, 114, 117, 115, 116, 0, 0, 0, 1, 0, 15, 116, 101, 115,
            116, 101, 114, 45, 99, 114, 101, 97, 116, 105, 111, 110, 0, 0, 7, 208,
        ];

        let correlation_id = 1;
        let client_id = "rust";
        let topic_name = "tester-creation";
        let timeout_ms = 2000;

        let mut req =
            request::DeleteTopicsRequest::new(correlation_id, client_id, timeout_ms).unwrap();

        req.add(topic_name);

        let mut buffer: Vec<u8> = vec![];

        req.encode(&mut buffer).unwrap();

        assert_eq!(buffer, b);
    }

    #[test]
    fn parse() {
        let b = b"\0\0\0\x01\0\0\0\0\0\0\0\x01\0\x0ftester-creation\0\0";

        let res = response::DeleteTopicsResponse {
            header: protocol::HeaderResponse { correlation_id: 1 },
            throttle_time_ms: 0,
            topics: vec![response::Topic {
                name: Bytes::from("tester-creation"),
                error_code: crate::prelude::KafkaCode::None,
            }],
        };

        let x = response::parse_delete_topics_response(NomBytes::new(Bytes::from_static(b)))
            .unwrap()
            .1;

        assert_eq!(res, x);
    }
}
