//! Keep a member alive in the group.

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
            0, 12, 0, 0, 0, 0, 0, 1, 0, 4, 114, 117, 115, 116, 0, 8, 66, 105, 103, 32, 68, 111,
            103, 115, 0, 0, 0, 2, 0, 26, 97, 98, 99, 100, 101, 102, 103, 104, 105, 106, 107, 108,
            109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122,
        ];
        let correlation_id = 1;
        let client_id = "rust";
        let consumer_group_key = "Big Dogs";
        let generation_id = 2;
        let member_id = "abcdefghijklmnopqrstuvwxyz";

        let req = request::HeartbeatRequest::new(
            correlation_id,
            client_id,
            consumer_group_key,
            generation_id,
            member_id.into(),
        )
        .unwrap();

        let mut buffer: Vec<u8> = vec![];

        req.encode(&mut buffer).unwrap();

        assert_eq!(buffer, b);
    }

    #[test]
    fn parse() {
        let b = b"\0\0\0\x01\0\0";

        let res = response::HeartbeatResponse {
            header: protocol::HeaderResponse { correlation_id: 1 },
            error_code: KafkaCode::None,
        };

        let x = response::parse_heartbeat_response(NomBytes::new(Bytes::from_static(b)))
            .unwrap()
            .1;

        assert_eq!(res, x);
    }
}
