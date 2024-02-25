//! Directly depart a group.

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
            0, 13, 0, 0, 0, 0, 0, 1, 0, 4, 114, 117, 115, 116, 0, 8, 66, 105, 103, 32, 68, 111,
            103, 115, 0, 0,
        ];
        let correlation_id = 1;
        let client_id = "rust";
        let consumer_group_key = "Big Dogs";
        let member_id = "";

        let req = request::LeaveGroupRequest::new(
            correlation_id,
            client_id,
            consumer_group_key,
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

        let res = response::LeaveGroupResponse {
            header: protocol::HeaderResponse { correlation_id: 1 },
            error_code: KafkaCode::None,
        };

        let x = response::parse_leave_group_response(NomBytes::new(Bytes::from_static(b)))
            .unwrap()
            .1;

        assert_eq!(res, x);
    }
}
