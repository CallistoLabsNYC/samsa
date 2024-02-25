//! Fetch messages from a broker.

pub mod request;
pub mod response;
pub mod response_optimized;

#[cfg(test)]
mod test {
    use bytes::Bytes;
    use nombytes::NomBytes;

    use super::*;
    use crate::{encode::ToByte, error::KafkaCode, protocol};

    #[test]
    fn encode() {
        let b = [
            0, 1, 0, 4, 0, 0, 0, 1, 0, 4, 114, 117, 115, 116, 255, 255, 255, 255, 0, 0, 7, 208, 0,
            0, 0, 100, 0, 0, 117, 48, 0, 0, 0, 0, 1, 0, 9, 112, 117, 114, 99, 104, 97, 115, 101,
            115, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 117, 48, 0, 0, 117, 48,
        ];

        let correlation_id = 1;
        let client_id = "rust";
        let max_wait_ms = 2000;
        let min_bytes = 100;
        let max_bytes = 30000;
        let isolation_level = 0;
        let partition = 1;
        let topic_name = "purchases";
        let committed_offset = 30000;

        let mut req = request::FetchRequest::new(
            correlation_id,
            client_id,
            max_wait_ms,
            min_bytes,
            max_bytes,
            isolation_level,
        );

        // for partition in partitions {
        req.add(topic_name, partition, committed_offset, max_bytes);
        // }

        let mut buffer: Vec<u8> = vec![];

        req.encode(&mut buffer).unwrap();

        assert_eq!(buffer, b);
    }

    #[test]
    fn parse() {
        let b = b"\0\0\0\x01\0\0\0\0\0\0\0\x01\0\rprice-updates\0\0\0\x01\0\0\0\0\0\0\0\0\0\0\0\0\0\x0e\0\0\0\0\0\0\0\x0e\xff\xff\xff\xff\0\0\x0e\xde\0\0\0\0\0\0\0\0\0\0\x01\x04\0\0\0\x01\x02\xd7\x8d\xc7G\0\0\0\0\0\0\0\0\x01\x8bH \xef\xc0\0\0\x01\x8bH \xef\xc0\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\0\0\0\x01\xa2\x03\0\0\0\x08TSLA\x8c\x03{\"symbol\": \"TSLA\", \"timestamp\": 1697722200000, \"open\": 225.56, \"high\": 227.17, \"low\": 224.44, \"close\": 227.17, \"volume\": 24265.0, \"trade_count\": 502.0, \"vwap\": 225.508012, \"data_provider\": \"alpaca\"}\0\0\0\0\0\0\0\0\x01\0\0\x01\x07\0\0\0\x01\x02\x0e\xbd[\xd6\0\0\0\0\0\0\0\0\x01\x8bH!\xda \0\0\x01\x8bH!\xda \xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\0\0\0\x01\xa8\x03\0\0\0\x08TSLA\x92\x03{\"symbol\": \"TSLA\", \"timestamp\": 1697722260000, \"open\": 227.215, \"high\": 228.88, \"low\": 226.955, \"close\": 228.845, \"volume\": 28919.0, \"trade_count\": 303.0, \"vwap\": 227.811826, \"data_provider\": \"alpaca\"}\0\0\0\0\0\0\0\0\x02\0\0\x01\x06\0\0\0\x01\x02\x85\xc3\xb3\xb3\0\0\0\0\0\0\0\0\x01\x8bH\"\xc4\x80\0\0\x01\x8bH\"\xc4\x80\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\0\0\0\x01\xa6\x03\0\0\0\x08TSLA\x90\x03{\"symbol\": \"TSLA\", \"timestamp\": 1697722320000, \"open\": 229.12, \"high\": 230.17, \"low\": 227.915, \"close\": 230.165, \"volume\": 33891.0, \"trade_count\": 390.0, \"vwap\": 229.520416, \"data_provider\": \"alpaca\"}\0\0\0\0\0\0\0\0\x03\0\0\x01\x05\0\0\0\x01\x02\xea&\xce\x0f\0\0\0\0\0\0\0\0\x01\x8bH#\xae\xe0\0\0\x01\x8bH#\xae\xe0\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\0\0\0\x01\xa4\x03\0\0\0\x08TSLA\x8e\x03{\"symbol\": \"TSLA\", \"timestamp\": 1697722380000, \"open\": 230.21, \"high\": 230.525, \"low\": 229.13, \"close\": 229.22, \"volume\": 33625.0, \"trade_count\": 401.0, \"vwap\": 229.998015, \"data_provider\": \"alpaca\"}\0\0\0\0\0\0\0\0\x04\0\0\x01\x05\0\0\0\x01\x02s\x95\x0c\x8f\0\0\0\0\0\0\0\0\x01\x8bH$\x99@\0\0\x01\x8bH$\x99@\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\0\0\0\x01\xa4\x03\0\0\0\x08TSLA\x8e\x03{\"symbol\": \"TSLA\", \"timestamp\": 1697722440000, \"open\": 228.84, \"high\": 229.305, \"low\": 227.93, \"close\": 228.44, \"volume\": 26574.0, \"trade_count\": 362.0, \"vwap\": 228.548357, \"data_provider\": \"alpaca\"}\0\0\0\0\0\0\0\0\x05\0\0\x01\x04\0\0\0\x01\x029@Eu\0\0\0\0\0\0\0\0\x01\x8bH%\x83\xa0\0\0\x01\x8bH%\x83\xa0\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\0\0\0\x01\xa2\x03\0\0\0\x08TSLA\x8c\x03{\"symbol\": \"TSLA\", \"timestamp\": 1697722500000, \"open\": 228.53, \"high\": 229.22, \"low\": 228.3, \"close\": 228.995, \"volume\": 11997.0, \"trade_count\": 142.0, \"vwap\": 228.818005, \"data_provider\": \"alpaca\"}\0\0\0\0\0\0\0\0\x06\0\0\x01\x03\0\0\0\x01\x02\xf5k\x0c\x83\0\0\0\0\0\0\0\0\x01\x8bH&n\0\0\0\x01\x8bH&n\0\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\0\0\0\x01\xa0\x03\0\0\0\x08TSLA\x8a\x03{\"symbol\": \"TSLA\", \"timestamp\": 1697722560000, \"open\": 228.88, \"high\": 229.4, \"low\": 228.3, \"close\": 228.375, \"volume\": 17851.0, \"trade_count\": 259.0, \"vwap\": 228.727112, \"data_provider\": \"alpaca\"}\0\0\0\0\0\0\0\0\x07\0\0\x01\x05\0\0\0\x01\x02\x9bu\x82,\0\0\0\0\0\0\0\0\x01\x8bH'X`\0\0\x01\x8bH'X`\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\0\0\0\x01\xa4\x03\0\0\0\x08TSLA\x8e\x03{\"symbol\": \"TSLA\", \"timestamp\": 1697722620000, \"open\": 228.39, \"high\": 228.39, \"low\": 226.89, \"close\": 227.425, \"volume\": 12807.0, \"trade_count\": 254.0, \"vwap\": 227.514886, \"data_provider\": \"alpaca\"}\0\0\0\0\0\0\0\0\x08\0\0\x01\x03\0\0\0\x01\x02\xdcI\xc6\xc9\0\0\0\0\0\0\0\0\x01\x8bH(B\xc0\0\0\x01\x8bH(B\xc0\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\0\0\0\x01\xa0\x03\0\0\0\x08TSLA\x8a\x03{\"symbol\": \"TSLA\", \"timestamp\": 1697722680000, \"open\": 227.13, \"high\": 228.53, \"low\": 226.78, \"close\": 228.53, \"volume\": 7273.0, \"trade_count\": 123.0, \"vwap\": 227.633268, \"data_provider\": \"alpaca\"}\0\0\0\0\0\0\0\0\t\0\0\x01\x05\0\0\0\x01\x02\xf9\xd5\x0f\xd7\0\0\0\0\0\0\0\0\x01\x8bH+\xec@\0\0\x01\x8bH+\xec@\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\0\0\0\x01\xa4\x03\0\0\0\x08TSLA\x8e\x03{\"symbol\": \"TSLA\", \"timestamp\": 1697722920000, \"open\": 225.41, \"high\": 226.87, \"low\": 225.22, \"close\": 226.045, \"volume\": 10062.0, \"trade_count\": 159.0, \"vwap\": 226.119019, \"data_provider\": \"alpaca\"}\0\0\0\0\0\0\0\0\n\0\0\x01\x03\0\0\0\x01\x02KhN\x01\0\0\0\0\0\0\0\0\x01\x8bH,\xd6\xa0\0\0\x01\x8bH,\xd6\xa0\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\0\0\0\x01\xa0\x03\0\0\0\x08TSLA\x8a\x03{\"symbol\": \"TSLA\", \"timestamp\": 1697722980000, \"open\": 226.05, \"high\": 226.69, \"low\": 225.45, \"close\": 225.45, \"volume\": 7281.0, \"trade_count\": 129.0, \"vwap\": 225.980049, \"data_provider\": \"alpaca\"}\0\0\0\0\0\0\0\0\x0b\0\0\x01\x01\0\0\0\x01\x02\xe8\xd9yi\0\0\0\0\0\0\0\0\x01\x8bHI8@\0\0\x01\x8bHI8@\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\0\0\0\x01\x9c\x03\0\0\0\x08TSLA\x86\x03{\"symbol\": \"TSLA\", \"timestamp\": 1697724840000, \"open\": 225.89, \"high\": 226.0, \"low\": 225.46, \"close\": 225.47, \"volume\": 3886.0, \"trade_count\": 90.0, \"vwap\": 225.741834, \"data_provider\": \"alpaca\"}\0\0\0\0\0\0\0\0\x0c\0\0\x01\x01\0\0\0\x01\x02\xb2`\x9e\x15\0\0\0\0\0\0\0\0\x01\x8bHJ\"\xa0\0\0\x01\x8bHJ\"\xa0\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\0\0\0\x01\x9c\x03\0\0\0\x08TSLA\x86\x03{\"symbol\": \"TSLA\", \"timestamp\": 1697724900000, \"open\": 225.7, \"high\": 225.96, \"low\": 225.34, \"close\": 225.55, \"volume\": 3588.0, \"trade_count\": 74.0, \"vwap\": 225.642698, \"data_provider\": \"alpaca\"}\0\0\0\0\0\0\0\0\r\0\0\x01\x02\0\0\0\x01\x02\xb4\x02\xa4\x1c\0\0\0\0\0\0\0\0\x01\x8bHK\r\0\0\0\x01\x8bHK\r\0\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\0\0\0\x01\x9e\x03\0\0\0\x08TSLA\x88\x03{\"symbol\": \"TSLA\", \"timestamp\": 1697724960000, \"open\": 225.55, \"high\": 225.55, \"low\": 225.07, \"close\": 225.07, \"volume\": 1674.0, \"trade_count\": 38.0, \"vwap\": 225.256195, \"data_provider\": \"alpaca\"}\0";

        let res = response::FetchResponse {
             header_response: protocol::HeaderResponse {
             correlation_id: 1 }, trottle_time: 0, topics: vec![response::Topic {
             name: Bytes::from_static(b"price-updates"), partitions: vec![response::Partition {
             id: 0, error_code: KafkaCode::None, high_water_mark: 14, last_stable_offset: 14, aborted_transactions: vec![], record_batch: vec![response::RecordBatch {
             base_offset: 0, batch_length: 260, partition_leader_epoch: 1, magic: 2, crc: -678574265, attributes: 0, last_offset_delta: 0, base_timestamp: 1697722200000, max_timestamp: 1697722200000, producer_id: -1, producer_epoch: -1, base_sequence: -1, records: vec![response::Record {
             length: 418, attributes: 0, timestamp_delta: 0, offset_delta: 0, key_length: 8, key: Bytes::from_static(b"TSLA"), value_len: 396, value: Bytes::from_static(b"{\"symbol\": \"TSLA\", \"timestamp\": 1697722200000, \"open\": 225.56, \"high\": 227.17, \"low\": 224.44, \"close\": 227.17, \"volume\": 24265.0, \"trade_count\": 502.0, \"vwap\": 225.508012, \"data_provider\": \"alpaca\"}"), headers: vec![] }] }, response::RecordBatch {
             base_offset: 1, batch_length: 263, partition_leader_epoch: 1, magic: 2, crc: 247290838, attributes: 0, last_offset_delta: 0, base_timestamp: 1697722260000, max_timestamp: 1697722260000, producer_id: -1, producer_epoch: -1, base_sequence: -1, records: vec![response::Record {
             length: 424, attributes: 0, timestamp_delta: 0, offset_delta: 0, key_length: 8, key: Bytes::from_static(b"TSLA"), value_len: 402, value: Bytes::from_static(b"{\"symbol\": \"TSLA\", \"timestamp\": 1697722260000, \"open\": 227.215, \"high\": 228.88, \"low\": 226.955, \"close\": 228.845, \"volume\": 28919.0, \"trade_count\": 303.0, \"vwap\": 227.811826, \"data_provider\": \"alpaca\"}"), headers: vec![] }] }, response::RecordBatch {
             base_offset: 2, batch_length: 262, partition_leader_epoch: 1, magic: 2, crc: -2050772045, attributes: 0, last_offset_delta: 0, base_timestamp: 1697722320000, max_timestamp: 1697722320000, producer_id: -1, producer_epoch: -1, base_sequence: -1, records: vec![response::Record {
             length: 422, attributes: 0, timestamp_delta: 0, offset_delta: 0, key_length: 8, key: Bytes::from_static(b"TSLA"), value_len: 400, value: Bytes::from_static(b"{\"symbol\": \"TSLA\", \"timestamp\": 1697722320000, \"open\": 229.12, \"high\": 230.17, \"low\": 227.915, \"close\": 230.165, \"volume\": 33891.0, \"trade_count\": 390.0, \"vwap\": 229.520416, \"data_provider\": \"alpaca\"}"), headers: vec![] }] }, response::RecordBatch {
             base_offset: 3, batch_length: 261, partition_leader_epoch: 1, magic: 2, crc: -366555633, attributes: 0, last_offset_delta: 0, base_timestamp: 1697722380000, max_timestamp: 1697722380000, producer_id: -1, producer_epoch: -1, base_sequence: -1, records: vec![response::Record {
             length: 420, attributes: 0, timestamp_delta: 0, offset_delta: 0, key_length: 8, key: Bytes::from_static(b"TSLA"), value_len: 398, value: Bytes::from_static(b"{\"symbol\": \"TSLA\", \"timestamp\": 1697722380000, \"open\": 230.21, \"high\": 230.525, \"low\": 229.13, \"close\": 229.22, \"volume\": 33625.0, \"trade_count\": 401.0, \"vwap\": 229.998015, \"data_provider\": \"alpaca\"}"), headers: vec![] }] }, response::RecordBatch {
             base_offset: 4, batch_length: 261, partition_leader_epoch: 1, magic: 2, crc: 1939147919, attributes: 0, last_offset_delta: 0, base_timestamp: 1697722440000, max_timestamp: 1697722440000, producer_id: -1, producer_epoch: -1, base_sequence: -1, records: vec![response::Record {
             length: 420, attributes: 0, timestamp_delta: 0, offset_delta: 0, key_length: 8, key: Bytes::from_static(b"TSLA"), value_len: 398, value: Bytes::from_static(b"{\"symbol\": \"TSLA\", \"timestamp\": 1697722440000, \"open\": 228.84, \"high\": 229.305, \"low\": 227.93, \"close\": 228.44, \"volume\": 26574.0, \"trade_count\": 362.0, \"vwap\": 228.548357, \"data_provider\": \"alpaca\"}"), headers: vec![] }] }, response::RecordBatch {
             base_offset: 5, batch_length: 260, partition_leader_epoch: 1, magic: 2, crc: 960513397, attributes: 0, last_offset_delta: 0, base_timestamp: 1697722500000, max_timestamp: 1697722500000, producer_id: -1, producer_epoch: -1, base_sequence: -1, records: vec![response::Record {
             length: 418, attributes: 0, timestamp_delta: 0, offset_delta: 0, key_length: 8, key: Bytes::from_static(b"TSLA"), value_len: 396, value: Bytes::from_static(b"{\"symbol\": \"TSLA\", \"timestamp\": 1697722500000, \"open\": 228.53, \"high\": 229.22, \"low\": 228.3, \"close\": 228.995, \"volume\": 11997.0, \"trade_count\": 142.0, \"vwap\": 228.818005, \"data_provider\": \"alpaca\"}"), headers: vec![] }] }, response::RecordBatch {
             base_offset: 6, batch_length: 259, partition_leader_epoch: 1, magic: 2, crc: -177533821, attributes: 0, last_offset_delta: 0, base_timestamp: 1697722560000, max_timestamp: 1697722560000, producer_id: -1, producer_epoch: -1, base_sequence: -1, records: vec![response::Record {
             length: 416, attributes: 0, timestamp_delta: 0, offset_delta: 0, key_length: 8, key: Bytes::from_static(b"TSLA"), value_len: 394, value: Bytes::from_static(b"{\"symbol\": \"TSLA\", \"timestamp\": 1697722560000, \"open\": 228.88, \"high\": 229.4, \"low\": 228.3, \"close\": 228.375, \"volume\": 17851.0, \"trade_count\": 259.0, \"vwap\": 228.727112, \"data_provider\": \"alpaca\"}"), headers: vec![] }] }, response::RecordBatch {
             base_offset: 7, batch_length: 261, partition_leader_epoch: 1, magic: 2, crc: -1686797780, attributes: 0, last_offset_delta: 0, base_timestamp: 1697722620000, max_timestamp: 1697722620000, producer_id: -1, producer_epoch: -1, base_sequence: -1, records: vec![response::Record {
             length: 420, attributes: 0, timestamp_delta: 0, offset_delta: 0, key_length: 8, key: Bytes::from_static(b"TSLA"), value_len: 398, value: Bytes::from_static(b"{\"symbol\": \"TSLA\", \"timestamp\": 1697722620000, \"open\": 228.39, \"high\": 228.39, \"low\": 226.89, \"close\": 227.425, \"volume\": 12807.0, \"trade_count\": 254.0, \"vwap\": 227.514886, \"data_provider\": \"alpaca\"}"), headers: vec![] }] }, response::RecordBatch {
             base_offset: 8, batch_length: 259, partition_leader_epoch: 1, magic: 2, crc: -599144759, attributes: 0, last_offset_delta: 0, base_timestamp: 1697722680000, max_timestamp: 1697722680000, producer_id: -1, producer_epoch: -1, base_sequence: -1, records: vec![response::Record {
             length: 416, attributes: 0, timestamp_delta: 0, offset_delta: 0, key_length: 8, key: Bytes::from_static(b"TSLA"), value_len: 394, value: Bytes::from_static(b"{\"symbol\": \"TSLA\", \"timestamp\": 1697722680000, \"open\": 227.13, \"high\": 228.53, \"low\": 226.78, \"close\": 228.53, \"volume\": 7273.0, \"trade_count\": 123.0, \"vwap\": 227.633268, \"data_provider\": \"alpaca\"}"), headers: vec![] }] }, response::RecordBatch {
             base_offset: 9, batch_length: 261, partition_leader_epoch: 1, magic: 2, crc: -103477289, attributes: 0, last_offset_delta: 0, base_timestamp: 1697722920000, max_timestamp: 1697722920000, producer_id: -1, producer_epoch: -1, base_sequence: -1, records: vec![response::Record {
             length: 420, attributes: 0, timestamp_delta: 0, offset_delta: 0, key_length: 8, key: Bytes::from_static(b"TSLA"), value_len: 398, value: Bytes::from_static(b"{\"symbol\": \"TSLA\", \"timestamp\": 1697722920000, \"open\": 225.41, \"high\": 226.87, \"low\": 225.22, \"close\": 226.045, \"volume\": 10062.0, \"trade_count\": 159.0, \"vwap\": 226.119019, \"data_provider\": \"alpaca\"}"), headers: vec![] }] }, response::RecordBatch {
             base_offset: 10, batch_length: 259, partition_leader_epoch: 1, magic: 2, crc: 1265126913, attributes: 0, last_offset_delta: 0, base_timestamp: 1697722980000, max_timestamp: 1697722980000, producer_id: -1, producer_epoch: -1, base_sequence: -1, records: vec![response::Record {
             length: 416, attributes: 0, timestamp_delta: 0, offset_delta: 0, key_length: 8, key: Bytes::from_static(b"TSLA"), value_len: 394, value: Bytes::from_static(b"{\"symbol\": \"TSLA\", \"timestamp\": 1697722980000, \"open\": 226.05, \"high\": 226.69, \"low\": 225.45, \"close\": 225.45, \"volume\": 7281.0, \"trade_count\": 129.0, \"vwap\": 225.980049, \"data_provider\": \"alpaca\"}"), headers: vec![] }] }, response::RecordBatch {
             base_offset: 11, batch_length: 257, partition_leader_epoch: 1, magic: 2, crc: -388400791, attributes: 0, last_offset_delta: 0, base_timestamp: 1697724840000, max_timestamp: 1697724840000, producer_id: -1, producer_epoch: -1, base_sequence: -1, records: vec![response::Record {
             length: 412, attributes: 0, timestamp_delta: 0, offset_delta: 0, key_length: 8, key: Bytes::from_static(b"TSLA"), value_len: 390, value: Bytes::from_static(b"{\"symbol\": \"TSLA\", \"timestamp\": 1697724840000, \"open\": 225.89, \"high\": 226.0, \"low\": 225.46, \"close\": 225.47, \"volume\": 3886.0, \"trade_count\": 90.0, \"vwap\": 225.741834, \"data_provider\": \"alpaca\"}"), headers: vec![] }] }, response::RecordBatch {
             base_offset: 12, batch_length: 257, partition_leader_epoch: 1, magic: 2, crc: -1302290923, attributes: 0, last_offset_delta: 0, base_timestamp: 1697724900000, max_timestamp: 1697724900000, producer_id: -1, producer_epoch: -1, base_sequence: -1, records: vec![response::Record {
             length: 412, attributes: 0, timestamp_delta: 0, offset_delta: 0, key_length: 8, key: Bytes::from_static(b"TSLA"), value_len: 390, value: Bytes::from_static(b"{\"symbol\": \"TSLA\", \"timestamp\": 1697724900000, \"open\": 225.7, \"high\": 225.96, \"low\": 225.34, \"close\": 225.55, \"volume\": 3588.0, \"trade_count\": 74.0, \"vwap\": 225.642698, \"data_provider\": \"alpaca\"}"), headers: vec![] }] }, response::RecordBatch {
             base_offset: 13, batch_length: 258, partition_leader_epoch: 1, magic: 2, crc: -1274895332, attributes: 0, last_offset_delta: 0, base_timestamp: 1697724960000, max_timestamp: 1697724960000, producer_id: -1, producer_epoch: -1, base_sequence: -1, records: vec![response::Record {
             length: 414, attributes: 0, timestamp_delta: 0, offset_delta: 0, key_length: 8, key: Bytes::from_static(b"TSLA"), value_len: 392, value: Bytes::from_static(b"{\"symbol\": \"TSLA\", \"timestamp\": 1697724960000, \"open\": 225.55, \"high\": 225.55, \"low\": 225.07, \"close\": 225.07, \"volume\": 1674.0, \"trade_count\": 38.0, \"vwap\": 225.256195, \"data_provider\": \"alpaca\"}"), headers: vec![] }] }] }] }] };

        let x = response::parse_fetch_response(NomBytes::new(Bytes::from_static(b)))
            .unwrap()
            .1;

        assert_eq!(res, x);
    }

    #[test]
    fn add_to_req() {
        let correlation_id = 1;
        let client_id = "rust";
        let max_wait_ms = 2000;
        let min_bytes = 100;
        let max_bytes = 30000;
        let isolation_level = 0;
        let partitions = vec![1, 2, 3, 4];
        let topic_name = "purchases";
        let committed_offset = 30000;

        let mut req = request::FetchRequest::new(
            correlation_id,
            client_id,
            max_wait_ms,
            min_bytes,
            max_bytes,
            isolation_level,
        );

        for partition in partitions.clone() {
            req.add(topic_name, partition, committed_offset, max_bytes);
        }

        // wont register duplicates
        for partition in partitions.clone() {
            req.add(topic_name, partition, committed_offset, max_bytes);
        }

        assert_eq!(req.topics.len(), 1);
        assert_eq!(req.topics[0].topic_name, topic_name);
        assert_eq!(req.topics[0].partitions.len(), 4);

        for partition in req.topics[0].partitions.iter() {
            assert_eq!(partition.offset, committed_offset);
            assert_eq!(partition.max_bytes, max_bytes);
        }
    }
}
