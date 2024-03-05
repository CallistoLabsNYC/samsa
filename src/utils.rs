use crc::Crc;
use std::time::{SystemTime, UNIX_EPOCH};

pub fn to_crc(data: &[u8]) -> u32 {
    Crc::<u32>::new(&crc::CRC_32_ISO_HDLC).checksum(data)
}

pub fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_millis() as i64
}