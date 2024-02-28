use crc::Crc;

pub fn to_crc(data: &[u8]) -> u32 {
    Crc::<u32>::new(&crc::CRC_32_ISO_HDLC).checksum(data)
}
