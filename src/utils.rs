use crc::Crc;
use std::io::{Read, Write};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::prelude::{Error, Result};
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;

pub fn to_crc(data: &[u8]) -> u32 {
    Crc::<u32>::new(&crc::CRC_32_ISCSI).checksum(data)
}

pub fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_millis() as i64
}

pub fn compress(src: &[u8]) -> Result<Vec<u8>> {
    let mut e = GzEncoder::new(Vec::new(), Compression::best());

    e.write_all(src).map_err(|e| Error::IoError(e.kind()))?;
    e.finish().map_err(|e| Error::IoError(e.kind()))
}

pub fn uncompress<T: Read>(src: T) -> Result<Vec<u8>> {
    let mut d = GzDecoder::new(src);

    let mut buffer: Vec<u8> = Vec::new();
    d.read_to_end(&mut buffer)
        .map_err(|e| {
            tracing::error!("Error uncompressing buffer {:?}", e);
            Error::IoError(e.kind())
        })?;
    Ok(buffer)
}

#[test]
fn test_uncompress() {
    use std::io::Cursor;
    // The vector should uncompress to "test"
    let msg: Vec<u8> = vec![
        31, 139, 8, 0, 192, 248, 79, 85, 2, 255, 43, 73, 45, 46, 1, 0, 12, 126, 127, 216, 4, 0, 0,
        0,
    ];
    let uncomp_msg = String::from_utf8(uncompress(Cursor::new(msg)).unwrap()).unwrap();
    assert_eq!(&uncomp_msg[..], "test");
}

#[test]
#[should_panic]
fn test_uncompress_panic() {
    use std::io::Cursor;
    let msg: Vec<u8> = vec![
        12, 42, 84, 104, 105, 115, 32, 105, 115, 32, 116, 101, 115, 116,
    ];
    let uncomp_msg = String::from_utf8(uncompress(Cursor::new(msg)).unwrap()).unwrap();
    assert_eq!(&uncomp_msg[..], "This is test");
}
