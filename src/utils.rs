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
    d.read_to_end(&mut buffer).map_err(|e| {
        tracing::error!("Error uncompressing buffer {:?}", e);
        Error::IoError(e.kind())
    })?;
    Ok(buffer)
}

pub fn compress_snappy(src: &[u8]) -> Result<Vec<u8>> {
    snap::raw::Encoder::new()
        .compress_vec(src)
        .map_err(|e| {
            tracing::error!("Error compressing with Snappy: {:?}", e);
            Error::IoError(std::io::ErrorKind::Other)
        })
}

pub fn uncompress_snappy(src: &[u8]) -> Result<Vec<u8>> {
    snap::raw::Decoder::new()
        .decompress_vec(src)
        .map_err(|e| {
            tracing::error!("Error decompressing Snappy: {:?}", e);
            Error::IoError(std::io::ErrorKind::Other)
        })
}

pub fn compress_lz4(src: &[u8]) -> Result<Vec<u8>> {
    Ok(lz4_flex::compress_prepend_size(src))
}

pub fn uncompress_lz4(src: &[u8]) -> Result<Vec<u8>> {
    lz4_flex::decompress_size_prepended(src)
        .map_err(|e| {
            tracing::error!("Error decompressing LZ4: {:?}", e);
            Error::IoError(std::io::ErrorKind::Other)
        })
}

pub fn compress_zstd(src: &[u8]) -> Result<Vec<u8>> {
    use ruzstd::encoding::{compress_to_vec, CompressionLevel};
    
    let compressed = compress_to_vec(src, CompressionLevel::Fastest);
    Ok(compressed)
}

pub fn uncompress_zstd(src: &[u8]) -> Result<Vec<u8>> {
    use ruzstd::decoding::StreamingDecoder;
    use std::io::Read;
    
    let mut decoder = StreamingDecoder::new(src)
        .map_err(|e| {
            tracing::error!("Error creating Zstd decoder: {:?}", e);
            Error::IoError(std::io::ErrorKind::Other)
        })?;
    
    let mut result = Vec::new();
    decoder.read_to_end(&mut result)
        .map_err(|e| {
            tracing::error!("Error decompressing Zstd: {:?}", e);
            Error::IoError(std::io::ErrorKind::Other)
        })?;
    Ok(result)
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

#[test]
fn test_compress_snappy() {
    let data = b"test data for snappy compression";
    let compressed = compress_snappy(data).unwrap();
    let decompressed = uncompress_snappy(&compressed).unwrap();
    assert_eq!(data, decompressed.as_slice());
}

#[test]
fn test_compress_lz4() {
    let data = b"test data for lz4 compression";
    let compressed = compress_lz4(data).unwrap();
    let decompressed = uncompress_lz4(&compressed).unwrap();
    assert_eq!(data, decompressed.as_slice());
}

#[test]
fn test_compress_zstd() {
    let data = b"test data for zstd compression";
    let compressed = compress_zstd(data).unwrap();
    let decompressed = uncompress_zstd(&compressed).unwrap();
    assert_eq!(data, decompressed.as_slice());
}

#[test]
fn test_all_compression_roundtrip() {
    let test_data = b"This is a longer test message that should compress well and demonstrate that all compression methods work correctly with round-trip compression and decompression.";
    
    // Test Gzip
    let gzip_compressed = compress(test_data).unwrap();
    let gzip_decompressed = uncompress(std::io::Cursor::new(gzip_compressed)).unwrap();
    assert_eq!(test_data, gzip_decompressed.as_slice());
    
    // Test Snappy
    let snappy_compressed = compress_snappy(test_data).unwrap();
    let snappy_decompressed = uncompress_snappy(&snappy_compressed).unwrap();
    assert_eq!(test_data, snappy_decompressed.as_slice());
    
    // Test LZ4
    let lz4_compressed = compress_lz4(test_data).unwrap();
    let lz4_decompressed = uncompress_lz4(&lz4_compressed).unwrap();
    assert_eq!(test_data, lz4_decompressed.as_slice());
    
    // Test Zstd
    let zstd_compressed = compress_zstd(test_data).unwrap();
    let zstd_decompressed = uncompress_zstd(&zstd_compressed).unwrap();
    assert_eq!(test_data, zstd_decompressed.as_slice());
}
