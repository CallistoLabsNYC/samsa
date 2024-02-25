//! Parsing and processing for Fetch responses.

use bytes::Bytes;
use nom::{
    bytes::complete::take,
    multi::many0,
    number::complete::{be_i16, be_i32, be_i64, be_i8},
    IResult,
};
use nombytes::NomBytes;

use crate::{
    error::{Error, KafkaCode, Result},
    parser,
    protocol::{parse_header_response, HeaderResponse},
};

/*
Fetch Response (Version: 4) => throttle_time_ms [responses]
  throttle_time_ms => INT32
  responses => topic [partitions]
    topic => STRING
    partitions => partition_index error_code high_watermark last_stable_offset [aborted_transactions] records
      partition_index => INT32
      error_code => INT16
      high_watermark => INT64
      last_stable_offset => INT64
      aborted_transactions => producer_id first_offset
        producer_id => INT64
        first_offset => INT64
      records => RECORD BATCH

RECORD BATCH
    baseOffset: int64
    batchLength: int32
    partitionLeaderEpoch: int32
    magic: int8 (current magic value is 2)
    crc: int32
    attributes: int16
        bit 0~2:
            0: no compression
            1: gzip
            2: snappy
            3: lz4
            4: zstd
        bit 3: timestampType
        bit 4: isTransactional (0 means not transactional)
        bit 5: isControlBatch (0 means not a control batch)
        bit 6: hasDeleteHorizonMs (0 means baseTimestamp is not set as the delete horizon for compaction)
        bit 7~15: unused
    lastOffsetDelta: int32
    baseTimestamp: int64
    maxTimestamp: int64
    producerId: int64
    producerEpoch: int16
    baseSequence: int32
    records: [Record]

Record
    length: varint
    attributes: int8
        bit 0~7: unused
    timestampDelta: varlong
    offsetDelta: varint
    keyLength: varint
    key: byte[]
    valueLen: varint
    value: byte[]
    Headers => [Header]

Header
    headerKeyLength: varint
    headerKey: String
    headerValueLength: varint
    Value: byte[]
*/

#[derive(Debug, Default, PartialEq)]
pub struct FetchResponse {
    pub header_response: HeaderResponse,
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    pub trottle_time: i32,
    /// The response topics.
    pub topics: Vec<Topic>,
}

// this helps us cast the server response into this type
impl TryFrom<Bytes> for FetchResponse {
    type Error = Error;

    fn try_from(s: Bytes) -> Result<Self> {
        tracing::trace!("Parsing FetchResponse {:?}", s);
        let (_, fetch_response) =
            parse_fetch_response(NomBytes::new(s.clone())).map_err(|err| {
                tracing::error!("ERROR: Failed parsing FetchResponse {:?}", err);
                tracing::error!("ERROR: FetchResponse Bytes {:?}", s);
                Error::ParsingError(s)
            })?;
        tracing::trace!("Parsed FetchResponse {:?}", fetch_response);
        Ok(fetch_response)
    }
}

/// The response topics.
#[derive(Debug, Clone, PartialEq)]
pub struct Topic {
    pub name: Bytes,
    pub partitions: Vec<Partition>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Partition {
    pub id: i32,
    pub error_code: KafkaCode,
    pub high_water_mark: i64,
    pub last_stable_offset: i64,
    pub aborted_transactions: Vec<AbortedTransactions>,
    pub record_batch: Vec<RecordBatch>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AbortedTransactions {
    pub producer_id: i64,
    pub first_offset: i64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RecordBatch {
    pub base_offset: i64,
    pub batch_length: i32,
    pub partition_leader_epoch: i32,
    pub magic: i8,
    pub crc: i32,
    pub attributes: i16,
    pub last_offset_delta: i32,
    pub base_timestamp: i64,
    pub max_timestamp: i64,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub base_sequence: i32,
    pub records: Vec<Record>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Record {
    pub length: usize,
    pub attributes: i8,
    pub timestamp_delta: usize,
    pub offset_delta: usize,
    pub key_length: usize,
    pub key: Bytes,
    pub value_len: usize,
    pub value: Bytes,
    pub headers: Vec<Header>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Header {
    pub header_key_length: usize,
    pub header_key: Bytes,
    pub header_value_length: usize,
    pub value: Bytes,
}

impl Partition {
    // this should probably be a type?
    pub fn into_box_iter(self) -> Box<impl Iterator<Item = (i32, KafkaCode, i64, i64, Record)>> {
        Box::new(self.record_batch.into_iter().flat_map(move |batch| {
            batch.records.into_iter().map(move |record| {
                (
                    self.id,
                    self.error_code,
                    batch.base_offset,
                    batch.base_timestamp,
                    record,
                )
            })
        }))
    }
}

pub fn parse_fetch_response(s: NomBytes) -> IResult<NomBytes, FetchResponse> {
    let (s, header_response) = parse_header_response(s)?;
    let (s, trottle_time) = be_i32::<NomBytes, nom::error::Error<NomBytes>>(s)?;
    let (s, topics) = parser::parse_array(parse_topic)(s)?;

    Ok((
        s,
        FetchResponse {
            header_response,
            trottle_time,
            topics,
        },
    ))
}

fn parse_topic(s: NomBytes) -> IResult<NomBytes, Topic> {
    let (s, name) = parser::parse_string(s)?;
    let (s, partitions) = parser::parse_array(parse_partition)(s)?;

    Ok((s, Topic { name, partitions }))
}

fn parse_partition(s: NomBytes) -> IResult<NomBytes, Partition> {
    let (s, id) = be_i32(s)?;
    let (s, error_code) = parser::parse_kafka_code(s)?;
    let (s, high_water_mark) = be_i64(s)?;
    let (s, last_stable_offset) = be_i64(s)?;
    let (s, aborted_transactions) = parser::parse_array(parse_aborted_transactions)(s)?;
    let (s, _) = be_i32(s)?;

    let (s, record_batch) = many0(parse_record_batch)(s)?;

    Ok((
        s,
        Partition {
            id,
            error_code,
            high_water_mark,
            last_stable_offset,
            aborted_transactions,
            record_batch,
        },
    ))
}

fn parse_aborted_transactions(s: NomBytes) -> IResult<NomBytes, AbortedTransactions> {
    let (s, producer_id) = be_i64(s)?;
    let (s, first_offset) = be_i64(s)?;

    Ok((
        s,
        AbortedTransactions {
            producer_id,
            first_offset,
        },
    ))
}

fn parse_record_batch(s: NomBytes) -> IResult<NomBytes, RecordBatch> {
    let (s, base_offset) = be_i64(s)?;
    let (s, batch_length) = be_i32(s)?;
    let (s, partition_leader_epoch) = be_i32(s)?;
    let (s, magic) = be_i8(s)?;
    let (s, crc) = be_i32(s)?;
    let (s, attributes) = be_i16(s)?;
    let (s, last_offset_delta) = be_i32(s)?;
    let (s, base_timestamp) = be_i64(s)?;
    let (s, max_timestamp) = be_i64(s)?;
    let (s, producer_id) = be_i64(s)?;
    let (s, producer_epoch) = be_i16(s)?;
    let (s, base_sequence) = be_i32(s)?;

    let (s, records) = parser::parse_array(parse_record)(s)?;

    Ok((
        s,
        RecordBatch {
            base_offset,
            batch_length,
            partition_leader_epoch,
            magic,
            crc,
            attributes,
            last_offset_delta,
            base_timestamp,
            max_timestamp,
            producer_id,
            producer_epoch,
            base_sequence,
            records,
        },
    ))
}

fn parse_record(s: NomBytes) -> IResult<NomBytes, Record> {
    let (s, length) = parser::take_varint(s)?;
    let (s, attributes) = be_i8(s)?;
    let (s, timestamp_delta) = parser::take_varint(s)?;
    let (s, offset_delta) = parser::take_varint(s)?;
    let (s, key_length) = parser::take_varint(s)?;
    let (s, key) = take(key_length / 2)(s)?;
    let (s, value_len) = parser::take_varint(s)?;
    let (s, value) = take(value_len / 2)(s)?;

    // let (s, headers) = parser::parse_array(parse_header)(s)?;
    let (s, _) = parser::take_varint(s)?;

    Ok((
        s,
        Record {
            length,
            attributes,
            timestamp_delta,
            offset_delta,
            key_length,
            key: key.into_bytes(),
            value_len,
            value: value.into_bytes(),
            headers: vec![],
        },
    ))
}
