use bytes::Bytes;
use nom::{
    bytes::complete::take,
    number::complete::{be_i16, be_i32, be_i64, be_i8},
    IResult,
};
use nombytes::NomBytes;

use crate::{
    parser,
    protocol::{parse_header_response, HeaderResponse},
};

#[derive(Debug, Default, PartialEq)]
pub struct FetchResponse<'a> {
    pub header_response: HeaderResponse,
    pub throttle_time: i32,
    pub topics: &'a mut [Topic<'a>],
}

#[derive(Debug, Default, PartialEq)]
pub struct Topic<'a> {
    pub name: Bytes,
    pub partitions: &'a mut [Partition<'a>],
}

#[derive(Debug, Default, PartialEq)]
pub struct Partition<'a> {
    pub id: i32,
    pub error_code: i16,
    pub high_water_mark: i64,
    pub last_stable_offset: i64,
    pub aborted_transactions: Vec<AbortedTransactions>,
    pub record_batch: &'a mut [RecordBatch<'a>],
}

#[derive(Debug, Default, PartialEq)]
pub struct AbortedTransactions {
    pub producer_id: i64,
    pub first_offset: i64,
}

#[derive(Debug, Default, PartialEq)]
pub struct RecordBatch<'a> {
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
    pub records: &'a mut [Record],
}

#[derive(Clone, Debug, Default, PartialEq)]
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

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Header {
    pub header_key_length: usize,
    pub header_key: Bytes,
    pub header_value_length: usize,
    pub value: Bytes,
}

pub fn parse_fetch_response(
    s: NomBytes,
    fetch_response: &mut FetchResponse,
) -> IResult<NomBytes, ()> {
    let (s, header_response) = parse_header_response(s)?;
    fetch_response.header_response = header_response;

    let (s, throttle_time) = be_i32(s)?;
    fetch_response.throttle_time = throttle_time;

    let (s, _) = be_i32(s)?;

    let mut s = s;
    for topic in fetch_response.topics.iter_mut() {
        (s, _) = parse_topic(s, topic)?;
    }

    Ok((s, ()))
}

fn parse_topic(s: NomBytes, topic: &mut Topic) -> IResult<NomBytes, ()> {
    let (s, name) = parser::parse_string(s)?;
    topic.name = name;

    let (s, _) = be_i32(s)?;
    let mut s = s;
    for partition in topic.partitions.iter_mut() {
        (s, _) = parse_partition(s, partition)?;
    }

    Ok((s, ()))
}

fn parse_partition(s: NomBytes, partition: &mut Partition) -> IResult<NomBytes, ()> {
    let (s, id) = be_i32(s)?;
    partition.id = id;

    let (s, error_code) = be_i16(s)?;
    partition.error_code = error_code;

    let (s, high_water_mark) = be_i64(s)?;
    partition.high_water_mark = high_water_mark;

    let (s, last_stable_offset) = be_i64(s)?;
    partition.last_stable_offset = last_stable_offset;

    let (s, aborted_transactions) = parser::parse_array(parse_aborted_transactions)(s)?;
    partition.aborted_transactions = aborted_transactions;

    let (s, _) = be_i32(s)?;
    // let (s, record_batch) = many0(parse_record_batch)(s)?;

    let mut s = s;
    for batch in partition.record_batch.iter_mut() {
        match parse_record_batch(s.clone(), batch) {
            Ok((i, _)) => s = i,
            Err(nom::Err::Error(_)) => break,
            e => return e,
        }
    }

    Ok((s, ()))
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

fn parse_record_batch(s: NomBytes, record_batch: &mut RecordBatch) -> IResult<NomBytes, ()> {
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
    // let (s, records) = parser::parse_array(parse_record)(s)?;

    let (s, _) = be_i32(s)?;
    let mut s = s;
    for record in record_batch.records.iter_mut() {
        (s, _) = parse_record(s, record)?;
    }

    record_batch.base_offset = base_offset;
    record_batch.batch_length = batch_length;
    record_batch.partition_leader_epoch = partition_leader_epoch;
    record_batch.magic = magic;
    record_batch.crc = crc;
    record_batch.attributes = attributes;
    record_batch.last_offset_delta = last_offset_delta;
    record_batch.base_timestamp = base_timestamp;
    record_batch.max_timestamp = max_timestamp;
    record_batch.producer_id = producer_id;
    record_batch.producer_epoch = producer_epoch;
    record_batch.base_sequence = base_sequence;

    Ok((s, ()))
}

fn parse_record(s: NomBytes, record: &mut Record) -> IResult<NomBytes, ()> {
    tracing::trace!("parsing record");
    tracing::trace!("{:?}", s);

    let (s, length) = parser::take_varint(s)?;
    let (s, attributes) = be_i8(s)?;
    let (s, timestamp_delta) = parser::take_varint(s)?;
    let (s, offset_delta) = parser::take_varint(s)?;
    let (s, key_length) = parser::take_varint(s)?;
    let (s, key) = take(key_length / 2)(s)?;
    let (s, value_len) = parser::take_varint(s)?;
    let (s, value) = take(value_len / 2)(s)?;

    record.length = length;
    record.attributes = attributes;
    record.timestamp_delta = timestamp_delta;
    record.offset_delta = offset_delta;
    record.key_length = key_length;
    record.key = key.into_bytes();
    record.value_len = value_len;
    record.value = value.into_bytes();
    record.headers = vec![];

    // let (s, headers) = parser::parse_array(parse_header)(s)?;
    let (s, _) = parser::take_varint(s)?;

    Ok((s, ()))
}
