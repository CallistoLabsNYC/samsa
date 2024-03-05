//! Encoding and creation for Fetch Offsets requests.

use bytes::{BufMut, Bytes};

use crate::{encode::ToByte, error::Result, protocol::HeaderRequest, utils::{now, to_crc}};

const API_KEY_PRODUCE: i16 = 0;
const API_VERSION: i16 = 3;

/// The magic byte (a.k.a version) we use for sent messages.
const MESSAGE_MAGIC_BYTE: i8 = 2;

/*
Produce Request (Version: 3) => transactional_id acks timeout [topic_data] 
  transactional_id => NULLABLE_STRING
  acks => INT16
  timeout => INT32
  topic_data => topic [data] 
    topic => STRING
    data => partition record_set 
      partition => INT32
      record_set => RECORDS
*/

#[derive(Debug)]
pub struct ProduceRequest<'a> {
    pub header: HeaderRequest<'a>,
    /// The transactional ID of the producer. This is used to authorize transaction produce requests. This can be null for non-transactional producers.
    pub transactional_id: Option<String>,
    /// The number of acknowledgments the producer requires the leader to have received before considering a request complete. Allowed values: 0 for no acknowledgments, 1 for only the leader and -1 for the full ISR.
    pub required_acks: i16,
    /// The timeout to await a response in milliseconds.
    pub timeout_ms: i32,
    /// Each topic to produce to.
    topic_partitions: Vec<TopicPartition<'a>>,
}

impl<'a> ProduceRequest<'a> {
    pub fn new(
        required_acks: i16,
        timeout_ms: i32,
        correlation_id: i32,
        client_id: &'a str,
    ) -> ProduceRequest {
        ProduceRequest {
            header: HeaderRequest::new(API_KEY_PRODUCE, API_VERSION, correlation_id, client_id),
            transactional_id: None,
            required_acks,
            timeout_ms,
            topic_partitions: vec![],
        }
    }

    pub fn add(&mut self, topic: &'a str, partition: i32, message: Message) {
        match self
            .topic_partitions
            .iter_mut()
            .find(|tp| tp.index == topic)
        {
            Some(tp) => {
                tp.add(partition, message);
            }
            None => {
                let mut tp = TopicPartition::new(topic);
                tp.add(partition, message);
                self.topic_partitions.push(tp);
            }
        }
    }
}

impl<'a> ToByte for ProduceRequest<'a> {
    fn encode<W: BufMut>(&self, buffer: &mut W) -> Result<()> {
        tracing::trace!("Encoding ProduceRequest {:?}", self);
        self.header.encode(buffer)?;
        self.transactional_id.encode(buffer)?;
        self.required_acks.encode(buffer)?;
        self.timeout_ms.encode(buffer)?;
        self.topic_partitions.encode(buffer)?;
        Ok(())
    }
}

#[derive(Debug)]
struct TopicPartition<'a> {
    /// The topic name.
    pub index: &'a str,
    /// Each partition to produce to.
    pub partitions: Vec<Partition>,
}

impl<'a> TopicPartition<'a> {
    pub fn new(index: &'a str) -> TopicPartition {
        TopicPartition {
            index,
            partitions: vec![],
        }
    }

    pub fn add(&mut self, partition: i32, message: Message) {
        match self
            .partitions
            .iter_mut()
            .find(|p| p.partition == partition)
        {
            Some(p) => {
                p.add(message);
            }
            None => {
                let mut p = Partition::new(partition);
                p.add(message);
                self.partitions.push(p);
            }
        }
    }
}

impl<'a> ToByte for TopicPartition<'a> {
    fn encode<W: BufMut>(&self, buffer: &mut W) -> Result<()> {
        tracing::trace!("Encoding TopicPartition {:?}", self);
        self.index.encode(buffer)?;
        self.partitions.encode(buffer)?;
        Ok(())
    }
}

#[derive(Debug)]
struct Partition {
    /// The partition index.
    pub partition: i32,
    /// The record data to be produced.
    pub batches: Vec<RecordBatch>,
}

impl Partition {
    pub fn new(partition: i32) -> Partition {
        Partition {
            partition,
            batches: Vec::new(),
        }
    }

    // all records go into one batch, we have to find out how to
    pub fn add(&mut self, message: Message) {
        if self.batches.len() == 0 {
            self.batches.push(RecordBatch::new());
        }

        self.batches[0].add(message);
    }
}

impl ToByte for Partition {
    fn encode<W: BufMut>(&self, out: &mut W) -> Result<()> {
        tracing::trace!("Encoding Partition {:?}", self);
        self.partition.encode(out)?;

        // hack to encode the record batches as a bytestring
        let mut buf = Vec::with_capacity(4);
        for msg in &self.batches {
            msg._encode_to_buf(&mut buf)?;
        }

        buf.encode(out)
    }
}

#[derive(Clone, Debug)]
pub struct Message {
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
    pub headers: Vec<Header>,
}

impl Message {
    pub fn new(key: Option<Bytes>, value: Option<Bytes>, headers: Vec<Header>) -> Message {
        Message {
            key,
            value,
            headers,
        }
    }
}

// baseOffset: int64
// batchLength: int32
// partitionLeaderEpoch: int32
// magic: int8 (current magic value is 2)
// crc: uint32
// attributes: int16
//     bit 0~2:
//         0: no compression
//         1: gzip
//         2: snappy
//         3: lz4
//         4: zstd
//     bit 3: timestampType
//     bit 4: isTransactional (0 means not transactional)
//     bit 5: isControlBatch (0 means not a control batch)
//     bit 6: hasDeleteHorizonMs (0 means baseTimestamp is not set as the delete horizon for compaction)
//     bit 7~15: unused
// lastOffsetDelta: int32
// baseTimestamp: int64
// maxTimestamp: int64
// producerId: int64
// producerEpoch: int16
// baseSequence: int32
// records: [Record]
#[derive(Debug)]
struct RecordBatch {
    /// Denotes the first offset in the RecordBatch. The 'offsetDelta' of each Record in the batch would be be computed relative to this FirstOffset. In particular, the offset of each Record in the Batch is its 'OffsetDelta' + 'FirstOffset'.
    base_offset: i64,

    batch_length: i32,
    /// Introduced with KIP-101, this is set by the broker upon receipt of a produce request and is used to ensure no loss of data when there are leader changes with log truncation. Client developers do not need to worry about setting this value.
    partition_leader_epoch: i32,
    /// This is a version id used to allow backwards compatible evolution of the message binary format.
    magic: i8,
    /// The CRC is the CRC32 of the remainder of the message bytes. This is used to check the integrity of the message on the broker and consumer.
    crc: u32,
    attributes: i16,
    /// The offset of the last message in the RecordBatch. This is used by the broker to ensure correct behavior even when Records within a batch are compacted out.
    last_offset_delta: i32,
    /// The timestamp of the first Record in the batch. The timestamp of each Record in the RecordBatch is its 'TimestampDelta' + 'FirstTimestamp'.
    base_timestamp: i64,
    /// The timestamp of the last Record in the batch. This is used by the broker to ensure the correct behavior even when Records within the batch are compacted out.
    max_timestamp: i64,
    /// Introduced in 0.11.0.0 for KIP-98, this is the broker assigned producerId received by the 'InitProducerId' request. Clients which want to support idempotent message delivery and transactions must set this field.
    producer_id: i64,
    /// Introduced in 0.11.0.0 for KIP-98, this is the broker assigned producerEpoch received by the 'InitProducerId' request. Clients which want to support idempotent message delivery and transactions must set this field.
    producer_epoch: i16,
    /// Introduced in 0.11.0.0 for KIP-98, this is the producer assigned sequence number which is used by the broker to deduplicate messages. Clients which want to support idempotent message delivery and transactions must set this field. The sequence number for each Record in the RecordBatch is its OffsetDelta + FirstSequence.
    base_sequence: i32,
    records: Vec<Record>,
}

impl RecordBatch {
    pub fn new() -> Self {
        Self {
            base_offset: 0,
            batch_length: 0,
            partition_leader_epoch: 0,
            magic: MESSAGE_MAGIC_BYTE,
            crc: 0,
            attributes: 0,
            last_offset_delta: 0,
            base_timestamp: now(),
            max_timestamp: 0,
            producer_id: 0,
            producer_epoch: 0,
            base_sequence: 0,
            records: Vec::new(),
        }
    }

    pub fn add(&mut self, message: Message) {
        // update the state of the batch
        self.last_offset_delta += 1;
        self.max_timestamp = now();

        // find out our deltas
        let timestamp_delta = self.max_timestamp - self.base_timestamp;
        let offset_delta = self.last_offset_delta;

        let record = Record::new(message, timestamp_delta as usize, offset_delta as usize);
        self.records.push(record);

        self.batch_length = self.batch_length + 1;
    }

    pub fn _encode_to_buf(&self, out: &mut Vec<u8>) -> Result<()> {
        self.base_offset.encode(out)?;
        self.batch_length.encode(out)?;
        self.partition_leader_epoch.encode(out)?;
        self.magic.encode(out)?;
        // counted up the bytes of data used so far
        // 8 + 4 + 4 + 1
        let crc_pos = 17;
        self.crc.encode(out)?;

        self.attributes.encode(out)?;
        self.last_offset_delta.encode(out)?;
        self.base_timestamp.encode(out)?;
        self.max_timestamp.encode(out)?;
        self.producer_id.encode(out)?;
        self.producer_epoch.encode(out)?;
        self.base_sequence.encode(out)?;
        for record in &self.records {
            record.encode(out)?;
        }

        let crc = to_crc(&out[(crc_pos + 4)..]) as i32;
        crc.encode(&mut &mut out[crc_pos..crc_pos + 4])?;

        Ok(())
    }
}

// length: varint
// attributes: int8
//     bit 0~7: unused
// timestampDelta: varlong
// offsetDelta: varint
// keyLength: varint
// key: byte[]
// valueLen: varint
// value: byte[]
// Headers => [Header]
#[derive(Debug)]
struct Record {
    attributes: i8,
    timestamp_delta: usize,
    offset_delta: usize,
    key_length: usize,
    key: Option<Bytes>,
    value_length: usize,
    value: Option<Bytes>,
    headers: Vec<Header>,
}

impl Record {
    pub fn new(message: Message, timestamp_delta: usize, offset_delta: usize) -> Self {
        Self {
            attributes: 0,
            timestamp_delta,
            offset_delta,
            key_length: match &message.key {
                Some(key) => key.len(),
                None => 0
            },
            key: message.key,
            value_length: match &message.value {
                Some(value) => value.len(),
                None => 0
            },
            value: message.value,
            headers: message.headers,
        }
    }

    pub fn _encode_to_buf(&self, out: &mut Vec<u8>) -> Result<()> {
        // self.length.encode(out)?;
        self.attributes.encode(out)?;
        self.timestamp_delta.encode(out)?;
        self.offset_delta.encode(out)?;
        self.key_length.encode(out)?;
        out.put(self.key.clone().unwrap_or(Bytes::from("")));
        self.value_length.encode(out)?;
        out.put(self.value.clone().unwrap_or(Bytes::from("")));
        self.headers.encode(out)?;
        Ok(())
    }
}

impl ToByte for Record {
    fn encode<W: BufMut>(&self, out: &mut W) -> Result<()> {
        let mut buf = Vec::with_capacity(4);
        self._encode_to_buf(&mut buf)?;
        let length = buf.len();
        println!("{} length", length);
        println!("{:?} buf", buf);
        length.encode(out)?;
        out.put(buf.as_ref());

        Ok(())
    }


}

// headerKeyLength: varint
// headerKey: String
// headerValueLength: varint
// Value: byte[]
#[derive(Clone, Debug)]
pub struct Header {
    header_key_length: usize,
    header_key: String,
    header_value_length: usize,
    value: Bytes,
}

impl Header {
    pub fn new(key: String, value: Bytes) -> Self {
        Self {
            header_key_length: key.len(),
            header_key: key,
            header_value_length: value.len(),
            value,
        }
    }
}

impl ToByte for Header {
    fn encode<W: BufMut>(&self, out: &mut W) -> Result<()> {
        self.header_key_length.encode(out)?;
        self.header_key.encode(out)?;
        self.header_value_length.encode(out)?;
        self.value.encode(out)?;
        Ok(())
    }
}

/*

[0, 0, 0, 176, // len
0, 0, // api key
0, 3, // api version
0, 0, 0, 1, // correlation id
 0, 41, 112, 114, 111, 100, 117, 99, 101, 32, 38, 32, 102, 101, 116, 99, 104, 32, 112, 114, 111, 116, 111, 99, 111, 108, 32, 105, 110, 116, 101, 103, 114, 97, 116, 105, 111, 110, 32, 116, 101, 115, 116, 
255, 255, // transactional id 
0, 1, // acks
0, 0, 3, 232, // timeout
0, 0, 0, 1, // topic count
0, 6, 116, 101, 115, 116, 101, 114, // topic name
0, 0, 0, 1, // partition count
0, 0, 0, 0, // partition index

0, 0, 0, 93, // batch bytecount
0, 0, 0, 0, 0, 0, 0, 0, // batch offset
0, 0, 0, 1, // count of records
0, 0, 0, 0, // partition leader epoch
2, // magic number
42, 1, 145, 34, // crc
0, 0, // attributes
0, 0, 0, 1, // lastOffsetDelta
0, 0, 1, 142, 14, 253, 61, 75, // base timestamp
0, 0, 1, 142, 14, 253, 61, 75, // max timestamp
0, 0, 0, 0, 0, 0, 0, 0, // producer_id
0, 0, // producer epoch
0, 0, 0, 0, // base seq
0, 0, 0, 1, // length of records?
62, 0, 0, 2, 36, 116, 101, 115, 116, 105, 110, 103, 32, 116, 101, 115, 116, 105, 110, 103, 46, 46, 46, 8, 49, 50, 51, 33, 0, 0, 0, 0]

*/