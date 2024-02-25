//! Encoding and creation for Fetch Offsets requests.

use bytes::{BufMut, Bytes};

use crate::{encode::ToByte, error::Result, protocol::HeaderRequest, utils::to_crc};

const API_KEY_PRODUCE: i16 = 0;
const API_VERSION: i16 = 0;

/// The magic byte (a.k.a version) we use for sent messages.
const MESSAGE_MAGIC_BYTE: i8 = 0;

/*
Produce Request (Version: 0) => acks timeout_ms [topic_data]
  acks => INT16
  timeout_ms => INT32
  topic_data => name [partition_data]
    name => STRING
    partition_data => index records
      index => INT32
      records => RECORDS
*/

#[derive(Debug)]
pub struct ProduceRequest<'a> {
    pub header: HeaderRequest<'a>,
    /// The number of acknowledgments the producer requires the leader to have received before considering a request complete. Allowed values: 0 for no acknowledgments, 1 for only the leader and -1 for the full ISR.
    pub required_acks: i16,
    /// The timeout to await a response in milliseconds.
    pub timeout_ms: i32,
    /// Each topic to produce to.
    pub topic_partitions: Vec<TopicPartitionProduceRequest<'a>>,
}

#[derive(Debug)]
pub struct TopicPartitionProduceRequest<'a> {
    /// The topic name.
    pub index: &'a str,
    /// Each partition to produce to.
    pub partitions: Vec<PartitionProduceRequest>,
}

#[derive(Debug)]
pub struct PartitionProduceRequest {
    /// The partition index.
    pub partition: i32,
    /// The record data to be produced.
    pub messages: Vec<MessageProduceRequest>,
}

#[derive(Debug)]
pub struct MessageProduceRequest {
    key: Option<Bytes>,
    value: Option<Bytes>,
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
            required_acks,
            timeout_ms,
            topic_partitions: vec![],
        }
    }

    pub fn add(
        &mut self,
        topic: &'a str,
        partition: i32,
        key: Option<Bytes>,
        value: Option<Bytes>,
    ) {
        for tp in &mut self.topic_partitions {
            if tp.index == topic {
                tp.add(partition, key, value);
                return;
            }
        }
        let mut tp = TopicPartitionProduceRequest::new(topic);
        tp.add(partition, key, value);
        self.topic_partitions.push(tp);
    }
}

impl<'a> TopicPartitionProduceRequest<'a> {
    pub fn new(index: &'a str) -> TopicPartitionProduceRequest {
        TopicPartitionProduceRequest {
            index,
            partitions: vec![],
        }
    }

    pub fn add(&mut self, partition: i32, key: Option<Bytes>, value: Option<Bytes>) {
        for pp in &mut self.partitions {
            if pp.partition == partition {
                pp.add(key, value);
                return;
            }
        }
        self.partitions
            .push(PartitionProduceRequest::new(partition, key, value));
    }
}

impl PartitionProduceRequest {
    pub fn new(
        partition: i32,
        key: Option<Bytes>,
        value: Option<Bytes>,
    ) -> PartitionProduceRequest {
        let mut r = PartitionProduceRequest {
            partition,
            messages: Vec::new(),
        };
        r.add(key, value);
        r
    }

    pub fn add(&mut self, key: Option<Bytes>, value: Option<Bytes>) {
        self.messages.push(MessageProduceRequest::new(key, value));
    }
}

impl<'a> ToByte for ProduceRequest<'a> {
    fn encode<W: BufMut>(&self, buffer: &mut W) -> Result<()> {
        tracing::trace!("Encoding ProduceRequest {:?}", self);
        self.header.encode(buffer)?;
        self.required_acks.encode(buffer)?;
        self.timeout_ms.encode(buffer)?;
        self.topic_partitions.encode(buffer)?;
        Ok(())
    }
}

impl<'a> ToByte for TopicPartitionProduceRequest<'a> {
    // render: TopicName [Partition MessageSetSize MessageSet]
    fn encode<W: BufMut>(&self, buffer: &mut W) -> Result<()> {
        self.index.encode(buffer)?;
        (self.partitions.len() as i32).encode(buffer)?;
        for e in &self.partitions {
            e.encode(buffer)?
        }
        Ok(())
    }
}

impl ToByte for PartitionProduceRequest {
    // render: Partition MessageSetSize MessageSet
    //
    // MessetSet => [Offset MessageSize Message]
    // MessageSets are not preceded by an int32 like other array elements in the protocol.
    fn encode<W: BufMut>(&self, out: &mut W) -> Result<()> {
        self.partition.encode(out)?;

        // ~ render the whole MessageSet first to a temporary buffer
        let mut buf = Vec::new();
        for msg in &self.messages {
            msg._encode_to_buf(&mut buf, MESSAGE_MAGIC_BYTE, 0)?;
        }

        buf.encode(out)
    }
}

impl MessageProduceRequest {
    fn new(key: Option<Bytes>, value: Option<Bytes>) -> MessageProduceRequest {
        MessageProduceRequest { key, value }
    }

    // render a single message as: Offset MessageSize Message
    //
    // Offset => int64 (always encoded as zero here)
    // MessageSize => int32
    // Message => Crc MagicByte Attributes Key Value
    // Crc => int32
    // MagicByte => int8
    // Attributes => int8
    // Key => bytes
    // Value => bytes
    //
    // note: the rendered data corresponds to a single MessageSet in the kafka protocol
    fn _encode_to_buf(&self, buffer: &mut Vec<u8>, magic: i8, attributes: i8) -> Result<()> {
        (0i64).encode(buffer)?; // offset in the response request can be anything

        let size_pos = buffer.len();
        let mut size: i32 = 0;
        size.encode(buffer)?; // reserve space for the size to be computed later

        let crc_pos = buffer.len(); // remember the position where to update the crc later
        let mut crc: i32 = 0;
        crc.encode(buffer)?; // reserve space for the crc to be computed later
        magic.encode(buffer)?;
        attributes.encode(buffer)?;
        self.key.encode(buffer)?;
        self.value.encode(buffer)?;

        // compute the crc and store it back in the reserved space
        crc = to_crc(&buffer[(crc_pos + 4)..]) as i32;
        crc.encode(&mut &mut buffer[crc_pos..crc_pos + 4])?;

        // compute the size and store it back in the reserved space
        size = (buffer.len() - crc_pos) as i32;
        size.encode(&mut &mut buffer[size_pos..size_pos + 4])?;

        Ok(())
    }
}
