//! Rust-native Kafka protocol and client implementation.
//!
//! This crate provides Rust native consumers and producers as well as
//! low level bindings for the Apache Kafka protocol. Unlike crates that
//! use librdkafka in an FFI, users of this crate actually benefit from Rust
//! all the way down; meaning memory safety, safe concurrency, low
//! resource usage, and of course blazing speed.

mod assignor;
mod consumer;
mod consumer_builder;
mod consumer_group;
mod encode;
mod error;
mod metadata;
mod network;
mod parser;
mod producer;
mod producer_builder;
mod protocol;
mod utils;

const DEFAULT_CORRELATION_ID: i32 = 1;
const DEFAULT_CLIENT_ID: &str = "samsa";

pub mod prelude {
    pub use crate::assignor::ROUND_ROBIN_PROTOCOL;
    pub use crate::consumer::{commit_offset, fetch, PartitionOffsets};
    pub use crate::consumer_builder::ConsumerBuilder;
    pub use crate::consumer_group::{
        find_coordinator, heartbeat, join_group, leave_group, sync_group, ConsumerGroupBuilder,
    };
    pub use crate::error::{Error, KafkaCode, Result};
    pub use crate::network::BrokerConnection;
    pub use crate::producer::{produce, ProduceMessage};
    pub use crate::producer_builder::ProducerBuilder;

    pub use bytes;

    pub mod encode {
        pub use crate::encode::*;
    }

    pub mod protocol {
        pub use crate::protocol::*;
    }

    pub mod utils {
        pub use crate::utils::{fetch_offset, into_flat_stream};
    }
}
