//! # Samsa
//! Rust-native Kafka/Redpanda protocol and client implementation.
//!
//! This crate provides Rust native consumers and producers as well as low level bindings for the Apache Kafka protocol. Unlike crates that use librdkafka in an FFI, users of this crate will *not* need the C lib and will benefit from Rust all the way down; meaning memory safety, safe concurrency, low resource usage, and of course blazing speed.
//!
//! [Documentation](https://docs.rs/samsa/latest/samsa/)
//!
//! # Goals
//! - Easy to understand code
//! - Leverage best in class libraries such as [Tokio](https://tokio.rs/), [Nom](https://docs.rs/nom/latest/nom/) to do the heavy lifting
//! - Start with a robust foundation and add more advanced features over time
//! - Provide a pure rust implementation of the Kafka protocol
//! - Be a good building block for future works based around Kafka
//!
//! ## Table of contents
//! - [Getting started](#getting-started)
//!     - [Producer](#producer)
//!     - [Consumer](#consumer)
//!     - [Consumer group](#consumer-group)
//!     - [TLS support](#tls-support)
//!     - [Compression support](#compression-support)
//!     - [SASL support](#sasl-support)
//! - [Resources](#resources)
//!
//!
//! ## Getting started
//! Install `samsa` to your rust project with `cargo add samsa` or include the following snippet in your `Cargo.toml` dependencies:
//! ```toml
//! samsa = "0.1"
//! ```
//!
//! This project includes Docker Compose files to help set up Redpanda and Kafka clusters to ease with testing. The easiest way to do this is to run `docker-compose up` to spin up a 2 broker Redpanda cluster. If you want to use different versions of Kafka, check out the [DockerCompose.README.md](/DockerCompose.README.md)
//!
//! ### Producer
//! A [`Producer`](prelude::Producer) sends messages to the given topic and partition.
//!
//! It is buffered, with both a timeout and volume threshold that clears the buffer when reached. This is how letency and throughout can be tweaked to achieve the desired rates.
//!
//! To instantiate one, it is easiest to use a Stream and the [`ProducerBuilder`](prelude::ProducerBuilder).
//! ```rust
//! use samsa::prelude::*;
//!
//! let bootstrap_addrs = vec![BrokerAddress {
//!         host: "127.0.0.1".to_owned(),
//!         port: 9092,
//!     }];
//! let topic_name = "my-topic".to_string();
//! let partition_id = 0;
//!
//! // create a stream of 5k messages in batches of 100
//! let stream = iter(0..5000).map(|_| ProduceMessage {
//!     topic: topic_name.to_string(),
//!     partition_id,
//!     key: Some(bytes::Bytes::from_static(b"Tester")),
//!     value: Some(bytes::Bytes::from_static(b"Value")),
//!     headers: vec![
//!         Header::new(String::from("Key"), bytes::Bytes::from("Value"))
//!     ],
//! }).chunks(100);
//!
//! let output_stream =
//! ProducerBuilder::<TcpConnection>::new(bootstrap_addrs, vec![topic_name.to_string()])
//!     .await?
//!     .batch_timeout_ms(1000)
//!     .max_batch_size(100)
//!     .clone()
//!     .build_from_stream(stream)
//!     .await;
//!
//! tokio::pin!(output_stream);
//! while (output_stream.next().await).is_some() {}
//! ```
//!
//! ### Consumer
//! A [`Consumer`](prelude::Consumer) is used to fetch messages from the broker. It is an asynchronous iterator that can be configured to auto-commit. To instantiate one, start with a [`ConsumerBuilder`](prelude::ConsumerBuilder).
//! ```rust
//! use samsa::prelude::*;
//!
//! let bootstrap_addrs = vec![BrokerAddress {
//!         host: "127.0.0.1".to_owned(),
//!         port: 9092,
//!     }];
//! let partitions = vec![0];
//! let topic_name = "my-topic".to_string();
//! let assignment = TopicPartitionsBuilder::new()
//!     .assign(topic_name, partitions)
//!     .build();
//!
//! let consumer = ConsumerBuilder::<TcpConnection>::new(
//!         bootstrap_addrs,
//!         assignment,
//!     )
//!     .await?
//!     .build();
//!
//! let stream = consumer.into_stream();
//! // have to pin streams before iterating
//! tokio::pin!(stream);
//!
//! // Stream will do nothing unless consumed.
//! while let Some(batch) = stream.next().await {
//!     println!("{:?} messages read", batch.unwrap().count());
//! }
//! ```
//!
//! ### Consumer group
//! You can set up a consumer group with a group id and an [`assignment`](prelude::TopicPartitions). The offsets are commit automatically for the member of the group.
//! ```rust
//! use samsa::prelude::*;
//!
//! let bootstrap_addrs = vec![BrokerAddress {
//!         host: "127.0.0.1".to_owned(),
//!         port: 9092,
//!     }];
//! let partitions = vec![0];
//! let topic_name = "my-topic".to_string();
//! let assignment = TopicPartitionsBuilder::new()
//!     .assign(topic_name, partitions)
//!     .build();
//! let group_id = "The Data Engineering Team".to_string();
//!
//! let consumer_group_member = ConsumerGroupBuilder::<TcpConnection>::new(
//!         bootstrap_addrs,
//!         group_id,
//!         assignment,
//!     ).await?
//!     .build()
//!     .await?;
//!
//! let stream = consumer_group_member.into_stream();
//! // have to pin streams before iterating
//! tokio::pin!(stream);
//!
//! // Stream will do nothing unless consumed.
//! while let Some(batch) = stream.next().await {
//!     println!("{:?} messages read", batch.unwrap().count());
//! }
//! ```
//!
//! ### TLS support
//! You can add TLS support to your consumer or producer for secured communication. To enable this, start with specifying the [`TlsConnectionOptions`](prelude::TlsConnectionOptions),
//! and pass it into an instance of the [`ProducerBuilder`](prelude::ProducerBuilder) or [`ConsumerBuilder`](prelude::ConsumerBuilder).
//!
//! Example for [`Consumer`](prelude::Consumer) with TLS support:
//! ```rust
//! use samsa::prelude::*;
//!
//! let tls_option = TlsConnectionOptions {
//!         broker_options: vec![BrokerAddress {
//!             host: "127.0.0.1".to_owned(),
//!             port: 9092,
//!         }],
//!         key: "/path_to_key_file".into(),
//!         cert: "/path_to_cert_file".into(),
//!         cafile: Some("/path_to_ca_file".into()),
//!     };
//! let partitions = vec![0];
//! let topic_name = "my-topic".to_string();
//! let assignment = TopicPartitionsBuilder::new()
//!     .assign(topic_name, partitions)
//!     .build();
//!
//! let consumer = ConsumerBuilder::<TlsConnection>::new(
//!         tls_option,
//!         assignment,
//!     )
//!     .await?
//!     .build();
//!
//! let stream = consumer.into_stream();
//! // have to pin streams before iterating
//! tokio::pin!(stream);
//!
//! // Stream will do nothing unless consumed.
//! while let Some(batch) = stream.next().await {
//!     println!("{:?} messages read", batch.unwrap().count());
//! }
//! ```
//!
//! ### Compression support
//! We provide support for compression in the producer using the [`Compression`](prelude::Compression) enum. The enum allows to specify what type of compression to use. The Consumer will automatically know to decompress the message.
//!
//! Example for Producer with GZIP compression enabled:
//! ```rust
//! use samsa::prelude::*;
//!
//! let bootstrap_addrs = vec![BrokerAddress {
//!         host: "127.0.0.1".to_owned(),
//!         port: 9092,
//!     }];
//! let topic_name = "my-topic".to_string();
//! let partition_id = 0;
//!
//! // create a stream of 5k messages in batches of 100
//! let stream = iter(0..5000).map(|_| ProduceMessage {
//!     topic: topic_name.to_string(),
//!     partition_id,
//!     key: Some(bytes::Bytes::from_static(b"Tester")),
//!     value: Some(bytes::Bytes::from_static(b"Value")),
//!     headers: vec![
//!         samsa::prelude::Header::new(String::from("Key"), bytes::Bytes::from("Value"))
//!     ],
//! }).chunks(100);
//!
//! let output_stream =
//! ProducerBuilder::<TcpConnection>::new(bootstrap_addrs, vec![topic_name.to_string()])
//!     .await?
//!     .batch_timeout_ms(1000)
//!     .max_batch_size(100)
//!     .compression(Compression::Gzip)
//!     .clone()
//!     .build_from_stream(stream)
//!     .await;
//!
//! tokio::pin!(output_stream);
//! while (output_stream.next().await).is_some() {}
//! ```
//!
//! ### SASL support
//! We include support for SASL using all typical mechanisms: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512. This is represented as another type of BrokerConnection that our Consumers and Producers recieve as a generic parameter. All that is needed is to provide the credentials.
//!
//! Example for Producer using both TLS and SASL:
//! ```rust
//! use samsa::prelude::*;
//!
//! let tls_config = TlsConnectionOptions {
//!     broker_options: vec![BrokerAddress {
//!         host: "127.0.0.1".to_owned(),
//!         port: 9092,
//!     }],
//!     key: "/path_to_key_file".into(),
//!     cert: "/path_to_cert_file".into(),
//!     cafile: Some("/path_to_ca_file".into()),
//! };
//!
//! let sasl_config = SaslConfig::new(String::from("myuser"), String::from("pass1234"), None, None);
//!
//! let options = SaslTlsConfig {
//!     tls_config,
//!     sasl_config,
//! };
//!
//! let topic_name = "atopic";
//!
//! let s = ConsumerBuilder::<SaslTlsConnection>::new(
//!     options,
//!     TopicPartitionsBuilder::new()
//!         .assign(topic_name.to_owned(), vec![0])
//!         .build(),
//! )
//! .await
//! .unwrap()
//! .build()
//! .into_stream();
//!
//! tokio::pin!(s);
//!
//! while let Some(m) = s.next().await {
//!     tracing::info!("{:?} messages read", m.unwrap().count());
//! }
//! ```
//! ## Resources
//! - [Kafka Protocol Spec](https://kafka.apache.org/protocol.html)
//! - [Confluence Docs](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol)

mod admin;
mod assignor;
mod consumer;
mod consumer_builder;
mod consumer_group;
mod consumer_group_builder;
mod encode;
mod error;
mod metadata;
mod network;
mod parser;
mod producer;
mod producer_builder;
mod protocol;
mod utils;

#[cfg(feature = "redpanda")]
mod redpanda;

const DEFAULT_CORRELATION_ID: i32 = 1;
const DEFAULT_CLIENT_ID: &str = "samsa";

pub mod prelude {
    //! Main export of various structures and methods
    //!
    //! We break the library into 4 main sections:
    //! - [Producing](#producing)
    //! - [Consuming](#consuming)
    //! - [Consumer Groups](#consumer-groups)
    //! - [Broker Connections](#broker-connections)
    //!
    //! # Producing
    //!
    //! We provide a Producer struct that takes care of the inner details
    //! relating to all things Kafka.
    //!
    //! We also provide low level methods that allow users to interface with
    //!  the Kafka API directly if they so choose. For those looking to get
    //! their hands even dirtier and handle the specific requests and
    //! responses that make up the Kafka protocol, visit the [protocol module].
    //!
    //! ## Producer
    //! The [`Producer`] struct is useful for easily sending messages to brokers.
    //! The producer is represented as a background worker containing a queue of messages to be
    //! sent upon meeting either of two conditions:
    //! - The maximum number of messages is filled
    //! - The wait time has ran out
    //! When either of these two are met, the record queue is flushed and sent to the appropriate
    //! brokers.
    //!
    //! To produce, simply provide the initial bootstrap brokers and the working topics
    //! to the [`ProducerBuilder`]. This you can use to configure the producing parameters as
    //! needed.
    //! ### Example
    //! ```rust
    //! use samsa::prelude::*;
    //!
    //! let bootstrap_addrs = vec![BrokerAddress {
    //!         host: "127.0.0.1".to_owned(),
    //!         port: 9092,
    //!     }];
    //! let topic_name = "my-topic".to_string();
    //! let partition_id = 0;
    //!
    //! // create a stream of 5k messages in batches of 100
    //! let stream = iter(0..5000).map(|_| ProduceMessage {
    //!     topic: topic_name.to_string(),
    //!     partition_id,
    //!     key: Some(bytes::Bytes::from_static(b"Tester")),
    //!     value: Some(bytes::Bytes::from_static(b"Value")),
    //!     headers: vec![
    //!         samsa::prelude::Header::new(String::from("Key"), bytes::Bytes::from("Value"))
    //!     ],
    //! }).chunks(100);
    //!
    //! let output_stream =
    //! ProducerBuilder::<TcpConnection>::new(bootstrap_addrs, vec![topic_name.to_string()])
    //!     .await?
    //!     .batch_timeout_ms(1000)
    //!     .max_batch_size(100)
    //!     .clone()
    //!     .build_from_stream(stream)
    //!     .await;
    //!
    //! tokio::pin!(output_stream);
    //! while (output_stream.next().await).is_some() {}
    //!
    //! ```
    //!
    //! ## Produce protocol functions
    //! We provide a set of protocol primitives for users to build their own clients.
    //! They are presented as the building blocks that we use to build the higher level
    //! abstractions.
    //!
    //! [`produce`] sends messages to a broker.
    //!
    //! # Consuming
    //!
    //! We provide a Consumer struct that takes care of the inner details relating
    //! to all things Kafka.
    //!
    //! We also provide low level methods that allow users to interface with the
    //! Kafka API directly if they so choose. For those looking to get their hands
    //! even dirtier and handle the specific requests and responses that make up
    //! the Kafka protocol, visit the [protocol module].
    //!
    //! ## Consumer
    //! The [`Consumer`] struct is useful to easily fetch messages from a broker. We use
    //! Asynchonous Iterators a.k.a. [`Streams`](https://docs.rs/futures/latest/futures/stream/trait.Stream.html) to represent the repeated fetching of
    //! records from topic partitions. The streams have a plethora of functions that
    //! allow for very powerful stream processing.
    //!
    //! To consume, simply provide the initial bootstrap broker and the assignments
    //! to the [`ConsumerBuilder`]. This you can use to configure the fetching parameters as
    //! needed.
    //! ### Example
    //! ```rust
    //! use samsa::prelude::*;
    //!
    //! let bootstrap_addrs = vec![BrokerAddress {
    //!         host: "127.0.0.1".to_owned(),
    //!         port: 9092,
    //!     }];
    //! let partitions = vec![0];
    //! let topic_name = "my-topic".to_string();
    //! let assignment = TopicPartitionsBuilder::new()
    //!     .assign(topic_name, partitions)
    //!     .build();
    //!
    //! let consumer = ConsumerBuilder::<TcpConnection>::new(
    //!         bootstrap_addrs,
    //!         assignment,
    //!     )
    //!     .await?
    //!     .build();
    //!
    //! let stream = consumer.into_stream();
    //! // have to pin streams before iterating
    //! tokio::pin!(stream);
    //!
    //! // Stream will do nothing unless consumed.
    //! while let Some(batch) = stream.next().await {
    //!     println!("{:?} messages read", batch.unwrap().count());
    //! }
    //! ```
    //!
    //! ## Consume protocol functions
    //! We provide a set of protocol primitives for users to build their own clients.
    //! They are presented as the building blocks that we use to build the higher level
    //! abstractions.
    //!
    //! [`list_offsets`] finds the offsets given a timestamp.
    //!
    //! [`fetch`] fetches a batch of messages.
    //!
    //! [`fetch_offset`] gets the offsets of a consumer group.
    //!
    //! [`commit_offset`] commits a set of offsets for a group.
    //!
    //! # Consumer Groups
    //!
    //! We provide a Consumer Group struct that takes care of the inner details relating
    //! to all things Kafka.
    //!
    //! We also provide low level methods that allow users to interface with the
    //! Kafka API directly if they so choose. For those looking to get their hands
    //! even dirtier and handle the specific requests and responses that make up
    //! the Kafka protocol, visit the [protocol module].
    //!
    //! ## ConsumerGroup
    //! The [`ConsumerGroup`] struct is an abstraction over the typical Kafka Consumer Groups.
    //! This struct represents one member. It is used similarly to the [`Consumer`] in that it
    //! is based on streams.
    //!
    //! To use, simply provide the initial bootstrap broker, the group id, and the assignments
    //! to the [`ConsumerGroupBuilder`]. This you can use to configure the fetching parameters as needed.
    //!
    //! ### Example
    //! ```rust
    //! use samsa::prelude::*;
    //!
    //! let bootstrap_addrs = vec![BrokerAddress {
    //!         host: "127.0.0.1".to_owned(),
    //!         port: 9092,
    //!     }];
    //! let partitions = vec![0];
    //! let topic_name = "my-topic".to_string();
    //! let assignment = TopicPartitionsBuilder::new()
    //!     .assign(topic_name, partitions)
    //!     .build();
    //! let group_id = "The Data Engineering Team".to_string();
    //!
    //! let consumer_group_member = ConsumerGroupBuilder::<TcpConnection>::new(
    //!         bootstrap_addrs,
    //!         group_id,
    //!         assignment,
    //!     ).await?
    //!     .build()
    //!     .await?;
    //!
    //! let stream = consumer_group_member.into_stream();
    //! // have to pin streams before iterating
    //! tokio::pin!(stream);
    //!
    //! // Stream will do nothing unless consumed.
    //! while let Some(batch) = stream.next().await {
    //!     println!("{:?} messages read", batch.unwrap().count());
    //! }
    //! ```
    //!
    //! ## Consumer Group Protocol functions
    //! We provide a set of protocol primitives for users to build their own clients.
    //! They are presented as the building blocks that we use to build the higher level
    //! abstractions.
    //!
    //! [`join_group`] Become a member of a group, creating it if there are no active members.
    //!
    //! [`sync_group`] Synchronize state for all members of a group.
    //!
    //! [`heartbeat`] Keep a member alive in the group.
    //!
    //! [`leave_group`] Directly depart a group.
    //!
    //! ## Broker Connections
    //! We provide mechanisms to connect to your brokers in many different formats:
    //! - [`TcpConnection`]
    //! - [`TlsConnection`]
    //! - [`SaslTcpConnection`]
    //! - [`SaslTlsConnection`]
    //!
    //! This is implemented through a common trait, called [`BrokerConnection`]. This allows users
    //! to just drop in the corresponding connection options struct. Here is how you use each one:
    //!
    //! ### Example for Producer connecting over TCP:
    //! ```rust
    //! use samsa::prelude::*;
    //!
    //! let bootstrap_addrs = vec![BrokerAddress {
    //!         host: "127.0.0.1".to_owned(),
    //!         port: 9092,
    //!     }];
    //! let topic_name = "my-topic".to_string();
    //! let partition_id = 0;
    //!
    //! // create a stream of 5k messages in batches of 100
    //! let stream = iter(0..5000).map(|_| ProduceMessage {
    //!     topic: topic_name.to_string(),
    //!     partition_id,
    //!     key: Some(bytes::Bytes::from_static(b"Tester")),
    //!     value: Some(bytes::Bytes::from_static(b"Value")),
    //!     headers: vec![
    //!         Header::new(String::from("Key"), bytes::Bytes::from("Value"))
    //!     ],
    //! }).chunks(100);
    //!
    //! let output_stream =
    //! ProducerBuilder::<TcpConnection>::new(bootstrap_addrs, vec![topic_name.to_string()])
    //!     .await?
    //!     .batch_timeout_ms(1000)
    //!     .max_batch_size(100)
    //!     .clone()
    //!     .build_from_stream(stream)
    //!     .await;
    //!
    //! tokio::pin!(output_stream);
    //! while (output_stream.next().await).is_some() {}
    //! ```
    //!
    //! ### Example for Producer connecting over TLS:
    //! ```rust
    //! use samsa::prelude::*;
    //!
    //! let tls_option = TlsConnectionOptions {
    //!         broker_options: vec![BrokerAddress {
    //!           host: "127.0.0.1".to_owned(),
    //!           port: 9092,
    //!         }],
    //!         key: "/path_to_key_file".into(),
    //!         cert: "/path_to_cert_file".into(),
    //!         cafile: Some("/path_to_ca_file".into()),
    //!     };
    //! let topic_name = "my-topic".to_string();
    //! let partition_id = 0;
    //!
    //! // create a stream of 5k messages in batches of 100
    //! let stream = iter(0..5000).map(|_| ProduceMessage {
    //!     topic: topic_name.to_string(),
    //!     partition_id,
    //!     key: Some(bytes::Bytes::from_static(b"Tester")),
    //!     value: Some(bytes::Bytes::from_static(b"Value")),
    //!     headers: vec![
    //!         Header::new(String::from("Key"), bytes::Bytes::from("Value"))
    //!     ],
    //! }).chunks(100);
    //!
    //! let output_stream =
    //! ProducerBuilder::<TlsConnection>::new(tls_option, vec![topic_name.to_string()])
    //!     .await?
    //!     .batch_timeout_ms(1000)
    //!     .max_batch_size(100)
    //!     .clone()
    //!     .build_from_stream(stream)
    //!     .await;
    //!
    //! tokio::pin!(output_stream);
    //! while (output_stream.next().await).is_some() {}
    //! ```
    //!
    //! ### Example for Producer connecting over SASL:
    //! ```rust
    //! use samsa::prelude::*;
    //!
    //! let tcp_config = vec![BrokerAddress {
    //!     host: "127.0.0.1".to_owned(),
    //!     port: 9092,
    //! }];
    //! let sasl_config = SaslConfig::new(String::from("myuser"), String::from("pass1234"), None, None);
    //!
    //! let options = SaslTcpConfig {
    //!     tcp_config,
    //!     sasl_config,
    //! };
    //! let topic_name = "my-topic".to_string();
    //! let partition_id = 0;
    //!
    //! // create a stream of 5k messages in batches of 100
    //! let stream = iter(0..5000).map(|_| ProduceMessage {
    //!     topic: topic_name.to_string(),
    //!     partition_id,
    //!     key: Some(bytes::Bytes::from_static(b"Tester")),
    //!     value: Some(bytes::Bytes::from_static(b"Value")),
    //!     headers: vec![
    //!         Header::new(String::from("Key"), bytes::Bytes::from("Value"))
    //!     ],
    //! }).chunks(100);
    //!
    //! let output_stream =
    //! ProducerBuilder::<SaslConnection>::new(options, vec![topic_name.to_string()])
    //!     .await?
    //!     .batch_timeout_ms(1000)
    //!     .max_batch_size(100)
    //!     .clone()
    //!     .build_from_stream(stream)
    //!     .await;
    //!
    //! tokio::pin!(output_stream);
    //! while (output_stream.next().await).is_some() {}
    //! ```
    //!
    //! ### Example for Producer connecting over SASL/TLS:
    //! ```rust
    //! use samsa::prelude::*;
    //!
    //! let tls_config = TlsConnectionOptions {
    //!         broker_options: vec![BrokerAddress {
    //!           host: "127.0.0.1".to_owned(),
    //!           port: 9092,
    //!         }],
    //!         key: "/path_to_key_file".into(),
    //!         cert: "/path_to_cert_file".into(),
    //!         cafile: Some("/path_to_ca_file".into()),
    //!     };
    //! let sasl_config = SaslConfig::new(String::from("myuser"), String::from("pass1234"), None, None);
    //!
    //! let options = SaslTlsConfig {
    //!     tls_config,
    //!     sasl_config,
    //! };
    //! let topic_name = "my-topic".to_string();
    //! let partition_id = 0;
    //!
    //! // create a stream of 5k messages in batches of 100
    //! let stream = iter(0..5000).map(|_| ProduceMessage {
    //!     topic: topic_name.to_string(),
    //!     partition_id,
    //!     key: Some(bytes::Bytes::from_static(b"Tester")),
    //!     value: Some(bytes::Bytes::from_static(b"Value")),
    //!     headers: vec![
    //!         Header::new(String::from("Key"), bytes::Bytes::from("Value"))
    //!     ],
    //! }).chunks(100);
    //!
    //! let output_stream =
    //! ProducerBuilder::<SaslTlsConnection>::new(options, vec![topic_name.to_string()])
    //!     .await?
    //!     .batch_timeout_ms(1000)
    //!     .max_batch_size(100)
    //!     .clone()
    //!     .build_from_stream(stream)
    //!     .await;
    //!
    //! tokio::pin!(output_stream);
    //! while (output_stream.next().await).is_some() {}
    //! ```
    //!
    pub use crate::admin::{create_topics, delete_topics};
    pub use crate::assignor::ROUND_ROBIN_PROTOCOL;
    pub use crate::consumer::{
        commit_offset, fetch, ConsumeMessage, Consumer, PartitionOffsets, TopicPartitions,
        TopicPartitionsBuilder,
    };
    pub use crate::consumer_builder::{fetch_offset, list_offsets, ConsumerBuilder};
    pub use crate::consumer_group::{
        heartbeat, join_group, leave_group, sync_group, ConsumerGroup,
    };
    pub use crate::consumer_group_builder::{find_coordinator, ConsumerGroupBuilder};
    pub use crate::error::{Error, KafkaCode, Result};
    pub use crate::metadata::ClusterMetadata;
    pub use crate::network::{
        sasl::{do_sasl, SaslConfig},
        tcp::{SaslTcpConfig, SaslTcpConnection, TcpConnection},
        tls::{SaslTlsConfig, SaslTlsConnection, TlsConnection, TlsConnectionOptions},
        BrokerAddress, BrokerConnection,
    };
    pub use crate::producer::{produce, ProduceMessage, Producer};
    pub use crate::producer_builder::ProducerBuilder;
    /// Message Header.
    pub use crate::protocol::Header;

    pub use bytes;

    pub mod encode {
        //! Serialize data into the bytecode protocol.
        pub use crate::encode::*;
    }

    pub mod protocol {
        //! Bytecode protocol requests & responses.
        //!
        //! This module aims to implement the bytecode protocol outlined in the
        //! [Kafka Documentation](https://kafka.apache.org/protocol.html)
        //!
        //! The module is set up as a list of message pairs containing two files
        //! each corresponding to the request and response.
        //!
        //! The request files hold the logic for creating and encoding structs that
        //! will be sent to the broker. The response files hold the logic for parsing
        //! and processing the messages coming from the broker.
        pub use crate::protocol::*;
    }

    #[cfg(feature = "redpanda")]
    pub mod redpanda {
        pub use crate::redpanda::*;
    }

    #[derive(Clone, Debug, PartialEq)]
    pub enum Compression {
        Gzip,
    }
}
