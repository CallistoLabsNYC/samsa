//! # Samsa
//! Rust-native Kafka protocol and client implementation.
//!
//! This crate provides Rust native consumers and producers as well as
//! low level bindings for the Apache Kafka protocol. Unlike crates that
//! use librdkafka in an FFI, users of this crate actually benefit from Rust
//! all the way down; meaning memory safety, safe concurrency, low
//! resource usage, and of course blazing speed.
//!
//! ## Goals
//! - Easy to understand code
//! - Leverage best in class libraries such as Tokio, Nom to do the heavy lifting
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
//! A [`Producer`](prelude::Producer) sends messages to the given topic and partition. To instantiate one, start with a [`ProducerBuilder`](prelude::ProducerBuilder).
//!
//! It is buffered, with both a timeout and volume threshold that clears the buffer when reached. This is how letency and throughout can be tweaked to achieve the desired rates.
//! ```rust
//! use samsa::prelude::TcpConnection;
//! let bootstrap_addrs = vec![samsa::prelude::BrokerAddress {
//!         host: "127.0.0.1:9092".to_owned(),
//!         port: 9092,
//!     }];
//! let topic_name = "my-topic";
//! let partition_id = 0;
//!
//! let message = samsa::prelude::ProduceMessage {
//!         topic: topic_name.to_string(),
//!         partition_id,
//!         key: Some(bytes::Bytes::from_static(b"Tester")),
//!         value: Some(bytes::Bytes::from_static(b"Value")),
//!         headers: vec![
//!             samsa::prelude::Header::new(String::from("Key"), bytes::Bytes::from("Value"))
//!         ]
//!     };
//!
//! let producer_client = samsa::prelude::ProducerBuilder::<TcpConnection>::new(
//!         bootstrap_addrs,
//!         vec![topic_name.to_string()]
//!     )
//!     .await?
//!     .batch_timeout_ms(1)
//!     .max_batch_size(2)
//!     .clone()
//!     .build()
//!     .await;
//!
//! producer_client
//!     .produce(message)
//!     .await;
//! ```
//!
//! ### Consumer
//! A [`Consumer`](prelude::Consumer) is used to fetch messages from the broker. It is an asynchronous iterator that can be configured to auto-commit. To instantiate one, start with a [`ConsumerBuilder`](prelude::ConsumerBuilder).
//! ```rust
//! use samsa::prelude::TcpConnection;
//! let bootstrap_addrs = vec![samsa::prelude::BrokerAddress {
//!         host: "127.0.0.1:9092".to_owned(),
//!         port: 9092,
//!     }];
//! let partitions = vec![0];
//! let topic_name = "my-topic".to_string();
//! let assignment = samsa::prelude::TopicPartitionsBuilder::new()
//!     .assign(topic_name, partitions)
//!     .build();
//!
//! let consumer = samsa::prelude::ConsumerBuilder::<TcpConnection>::new(
//!     bootstrap_addrs,
//!     assignment,
//! )
//! .await?
//! .build();
//!
//! let stream = consumer.into_stream();
//! // have to pin streams before iterating
//! tokio::pin!(stream);
//!
//! // Stream will do nothing unless consumed.
//! while let Some(Ok((batch, offsets))) = stream.next().await {
//!     println!("{:?}", batch);
//! }
//! ```
//!
//! ### Consumer group
//! You can set up a [`ConsumerGroup`](prelude::ConsumerGroup) with a group id and assignment. The offsets are commit automatically for the member of the group. To instantiate one, start with a [`ConsumerGroupBuilder`](prelude::ConsumerGroupBuilder).
//! ```rust
//! use samsa::prelude::TcpConnection;
//! let bootstrap_addrs = vec![samsa::prelude::BrokerAddress {
//!         host: "127.0.0.1:9092".to_owned(),
//!         port: 9092,
//!     }];
//! let partitions = vec![0];
//! let topic_name = "my-topic".to_string();
//! let assignment = samsa::prelude::TopicPartitionsBuilder::new()
//!     .assign(topic_name, partitions)
//!     .build();
//! let group_id = "The Data Team".to_string();
//!
//! let consumer_group_member = samsa::prelude::ConsumerGroupBuilder::<TcpConnection>::new(
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
//!     println!("{:?}", batch);
//! }
//! ```
//!
//! ### TLS support
//! You can add TLS support to your consumer or producer for secured communication. To enable this, start with specifying the `TlsConnectionOptions`,
//! and pass it into an instance of the `ProducerBuilder` or `ConsumerBuilder`.
//!
//! Example for Producer with TLS support:
//! ```rust
//! let tls_option = samsa::prelude::TlsConnectionOptions {
//!         broker_options: vec![samsa::prelude::BrokerAddress {
//!           host: "127.0.0.1:9092".to_owned(),
//!           port: 9092,
//!         }],
//!         key: "/path_to_key_file".into(),
//!         cert: "/path_to_cert_file".into(),
//!         cafile: Some("/path_to_ca_file".into()),
//!     };
//! let topic_name = "my-topic".to_string();
//! let partition_id = 0;
//!
//! let message = samsa::prelude::ProduceMessage {
//!         topic: topic_name.to_string(),
//!         partition_id,
//!         key: Some(bytes::Bytes::from_static(b"Tester")),
//!         value: Some(bytes::Bytes::from_static(b"Value")),
//!         headers: vec![
//!             samsa::prelude::Header::new(String::from("Key"), bytes::Bytes::from("Value"))
//!         ],
//!     };
//!
//! let producer_client = samsa::prelude::ProducerBuilder::<samsa::prelude::TlsConnection>::new(
//!         tls_option,
//!         vec![topic_name.to_string()]
//!     )
//!     .await?
//!     .batch_timeout_ms(1)
//!     .max_batch_size(2)
//!     .clone()
//!     .build()
//!     .await;
//!
//! producer_client
//!     .produce(message)
//!     .await;
//!
//! ```
//!
//! Example for Consumer with TLS support:
//! ```rust
//! let tls_option = samsa::prelude::TlsConnectionOptions {
//!         broker_options: vec![samsa::prelude::BrokerAddress {
//!             host: "127.0.0.1:9092".to_owned(),
//!             port: 9092,
//!         }],
//!         key: "/path_to_key_file".into(),
//!         cert: "/path_to_cert_file".into(),
//!         cafile: Some("/path_to_ca_file".into()),
//!     };
//! let partitions = vec![0];
//! let topic_name = "my-topic".to_string();
//! let assignment = samsa::prelude::TopicPartitionsBuilder::new()
//!     .assign(topic_name, partitions)
//!     .build();
//!
//! let consumer = samsa::prelude::ConsumerBuilder::<samsa::prelude::TlsConnection>::new(
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
//! while let Some(Ok((batch, offsets))) = stream.next().await {
//!     println!("{:?}", batch);
//! }
//! ```
//!
//! ### Compression support
//! We provide support for compression in the producer using the `Compression` enum. The enum allows to specify what type of compression to use.
//!
//! Example for Producer with TLS and GZIP compression support:
//! ```rust
//! let tls_option = samsa::prelude::TlsConnectionOptions {
//!         broker_options: vec![samsa::prelude::BrokerAddress {
//!             host: "127.0.0.1:9092".to_owned(),
//!             port: 9092,
//!         }],
//!         key: "/path_to_key_file".into(),
//!         cert: "/path_to_cert_file".into(),
//!         cafile: Some("/path_to_ca_file".into()),
//!     };
//! let topic_name = "my-topic".to_string();
//! let partition_id = 0;
//!
//! let message = samsa::prelude::ProduceMessage {
//!         topic: topic_name.to_string(),
//!         partition_id,
//!         key: Some(bytes::Bytes::from_static(b"Tester")),
//!         value: Some(bytes::Bytes::from_static(b"Value")),
//!         headers: vec![
//!             samsa::prelude::Header::new(String::from("Key"), bytes::Bytes::from("Value"))
//!         ],
//!     };
//!
//! let producer_client = samsa::prelude::ProducerBuilder::new(tls_option, vec![topic_name.to_string()])
//!     .await?
//!     .compression(samsa::prelude::Compression::Gzip)
//!     .batch_timeout_ms(1)
//!     .max_batch_size(2)
//!     .clone()
//!     .build()
//!     .await;
//!
//! producer_client
//!     .produce(message)
//!     .await;
//!
//! ```
//!
//!
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
    //! We break the library into three main secitons:
    //! - [Producing](#producing)
    //! - [Consuming](#consuming)
    //! - [Consumer Groups](#consumer-groups)
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
    //! use samsa::prelude::TcpConnection;
    //! let bootstrap_addrs = vec![samsa::prelude::BrokerAddress {
    //!             host: "127.0.0.1:9092".to_owned(),
    //!             port: 9092,
    //!         }];
    //! let topic_name = "my-topic";
    //! let partition_id = 0;
    //!
    //! let message = samsa::prelude::ProduceMessage {
    //!         topic: topic_name.to_string(),
    //!         partition_id,
    //!         key: Some(bytes::Bytes::from_static(b"Tester")),
    //!         value: Some(bytes::Bytes::from_static(b"Value")),
    //!         headers: vec![
    //!             samsa::prelude::Header::new(String::from("Key"), bytes::Bytes::from("Value"))
    //!         ]
    //!     };
    //!
    //! let producer_client = samsa::prelude::ProducerBuilder::<TcpConnection>::new(
    //!         bootstrap_addrs,
    //!         vec![topic_name.to_string()]
    //!     )
    //!     .await?
    //!     .batch_timeout_ms(1)
    //!     .max_batch_size(2)
    //!     .clone()
    //!     .build()
    //!     .await;
    //!
    //! producer_client
    //!     .produce(message)
    //!     .await;
    //! ```
    //!
    //! ## Produce protocol functions
    //! We provide a set of protocol primitives for users to build their own clients.
    //! They are presented as the building blocks that we use to build the higher level
    //! abstractions.
    //!
    //! ### Produce
    //! [`produce`] sends messages to a broker.
    //! #### Example
    //! ```rust
    //! use samsa::prelude::produce;
    //! produce(
    //!     broker_conn,
    //!     correlation_id,
    //!     client_id,
    //!     required_acks,
    //!     timeout_ms,
    //!     messages,
    //! ).await?;
    //! ```
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
    //! use samsa::prelude::TcpConnection;
    //! let bootstrap_addrs = vec![samsa::prelude::BrokerAddress {
    //!         host: "127.0.0.1:9092".to_owned(),
    //!         port: 9092,
    //!     }];
    //! let partitions = vec![0];
    //! let topic_name = "my-topic".to_string();
    //! let assignment = samsa::prelude::TopicPartitionsBuilder::new()
    //!     .assign(topic_name, partitions)
    //!     .build();
    //!
    //! let consumer = samsa::prelude::ConsumerBuilder::<TcpConnection>::new(
    //!     bootstrap_addrs,
    //!     assignment,
    //! )
    //! .await?
    //! .build();
    //!
    //! let stream = consumer.into_stream();
    //! // have to pin streams before iterating
    //! tokio::pin!(stream);
    //!
    //! // Stream will do nothing unless consumed.
    //! while let Some(Ok((batch, offsets))) = stream.next().await {
    //!     println!("{:?}", batch);
    //! }
    //! ```
    //!
    //! ## Consume protocol functions
    //! We provide a set of protocol primitives for users to build their own clients.
    //! They are presented as the building blocks that we use to build the higher level
    //! abstractions.
    //!
    //! ### List Offsets
    //! [`list_offsets`] finds the offsets given a timestamp.
    //! #### Example
    //! ```rust
    //! use std::collections::HashMap;
    //! use samsa::prelude::list_offsets;
    //! let partitions = vec![0];
    //! let topic_name = "my-topic".to_string();
    //! let topic_partitions = samsa::prelude::TopicPartitionsBuilder::new()
    //!     .assign(topic_name, partitions)
    //!     .build();
    //! let offset_response = list_offsets(
    //!     conn,
    //!     correlation_id,
    //!     client_id,
    //!     topic_partitions,
    //!     -1
    //! ).await?;
    //! ```
    //!
    //! ### Fetch
    //! [`fetch`] fetches a batch of messages.
    //! #### Example
    //! ```rust
    //! use samsa::prelude::fetch;
    //! let fetch_response = fetch(
    //!     broker_conn,
    //!     correlation_id,
    //!     client_id,
    //!     max_wait_ms,
    //!     min_bytes,
    //!     max_bytes,
    //!     max_partition_bytes,
    //!     isolation_level,
    //!     &topic_partitions,
    //!     offsets,
    //! ).await?;
    //! ```
    //!
    //! ### Fetch Offset
    //! [`fetch_offset`] gets the offsets of a consumer group.
    //! #### Example
    //! ```rust
    //! use samsa::prelude::fetch_offset;
    //! let offset_fetch_response = fetch_offset(
    //!     correlation_id,
    //!     client_id,
    //!     group_id,
    //!     coordinator_conn,
    //!     topic_partitions
    //! ).await?;
    //! ```
    //! ### Commit Offset
    //! [`commit_offset`] commits a set of offsets for a group.
    //! #### Example
    //! ```rust
    //! use samsa::prelude::commit_offset;
    //! let offset_commit_response = commit_offset(
    //!     correlation_id,
    //!     client_id,
    //!     group_id,
    //!     coordinator_conn,
    //!     generation_id,
    //!     member_id,
    //!     offsets,
    //!     retention_time_ms,
    //! ).await?;
    //! ```
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
    //!
    //! ### Example
    //! ```rust
    //! use tokio_stream::StreamExt;
    //! use samsa::prelude::TcpConnection;
    //! let bootstrap_addrs = vec![samsa::prelude::BrokerAddress {
    //!         host: "127.0.0.1:9092".to_owned(),
    //!         port: 9092,
    //!     }];
    //! let partitions = vec![0];
    //! let topic_name = "my-topic".to_string();
    //! let assignment = samsa::prelude::TopicPartitionsBuilder::new()
    //!     .assign(topic_name, partitions)
    //!     .build();
    //! let group_id = "The Data Team".to_string();
    //!
    //! let consumer_group_member = samsa::prelude::ConsumerGroupBuilder::<TcpConnection>::new(
    //!     bootstrap_addrs,
    //!     group_id,
    //!     assignment,
    //! ).await?
    //! .build().await?;
    //!
    //! let stream = consumer_group_member.into_stream();
    //! // have to pin streams before iterating
    //! tokio::pin!(stream);
    //!
    //! // Stream will do nothing unless consumed.
    //! while let Some(batch) = stream.next().await {
    //!     println!("{:?}", batch);
    //! }
    //! ```
    //!
    //! ## Consumer Group Protocol functions
    //! We provide a set of protocol primitives for users to build their own clients.
    //! They are presented as the building blocks that we use to build the higher level
    //! abstractions.
    //!
    //! ### Join Group
    //! [`join_group`] Become a member of a group, creating it if there are no active members..
    //! #### Example
    //! ```rust
    //! use samsa::prelude::join_group;
    //! let join_response = join_group(
    //!     correlation_id,
    //!     client_id,
    //!     group_id,
    //!     session_timeout_ms,
    //!     rebalance_timeout_ms,
    //!     member_id,
    //!     protocol_type,
    //!     protocols,
    //! ).await?;
    //! ```
    //!  ### Sync Group
    //! [`sync_group`] Synchronize state for all members of a group.
    //! #### Example
    //! ```rust
    //! use samsa::prelude::sync_group;
    //! let sync_response = sync_group(
    //!     correlation_id,
    //!     client_id,
    //!     group_id,
    //!     generation_id,
    //!     member_id,
    //!     assignments,
    //! ).await?;
    //! ```
    //!
    //! ### Heartbeat
    //! [`heartbeat`] Keep a member alive in the group.
    //! #### Example
    //! ```rust
    //! use samsa::prelude::heartbeat;
    //! let heartbeat_response = heartbeat(
    //!     correlation_id,
    //!     client_id,
    //!     group_id,
    //!     generation_id,
    //!     member_id,
    //! ).await?;
    //! ```
    //!
    //! ### Leave Group
    //! [`leave_group`] Directly depart a group.
    //! #### Example
    //! ```rust
    //! use samsa::prelude::leave_group;
    //! let leave_response = leave_group(
    //!     correlation_id, client_id, group_id, member_id
    //! ).await?;
    //! ```
    //!
    //! ## TLS support
    //! We provide TLS support to your consumer or producer for secured communication. To enable this, start with specifying the `TlsConnectionOptions`,
    //! and pass it into an instance of the `ProducerBuilder` or `ConsumerBuilder`.
    //!
    //! ### Example for Producer with TLS support:
    //! ```rust
    //! let tls_option = samsa::prelude::TlsConnectionOptions {
    //!         broker_options: vec![samsa::prelude::BrokerAddress {
    //!           host: "127.0.0.1:9092".to_owned(),
    //!           port: 9092,
    //!         }],
    //!         key: "/path_to_key_file".into(),
    //!         cert: "/path_to_cert_file".into(),
    //!         cafile: Some("/path_to_ca_file".into()),
    //!     };
    //! let topic_name = "my-topic".to_string();
    //! let partition_id = 0;
    //!
    //! let message = samsa::prelude::ProduceMessage {
    //!         topic: topic_name.to_string(),
    //!         partition_id,
    //!         key: Some(bytes::Bytes::from_static(b"Tester")),
    //!         value: Some(bytes::Bytes::from_static(b"Value")),
    //!         headers: vec![
    //!             samsa::prelude::Header::new(String::from("Key"), bytes::Bytes::from("Value"))
    //!         ],
    //!     };
    //!
    //! let producer_client = samsa::prelude::ProducerBuilder::<samsa::prelude::TlsConnection>::new(
    //!         tls_option,
    //!         vec![topic_name.to_string()]
    //!     )
    //!     .await?
    //!     .batch_timeout_ms(1)
    //!     .max_batch_size(2)
    //!     .clone()
    //!     .build()
    //!     .await;
    //!
    //! producer_client
    //!     .produce(message)
    //!     .await;
    //!
    //! ```
    //!
    //! ### Example for Consumer with TLS support:
    //! ```rust
    //! let tls_option = samsa::prelude::TlsConnectionOptions {
    //!         broker_options: vec![samsa::prelude::BrokerAddress {
    //!             host: "127.0.0.1:9092".to_owned(),
    //!             port: 9092,
    //!         }],
    //!         key: "/path_to_key_file".into(),
    //!         cert: "/path_to_cert_file".into(),
    //!         cafile: Some("/path_to_ca_file".into()),
    //!     };
    //! let partitions = vec![0];
    //! let topic_name = "my-topic".to_string();
    //! let assignment = samsa::prelude::TopicPartitionsBuilder::new()
    //!     .assign(topic_name, partitions)
    //!     .build();
    //!
    //! let consumer = samsa::prelude::ConsumerBuilder::<samsa::prelude::TlsConnection>::new(
    //!     tls_option,
    //!     assignment,
    //! )
    //! .await?
    //! .build();
    //!
    //! let stream = consumer.into_stream();
    //! // have to pin streams before iterating
    //! tokio::pin!(stream);
    //!
    //! // Stream will do nothing unless consumed.
    //! while let Some(Ok((batch, offsets))) = stream.next().await {
    //!     println!("{:?}", batch);
    //! }
    //! ```
    //!
    //! ## Compression support
    //! We provide support for compression in the producer using the `Compression` enum. The enum allows to specify what type of compression to use.
    //!
    //! ### Example for Producer with TLS and GZIP compression support:
    //! ```rust
    //! let tls_option =samsa::prelude::TlsConnectionOptions {
    //!         broker_options: vec![samsa::prelude::BrokerAddress {
    //!             host: "127.0.0.1:9092".to_owned(),
    //!             port: 9092,
    //!         }],
    //!         key: "/path_to_key_file".into(),
    //!         cert: "/path_to_cert_file".into(),
    //!         cafile: Some("/path_to_ca_file".into()),
    //!     };
    //! let topic_name = "my-topic".to_string();
    //! let partition_id = 0;
    //!
    //! let message = samsa::prelude::ProduceMessage {
    //!         topic: topic_name.to_string(),
    //!         partition_id,
    //!         key: Some(bytes::Bytes::from_static(b"Tester")),
    //!         value: Some(bytes::Bytes::from_static(b"Value")),
    //!         headers: vec![
    //!             samsa::prelude::Header::new(String::from("Key"), bytes::Bytes::from("Value"))
    //!         ]
    //!     };
    //!
    //! let producer_client = samsa::prelude::ProducerBuilder::new(tls_option, vec![topic_name.to_string()])
    //!     .await?
    //!     .compression(samsa::prelude::Compression::Gzip)
    //!     .batch_timeout_ms(1)
    //!     .max_batch_size(2)
    //!     .clone()
    //!     .build()
    //!     .await;
    //!
    //! producer_client
    //!     .produce(message)
    //!     .await;
    //!
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
        tcp::TcpConnection,
        tls::{TlsConnection, TlsConnectionOptions},
        BrokerAddress, BrokerConnection,
    };
    pub use crate::producer::{produce, ProduceMessage, Producer};
    pub use crate::producer_builder::ProducerBuilder;
    pub use crate::protocol::Header;

    pub use bytes;

    pub mod encode {
        pub use crate::encode::*;
    }

    pub mod protocol {
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
