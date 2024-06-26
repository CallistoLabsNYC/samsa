# Samsa
Rust-native Kafka/Redpanda protocol and client implementation.

This crate provides Rust native consumers and producers as well as low level bindings for the Apache Kafka protocol. Unlike crates that use librdkafka in an FFI, users of this crate actually benefit from Rust all the way down; meaning memory safety, safe concurrency, low resource usage, and of course blazing speed.

We are happy to recently include **Gzip compression** and **TLS** support.

[Documentation](https://docs.rs/samsa/latest/samsa/)

# Goals
- Easy to understand code
- Leverage best in class libraries such as Tokio, Nom to do the heavy lifting
- Start with a robust foundation and add more advanced features over time
- Provide a pure rust implementation of the Kafka protocol 
- Be a good building block for future works based around Kafka

## Table of contents
- [Getting started](#getting-started)
    - [Producer](#producer)
    - [Consumer](#consumer)
    - [Consumer group](#consumer-group)
    - [TLS support](#tls-support)
    - [Compression support](#compression-support)
- [Examples](#examples)
- [Development setup](#development-setup)
- [Resources](#resources)


## Getting started
Install `samsa` to your rust project with `cargo add samsa` or include the following snippet in your `Cargo.toml` dependencies:
```toml
samsa = "0.1"
```

This project includes Docker Compose files to help set up Redpanda and Kafka clusters to ease with testing. The easiest way to do this is to run `docker-compose up` to spin up a 2 broker Redpanda cluster. If you want to use different versions of Kafka, check out the [DockerCompose.README.md](/DockerCompose.README.md)

### Producer
A producer sends messages to the given topic and partition. 

It is buffered, with both a timeout and volume threshold that clears the buffer when reached. This is how letency and throughout can be tweaked to achieve the desired rates.
```rust
let bootstrap_addrs = vec![samsa::prelude::BrokerAddress {
        host: "127.0.0.1:9092".to_owned(),
        port: 9092,
    }];
let topic_name = "my-topic".to_string();
let partition_id = 0;

let message = samsa::prelude::ProduceMessage {
        topic: topic_name.to_string(),
        partition_id,
        key: Some(bytes::Bytes::from_static(b"Tester")),
        value: Some(bytes::Bytes::from_static(b"Value")),
        headers: vec![
            samsa::prelude::Header::new(String::from("Key"), bytes::Bytes::from("Value"))
        ],
    };

let producer_client = samsa::prelude::ProducerBuilder::<samsa::prelude::TcpConnection>::new(
        bootstrap_addrs, 
        vec![topic_name.to_string()]
    )
    .await?
    .batch_timeout_ms(1)
    .max_batch_size(2)
    .clone()
    .build()
    .await;

producer_client
    .produce(message)
    .await;
```

### Consumer
A `Consumer` is used to fetch messages from the broker. It is an asynchronous iterator that can be configured to auto-commit. To instantiate one, start with a `ConsumerBuilder`.
```rust
let bootstrap_addrs = vec![samsa::prelude::BrokerAddress {
        host: "127.0.0.1:9092".to_owned(),
        port: 9092,
    }];
let partitions = vec![0];
let topic_name = "my-topic".to_string();
let assignment = samsa::prelude::TopicPartitionsBuilder::new()
    .assign(topic_name, partitions)
    .build();

let consumer = samsa::prelude::ConsumerBuilder::<samsa::prelude::TcpConnection>::new(
        bootstrap_addrs,
        assignment,
    )
    .await?
    .build();

let stream = consumer.into_stream();
// have to pin streams before iterating
tokio::pin!(stream);

// Stream will do nothing unless consumed.
while let Some(Ok((batch, offsets))) = stream.next().await {
    println!("{:?}", batch);
}
```

### Consumer group
You can set up a consumer group with a group id and assignment. The offsets are commit automatically for the member of the group.
```rust
let bootstrap_addrs = vec![samsa::prelude::BrokerAddress {
        host: "127.0.0.1:9092".to_owned(),
        port: 9092,
    }];
let partitions = vec![0];
let topic_name = "my-topic".to_string();
let assignment = samsa::prelude::TopicPartitionsBuilder::new()
    .assign(topic_name, partitions)
    .build();
let group_id = "The Data Boyz".to_string();

let consumer_group_member = samsa::prelude::ConsumerGroupBuilder::<samsa::prelude::TcpConnection>::new(
        bootstrap_addrs,
        group_id,
        assignment,
    ).await?
    .build()
    .await?;

let stream = consumer_group_member.into_stream();
// have to pin streams before iterating
tokio::pin!(stream);
 
// Stream will do nothing unless consumed.
while let Some(batch) = stream.next().await {
    println!("{:?}", batch);
}
```

### TLS support
You can add TLS support to your consumer or producer for secured communication. To enable this, start with specifying the `TlsConnectionOptions`,
and pass it into an instance of the `ProducerBuilder` or `ConsumerBuilder`.

Example for Producer with TLS support:
```rust
let tls_option = samsa::prelude::TlsConnectionOptions {
        broker_options: vec![samsa::prelude::BrokerAddress {
          host: "127.0.0.1:9092".to_owned(),
          port: 9092,
        }],
        key: "/path_to_key_file".into(),
        cert: "/path_to_cert_file".into(),
        cafile: Some("/path_to_ca_file".into()),
    };
let topic_name = "my-topic".to_string();
let partition_id = 0;

let message = samsa::prelude::ProduceMessage {
        topic: topic_name.to_string(),
        partition_id,
        key: Some(bytes::Bytes::from_static(b"Tester")),
        value: Some(bytes::Bytes::from_static(b"Value")),
        headers: vec![
            samsa::prelude::Header::new(String::from("Key"), bytes::Bytes::from("Value"))
        ],
    };

let producer_client = samsa::prelude::ProducerBuilder::<samsa::prelude::TlsConnection>::new(
        tls_option, 
        vec![topic_name.to_string()]
    )
    .await?
    .batch_timeout_ms(1)
    .max_batch_size(2)
    .clone()
    .build()
    .await;

producer_client
    .produce(message)
    .await;

```

Example for Consumer with TLS support:
```rust
let tls_option = samsa::prelude::TlsConnectionOptions {
        broker_options: vec![samsa::prelude::BrokerAddress {
            host: "127.0.0.1:9092".to_owned(),
            port: 9092,
        }],
        key: "/path_to_key_file".into(),
        cert: "/path_to_cert_file".into(),
        cafile: Some("/path_to_ca_file".into()),
    };
let partitions = vec![0];
let topic_name = "my-topic".to_string();
let assignment = samsa::prelude::TopicPartitionsBuilder::new()
    .assign(topic_name, partitions)
    .build();

let consumer = samsa::prelude::ConsumerBuilder::<samsa::prelude::TlsConnection>::new(
        tls_option,
        assignment,
    )
    .await?
    .build();

let stream = consumer.into_stream();
// have to pin streams before iterating
tokio::pin!(stream);

// Stream will do nothing unless consumed.
while let Some(Ok((batch, offsets))) = stream.next().await {
    println!("{:?}", batch);
}
```

### Compression support
We provide support for compression in the producer using the `Compression` enum. The enum allows to specify what type of compression to use.

Example for Producer with TLS and GZIP compression support:
```rust
let tls_option = samsa::prelude::TlsConnectionOptions {
        broker_options: vec![samsa::prelude::BrokerAddress {
            host: "127.0.0.1:9092".to_owned(),
            port: 9092,
        }],
        key: "/path_to_key_file".into(),
        cert: "/path_to_cert_file".into(),
        cafile: Some("/path_to_ca_file".into()),
    };
let topic_name = "my-topic".to_string();
let partition_id = 0;

let message = samsa::prelude::ProduceMessage {
        topic: topic_name.to_string(),
        partition_id,
        key: Some(bytes::Bytes::from_static(b"Tester")),
        value: Some(bytes::Bytes::from_static(b"Value")),
        headers: vec![
            samsa::prelude::Header::new(String::from("Key"), bytes::Bytes::from("Value"))
        ],
    };

let producer_client = samsa::prelude::ProducerBuilder::new(tls_option, vec![topic_name.to_string()])
    .await?
    .compression(samsa::prelude::Compression::Gzip)
    .batch_timeout_ms(1)
    .max_batch_size(2)
    .clone()
    .build()
    .await;

producer_client
    .produce(message)
    .await;

```

## Examples
We provide 3 high level [examples](/examples) for those interested in trying `samsa` out. There are TLS versions as well. The setup is as follows:

1. Use `docker-compose up` to start your redpanda cluster.
1. Go to http://localhost:8080/topics to view the redpanda console.
1. Create a topic named `my-topic` with 4 partitions.

### Producer
This one is good to run first to fill up your topic with data.
1. Run `cargo run --example producer`
1. Visit http://localhost:8080/topics/my-topic to see data flowing in!

### Consumer
Consume the messages in your topic.
1. Run `cargo run --example consumer`
1. Observe all the torrent of data in your terminal

### ConsumerGroup
Coordinate a group of 4 consumers. This one is throttled to see the group rebalancing.

### TLS
We have a replica of the Producer and Consumer examples that utilize the TLS support. You will need a Cluster running with TLS enabled and the correct certificates in this codebase.

1. Run `cargo run --example consumer_group`
1. Visit http://localhost:8080/groups/Squad to see the lone member.
1. In another terminal window, run `cargo run --example consumer_group`
1. Visit the console to see the new member join.
1. Repeat 2 more times to see the full group.

## Development setup
To set up the development environment, you will need the [Rust Toolchain](https://rustup.rs/) and [Docker](https://docs.docker.com/engine/install/). 

- Run `docker-compose up` to spin up a Redpanda cluster.

### Tests
To run the tests, be sure to have the cluster running. Run `KAFKA_BROKERS=[your cluster url] KAFKA_TOPIC=[tester topic name] cargo test --tests --all-features -- --show-output --test-threads=1`

## Resources
- [Kafka Protocol Spec](https://kafka.apache.org/protocol.html)
- [Confluence Docs](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol)
