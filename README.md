# Samsa
Rust-native Kafka/Redpanda protocol and client implementation.

This crate provides Rust native consumers and producers as well as low level bindings for the Apache Kafka protocol. Unlike crates that use librdkafka in an FFI, users of this crate will *not* need the C lib and will benefit from Rust all the way down; meaning memory safety, safe concurrency, low resource usage, and of course blazing speed.

[Documentation](https://docs.rs/samsa/latest/samsa/)

# Goals
- Easy to understand code
- Leverage best in class libraries such as [Tokio](https://tokio.rs/), [Nom](https://docs.rs/nom/latest/nom/) to do the heavy lifting
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
    - [SASL support](#sasl-support)
- [Examples](#examples)
- [Development setup](#development-setup)
    - [Tests](#tests)
- [Benchmarks](#benchmarks)
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

To instantiate one, it is easiest to use a Stream and the `ProducerBuilder`.
```rust
use samsa::prelude::*;

let bootstrap_addrs = vec![BrokerAddress {
        host: "127.0.0.1".to_owned(),
        port: 9092,
    }];
let topic_name = "my-topic".to_string();
let partition_id = 0;

// create a stream of 5k messages in batches of 100
let stream = iter(0..5000).map(|_| ProduceMessage {
    topic: topic_name.to_string(),
    partition_id,
    key: Some(bytes::Bytes::from_static(b"Tester")),
    value: Some(bytes::Bytes::from_static(b"Value")),
    headers: vec![
        samsa::prelude::Header::new(String::from("Key"), bytes::Bytes::from("Value"))
    ],
}).chunks(100);

let output_stream =
ProducerBuilder::<TcpConnection>::new(bootstrap_addrs, vec![topic_name.to_string()])
    .await?
    .batch_timeout_ms(100)
    .max_batch_size(100)
    .clone()
    .build_from_stream(stream)
    .await;

tokio::pin!(output_stream);
while (output_stream.next().await).is_some() {}

```

### Consumer
A `Consumer` is used to fetch messages from the broker. It is an asynchronous iterator that can be configured to auto-commit. To instantiate one, start with a `ConsumerBuilder`.
```rust
use samsa::prelude::*;

let bootstrap_addrs = vec![BrokerAddress {
        host: "127.0.0.1".to_owned(),
        port: 9092,
    }];
let partitions = vec![0];
let topic_name = "my-topic".to_string();
let assignment = TopicPartitionsBuilder::new()
    .assign(topic_name, partitions)
    .build();

let consumer = ConsumerBuilder::<TcpConnection>::new(
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
use samsa::prelude::*;

let bootstrap_addrs = vec![BrokerAddress {
        host: "127.0.0.1".to_owned(),
        port: 9092,
    }];
let partitions = vec![0];
let topic_name = "my-topic".to_string();
let assignment = TopicPartitionsBuilder::new()
    .assign(topic_name, partitions)
    .build();
let group_id = "The Data Engineering Team".to_string();

let consumer_group_member = ConsumerGroupBuilder::<TcpConnection>::new(
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
use samsa::prelude::*;

let tls_option = TlsConnectionOptions {
        broker_options: vec![BrokerAddress {
          host: "127.0.0.1".to_owned(),
          port: 9092,
        }],
        key: "/path_to_key_file".into(),
        cert: "/path_to_cert_file".into(),
        cafile: Some("/path_to_ca_file".into()),
    };
let topic_name = "my-topic".to_string();
let partition_id = 0;

let message = ProduceMessage {
        topic: topic_name.to_string(),
        partition_id,
        key: Some(bytes::Bytes::from_static(b"Tester")),
        value: Some(bytes::Bytes::from_static(b"Value")),
        headers: vec![
            Header::new(String::from("Key"), bytes::Bytes::from("Value"))
        ],
    };

let producer_client = ProducerBuilder::<TlsConnection>::new(
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
use samsa::prelude::*;

let tls_option = TlsConnectionOptions {
        broker_options: vec![BrokerAddress {
            host: "127.0.0.1".to_owned(),
            port: 9092,
        }],
        key: "/path_to_key_file".into(),
        cert: "/path_to_cert_file".into(),
        cafile: Some("/path_to_ca_file".into()),
    };
let partitions = vec![0];
let topic_name = "my-topic".to_string();
let assignment = TopicPartitionsBuilder::new()
    .assign(topic_name, partitions)
    .build();

let consumer = ConsumerBuilder::<TlsConnection>::new(
        tls_option,
        assignment,
    )
    .await?
    .build();

let stream = consumer.into_stream();
// have to pin streams before iterating
tokio::pin!(stream);

// Stream will do nothing unless consumed.
while let Some(batch) = stream.next().await {
    println!("{:?} messages read", batch.unwrap().count());
}
```

### Compression support
We provide support for compression in the producer using the `Compression` enum. The enum allows to specify what type of compression to use. The Consumer will automatically know to decompress the message.

Example for Producer with TLS and GZIP compression support:
```rust
use samsa::prelude::*;

let tls_option = TlsConnectionOptions {
        broker_options: vec![BrokerAddress {
            host: "127.0.0.1".to_owned(),
            port: 9092,
        }],
        key: "/path_to_key_file".into(),
        cert: "/path_to_cert_file".into(),
        cafile: Some("/path_to_ca_file".into()),
    };
let topic_name = "my-topic".to_string();
let partition_id = 0;

let message = ProduceMessage {
        topic: topic_name.to_string(),
        partition_id,
        key: Some(bytes::Bytes::from_static(b"Tester")),
        value: Some(bytes::Bytes::from_static(b"Value")),
        headers: vec![
            Header::new(String::from("Key"), bytes::Bytes::from("Value"))
        ],
    };

let producer_client = ProducerBuilder::new(tls_option, vec![topic_name.to_string()])
    .await?
    .compression(Compression::Gzip)
    .batch_timeout_ms(1)
    .max_batch_size(2)
    .clone()
    .build()
    .await;

producer_client
    .produce(message)
    .await;
```

### SASL support
We include support for SASL using all typical mechanisms: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512. This is represented as another type of BrokerConnection that our Consumers and Producers recieve as a generic parameter. All that is needed is to provide the credentials.

Example for Producer using both TLS and SASL:
```rust
use samsa::prelude::*;

let tls_config = TlsConnectionOptions {
    broker_options: vec![BrokerAddress {
        host: "127.0.0.1".to_owned(),
        port: 9092,
    }],
    key: "/path_to_key_file".into(),
    cert: "/path_to_cert_file".into(),
    cafile: Some("/path_to_ca_file".into()),
};

let sasl_config = SaslConfig::new(String::from("myuser"), String::from("pass1234"), None, None);

let options = SaslTlsConfig {
    tls_config,
    sasl_config,
};

let topic_name = "atopic";

let s = ConsumerBuilder::<SaslTlsConnection>::new(
    options,
    TopicPartitionsBuilder::new()
        .assign(topic_name.to_owned(), vec![0])
        .build(),
)
.await
.unwrap()
.build()
.into_stream();

tokio::pin!(s);

while let Some(m) = s.next().await {
    tracing::info!("{:?} messages read", m.unwrap().count());
}
```

## Examples
We provide high level [examples](/examples) for those interested in trying `samsa` out. The setup is as follows:

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

1. Run `cargo run --example consumer_group`
1. Visit http://localhost:8080/groups/Squad to see the lone member.
1. In another terminal window, run `cargo run --example consumer_group`
1. Visit the console to see the new member join.
1. Repeat 2 more times to see the full group.

### TLS
We have a replica of the Producer and Consumer examples that utilize the TLS support. You will need a Cluster running with TLS enabled and the correct certificates in this codebase.

1. Set up a cluster that uses the correct TLS configuration
1. Copy your certs into the project
1. Update the paths in the following examples to be correct
1. Run `cargo run --example tls_produce`
1. Run `cargo run --example tls_consume`

### SASL
We have a replica of the Producer and Consumer examples that utilize the SASL support. You will need a Cluster running with SASL enabled.

1. Set up a cluster that uses the correct SASL configuration
1. Update the credentials in the following examples to be correct
1. Run `cargo run --example sasl`
1. Run `cargo run --example sasl_tls`

## Development setup
To set up the development environment, you will need the [Rust Toolchain](https://rustup.rs/) and [Docker](https://docs.docker.com/engine/install/). 

- Run `docker-compose up` to spin up a Redpanda cluster.

### Tests
To run the tests, be sure to have the cluster running. Run `KAFKA_BROKERS=[your cluster url] cargo test --tests --all-features -- --show-output --test-threads=1`

## Benchmarks
We provide a way to benchmark the library's performance through Criterion. This requires a small amount of setup:
1. Set up a local redpanda cluster
1. Create a topic with 1 partition named `benchmark`
1. Run the `producer` example using `cargo run --example producer` to put data in that topic
1. Run `cargo bench`

### Results
On a 2020 Macbook Pro, 2 GHz Quad-Core Intel Core i5, 16 GB 3733 MHz LPDDR4X
#### Produce 1 million 10 byte messages
1.9s

#### Consume 1 million 10 byte messages
1.6s


## Resources
- [Kafka Protocol Spec](https://kafka.apache.org/protocol.html)
- [Confluence Docs](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol)
