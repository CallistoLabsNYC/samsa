use bytes::Bytes;
use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::{criterion_group, criterion_main};
use futures::stream::{iter, StreamExt};
use samsa::prelude::*;

// Here we have an async function to benchmark
async fn do_something(size: usize) {
    let bootstrap_addrs = vec![samsa::prelude::BrokerAddress {
        host: "127.0.0.1".to_owned(),
        port: 9092,
    }];

    let topic = "benchmark";

    let stream = iter(0..size).map(|_| ProduceMessage {
        topic: topic.to_string(),
        partition_id: 0,
        key: None,
        value: Some(Bytes::from_static(b"0123456789")),
        headers: vec![],
    });

    tracing::info!("Connecting to cluster");
    let output_stream =
        ProducerBuilder::<TcpConnection>::new(bootstrap_addrs, vec![topic.to_string()])
            .await
            .map_err(|err| tracing::error!("{:?}", err))
            .unwrap()
            .required_acks(1)
            .clone()
            .build_from_stream(stream.chunks(50000))
            .await;

    tokio::pin!(output_stream);
    while (output_stream.next().await).is_some() {}
}

fn from_elem(c: &mut Criterion) {
    let size: usize = 1_000_000;

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .build()
        .unwrap();

    c.bench_with_input(
        BenchmarkId::new("produce_10byte_messages", size),
        &size,
        |b, &s| {
            // Insert a call to `to_async` to convert the bencher to async mode.
            // The timing loops are the same as with the normal bencher.
            b.to_async(&runtime).iter(|| do_something(s));
        },
    );
}

criterion_group!(benches, from_elem);
criterion_main!(benches);
