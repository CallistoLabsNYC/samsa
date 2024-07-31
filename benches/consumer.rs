use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::{criterion_group, criterion_main};
use futures::stream::StreamExt;
use samsa::prelude::*;

// Here we have an async function to benchmark
async fn do_something(size: usize) {
    let bootstrap_addrs = vec![samsa::prelude::BrokerAddress {
        host: "127.0.0.1".to_owned(),
        port: 9092,
    }];

    let topic = "benchmark";

    let stream = ConsumerBuilder::<TcpConnection>::new(
        bootstrap_addrs.clone(),
        TopicPartitionsBuilder::new()
            .assign(topic.to_string(), vec![0])
            .build(),
    )
    .await
    .map_err(|err| tracing::error!("{:?}", err))
    .unwrap()
    .max_bytes(3000000)
    .max_partition_bytes(3000000)
    .build()
    .into_stream();

    let mut count = 0;
    tokio::pin!(stream);
    while let Some(message) = stream.next().await {
        let new = message.unwrap().count();
        count += new;
        if count == size {
            break;
        }
    }
}

fn from_elem(c: &mut Criterion) {
    let size: usize = 1_000_000;

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .build()
        .unwrap();

    c.bench_with_input(
        BenchmarkId::new("consume_10byte_messages", size),
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
