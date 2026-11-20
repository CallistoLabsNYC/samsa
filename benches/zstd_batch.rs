//! Zstd record batch encode (produce) and decode (fetch) through the public
//! protocol API.
//!
//! Run with: cargo bench --bench zstd_batch

use bytes::{BufMut, Bytes};
use criterion::*;
use samsa::prelude::{
    encode::ToByte,
    protocol::{self, produce::request::Attributes, FetchResponse},
    Compression,
};

const CLIENT_ID: &str = "bench";
const TOPIC: &str = "events";
const PARTITION: i32 = 0;

/// Uncompressed payload sizes (keys plus values) to benchmark.
const SIZES: [(&str, usize); 3] = [
    ("1KiB", 1 << 10),
    ("16KiB", 16 << 10),
    ("256KiB", 256 << 10),
];

/// Xorshift64 with a fixed seed, so every run sees the same records.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }

    fn pick<T: Copy>(&mut self, items: &[T]) -> T {
        items[self.below(items.len() as u64) as usize]
    }
}

/// JSON event records keyed by user, until keys plus values reach `target`
/// bytes.
fn event_records(target: usize) -> Vec<(Bytes, Bytes)> {
    const EVENTS: [&str; 7] = [
        "page_view",
        "click",
        "add_to_cart",
        "checkout",
        "login",
        "logout",
        "search",
    ];
    const PATHS: [&str; 7] = [
        "/",
        "/products",
        "/products/detail",
        "/cart",
        "/checkout",
        "/account/settings",
        "/search",
    ];
    const REGIONS: [&str; 5] = [
        "us-east-1",
        "us-west-2",
        "eu-west-1",
        "eu-central-1",
        "ap-southeast-1",
    ];
    const AGENTS: [&str; 4] = [
        "Mozilla/5.0 (X11; Linux x86_64)",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5)",
        "Mozilla/5.0 (iPhone; CPU iPhone OS 17_5 like Mac OS X)",
        "okhttp/4.12.0",
    ];
    const STATUS: [u16; 10] = [200, 200, 200, 200, 200, 201, 304, 400, 404, 500];

    let mut rng = Rng(0x9e37_79b9_7f4a_7c15);
    let mut ts = 1_700_000_000_000u64;
    let mut records = Vec::new();
    let mut total = 0;
    while total < target {
        ts += rng.below(50);
        let user = rng.below(50_000);
        let key = format!("user-{user}");
        let value = format!(
            "{{\"ts\":{ts},\"user_id\":{user},\"session\":\"{:016x}\",\"event\":\"{}\",\
             \"path\":\"{}\",\"item_id\":{},\"status\":{},\"latency_ms\":{},\
             \"region\":\"{}\",\"user_agent\":\"{}\"}}",
            rng.next(),
            rng.pick(&EVENTS),
            rng.pick(&PATHS),
            rng.below(10_000),
            rng.pick(&STATUS),
            5 + rng.below(400),
            rng.pick(&REGIONS),
            rng.pick(&AGENTS),
        );
        total += key.len() + value.len();
        records.push((Bytes::from(key), Bytes::from(value)));
    }
    records
}

fn produce_request(records: &[(Bytes, Bytes)]) -> protocol::ProduceRequest<'static> {
    let attributes = Attributes::new(Some(Compression::Zstd));
    let mut request = protocol::ProduceRequest::new(1, 1000, 1, CLIENT_ID, attributes);
    for (key, value) in records {
        request.add(
            TOPIC,
            PARTITION,
            Some(key.clone()),
            Some(value.clone()),
            vec![],
        );
    }
    request
}

/// The record batch at the end of an encoded single-partition Produce v3
/// request.
fn record_batch(request: &[u8]) -> &[u8] {
    // Header (api key, version, correlation id, client id), null
    // transactional id, acks, timeout, topic array, topic name, partition
    // array, partition id, record set length.
    let prefix = 2 + 2 + 4 + 2 + CLIENT_ID.len() + 2 + 2 + 4 + 4 + 2 + TOPIC.len() + 4 + 4 + 4;
    let declared = i32::from_be_bytes(request[prefix - 4..prefix].try_into().unwrap());
    assert_eq!(declared as usize, request.len() - prefix);
    &request[prefix..]
}

/// A Fetch v4 response carrying `batch` for one topic partition.
fn fetch_response(batch: &[u8]) -> Bytes {
    let mut out = Vec::new();
    out.put_i32(1); // correlation id
    out.put_i32(0); // throttle time
    out.put_i32(1); // topics
    out.put_i16(TOPIC.len() as i16);
    out.put_slice(TOPIC.as_bytes());
    out.put_i32(1); // partitions
    out.put_i32(PARTITION);
    out.put_i16(0); // error code
    out.put_i64(1_000); // high watermark
    out.put_i64(1_000); // last stable offset
    out.put_i32(0); // aborted transactions
    out.put_i32(batch.len() as i32);
    out.put_slice(batch);
    Bytes::from(out)
}

fn bench_zstd_batch(c: &mut Criterion) {
    let mut encode = c.benchmark_group("samsa_zstd_encode");
    let mut cases = Vec::new();
    for (label, size) in SIZES {
        let records = event_records(size);
        let raw: usize = records.iter().map(|(k, v)| k.len() + v.len()).sum();
        let request = produce_request(&records);

        let mut wire = Vec::new();
        request.encode(&mut wire).unwrap();
        let batch = record_batch(&wire);
        eprintln!(
            "samsa zstd {label}: {} records, {raw} B keys+values -> {} B record batch",
            records.len(),
            batch.len()
        );
        let response = fetch_response(batch);
        let decoded = FetchResponse::try_from(response.clone()).unwrap();
        assert_eq!(decoded.record_count(), records.len());

        encode.throughput(Throughput::Bytes(raw as u64));
        encode.bench_with_input(
            BenchmarkId::from_parameter(label),
            &request,
            |b, request| {
                let mut out = Vec::with_capacity(wire.len());
                b.iter(|| {
                    out.clear();
                    request.encode(&mut out).unwrap();
                    black_box(out.len())
                });
            },
        );
        cases.push((label, raw, response));
    }
    encode.finish();

    let mut decode = c.benchmark_group("samsa_zstd_decode");
    for (label, raw, response) in &cases {
        decode.throughput(Throughput::Bytes(*raw as u64));
        decode.bench_with_input(
            BenchmarkId::from_parameter(label),
            response,
            |b, response| {
                b.iter(|| black_box(FetchResponse::try_from(response.clone()).unwrap()));
            },
        );
    }
    decode.finish();
}

criterion_group!(benches, bench_zstd_batch);
criterion_main!(benches);
