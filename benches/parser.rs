use bytes::Bytes;
use criterion::*;
use nombytes::NomBytes;
use samsa::prelude::protocol::fetch::response_optimized::{
    parse_fetch_response, FetchResponse, Partition, Record, RecordBatch, Topic,
};

fn criterion_benchmark(c: &mut Criterion) {
    let data = NomBytes::new(Bytes::from_static(b"\0\0\0\x01\0\0\0\0\0\0\0\x01\0\rprice-updates\0\0\0\x01\0\0\0\0\0\0\0\0\0\0\0\0\0\x0e\0\0\0\0\0\0\0\x0e\xff\xff\xff\xff\0\0\x0e\xde\0\0\0\0\0\0\0\0\0\0\x01\x04\0\0\0\x01\x02\xd7\x8d\xc7G\0\0\0\0\0\0\0\0\x01\x8bH \xef\xc0\0\0\x01\x8bH \xef\xc0\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\0\0\0\x01\xa2\x03\0\0\0\x08TSLA\x8c\x03{\"symbol\": \"TSLA\", \"timestamp\": 1697722200000, \"open\": 225.56, \"high\": 227.17, \"low\": 224.44, \"close\": 227.17, \"volume\": 24265.0, \"trade_count\": 502.0, \"vwap\": 225.508012, \"data_provider\": \"alpaca\"}\0\0\0\0\0\0\0\0\x01\0\0\x01\x07\0\0\0\x01\x02\x0e\xbd[\xd6\0\0\0\0\0\0\0\0\x01\x8bH!\xda \0\0\x01\x8bH!\xda \xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\0\0\0\x01\xa8\x03\0\0\0\x08TSLA\x92\x03{\"symbol\": \"TSLA\", \"timestamp\": 1697722260000, \"open\": 227.215, \"high\": 228.88, \"low\": 226.955, \"close\": 228.845, \"volume\": 28919.0, \"trade_count\": 303.0, \"vwap\": 227.811826, \"data_provider\": \"alpaca\"}\0\0\0\0\0\0\0\0\x02\0\0\x01\x06\0\0\0\x01\x02\x85\xc3\xb3\xb3\0\0\0\0\0\0\0\0\x01\x8bH\"\xc4\x80\0\0\x01\x8bH\"\xc4\x80\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\0\0\0\x01\xa6\x03\0\0\0\x08TSLA\x90\x03{\"symbol\": \"TSLA\", \"timestamp\": 1697722320000, \"open\": 229.12, \"high\": 230.17, \"low\": 227.915, \"close\": 230.165, \"volume\": 33891.0, \"trade_count\": 390.0, \"vwap\": 229.520416, \"data_provider\": \"alpaca\"}\0\0\0\0\0\0\0\0\x03\0\0\x01\x05\0\0\0\x01\x02\xea&\xce\x0f\0\0\0\0\0\0\0\0\x01\x8bH#\xae\xe0\0\0\x01\x8bH#\xae\xe0\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\0\0\0\x01\xa4\x03\0\0\0\x08TSLA\x8e\x03{\"symbol\": \"TSLA\", \"timestamp\": 1697722380000, \"open\": 230.21, \"high\": 230.525, \"low\": 229.13, \"close\": 229.22, \"volume\": 33625.0, \"trade_count\": 401.0, \"vwap\": 229.998015, \"data_provider\": \"alpaca\"}\0\0\0\0\0\0\0\0\x04\0\0\x01\x05\0\0\0\x01\x02s\x95\x0c\x8f\0\0\0\0\0\0\0\0\x01\x8bH$\x99@\0\0\x01\x8bH$\x99@\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\0\0\0\x01\xa4\x03\0\0\0\x08TSLA\x8e\x03{\"symbol\": \"TSLA\", \"timestamp\": 1697722440000, \"open\": 228.84, \"high\": 229.305, \"low\": 227.93, \"close\": 228.44, \"volume\": 26574.0, \"trade_count\": 362.0, \"vwap\": 228.548357, \"data_provider\": \"alpaca\"}\0\0\0\0\0\0\0\0\x05\0\0\x01\x04\0\0\0\x01\x029@Eu\0\0\0\0\0\0\0\0\x01\x8bH%\x83\xa0\0\0\x01\x8bH%\x83\xa0\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\0\0\0\x01\xa2\x03\0\0\0\x08TSLA\x8c\x03{\"symbol\": \"TSLA\", \"timestamp\": 1697722500000, \"open\": 228.53, \"high\": 229.22, \"low\": 228.3, \"close\": 228.995, \"volume\": 11997.0, \"trade_count\": 142.0, \"vwap\": 228.818005, \"data_provider\": \"alpaca\"}\0\0\0\0\0\0\0\0\x06\0\0\x01\x03\0\0\0\x01\x02\xf5k\x0c\x83\0\0\0\0\0\0\0\0\x01\x8bH&n\0\0\0\x01\x8bH&n\0\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\0\0\0\x01\xa0\x03\0\0\0\x08TSLA\x8a\x03{\"symbol\": \"TSLA\", \"timestamp\": 1697722560000, \"open\": 228.88, \"high\": 229.4, \"low\": 228.3, \"close\": 228.375, \"volume\": 17851.0, \"trade_count\": 259.0, \"vwap\": 228.727112, \"data_provider\": \"alpaca\"}\0\0\0\0\0\0\0\0\x07\0\0\x01\x05\0\0\0\x01\x02\x9bu\x82,\0\0\0\0\0\0\0\0\x01\x8bH'X`\0\0\x01\x8bH'X`\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\0\0\0\x01\xa4\x03\0\0\0\x08TSLA\x8e\x03{\"symbol\": \"TSLA\", \"timestamp\": 1697722620000, \"open\": 228.39, \"high\": 228.39, \"low\": 226.89, \"close\": 227.425, \"volume\": 12807.0, \"trade_count\": 254.0, \"vwap\": 227.514886, \"data_provider\": \"alpaca\"}\0\0\0\0\0\0\0\0\x08\0\0\x01\x03\0\0\0\x01\x02\xdcI\xc6\xc9\0\0\0\0\0\0\0\0\x01\x8bH(B\xc0\0\0\x01\x8bH(B\xc0\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\0\0\0\x01\xa0\x03\0\0\0\x08TSLA\x8a\x03{\"symbol\": \"TSLA\", \"timestamp\": 1697722680000, \"open\": 227.13, \"high\": 228.53, \"low\": 226.78, \"close\": 228.53, \"volume\": 7273.0, \"trade_count\": 123.0, \"vwap\": 227.633268, \"data_provider\": \"alpaca\"}\0\0\0\0\0\0\0\0\t\0\0\x01\x05\0\0\0\x01\x02\xf9\xd5\x0f\xd7\0\0\0\0\0\0\0\0\x01\x8bH+\xec@\0\0\x01\x8bH+\xec@\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\0\0\0\x01\xa4\x03\0\0\0\x08TSLA\x8e\x03{\"symbol\": \"TSLA\", \"timestamp\": 1697722920000, \"open\": 225.41, \"high\": 226.87, \"low\": 225.22, \"close\": 226.045, \"volume\": 10062.0, \"trade_count\": 159.0, \"vwap\": 226.119019, \"data_provider\": \"alpaca\"}\0\0\0\0\0\0\0\0\n\0\0\x01\x03\0\0\0\x01\x02KhN\x01\0\0\0\0\0\0\0\0\x01\x8bH,\xd6\xa0\0\0\x01\x8bH,\xd6\xa0\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\0\0\0\x01\xa0\x03\0\0\0\x08TSLA\x8a\x03{\"symbol\": \"TSLA\", \"timestamp\": 1697722980000, \"open\": 226.05, \"high\": 226.69, \"low\": 225.45, \"close\": 225.45, \"volume\": 7281.0, \"trade_count\": 129.0, \"vwap\": 225.980049, \"data_provider\": \"alpaca\"}\0\0\0\0\0\0\0\0\x0b\0\0\x01\x01\0\0\0\x01\x02\xe8\xd9yi\0\0\0\0\0\0\0\0\x01\x8bHI8@\0\0\x01\x8bHI8@\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\0\0\0\x01\x9c\x03\0\0\0\x08TSLA\x86\x03{\"symbol\": \"TSLA\", \"timestamp\": 1697724840000, \"open\": 225.89, \"high\": 226.0, \"low\": 225.46, \"close\": 225.47, \"volume\": 3886.0, \"trade_count\": 90.0, \"vwap\": 225.741834, \"data_provider\": \"alpaca\"}\0\0\0\0\0\0\0\0\x0c\0\0\x01\x01\0\0\0\x01\x02\xb2`\x9e\x15\0\0\0\0\0\0\0\0\x01\x8bHJ\"\xa0\0\0\x01\x8bHJ\"\xa0\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\0\0\0\x01\x9c\x03\0\0\0\x08TSLA\x86\x03{\"symbol\": \"TSLA\", \"timestamp\": 1697724900000, \"open\": 225.7, \"high\": 225.96, \"low\": 225.34, \"close\": 225.55, \"volume\": 3588.0, \"trade_count\": 74.0, \"vwap\": 225.642698, \"data_provider\": \"alpaca\"}\0\0\0\0\0\0\0\0\r\0\0\x01\x02\0\0\0\x01\x02\xb4\x02\xa4\x1c\0\0\0\0\0\0\0\0\x01\x8bHK\r\0\0\0\x01\x8bHK\r\0\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\0\0\0\x01\x9e\x03\0\0\0\x08TSLA\x88\x03{\"symbol\": \"TSLA\", \"timestamp\": 1697724960000, \"open\": 225.55, \"high\": 225.55, \"low\": 225.07, \"close\": 225.07, \"volume\": 1674.0, \"trade_count\": 38.0, \"vwap\": 225.256195, \"data_provider\": \"alpaca\"}\0"));

    let mut parser_group = c.benchmark_group("parser");

    parser_group.throughput(Throughput::Bytes(data.to_bytes().len() as u64));
    parser_group.bench_with_input(
        BenchmarkId::new("parse", data.to_bytes().len()),
        &data,
        |b, data: &NomBytes| {
            let mut record_batch: [RecordBatch<'_>; 20] = [
                RecordBatch::default(),
                RecordBatch::default(),
                RecordBatch::default(),
                RecordBatch::default(),
                RecordBatch::default(),
                RecordBatch::default(),
                RecordBatch::default(),
                RecordBatch::default(),
                RecordBatch::default(),
                RecordBatch::default(),
                RecordBatch::default(),
                RecordBatch::default(),
                RecordBatch::default(),
                RecordBatch::default(),
                RecordBatch::default(),
                RecordBatch::default(),
                RecordBatch::default(),
                RecordBatch::default(),
                RecordBatch::default(),
                RecordBatch::default(),
            ];
            // record_batch.fill_with(RecordBatch::default);
            assert_eq!(record_batch.len(), 20);

            let mut records = Vec::with_capacity(5);
            records.fill_with(Record::default);
            record_batch[0].records = &mut records;

            let mut records = Vec::with_capacity(5);
            records.fill_with(Record::default);
            record_batch[1].records = &mut records;

            let mut records = Vec::with_capacity(5);
            records.fill_with(Record::default);
            record_batch[2].records = &mut records;

            let mut records = Vec::with_capacity(5);
            records.fill_with(Record::default);
            record_batch[3].records = &mut records;

            let mut records = Vec::with_capacity(5);
            records.fill_with(Record::default);
            record_batch[4].records = &mut records;

            let partitions = &mut [Partition::default(); 1];
            partitions[0].record_batch = &mut record_batch;

            let topics = &mut [Topic::default(); 1];
            topics[0].partitions = partitions;

            let mut res = FetchResponse {
                topics,
                ..Default::default()
            };
            b.iter(|| parse_fetch_response(data.clone(), &mut res).unwrap());
        },
    );

    parser_group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
