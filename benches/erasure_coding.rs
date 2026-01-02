//! Benchmarks for erasure coding performance.

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use strata::erasure::ErasureCoder;
use strata::types::ErasureCodingConfig;

fn create_coder() -> ErasureCoder {
    let config = ErasureCodingConfig {
        data_shards: 4,
        parity_shards: 2,
    };
    ErasureCoder::new(config).unwrap()
}

fn bench_encode(c: &mut Criterion) {
    let coder = create_coder();
    let mut group = c.benchmark_group("erasure_encode");

    for size in [1024, 4096, 65536, 1048576].iter() {
        let data = vec![0u8; *size];
        group.throughput(Throughput::Bytes(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, _| {
            b.iter(|| coder.encode(black_box(&data)))
        });
    }
    group.finish();
}

fn bench_decode(c: &mut Criterion) {
    let coder = create_coder();
    let mut group = c.benchmark_group("erasure_decode");

    for size in [1024, 4096, 65536, 1048576].iter() {
        let data = vec![0u8; *size];
        let encoded = coder.encode(&data).unwrap();

        group.throughput(Throughput::Bytes(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, _| {
            let mut shards: Vec<Option<Vec<u8>>> =
                encoded.iter().map(|s| Some(s.clone())).collect();
            b.iter(|| {
                let mut test_shards = shards.clone();
                // Simulate 2 lost shards
                test_shards[0] = None;
                test_shards[1] = None;
                coder.decode(black_box(&mut test_shards), *size)
            })
        });
    }
    group.finish();
}

criterion_group!(benches, bench_encode, bench_decode);
criterion_main!(benches);
