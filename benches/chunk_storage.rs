//! Benchmarks for chunk storage performance.

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use strata::data::ChunkStorage;
use strata::types::ChunkId;
use tempfile::TempDir;

fn create_storage() -> (ChunkStorage, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let storage = ChunkStorage::new(temp_dir.path(), 100).unwrap();
    (storage, temp_dir)
}

fn bench_write(c: &mut Criterion) {
    let (storage, _temp) = create_storage();
    let mut group = c.benchmark_group("chunk_write");

    for size in [4096, 65536, 1048576, 4194304].iter() {
        let data = vec![0u8; *size];
        let mut counter = 0u64;

        group.throughput(Throughput::Bytes(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, _| {
            b.iter(|| {
                counter += 1;
                let chunk_id = ChunkId::new();
                storage.write_shard(black_box(&chunk_id), black_box(0), black_box(&data))
            })
        });
    }
    group.finish();
}

fn bench_read(c: &mut Criterion) {
    let (storage, _temp) = create_storage();
    let mut group = c.benchmark_group("chunk_read");

    for size in [4096, 65536, 1048576, 4194304].iter() {
        let data = vec![0u8; *size];
        let chunk_id = ChunkId::new();
        storage.write_shard(&chunk_id, 0, &data).unwrap();

        group.throughput(Throughput::Bytes(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, _| {
            b.iter(|| storage.read_shard(black_box(&chunk_id), black_box(0)))
        });
    }
    group.finish();
}

criterion_group!(benches, bench_write, bench_read);
criterion_main!(benches);
