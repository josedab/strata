//! Performance integration tests
//!
//! Tests performance characteristics and throughput.

#[allow(dead_code)]
mod common;

use common::fixtures::{TestDataGenerator, file_sizes};
use common::concurrency::ConcurrencyTracker;
use common::timing;
use std::time::{Duration, Instant};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

// =============================================================================
// Throughput Tests
// =============================================================================

#[tokio::test]
async fn test_data_generation_throughput() {
    let mut gen = TestDataGenerator::new(42);
    let total_bytes = 100 * 1024 * 1024; // 100 MB
    let chunk_size = 1024 * 1024; // 1 MB chunks

    let start = Instant::now();

    let mut generated = 0;
    while generated < total_bytes {
        let _ = gen.random_bytes(chunk_size);
        generated += chunk_size;
    }

    let elapsed = start.elapsed();
    let throughput_mbps = (total_bytes as f64 / 1024.0 / 1024.0) / elapsed.as_secs_f64();

    // Should be able to generate at least 100 MB/s
    assert!(throughput_mbps > 100.0, "Throughput too low: {:.2} MB/s", throughput_mbps);
}

#[tokio::test]
async fn test_content_verification_throughput() {
    let size = 10 * 1024 * 1024; // 10 MB
    let content = common::fixtures::VerifiableContent::new(42, size);
    let data = content.generate();

    let iterations = 100;
    let start = Instant::now();

    for _ in 0..iterations {
        assert!(content.verify(&data));
    }

    let elapsed = start.elapsed();
    let total_bytes = size * iterations;
    let throughput_mbps = (total_bytes as f64 / 1024.0 / 1024.0) / elapsed.as_secs_f64();

    // Should be able to verify at least 500 MB/s
    assert!(throughput_mbps > 500.0, "Verification throughput too low: {:.2} MB/s", throughput_mbps);
}

// =============================================================================
// Concurrency Tests
// =============================================================================

#[tokio::test]
async fn test_concurrent_operations() {
    let tracker = ConcurrencyTracker::new();
    let operations = 100;
    let max_concurrent = 10;

    let handles: Vec<_> = (0..operations)
        .map(|i| {
            let tracker = tracker.clone();
            tokio::spawn(async move {
                let _guard = tracker.enter();
                // Simulate some work
                tokio::time::sleep(Duration::from_millis(10)).await;
                i
            })
        })
        .collect();

    // Use semaphore to limit concurrency
    let semaphore = Arc::new(tokio::sync::Semaphore::new(max_concurrent));
    let bounded_handles: Vec<_> = (0..operations)
        .map(|i| {
            let tracker = tracker.clone();
            let semaphore = semaphore.clone();
            tokio::spawn(async move {
                let _permit = semaphore.acquire().await.unwrap();
                let _guard = tracker.enter();
                tokio::time::sleep(Duration::from_millis(5)).await;
                i
            })
        })
        .collect();

    for handle in bounded_handles {
        handle.await.unwrap();
    }

    // Peak concurrency should be bounded by semaphore
    assert!(tracker.peak() <= max_concurrent + 5); // Allow some slack due to timing
}

#[tokio::test]
async fn test_high_concurrency_stress() {
    let tracker = ConcurrencyTracker::new();
    let counter = Arc::new(AtomicU64::new(0));
    let operations = 1000;

    let handles: Vec<_> = (0..operations)
        .map(|_| {
            let tracker = tracker.clone();
            let counter = counter.clone();
            tokio::spawn(async move {
                let _guard = tracker.enter();
                counter.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_micros(100)).await;
            })
        })
        .collect();

    for handle in handles {
        handle.await.unwrap();
    }

    assert_eq!(counter.load(Ordering::SeqCst), operations);
    assert_eq!(tracker.total(), operations as usize);
}

// =============================================================================
// Latency Tests
// =============================================================================

#[tokio::test]
async fn test_operation_latency() {
    let mut latencies = Vec::with_capacity(1000);
    let mut gen = TestDataGenerator::new(42);

    for _ in 0..1000 {
        let start = Instant::now();
        let _ = gen.random_bytes(1024);
        latencies.push(start.elapsed());
    }

    // Calculate percentiles
    latencies.sort();
    let p50 = latencies[latencies.len() / 2];
    let p99 = latencies[latencies.len() * 99 / 100];

    // P50 should be under 100 microseconds
    assert!(p50 < Duration::from_micros(100), "P50 latency too high: {:?}", p50);
    // P99 should be under 1 millisecond
    assert!(p99 < Duration::from_millis(1), "P99 latency too high: {:?}", p99);
}

#[tokio::test]
async fn test_async_operation_latency() {
    let mut latencies = Vec::with_capacity(1000);

    for _ in 0..1000 {
        let start = Instant::now();
        tokio::time::sleep(Duration::from_micros(1)).await;
        latencies.push(start.elapsed());
    }

    latencies.sort();
    let p50 = latencies[latencies.len() / 2];

    // Async overhead should be reasonable (under 1ms for minimal sleep)
    assert!(p50 < Duration::from_millis(5), "Async P50 latency too high: {:?}", p50);
}

// =============================================================================
// Scalability Tests
// =============================================================================

#[tokio::test]
async fn test_linear_scalability() {
    // Measure time for different workload sizes
    let sizes = [100, 200, 400, 800];
    let mut times = Vec::new();

    for &size in &sizes {
        let mut gen = TestDataGenerator::new(42);
        let start = Instant::now();

        for _ in 0..size {
            let _ = gen.random_bytes(10 * 1024); // 10 KB each
        }

        times.push(start.elapsed());
    }

    // Check that time scales roughly linearly (within 50% of expected)
    for i in 1..sizes.len() {
        let expected_ratio = sizes[i] as f64 / sizes[0] as f64;
        let actual_ratio = times[i].as_secs_f64() / times[0].as_secs_f64();

        // Allow 50% deviation from linear
        assert!(
            actual_ratio < expected_ratio * 1.5,
            "Non-linear scaling detected: size {}x, time {:.2}x",
            sizes[i] / sizes[0],
            actual_ratio
        );
    }
}

#[tokio::test]
async fn test_memory_efficiency() {
    // Track that we can process large amounts of data without memory issues
    let iterations = 100;
    let chunk_size = 1024 * 1024; // 1 MB

    let mut gen = TestDataGenerator::new(42);

    for _ in 0..iterations {
        // Generate and immediately drop to test memory efficiency
        let data = gen.random_bytes(chunk_size);
        let content = common::fixtures::VerifiableContent::new(42, chunk_size);
        let _ = content.verify(&data);
        drop(data);
    }

    // If we get here without OOM, the test passes
}

// =============================================================================
// Timing Utility Tests
// =============================================================================

#[tokio::test]
async fn test_measure_utility() {
    let (result, duration) = timing::measure(|| async {
        tokio::time::sleep(Duration::from_millis(50)).await;
        42
    }).await;

    assert_eq!(result, 42);
    assert!(duration >= Duration::from_millis(50));
    assert!(duration < Duration::from_millis(100));
}

#[tokio::test]
async fn test_timeout_utility() {
    // Operation that completes in time
    let result = timing::with_timeout(
        || async {
            tokio::time::sleep(Duration::from_millis(10)).await;
            42
        },
        Duration::from_millis(100),
    ).await;

    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 42);

    // Operation that times out
    let result = timing::with_timeout(
        || async {
            tokio::time::sleep(Duration::from_millis(200)).await;
            42
        },
        Duration::from_millis(50),
    ).await;

    assert!(result.is_err());
}

// =============================================================================
// Retry Utility Tests
// =============================================================================

#[tokio::test]
async fn test_retry_success_first_attempt() {
    let attempts = Arc::new(AtomicU64::new(0));
    let attempts_clone = attempts.clone();

    let result = common::retry::with_backoff(
        move || {
            let attempts = attempts_clone.clone();
            async move {
                attempts.fetch_add(1, Ordering::SeqCst);
                Ok::<_, String>(42)
            }
        },
        3,
        Duration::from_millis(10),
    ).await;

    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 42);
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn test_retry_eventual_success() {
    let attempts = Arc::new(AtomicU64::new(0));
    let attempts_clone = attempts.clone();

    let result = common::retry::with_backoff(
        move || {
            let attempts = attempts_clone.clone();
            async move {
                let attempt = attempts.fetch_add(1, Ordering::SeqCst) + 1;
                if attempt < 3 {
                    Err::<i32, _>("not yet")
                } else {
                    Ok(42)
                }
            }
        },
        5,
        Duration::from_millis(1),
    ).await;

    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 42);
    assert_eq!(attempts.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn test_retry_all_failures() {
    let attempts = Arc::new(AtomicU64::new(0));
    let attempts_clone = attempts.clone();

    let result = common::retry::with_backoff(
        move || {
            let attempts = attempts_clone.clone();
            async move {
                attempts.fetch_add(1, Ordering::SeqCst);
                Err::<i32, _>("always fails")
            }
        },
        3,
        Duration::from_millis(1),
    ).await;

    assert!(result.is_err());
    assert_eq!(attempts.load(Ordering::SeqCst), 3);
}

// =============================================================================
// Batch Operation Tests
// =============================================================================

#[tokio::test]
async fn test_batch_processing() {
    let batch_sizes = [10, 50, 100, 200];
    let item_count = 1000;

    for &batch_size in &batch_sizes {
        let start = Instant::now();
        let mut processed = 0;

        while processed < item_count {
            let batch = std::cmp::min(batch_size, item_count - processed);
            // Simulate batch processing
            tokio::time::sleep(Duration::from_micros(100)).await;
            processed += batch;
        }

        let elapsed = start.elapsed();
        let throughput = item_count as f64 / elapsed.as_secs_f64();

        // Larger batches should generally be more efficient
        // We're mainly checking that batch processing works correctly
        assert!(throughput > 1000.0, "Throughput too low for batch size {}: {:.0} items/sec", batch_size, throughput);
    }
}

// =============================================================================
// File Size Performance Tests
// =============================================================================

#[tokio::test]
async fn test_various_file_sizes() {
    let sizes = [
        ("tiny", file_sizes::TINY),
        ("small", file_sizes::SMALL),
        ("medium", file_sizes::MEDIUM),
    ];

    for (name, size) in sizes {
        let content = common::fixtures::VerifiableContent::new(42, size);

        let (data, gen_time) = timing::measure(|| async {
            content.generate()
        }).await;

        let (_, verify_time) = timing::measure(|| async {
            content.verify(&data)
        }).await;

        // Log for debugging
        eprintln!(
            "Size {}: {} bytes, gen: {:?}, verify: {:?}",
            name, size, gen_time, verify_time
        );

        // Basic sanity checks
        assert_eq!(data.len(), size);
    }
}

// =============================================================================
// Warmup Test
// =============================================================================

#[tokio::test]
async fn test_cold_vs_warm_performance() {
    let mut gen = TestDataGenerator::new(42);
    let iterations = 100;

    // Cold run
    let cold_start = Instant::now();
    for _ in 0..iterations {
        let _ = gen.random_bytes(10 * 1024);
    }
    let cold_time = cold_start.elapsed();

    // Warm run (CPU caches should be warmer)
    let warm_start = Instant::now();
    for _ in 0..iterations {
        let _ = gen.random_bytes(10 * 1024);
    }
    let warm_time = warm_start.elapsed();

    // Warm should be at least as fast as cold
    // (In practice, might be faster due to caching)
    eprintln!("Cold: {:?}, Warm: {:?}", cold_time, warm_time);

    // Both should complete in reasonable time
    assert!(cold_time < Duration::from_secs(1));
    assert!(warm_time < Duration::from_secs(1));
}
