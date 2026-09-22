//! The resource-budget check (implementation plan P5-06).
//!
//! The write pipeline must hold a chunk, not an object: a body of any size
//! streams through the same buffers, so the process's peak resident set must not
//! follow the object size. This check streams generated bodies through the real
//! [`ObjectService`] into a counting sink that drops every byte it receives, so
//! the sizes the plan names can be pushed through without materialising them.
//!
//! Budget and sizes live in `bench/resource-budget.toml`. By default the check
//! runs the smallest size; set `ALLINS3_BENCH_FULL=1` (with `--release`) to run
//! all of them.

#![cfg(feature = "testing")]

use std::sync::Arc;

use allins3::domain::ObjectKey;
use allins3::driver::Profile;
use allins3::service::{ObjectService, PutParams};
use allins3::testing::counting::CountingDriverFactory;
use allins3::testing::fixture::{self, BucketSpec};
use allins3::testing::support::generated_stream;
use serde::Deserialize;

const BUCKET: &str = "bucket-a";
const DRIVER: &str = "inst-1";
const MIB: u64 = 1024 * 1024;

/// One decoded chunk in flight.
const CHUNK: usize = 256 * 1024;

/// The thresholds and sizes to measure, from `bench/resource-budget.toml`.
#[derive(Deserialize)]
struct Budget {
    per_request_buffer_bytes: u64,
    max_concurrency: u64,
    slack_bytes: u64,
    sizes_mib: Vec<u64>,
}

impl Budget {
    fn load() -> Self {
        let text = std::fs::read_to_string("bench/resource-budget.toml")
            .expect("bench/resource-budget.toml is part of the repository");
        toml::from_str(&text).expect("the budget file is well formed")
    }

    /// The incremental peak RSS a streamed body is allowed to add, derived from
    /// the buffer budget, the fixed concurrency and the documented slack.
    fn max_delta_rss_bytes(&self) -> u64 {
        self.per_request_buffer_bytes * self.max_concurrency + self.slack_bytes
    }

    /// The sizes to run: all of them only when explicitly asked.
    fn sizes(&self) -> Vec<u64> {
        let full = std::env::var("ALLINS3_BENCH_FULL").is_ok_and(|value| value == "1");
        let sizes = if full {
            self.sizes_mib.clone()
        } else {
            vec![self.sizes_mib[0]]
        };
        sizes.into_iter().map(|mib| mib * MIB).collect()
    }
}

/// The process's peak resident set, in bytes.
///
/// Peak rather than current: a flat peak across object sizes is the evidence,
/// independent of whatever the allocator did in between.
fn peak_rss_bytes() -> u64 {
    // SAFETY: `getrusage` only writes into the struct we hand it.
    let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
    let rc = unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) };
    assert_eq!(rc, 0, "getrusage failed");
    let raw = usage.ru_maxrss as u64;
    // Linux reports KiB, macOS reports bytes.
    if cfg!(target_os = "macos") {
        raw
    } else {
        raw * 1024
    }
}

fn object_key(value: &str) -> ObjectKey {
    ObjectKey::parse(value).expect("valid key")
}

/// A service over a counting sink with room for the sizes under test.
async fn sink_service() -> (ObjectService, Arc<CountingDriverFactory>) {
    let factory = Arc::new(CountingDriverFactory::new("counting-sink"));
    let mut limits = fixture::default_limits();
    limits.max_single_write_size = 16 * 1024 * MIB;
    let service = fixture::service(
        vec![(DRIVER, factory.clone())],
        vec![BucketSpec {
            bucket: BUCKET,
            driver: DRIVER,
            namespace: "ns-sink",
            profile: Profile::CoreWrite,
        }],
        limits,
        fixture::full_access(),
    )
    .await;
    (service, factory)
}

#[tokio::test(flavor = "current_thread")]
async fn a_generated_stream_holds_the_memory_budget() {
    let budget = Budget::load();
    let (service, factory) = sink_service().await;
    let sink = factory.sink();
    let key = object_key("bench/stream.bin");
    let ctx = fixture::ctx("resource-budget");

    // The idle baseline first, so the deltas below are incremental.
    let baseline = peak_rss_bytes();
    println!(
        "idle baseline peak RSS: {} MiB (budget {} MiB, chunk {CHUNK} B)",
        baseline / MIB,
        budget.max_delta_rss_bytes() / MIB
    );

    for size in budget.sizes() {
        let received_before = sink.received();
        let before = peak_rss_bytes();

        let meta = service
            .put_object(
                &ctx,
                BUCKET,
                &key,
                generated_stream(size, CHUNK),
                PutParams {
                    expected_length: Some(size),
                    ..Default::default()
                },
            )
            .await
            .expect("the streaming write publishes");

        let after = peak_rss_bytes();
        let delta = after.saturating_sub(before);
        println!(
            "{:>6} MiB: delivered {} MiB in {} chunks, peak RSS delta {} MiB",
            size / MIB,
            (sink.received() - received_before) / MIB,
            sink.chunks(),
            delta / MIB
        );

        assert_eq!(
            sink.received() - received_before,
            size,
            "the sink must receive exactly the generated body"
        );
        assert_eq!(meta.size, size, "the published size must match the body");
        assert!(
            delta <= budget.max_delta_rss_bytes(),
            "streaming {size} bytes grew peak RSS by {delta} B, over the {} B budget",
            budget.max_delta_rss_bytes()
        );
    }

    // The same object key was replaced each time, and nothing was aborted: the
    // measurement did not come from a half-finished write path.
    assert_eq!(sink.aborts(), 0, "no write under test was aborted");
    assert_eq!(
        sink.commits(),
        budget.sizes().len() as u64,
        "every size published exactly once"
    );
}
