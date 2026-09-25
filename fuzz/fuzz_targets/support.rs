//! Shared runtime and harness for the fuzz targets.
//!
//! Each fuzz target compiles this module separately, so helpers one target does
//! not use are still part of the others.
#![allow(dead_code)]
//!
//! The harness is built once per process and reused for every input; a fresh
//! service per input would spend the whole run in setup.

use std::sync::OnceLock;

use allins3::testing::protocol::ProtocolHarness;
use tokio::runtime::Runtime;

static RUNTIME: OnceLock<Runtime> = OnceLock::new();
static HARNESS: OnceLock<ProtocolHarness> = OnceLock::new();

/// Runs one fuzz case to completion on the shared runtime.
pub fn block_on<F: std::future::Future<Output = ()>>(future: F) {
    RUNTIME
        .get_or_init(|| Runtime::new().expect("tokio runtime"))
        .block_on(future);
}

/// The service under fuzz, built on first use.
pub async fn harness() -> &'static ProtocolHarness {
    if let Some(harness) = HARNESS.get() {
        return harness;
    }
    let built = ProtocolHarness::new().await;
    let _ = HARNESS.set(built);
    HARNESS.get().expect("harness is set")
}

/// A short, input-derived tag, so each fuzz case uses its own key.
pub fn hash(data: &[u8]) -> u64 {
    let mut value: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in data {
        value ^= u64::from(*byte);
        value = value.wrapping_mul(0x0000_0100_0000_01b3);
    }
    value
}
