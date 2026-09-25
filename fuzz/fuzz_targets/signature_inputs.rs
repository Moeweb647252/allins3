//! Fuzz the signature inputs.
//!
//! The input is split into an `Authorization` header value and a presigned
//! query string; both are sent to the real service. Invariants (`INV-08`):
//! never a server fault, never a successful response for an input the server
//! did not sign itself, and never a published object.

#![no_main]

use allins3::testing::protocol::{BUCKET, ProtocolHarness};
use hyper::http::{HeaderValue, Request};
use libfuzzer_sys::fuzz_target;

#[path = "support.rs"]
mod support;

const KEY: &str = "fuzz/signature/object.bin";

fuzz_target!(|data: &[u8]| {
    support::block_on(async {
        let harness: &ProtocolHarness = support::harness().await;
        let (authorization, query) = data.split_at(data.len() / 2);

        // A signed request whose Authorization header is then replaced by the
        // fuzzer's bytes. The signature covers the rest of the request, so
        // nothing but the fuzzed value is under test.
        let request = harness.signed("PUT", &format!("/{BUCKET}/{KEY}"), b"fuzz", &[]);
        let (mut parts, body) = request.into_parts();
        match std::str::from_utf8(authorization) {
            Ok(value) => match HeaderValue::from_str(value) {
                Ok(header) => {
                    parts.headers.insert("authorization", header);
                }
                Err(_) => {
                    parts.headers.remove("authorization");
                }
            },
            Err(_) => {
                parts.headers.remove("authorization");
            }
        }
        let response = harness.send(Request::from_parts(parts, body)).await;
        assert!(
            !response.is_server_error(),
            "a fuzzed Authorization header faulted the server: {} {}",
            response.status,
            response.text()
        );
        assert!(
            !response.status.is_success(),
            "a fuzzed Authorization header was accepted: {} {}",
            response.status,
            response.text()
        );

        // The presigned query: whatever the fuzzer produces, it must be
        // rejected and never fault the server.
        let query = String::from_utf8_lossy(query);
        let target = format!(
            "/{BUCKET}/{KEY}?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=fuzz&X-Amz-Date=20260101T000000Z&X-Amz-Expires=900&X-Amz-SignedHeaders=host&X-Amz-Signature={query}"
        );
        let Ok(uri) = hyper::http::Uri::try_from(target.as_str()) else {
            return;
        };
        let request = Request::builder()
            .method("GET")
            .uri(uri)
            .body(allins3::testing::protocol::empty_body());
        let Ok(request) = request else {
            return;
        };
        let response = harness.send(request).await;
        assert!(
            !response.is_server_error(),
            "a fuzzed presigned query faulted the server: {} {}",
            response.status,
            response.text()
        );
        assert!(
            !response.status.is_success(),
            "a fuzzed presigned query was accepted: {} {}",
            response.status,
            response.text()
        );
    });
});
