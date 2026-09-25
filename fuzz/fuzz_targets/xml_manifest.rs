//! Fuzz the XML body of `CompleteMultipartUpload`.
//!
//! The input is the request body verbatim and the rest of the request is valid,
//! so the fuzzer explores XML parsing and manifest validation rather than the
//! signature path. Part 1 is always uploaded with a fixed payload, so a seed
//! manifest can be a *valid* one and the fuzzer mutates from an accepted
//! request instead of from garbage. Invariants: no server fault, and an object
//! appears only when the request succeeded.

#![no_main]

use allins3::domain::{ContentProperties, ObjectKey, UserMetadata};
use allins3::testing::protocol::{BUCKET, ProtocolHarness};
use libfuzzer_sys::fuzz_target;

#[path = "support.rs"]
mod support;

/// The payload part 1 always carries.
const UPLOADED: &[u8] = b"fuzz manifest body";
/// Its ETag, which the seed manifests name.
const SEED_ETAG: &str = "bc7b68f3796c903be8f57fd8b61b251f";

fuzz_target!(|data: &[u8]| {
    support::block_on(async {
        let harness: &ProtocolHarness = support::harness().await;
        let key = format!("fuzz/xml/{:016x}.bin", support::hash(data));
        let object = ObjectKey::parse(&key).expect("key");
        let Ok(upload) = harness.objects().create_multipart(
            BUCKET,
            &object,
            Some("tester"),
            ContentProperties::default(),
            UserMetadata::new(),
            None,
        ) else {
            return;
        };
        let upload_id = upload.upload_id.as_str().to_string();
        let Ok(part) = harness
            .objects()
            .upload_part(
                &allins3::testing::fixture::ctx("fuzz"),
                BUCKET,
                &object,
                &upload_id,
                1,
                Default::default(),
                allins3::testing::support::one_shot_stream(bytes::Bytes::from_static(UPLOADED)),
            )
            .await
        else {
            return;
        };
        // The seed manifests name this part, so keep the tag in one place.
        assert_eq!(part.etag.as_str(), SEED_ETAG);
        let path = format!("/{BUCKET}/{key}?uploadId={upload_id}");
        let response = harness.call("POST", &path, data.to_vec(), &[]).await;
        assert!(
            !response.is_server_error(),
            "a fuzzed XML body faulted the server: {} {}",
            response.status,
            response.text()
        );
        if !response.status.is_success() {
            let head = harness
                .call("HEAD", &format!("/{BUCKET}/{key}"), Vec::new(), &[])
                .await;
            assert!(
                !head.status.is_success(),
                "a rejected XML body published an object"
            );
        }
        // A successful completion publishes; delete it so a long run does not
        // grow the in-memory driver without bound.
        let _ = harness
            .call("DELETE", &format!("/{BUCKET}/{key}"), Vec::new(), &[])
            .await;
        let _ = harness
            .objects()
            .abort_multipart(BUCKET, &object, &upload_id);
    });
});
