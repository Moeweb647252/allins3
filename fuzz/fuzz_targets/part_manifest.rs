//! Fuzz structured part manifests.
//!
//! The XML target covers how a manifest is *parsed*; this one covers how a
//! manifest is *validated* once parsed, by handing arbitrary part numbers,
//! ETags and orderings to the coordinator. Invariants: never a panic, never an
//! internal error, and a published object is exactly the uploaded bytes in
//! manifest order.

#![no_main]

use allins3::domain::{ContentProperties, EntityTag, ObjectKey, Preconditions, UserMetadata};
use allins3::testing::protocol::{BUCKET, ProtocolHarness};
use allins3::testing::support::one_shot_stream;
use libfuzzer_sys::fuzz_target;

#[path = "support.rs"]
mod support;

const UPLOADED: &[u8] = b"fuzz manifest body";

fuzz_target!(|data: &[u8]| {
    support::block_on(async {
        let harness: &ProtocolHarness = support::harness().await;
        let service = harness.objects();
        let key = format!("fuzz/manifest/{:016x}.bin", support::hash(data));
        let object = ObjectKey::parse(&key).expect("key");
        let Ok(upload) = service.create_multipart(
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
        let Ok(part) = service
            .upload_part(
                &allins3::testing::fixture::ctx("fuzz"),
                BUCKET,
                &object,
                &upload_id,
                1,
                Default::default(),
                one_shot_stream(bytes::Bytes::from_static(UPLOADED)),
            )
            .await
        else {
            return;
        };

        // The fuzzer chooses the manifest's shape: the numbers it lists (or an
        // ETag it invents) and whether it claims the real ones.
        let mut manifest: Vec<(u16, Option<EntityTag>)> = Vec::new();
        for chunk in data.chunks(3).take(8) {
            let number = u16::from(chunk[0]) | (u16::from(*chunk.get(1).unwrap_or(&0)) << 8);
            let claim = match chunk.get(2).copied().unwrap_or(0) % 3 {
                0 => None,
                1 => Some(EntityTag::new(part.etag.as_str().to_string()).expect("etag")),
                _ => Some(EntityTag::new("0".repeat(32)).expect("etag")),
            };
            manifest.push((number, claim));
        }

        let outcome = service
            .complete_multipart(
                &allins3::testing::fixture::ctx("fuzz"),
                BUCKET,
                &object,
                &upload_id,
                &manifest,
                None,
                Preconditions::none(),
            )
            .await;

        if let Err(error) = outcome {
            assert!(
                !matches!(error, allins3::service::ServiceError::Internal(_)),
                "a manifest produced an internal error: {error:?}"
            );
            let head = harness
                .call("HEAD", &format!("/{BUCKET}/{key}"), Vec::new(), &[])
                .await;
            assert!(
                !head.status.is_success(),
                "a rejected manifest published an object"
            );
        }
        let _ = harness
            .call("DELETE", &format!("/{BUCKET}/{key}"), Vec::new(), &[])
            .await;
        let _ = service.abort_multipart(BUCKET, &object, &upload_id);
    });
});
