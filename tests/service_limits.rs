//! Resource limits, streaming writes and instance fault isolation.
//!
//! These run the real [`ObjectService`] over the test drivers, so the checks
//! cover the service's buffering and admission behaviour rather than a driver's
//! own storage.

#![cfg(feature = "testing")]

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context as TaskContext, Poll};

use allins3::domain::{ChecksumAlgorithm, DriverContext, DriverError, ObjectKey};
use allins3::driver::{DriverFactory, Profile};
use allins3::integrity::{base64, compute_checksum};
use allins3::service::{ObjectService, PutParams, ReadConditions, ServiceError, TrailerChecksum};
use allins3::testing::fault::{FaultDriverFactory, FaultPlan};
use allins3::testing::fixture::{self, BucketSpec};
use allins3::testing::reference::ReferenceDriverFactory;
use allins3::testing::support::{generated_stream, one_shot_stream, read_to_end};
use bytes::Bytes;
use futures_core::Stream;
use tokio_util::sync::CancellationToken;

const BUCKET: &str = "bucket-a";
const DRIVER: &str = "inst-1";

/// A body that cancels the request after its first chunk, modelling a client
/// that disconnects mid-upload.
struct CancelAfterFirst {
    cancel: Option<CancellationToken>,
    yielded: bool,
    chunk: Bytes,
}

impl Stream for CancelAfterFirst {
    type Item = Result<Bytes, DriverError>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.yielded {
            return Poll::Ready(None);
        }
        this.yielded = true;
        if let Some(token) = this.cancel.take() {
            token.cancel();
        }
        Poll::Ready(Some(Ok(this.chunk.clone())))
    }
}

fn ctx() -> DriverContext {
    fixture::ctx("service-limits")
}

fn object_key(value: &str) -> ObjectKey {
    ObjectKey::parse(value).expect("valid key")
}

async fn service_with_limits(mut limits: allins3::config::Limits) -> ObjectService {
    limits.max_single_write_size = 1024 * 1024;
    let factory: Arc<dyn DriverFactory> = Arc::new(ReferenceDriverFactory::new("reference"));
    fixture::service(
        vec![(DRIVER, factory)],
        vec![BucketSpec {
            bucket: BUCKET,
            driver: DRIVER,
            namespace: "ns-a",
            profile: Profile::CoreWrite,
        }],
        limits,
        fixture::full_access(),
    )
    .await
}

async fn read_object(service: &ObjectService, key: &ObjectKey) -> Vec<u8> {
    let result = service
        .get_object(&ctx(), BUCKET, key, &ReadConditions::default(), None)
        .await
        .expect("get object");
    read_to_end(result.stream).await.expect("read body")
}

async fn object_is_absent(service: &ObjectService, key: &ObjectKey) -> bool {
    service
        .get_object(&ctx(), BUCKET, key, &ReadConditions::default(), None)
        .await
        .is_err()
}

#[tokio::test]
async fn an_object_over_the_single_write_limit_is_rejected_without_publishing() {
    let service = service_with_limits(fixture::default_limits()).await;
    let key = object_key("limits/too-big.bin");

    let body = generated_stream(2 * 1024 * 1024, 64 * 1024);
    let err = service
        .put_object(
            &ctx(),
            BUCKET,
            &key,
            Box::pin(body),
            PutParams {
                expected_length: Some(2 * 1024 * 1024),
                ..Default::default()
            },
        )
        .await
        .expect_err("an object over the limit must be rejected");

    assert_eq!(err, ServiceError::EntityTooLarge(err_message(&err)));
    assert_eq!(err.code(), "EntityTooLarge");
    assert!(
        object_is_absent(&service, &key).await,
        "a rejected write must not leave a partial object"
    );
}

fn err_message(err: &ServiceError) -> String {
    match err {
        ServiceError::EntityTooLarge(m) => m.clone(),
        other => panic!("unexpected error: {other:?}"),
    }
}

#[tokio::test]
async fn a_body_shorter_or_longer_than_declared_is_a_bad_digest() {
    let service = service_with_limits(fixture::default_limits()).await;

    let short = object_key("limits/short.bin");
    let err = service
        .put_object(
            &ctx(),
            BUCKET,
            &short,
            one_shot_stream(Bytes::from_static(b"12345")),
            PutParams {
                expected_length: Some(10),
                ..Default::default()
            },
        )
        .await
        .expect_err("a short body must be rejected");
    assert!(matches!(err, ServiceError::BadDigest(_)), "{err:?}");
    assert!(object_is_absent(&service, &short).await);

    let long = object_key("limits/long.bin");
    let err = service
        .put_object(
            &ctx(),
            BUCKET,
            &long,
            one_shot_stream(Bytes::from_static(b"12345")),
            PutParams {
                expected_length: Some(3),
                ..Default::default()
            },
        )
        .await
        .expect_err("a long body must be rejected");
    assert!(matches!(err, ServiceError::BadDigest(_)), "{err:?}");
    assert!(object_is_absent(&service, &long).await);
}

#[tokio::test]
async fn a_trailer_declared_checksum_is_verified_after_the_body() {
    let service = service_with_limits(fixture::default_limits()).await;
    let key = object_key("limits/trailer.bin");
    let body = b"trailer verified body".to_vec();
    let digest = compute_checksum(ChecksumAlgorithm::Crc32, &body);

    // A trailer value is only readable once the body has been consumed, so the
    // service asks for it at the end and treats it like a declared header.
    let matching = base64(&digest);
    let declared = matching.clone();
    let meta = service
        .put_object(
            &ctx(),
            BUCKET,
            &key,
            one_shot_stream(Bytes::from(body.clone())),
            PutParams {
                expected_length: Some(body.len() as u64),
                checksum_algorithm: Some(ChecksumAlgorithm::Crc32),
                trailer_checksum: Some(TrailerChecksum::new(ChecksumAlgorithm::Crc32, move || {
                    Some(declared.clone())
                })),
                ..Default::default()
            },
        )
        .await
        .expect("a matching trailer value publishes");
    assert_eq!(
        meta.checksums.first().map(|c| c.value().clone()),
        Some(digest.clone()),
        "the verified value is what gets stored"
    );

    // A trailer that disagrees with the bytes is a BadDigest, and the object is
    // not replaced.
    let err = service
        .put_object(
            &ctx(),
            BUCKET,
            &key,
            one_shot_stream(Bytes::from(body.clone())),
            PutParams {
                expected_length: Some(body.len() as u64),
                checksum_algorithm: Some(ChecksumAlgorithm::Crc32),
                trailer_checksum: Some(TrailerChecksum::new(ChecksumAlgorithm::Crc32, || {
                    Some("AAAAAA==".to_string())
                })),
                ..Default::default()
            },
        )
        .await
        .expect_err("a mismatched trailer must fail");
    assert!(matches!(err, ServiceError::BadDigest(_)), "{err:?}");

    // A declaration whose value never arrives is a mismatch too.
    let err = service
        .put_object(
            &ctx(),
            BUCKET,
            &key,
            one_shot_stream(Bytes::from(body.clone())),
            PutParams {
                expected_length: Some(body.len() as u64),
                checksum_algorithm: Some(ChecksumAlgorithm::Crc32),
                trailer_checksum: Some(TrailerChecksum::new(ChecksumAlgorithm::Crc32, || None)),
                ..Default::default()
            },
        )
        .await
        .expect_err("a declared trailer that never arrives must fail");
    assert!(matches!(err, ServiceError::BadDigest(_)), "{err:?}");

    // Neither rejection published: the first object is still the one stored.
    assert_eq!(read_object(&service, &key).await, body);
    let head = service
        .head_object(&ctx(), BUCKET, &key, &ReadConditions::default(), None)
        .await
        .expect("head");
    assert_eq!(
        head.metadata.checksums.first().map(|c| c.value().clone()),
        Some(digest)
    );
}

#[tokio::test]
async fn a_client_disconnect_mid_upload_publishes_nothing() {
    let service = service_with_limits(fixture::default_limits()).await;
    let key = object_key("limits/cancelled.bin");
    let ctx = ctx();
    let token = ctx.cancellation_token();

    let body = CancelAfterFirst {
        cancel: Some(token),
        yielded: false,
        chunk: Bytes::from_static(&[b'c'; 4096]),
    };
    let err = service
        .put_object(
            &ctx,
            BUCKET,
            &key,
            Box::pin(body),
            PutParams {
                expected_length: Some(1_000_000),
                ..Default::default()
            },
        )
        .await
        .expect_err("a cancelled upload must not report success");
    assert!(matches!(err, ServiceError::InvalidRequest(_)), "{err:?}");
    assert!(object_is_absent(&service, &key).await);
}

#[tokio::test]
async fn a_generated_multi_megabyte_body_streams_through_unchanged() {
    let mut limits = fixture::default_limits();
    limits.max_single_write_size = 16 * 1024 * 1024;
    limits.staging_quota_bytes = 64 * 1024 * 1024;
    let factory: Arc<dyn DriverFactory> = Arc::new(ReferenceDriverFactory::new("reference"));
    let service = fixture::service(
        vec![(DRIVER, factory)],
        vec![BucketSpec {
            bucket: BUCKET,
            driver: DRIVER,
            namespace: "ns-a",
            profile: Profile::CoreWrite,
        }],
        limits,
        fixture::full_access(),
    )
    .await;

    let key = object_key("limits/streamed.bin");
    let total = 12 * 1024 * 1024;
    let body = generated_stream(total, 256 * 1024);
    let meta = service
        .put_object(
            &ctx(),
            BUCKET,
            &key,
            Box::pin(body),
            PutParams {
                expected_length: Some(total),
                ..Default::default()
            },
        )
        .await
        .expect("a large streamed body must be accepted");

    assert_eq!(meta.size, total);
    let bytes = read_object(&service, &key).await;
    assert_eq!(bytes.len() as u64, total);
    assert!(bytes.iter().all(|b| *b == b'x'));
}

#[tokio::test]
async fn staging_quota_backpressure_keeps_earlier_parts() {
    let mut limits = fixture::default_limits();
    limits.staging_quota_bytes = 4 * 1024 * 1024;
    let factory: Arc<dyn DriverFactory> = Arc::new(ReferenceDriverFactory::new("reference"));
    let service = fixture::service(
        vec![(DRIVER, factory)],
        vec![BucketSpec {
            bucket: BUCKET,
            driver: DRIVER,
            namespace: "ns-a",
            profile: Profile::CoreWrite,
        }],
        limits,
        fixture::full_access(),
    )
    .await;

    let key = object_key("limits/quota.bin");
    let upload_id = service
        .create_multipart(
            BUCKET,
            &key,
            Some("tester"),
            Default::default(),
            Default::default(),
            None,
        )
        .expect("create upload")
        .upload_id
        .as_str()
        .to_string();

    let part = generated_stream(3 * 1024 * 1024, 256 * 1024);
    service
        .upload_part(&ctx(), BUCKET, &key, &upload_id, 1, None, part)
        .await
        .expect("first part fits the quota");

    let part = generated_stream(3 * 1024 * 1024, 256 * 1024);
    let err = service
        .upload_part(&ctx(), BUCKET, &key, &upload_id, 2, None, part)
        .await
        .expect_err("the staging quota must apply back-pressure");
    assert_eq!(err, ServiceError::SlowDown);

    // The rejected part must not destroy the accepted one.
    let parts = service
        .list_parts(BUCKET, &key, &upload_id, None, 100)
        .expect("list parts");
    assert_eq!(parts.parts.len(), 1);
    assert_eq!(parts.parts[0].number, 1);
}

#[tokio::test]
async fn one_failing_driver_does_not_break_another_bucket() {
    let healthy: Arc<dyn DriverFactory> = Arc::new(ReferenceDriverFactory::new("reference"));
    let broken: Arc<dyn DriverFactory> = Arc::new(FaultDriverFactory::new(
        Arc::new(ReferenceDriverFactory::new("reference")),
        FaultPlan {
            fail_stat: true,
            fail_read: true,
            fail_begin_write: true,
            ..FaultPlan::none()
        },
    ));
    let service = fixture::service(
        vec![("ok", healthy), ("bad", broken)],
        vec![
            BucketSpec {
                bucket: "bucket-ok",
                driver: "ok",
                namespace: "ns-ok",
                profile: Profile::CoreWrite,
            },
            BucketSpec {
                bucket: "bucket-bad",
                driver: "bad",
                namespace: "ns-bad",
                profile: Profile::CoreWrite,
            },
        ],
        fixture::default_limits(),
        fixture::full_access(),
    )
    .await;

    let key = object_key("shared/isolation.txt");
    service
        .put_object(
            &ctx(),
            "bucket-ok",
            &key,
            one_shot_stream(Bytes::from_static(b"healthy")),
            PutParams::default(),
        )
        .await
        .expect("the healthy driver keeps working");

    let err = service
        .put_object(
            &ctx(),
            "bucket-bad",
            &key,
            one_shot_stream(Bytes::from_static(b"unhealthy")),
            PutParams::default(),
        )
        .await
        .expect_err("the failing driver must report its failure");
    assert_eq!(err, ServiceError::SlowDown);

    // The failure is scoped: the other bucket still reads its own content.
    let got = service
        .get_object(&ctx(), "bucket-ok", &key, &ReadConditions::default(), None)
        .await
        .expect("healthy bucket still readable");
    assert_eq!(read_to_end(got.stream).await.expect("body"), b"healthy");
}
