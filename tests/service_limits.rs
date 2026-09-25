//! Resource limits, streaming writes and instance fault isolation.
//!
//! These run the real [`ObjectService`] over the test drivers, so the checks
//! cover the service's buffering and admission behaviour rather than a driver's
//! own storage.

#![cfg(feature = "testing")]

use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::task::{Context as TaskContext, Poll, Waker};
use std::time::Duration;

use allins3::domain::{
    ChecksumAlgorithm, ContentProperties, DriverContext, DriverError, ObjectKey, Preconditions,
    UserMetadata,
};
use allins3::driver::{DriverFactory, MetadataDirective, Profile};
use allins3::integrity::{base64, compute_checksum};
use allins3::service::multipart::PartChecksums;
use allins3::service::{
    CopyParams, ObjectService, PutParams, ReadConditions, ServiceError, TrailerChecksum,
};
use allins3::testing::fault::{FaultDriverFactory, FaultPlan};
use allins3::testing::fixture::{self, BucketSpec};
use allins3::testing::reference::ReferenceDriverFactory;
use allins3::testing::support::{generated_stream, one_shot_stream, read_to_end};
use bytes::Bytes;
use futures_core::Stream;
use tokio_util::sync::CancellationToken;

const BUCKET: &str = "bucket-a";
const DRIVER: &str = "inst-1";
/// A second bucket on a second driver instance, for per-driver isolation.
const BUCKET_B: &str = "bucket-b";

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

/// A body of `remaining` chunks that counts the bytes the service pulled from
/// it, so a test can tell whether a refusal happened while the part was still
/// streaming or only after the whole body had been read.
struct CountingBody {
    chunk: Bytes,
    remaining: usize,
    pulled: Arc<AtomicU64>,
}

impl Stream for CountingBody {
    type Item = Result<Bytes, DriverError>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.remaining == 0 {
            return Poll::Ready(None);
        }
        this.remaining -= 1;
        this.pulled
            .fetch_add(this.chunk.len() as u64, Ordering::SeqCst);
        Poll::Ready(Some(Ok(this.chunk.clone())))
    }
}

/// A body that yields one chunk and then stays pending until it is released.
///
/// It also reports when the first chunk was pulled, which is how a test learns
/// that the enclosing operation is past admission and holding its driver slot.
struct ParkedBody {
    first: Option<Bytes>,
    park: Arc<Park>,
}

#[derive(Default)]
struct Park {
    released: AtomicBool,
    waker: std::sync::Mutex<Option<Waker>>,
    pulled: tokio::sync::Notify,
}

impl Park {
    fn release(&self) {
        self.released.store(true, Ordering::SeqCst);
        if let Some(waker) = self.waker.lock().expect("parked wakeup").take() {
            waker.wake();
        }
    }
}

impl Stream for ParkedBody {
    type Item = Result<Bytes, DriverError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if let Some(chunk) = this.first.take() {
            this.park.pulled.notify_one();
            return Poll::Ready(Some(Ok(chunk)));
        }
        if this.park.released.load(Ordering::SeqCst) {
            return Poll::Ready(None);
        }
        *this.park.waker.lock().expect("parked wakeup") = Some(cx.waker().clone());
        Poll::Pending
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

/// Two buckets on two driver instances, so a driver's saturation can be told
/// apart from another driver's health.
async fn service_with_two_drivers(mut limits: allins3::config::Limits) -> ObjectService {
    limits.max_single_write_size = 1024 * 1024;
    let first: Arc<dyn DriverFactory> = Arc::new(ReferenceDriverFactory::new("reference"));
    let second: Arc<dyn DriverFactory> = Arc::new(ReferenceDriverFactory::new("reference"));
    fixture::service(
        vec![(DRIVER, first), ("inst-2", second)],
        vec![
            BucketSpec {
                bucket: BUCKET,
                driver: DRIVER,
                namespace: "ns-a",
                profile: Profile::CoreWrite,
            },
            BucketSpec {
                bucket: BUCKET_B,
                driver: "inst-2",
                namespace: "ns-b",
                profile: Profile::CoreWrite,
            },
        ],
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
    object_is_absent_in(service, BUCKET, key).await
}

async fn object_is_absent_in(service: &ObjectService, bucket: &str, key: &ObjectKey) -> bool {
    service
        .get_object(&ctx(), bucket, key, &ReadConditions::default(), None)
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
            &ctx(),
            BUCKET,
            &key,
            Some("tester"),
            Default::default(),
            Default::default(),
            None,
        )
        .await
        .expect("create upload")
        .upload_id
        .as_str()
        .to_string();

    let part = generated_stream(3 * 1024 * 1024, 256 * 1024);
    service
        .upload_part(
            &ctx(),
            BUCKET,
            &key,
            &upload_id,
            1,
            PartChecksums::default(),
            part,
        )
        .await
        .expect("first part fits the quota");

    let part = generated_stream(3 * 1024 * 1024, 256 * 1024);
    let err = service
        .upload_part(
            &ctx(),
            BUCKET,
            &key,
            &upload_id,
            2,
            PartChecksums::default(),
            part,
        )
        .await
        .expect_err("the staging quota must apply back-pressure");
    assert_eq!(err, ServiceError::SlowDown);

    // The rejected part must not destroy the accepted one.
    let parts = service
        .list_parts(&ctx(), BUCKET, &key, &upload_id, None, 100)
        .await
        .expect("list parts");
    assert_eq!(parts.parts.len(), 1);
    assert_eq!(parts.parts[0].number, 1);
}

#[tokio::test]
async fn the_staging_quota_refuses_a_part_while_its_body_is_streaming() {
    let mut limits = fixture::default_limits();
    limits.max_single_write_size = 32 * 1024 * 1024;
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

    let key = object_key("limits/streamed-quota.bin");
    let upload_id = service
        .create_multipart(
            &ctx(),
            BUCKET,
            &key,
            Some("tester"),
            Default::default(),
            Default::default(),
            None,
        )
        .await
        .expect("create upload")
        .upload_id
        .as_str()
        .to_string();

    // A body far larger than the quota, delivered in 1 MiB chunks. The quota is
    // charged as the body streams, so the part must be refused before the whole
    // 32 MiB was read and buffered.
    let pulled = Arc::new(AtomicU64::new(0));
    let body = CountingBody {
        chunk: Bytes::from(vec![b'q'; 1024 * 1024]),
        remaining: 32,
        pulled: pulled.clone(),
    };
    let err = service
        .upload_part(
            &ctx(),
            BUCKET,
            &key,
            &upload_id,
            1,
            PartChecksums::default(),
            Box::pin(body),
        )
        .await
        .expect_err("a part larger than the staging quota must be refused");
    assert_eq!(err, ServiceError::SlowDown);

    let read = pulled.load(Ordering::SeqCst);
    assert!(
        read <= 5 * 1024 * 1024,
        "the refusal must happen mid-body, but {read} bytes were read"
    );

    // Nothing was staged, and the refused part released the quota it charged.
    let parts = service
        .list_parts(&ctx(), BUCKET, &key, &upload_id, None, 100)
        .await
        .expect("list parts");
    assert!(parts.parts.is_empty(), "{:?}", parts.parts);

    let part = generated_stream(3 * 1024 * 1024, 256 * 1024);
    service
        .upload_part(
            &ctx(),
            BUCKET,
            &key,
            &upload_id,
            1,
            PartChecksums::default(),
            part,
        )
        .await
        .expect("a refused part must return its reservation to the quota");
    let parts = service
        .list_parts(&ctx(), BUCKET, &key, &upload_id, None, 100)
        .await
        .expect("list parts");
    assert_eq!(parts.parts.len(), 1);
    assert_eq!(parts.parts[0].size, 3 * 1024 * 1024);
}

#[tokio::test]
async fn a_part_cancelled_mid_body_stages_nothing_and_frees_the_quota() {
    let mut limits = fixture::default_limits();
    limits.max_single_write_size = 16 * 1024 * 1024;
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

    let key = object_key("limits/cancelled-part.bin");
    let upload_id = service
        .create_multipart(
            &ctx(),
            BUCKET,
            &key,
            Some("tester"),
            Default::default(),
            Default::default(),
            None,
        )
        .await
        .expect("create upload")
        .upload_id
        .as_str()
        .to_string();

    // A part whose request is cancelled mid-body: the bytes it did send are
    // discarded with the local buffer and never charged to the staging quota.
    let cancelled_ctx = ctx();
    let token = cancelled_ctx.cancellation_token();
    let body = CancelAfterFirst {
        cancel: Some(token),
        yielded: false,
        chunk: Bytes::from(vec![b'c'; 512 * 1024]),
    };
    let err = service
        .upload_part(
            &cancelled_ctx,
            BUCKET,
            &key,
            &upload_id,
            1,
            PartChecksums::default(),
            Box::pin(body),
        )
        .await
        .expect_err("a cancelled part must not report success");
    assert!(matches!(err, ServiceError::InvalidRequest(_)), "{err:?}");

    let parts = service
        .list_parts(&ctx(), BUCKET, &key, &upload_id, None, 100)
        .await
        .expect("list parts");
    assert!(parts.parts.is_empty(), "{:?}", parts.parts);

    // The abandoned body consumed none of the budget, so a part nearly as large
    // as the whole quota still fits.
    let part = generated_stream(3 * 1024 * 1024, 256 * 1024);
    service
        .upload_part(
            &ctx(),
            BUCKET,
            &key,
            &upload_id,
            1,
            PartChecksums::default(),
            part,
        )
        .await
        .expect("an abandoned body must not hold on to the quota");
    assert!(object_is_absent(&service, &key).await);
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

#[tokio::test]
async fn a_saturated_driver_is_rejected_without_starving_another() {
    let mut limits = fixture::default_limits();
    limits.max_concurrent_requests_per_driver = 1;
    let service = service_with_two_drivers(limits).await;

    // One upload against the first driver, parked mid-body: it holds the only
    // slot that driver has.
    let park = Arc::new(Park::default());
    let held_key = object_key("driver/held.bin");
    let held = {
        let service = service.clone();
        let park = park.clone();
        tokio::spawn(async move {
            let ctx = ctx();
            service
                .put_object(
                    &ctx,
                    BUCKET,
                    &held_key,
                    Box::pin(ParkedBody {
                        first: Some(Bytes::from_static(b"parked")),
                        park,
                    }),
                    PutParams::default(),
                )
                .await
        })
    };
    park.pulled.notified().await;

    // The same driver is saturated: answered with `SlowDown`, not queued behind
    // the parked request, and nothing is published for it.
    let rejected_key = object_key("driver/rejected.bin");
    let err = service
        .put_object(
            &ctx(),
            BUCKET,
            &rejected_key,
            one_shot_stream(Bytes::from_static(b"rejected")),
            PutParams::default(),
        )
        .await
        .expect_err("a saturated driver must reject the request");
    assert_eq!(err, ServiceError::SlowDown);
    assert!(object_is_absent(&service, &rejected_key).await);

    // A different driver instance is unaffected by that saturation.
    service
        .put_object(
            &ctx(),
            BUCKET_B,
            &object_key("driver/other.bin"),
            one_shot_stream(Bytes::from_static(b"other")),
            PutParams::default(),
        )
        .await
        .expect("another driver instance must keep working");

    // Releasing the parked upload frees the slot for the next request.
    park.release();
    held.await
        .expect("the parked upload task must not panic")
        .expect("the parked upload completes once released");
    service
        .put_object(
            &ctx(),
            BUCKET,
            &rejected_key,
            one_shot_stream(Bytes::from_static(b"rejected")),
            PutParams::default(),
        )
        .await
        .expect("the slot is released when the operation ends");
}

#[tokio::test]
async fn a_saturated_server_rejects_globally_across_drivers() {
    let mut limits = fixture::default_limits();
    limits.max_concurrent_requests = 1;
    let service = service_with_two_drivers(limits).await;

    // One upload parked mid-body holds the only server-wide slot. Its own
    // driver's budget is untouched, so only the global limit can be the
    // reason a second operation is refused.
    let park = Arc::new(Park::default());
    let held_key = object_key("global/held.bin");
    let held = {
        let service = service.clone();
        let park = park.clone();
        tokio::spawn(async move {
            let ctx = ctx();
            service
                .put_object(
                    &ctx,
                    BUCKET,
                    &held_key,
                    Box::pin(ParkedBody {
                        first: Some(Bytes::from_static(b"parked")),
                        park,
                    }),
                    PutParams::default(),
                )
                .await
        })
    };
    park.pulled.notified().await;

    // The server is saturated: the next operation is answered with `SlowDown`
    // rather than queued, and nothing is published for it.
    let rejected_key = object_key("global/rejected.bin");
    let err = service
        .put_object(
            &ctx(),
            BUCKET,
            &rejected_key,
            one_shot_stream(Bytes::from_static(b"rejected")),
            PutParams::default(),
        )
        .await
        .expect_err("a saturated server must reject the request");
    assert_eq!(err, ServiceError::SlowDown);
    assert!(object_is_absent(&service, &rejected_key).await);

    // Another driver instance does not help: the budget is server-wide, not
    // per driver.
    let other_key = object_key("global/other.bin");
    let err = service
        .put_object(
            &ctx(),
            BUCKET_B,
            &other_key,
            one_shot_stream(Bytes::from_static(b"other")),
            PutParams::default(),
        )
        .await
        .expect_err("the budget is server-wide, not per driver");
    assert_eq!(err, ServiceError::SlowDown);
    assert!(object_is_absent_in(&service, BUCKET_B, &other_key).await);

    // Releasing the parked upload frees the slot for the next operation.
    park.release();
    held.await
        .expect("the parked upload task must not panic")
        .expect("the parked upload completes once released");
    service
        .put_object(
            &ctx(),
            BUCKET_B,
            &other_key,
            one_shot_stream(Bytes::from_static(b"other")),
            PutParams::default(),
        )
        .await
        .expect("the slot is released when the operation ends");
}

#[tokio::test]
async fn draining_waits_for_the_last_operation() {
    let service = service_with_limits(fixture::default_limits()).await;
    let park = Arc::new(Park::default());
    let held_key = object_key("drain/held.bin");
    let held = {
        let service = service.clone();
        let park = park.clone();
        tokio::spawn(async move {
            let ctx = ctx();
            service
                .put_object(
                    &ctx,
                    BUCKET,
                    &held_key,
                    Box::pin(ParkedBody {
                        first: Some(Bytes::from_static(b"parked")),
                        park,
                    }),
                    PutParams::default(),
                )
                .await
        })
    };
    park.pulled.notified().await;

    // An operation is still in flight, so draining must not report an idle
    // server yet.
    assert!(
        tokio::time::timeout(Duration::from_millis(200), service.drain())
            .await
            .is_err(),
        "the drain must wait for the operation still in flight"
    );

    park.release();
    held.await
        .expect("the parked upload task must not panic")
        .expect("the parked upload completes once released");
    tokio::time::timeout(Duration::from_secs(5), service.drain())
        .await
        .expect("the drain completes once the operation ends");
}

#[tokio::test]
async fn a_part_upload_is_refused_when_the_global_scope_is_saturated() {
    let mut limits = fixture::default_limits();
    limits.max_concurrent_requests = 1;
    let service = service_with_two_drivers(limits).await;

    let key = object_key("global/part.bin");
    let upload_id = service
        .create_multipart(
            &ctx(),
            BUCKET,
            &key,
            Some("tester"),
            Default::default(),
            Default::default(),
            None,
        )
        .await
        .expect("create upload")
        .upload_id
        .as_str()
        .to_string();

    // One parked object upload holds the only server-wide slot.
    let park = Arc::new(Park::default());
    let held_key = object_key("global/held.bin");
    let held = {
        let service = service.clone();
        let park = park.clone();
        tokio::spawn(async move {
            let ctx = ctx();
            service
                .put_object(
                    &ctx,
                    BUCKET,
                    &held_key,
                    Box::pin(ParkedBody {
                        first: Some(Bytes::from_static(b"parked")),
                        park,
                    }),
                    PutParams::default(),
                )
                .await
        })
    };
    park.pulled.notified().await;

    // A part upload is an operation like any other: it must not slip past the
    // saturated scope, and it must not leave a part behind.
    let err = service
        .upload_part(
            &ctx(),
            BUCKET,
            &key,
            &upload_id,
            1,
            PartChecksums::default(),
            one_shot_stream(Bytes::from_static(b"part")),
        )
        .await
        .expect_err("a part upload must be admitted like any other operation");
    assert_eq!(err, ServiceError::SlowDown);
    let parts = service
        .list_parts(&ctx(), BUCKET, &key, &upload_id, None, 100)
        .await
        .expect("list parts");
    assert!(parts.parts.is_empty(), "{:?}", parts.parts);

    // Releasing the parked upload frees the slot for the part.
    park.release();
    held.await
        .expect("the parked upload task must not panic")
        .expect("the parked upload completes once released");
    service
        .upload_part(
            &ctx(),
            BUCKET,
            &key,
            &upload_id,
            1,
            PartChecksums::default(),
            one_shot_stream(Bytes::from_static(b"part")),
        )
        .await
        .expect("the slot is released when the operation ends");
}

#[tokio::test]
async fn draining_waits_for_an_in_flight_part_upload() {
    let service = service_with_limits(fixture::default_limits()).await;
    let key = object_key("drain/part.bin");
    let upload_id = service
        .create_multipart(
            &ctx(),
            BUCKET,
            &key,
            Some("tester"),
            Default::default(),
            Default::default(),
            None,
        )
        .await
        .expect("create upload")
        .upload_id
        .as_str()
        .to_string();

    let park = Arc::new(Park::default());
    let held = {
        let service = service.clone();
        let park = park.clone();
        let key = key.clone();
        let upload_id = upload_id.clone();
        tokio::spawn(async move {
            let ctx = ctx();
            service
                .upload_part(
                    &ctx,
                    BUCKET,
                    &key,
                    &upload_id,
                    1,
                    PartChecksums::default(),
                    Box::pin(ParkedBody {
                        first: Some(Bytes::from_static(b"parked")),
                        park,
                    }),
                )
                .await
        })
    };
    park.pulled.notified().await;

    // The part is in flight, so draining must not report an idle server yet:
    // a shutdown that exits here would cut the part off.
    assert!(
        tokio::time::timeout(Duration::from_millis(200), service.drain())
            .await
            .is_err(),
        "the drain must wait for the part upload still in flight"
    );

    park.release();
    held.await
        .expect("the parked part task must not panic")
        .expect("the parked part completes once released");
    tokio::time::timeout(Duration::from_secs(5), service.drain())
        .await
        .expect("the drain completes once the part ends");
}

#[tokio::test]
async fn an_emulated_copy_completes_at_a_limit_of_one() {
    let mut limits = fixture::default_limits();
    // One share of each scope. A copy that claimed a second share of either
    // would be refused by its own limit — at a limit of one, forever.
    limits.max_concurrent_requests = 1;
    limits.max_concurrent_requests_per_driver = 1;
    let service = service_with_two_drivers(limits).await;

    let source = object_key("copy/source.bin");
    service
        .put_object(
            &ctx(),
            BUCKET,
            &source,
            one_shot_stream(Bytes::from_static(b"copied bytes")),
            PutParams::default(),
        )
        .await
        .expect("write the source");

    // A cross-bucket copy cannot be published natively, so it streams the
    // source into a staged write on the destination: two steps, one slot.
    let destination = object_key("copy/destination.bin");
    service
        .copy_object(
            &ctx(),
            BUCKET_B,
            CopyParams {
                source_bucket: BUCKET.to_string(),
                source_key: source.clone(),
                source_conditions: ReadConditions::default(),
                destination_key: destination.clone(),
                directive: MetadataDirective::Copy,
                content: ContentProperties::default(),
                user_metadata: UserMetadata::new(),
                preconditions: Preconditions::none(),
            },
        )
        .await
        .expect("a copy must not be refused by its own limit");

    let copied = service
        .get_object(
            &ctx(),
            BUCKET_B,
            &destination,
            &ReadConditions::default(),
            None,
        )
        .await
        .expect("the copy is readable");
    assert_eq!(
        read_to_end(copied.stream).await.expect("read body"),
        b"copied bytes"
    );
}
