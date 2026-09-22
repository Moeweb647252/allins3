//! Resource limits, streaming writes and instance fault isolation.
//!
//! These run the real [`ObjectService`] over the test drivers, so the checks
//! cover the service's buffering and admission behaviour rather than a driver's
//! own storage.

#![cfg(feature = "testing")]

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context as TaskContext, Poll};

use allins3::domain::{DriverContext, DriverError, ObjectKey};
use allins3::driver::{DriverFactory, Profile};
use allins3::service::{ObjectService, PutParams, ReadConditions, ServiceError};
use allins3::testing::fault::{FaultDriverFactory, FaultPlan};
use allins3::testing::fixture::{self, BucketSpec};
use allins3::testing::reference::ReferenceDriverFactory;
use allins3::testing::support::{one_shot_stream, read_to_end};
use bytes::Bytes;
use futures_core::Stream;
use tokio_util::sync::CancellationToken;

const BUCKET: &str = "bucket-a";
const DRIVER: &str = "inst-1";

/// A generated body: `total` bytes delivered in `chunk`-sized pieces without
/// ever materialising the whole object in the test.
struct Generated {
    total: u64,
    chunk: usize,
    produced: u64,
}

impl Stream for Generated {
    type Item = Result<Bytes, DriverError>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.produced >= this.total {
            return Poll::Ready(None);
        }
        let remaining = this.total - this.produced;
        let size = (this.chunk as u64).min(remaining) as usize;
        this.produced += size as u64;
        Poll::Ready(Some(Ok(Bytes::from(vec![b'x'; size]))))
    }
}

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

    let body = Generated {
        total: 2 * 1024 * 1024,
        chunk: 64 * 1024,
        produced: 0,
    };
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
    let body = Generated {
        total,
        chunk: 256 * 1024,
        produced: 0,
    };
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
        )
        .expect("create upload")
        .upload_id
        .as_str()
        .to_string();

    let part = Generated {
        total: 3 * 1024 * 1024,
        chunk: 256 * 1024,
        produced: 0,
    };
    service
        .upload_part(&ctx(), BUCKET, &key, &upload_id, 1, Box::pin(part))
        .await
        .expect("first part fits the quota");

    let part = Generated {
        total: 3 * 1024 * 1024,
        chunk: 256 * 1024,
        produced: 0,
    };
    let err = service
        .upload_part(&ctx(), BUCKET, &key, &upload_id, 2, Box::pin(part))
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
