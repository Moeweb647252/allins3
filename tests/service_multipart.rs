//! Service-level multipart, completion-record and staging-sweep semantics.
//!
//! These tests drive the real [`ObjectService`] rather than the driver
//! interface, so the generic multipart coordinator, its four-state completion
//! records, its claim arbitration and its staging sweep are exercised through
//! the surface the protocol layer itself uses.

#![cfg(feature = "testing")]

use std::sync::Arc;
use std::time::Duration;

use allins3::domain::{
    CommitState, ContentProperties, DriverContext, EntityTag, ObjectKey, Preconditions,
    UserMetadata,
};
use allins3::driver::{DriverFactory, Profile};
use allins3::service::{ObjectService, ReadConditions, ServiceError};
use allins3::testing::fault::{FaultDriverFactory, FaultPlan};
use allins3::testing::fixture::{self, BucketSpec};
use allins3::testing::reference::ReferenceDriverFactory;
use allins3::testing::support::{one_shot_stream, read_to_end};

const BUCKET: &str = "bucket-a";
const DRIVER: &str = "inst-1";

fn ctx() -> DriverContext {
    fixture::ctx("service-multipart")
}

async fn service_with(factory: Arc<dyn DriverFactory>) -> ObjectService {
    fixture::service(
        vec![(DRIVER, factory)],
        vec![BucketSpec {
            bucket: BUCKET,
            driver: DRIVER,
            namespace: "ns-a",
            profile: Profile::CoreWrite,
        }],
        fixture::default_limits(),
        fixture::full_access(),
    )
    .await
}

fn object_key(value: &str) -> ObjectKey {
    ObjectKey::parse(value).expect("valid key")
}

async fn start_upload(service: &ObjectService, key: &ObjectKey) -> String {
    service
        .create_multipart(
            BUCKET,
            key,
            Some("tester"),
            ContentProperties::default(),
            UserMetadata::new(),
        )
        .expect("create multipart upload")
        .upload_id
        .as_str()
        .to_string()
}

async fn upload_part(
    service: &ObjectService,
    key: &ObjectKey,
    upload_id: &str,
    part_number: u16,
    data: &'static [u8],
) -> EntityTag {
    service
        .upload_part(
            &ctx(),
            BUCKET,
            key,
            upload_id,
            part_number,
            one_shot_stream(bytes::Bytes::from_static(data)),
        )
        .await
        .expect("upload part")
        .etag
}

async fn read_object(service: &ObjectService, key: &ObjectKey) -> Vec<u8> {
    let result = service
        .get_object(&ctx(), BUCKET, key, &ReadConditions::default(), None)
        .await
        .expect("get object");
    read_to_end(result.stream).await.expect("read body")
}

#[tokio::test]
async fn completion_publishes_once_and_records_the_attempt() {
    let service = service_with(Arc::new(ReferenceDriverFactory::new("reference"))).await;
    let key = object_key("multi/object.bin");
    let upload_id = start_upload(&service, &key).await;
    let etag = upload_part(&service, &key, &upload_id, 1, b"hello multipart").await;

    let manifest = [(1u16, Some(etag.clone()))];
    let meta = service
        .complete_multipart(
            &ctx(),
            BUCKET,
            &key,
            &upload_id,
            &manifest,
            Preconditions::none(),
        )
        .await
        .expect("complete");
    assert_eq!(meta.size, 15);
    let published = meta.etag.clone().expect("etag");
    assert!(
        published.as_str().ends_with("-1"),
        "multipart etag must carry the part count: {published:?}"
    );

    // The attempt is recorded as committed, so a lost reply can be reconciled
    // instead of replayed.
    let records = service.completion_records_for_upload(&upload_id);
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].state, CommitState::Committed);
    assert_eq!(records[0].key, key);

    // A retried completion of the same upload must not publish again.
    assert!(
        service
            .complete_multipart(
                &ctx(),
                BUCKET,
                &key,
                &upload_id,
                &manifest,
                Preconditions::none()
            )
            .await
            .is_err()
    );

    assert_eq!(read_object(&service, &key).await, b"hello multipart");
    assert!(service.multipart_is_idle());
}

#[tokio::test]
async fn a_changed_manifest_is_rejected_and_the_retry_succeeds() {
    let service = service_with(Arc::new(ReferenceDriverFactory::new("reference"))).await;
    let key = object_key("multi/retry.bin");
    let upload_id = start_upload(&service, &key).await;
    let etag = upload_part(&service, &key, &upload_id, 1, b"abcdef").await;

    let wrong = [(
        1u16,
        Some(EntityTag::new("not-the-part-etag").expect("tag")),
    )];
    let err = service
        .complete_multipart(
            &ctx(),
            BUCKET,
            &key,
            &upload_id,
            &wrong,
            Preconditions::none(),
        )
        .await
        .expect_err("a mismatched part ETag must be rejected");
    assert!(matches!(err, ServiceError::InvalidArgument(_)), "{err:?}");

    // A rejected completion must not consume the staged parts.
    let parts = service
        .list_parts(BUCKET, &key, &upload_id, None, 100)
        .expect("list parts");
    assert_eq!(parts.parts.len(), 1);
    assert!(service.latest_completion_state(&upload_id).is_none());

    let right = [(1u16, Some(etag))];
    let meta = service
        .complete_multipart(
            &ctx(),
            BUCKET,
            &key,
            &upload_id,
            &right,
            Preconditions::none(),
        )
        .await
        .expect("retry succeeds");
    assert_eq!(meta.size, 6);
    assert_eq!(read_object(&service, &key).await, b"abcdef");
}

#[tokio::test]
async fn reuploading_a_part_replaces_it_without_touching_the_published_object() {
    let service = service_with(Arc::new(ReferenceDriverFactory::new("reference"))).await;
    let key = object_key("multi/replace.bin");
    service
        .put_object(
            &ctx(),
            BUCKET,
            &key,
            one_shot_stream(bytes::Bytes::from_static(b"original")),
            Default::default(),
        )
        .await
        .expect("seed object");

    let upload_id = start_upload(&service, &key).await;
    upload_part(&service, &key, &upload_id, 1, b"first").await;
    let etag = upload_part(&service, &key, &upload_id, 1, b"second").await;

    // The re-uploaded part replaced the first without publishing anything.
    assert_eq!(read_object(&service, &key).await, b"original");

    let manifest = [(1u16, Some(etag))];
    service
        .complete_multipart(
            &ctx(),
            BUCKET,
            &key,
            &upload_id,
            &manifest,
            Preconditions::none(),
        )
        .await
        .expect("complete");
    assert_eq!(read_object(&service, &key).await, b"second");
}

#[tokio::test]
async fn a_lost_acknowledgement_publishes_once_and_still_answers_success() {
    let factory = Arc::new(FaultDriverFactory::new(
        Arc::new(ReferenceDriverFactory::new("reference")),
        FaultPlan::lost_ack(),
    ));
    let service = service_with(factory).await;
    let key = object_key("multi/lost-ack.bin");
    let upload_id = start_upload(&service, &key).await;
    let etag = upload_part(&service, &key, &upload_id, 1, b"published once").await;

    let manifest = [(1u16, Some(etag))];
    let meta = service
        .complete_multipart(
            &ctx(),
            BUCKET,
            &key,
            &upload_id,
            &manifest,
            Preconditions::none(),
        )
        .await
        .expect("the committed attempt is restored instead of reported as failed");

    assert_eq!(meta.size, 14);
    assert_eq!(
        service.latest_completion_state(&upload_id),
        Some(CommitState::Committed)
    );
    assert_eq!(read_object(&service, &key).await, b"published once");

    // No second publish, and the staging of the finished upload is gone.
    assert!(
        service
            .complete_multipart(
                &ctx(),
                BUCKET,
                &key,
                &upload_id,
                &manifest,
                Preconditions::none()
            )
            .await
            .is_err()
    );
    assert!(service.multipart_is_idle());
}

#[tokio::test]
async fn a_fenced_attempt_resolves_to_final_non_commit_and_allows_one_retry() {
    let factory = Arc::new(FaultDriverFactory::new(
        Arc::new(ReferenceDriverFactory::new("reference")),
        FaultPlan::fenced(),
    ));
    let service = service_with(factory).await;
    let key = object_key("multi/fenced.bin");
    let upload_id = start_upload(&service, &key).await;
    let etag = upload_part(&service, &key, &upload_id, 1, b"fenced").await;

    let manifest = [(1u16, Some(etag.clone()))];
    let err = service
        .complete_multipart(
            &ctx(),
            BUCKET,
            &key,
            &upload_id,
            &manifest,
            Preconditions::none(),
        )
        .await
        .expect_err("a fenced attempt must not report success");
    assert!(!err.to_string().is_empty());

    // Recovery proved the attempt can never publish, so a replacement attempt
    // with the same upload id is allowed.
    assert_eq!(
        service.latest_completion_state(&upload_id),
        Some(CommitState::NotCommittedFinal)
    );
    assert!(
        service
            .get_object(&ctx(), BUCKET, &key, &ReadConditions::default(), None)
            .await
            .is_err(),
        "the fenced attempt must not have published anything"
    );

    let meta = service
        .complete_multipart(
            &ctx(),
            BUCKET,
            &key,
            &upload_id,
            &manifest,
            Preconditions::none(),
        )
        .await
        .expect("the retry succeeds");
    assert_eq!(meta.size, 6);
    assert_eq!(read_object(&service, &key).await, b"fenced");
    assert_eq!(
        service.latest_completion_state(&upload_id),
        Some(CommitState::Committed)
    );
}

#[tokio::test]
async fn an_unresolvable_commit_outcome_blocks_replay_and_is_never_swept() {
    let factory = Arc::new(FaultDriverFactory::new(
        Arc::new(ReferenceDriverFactory::new("reference")),
        FaultPlan {
            hide_recovery: true,
            commit: allins3::testing::fault::CommitFault::LostAck,
            ..FaultPlan::none()
        },
    ));
    let service = service_with(factory).await;
    let key = object_key("multi/unknown.bin");
    let upload_id = start_upload(&service, &key).await;
    let etag = upload_part(&service, &key, &upload_id, 1, b"unknown outcome").await;

    let manifest = [(1u16, Some(etag))];
    service
        .complete_multipart(
            &ctx(),
            BUCKET,
            &key,
            &upload_id,
            &manifest,
            Preconditions::none(),
        )
        .await
        .expect_err("an unresolvable outcome cannot be reported as a failure either");

    assert_eq!(
        service.latest_completion_state(&upload_id),
        Some(CommitState::Unknown)
    );

    // The unresolved attempt must not be replayed...
    let replay = service
        .complete_multipart(
            &ctx(),
            BUCKET,
            &key,
            &upload_id,
            &manifest,
            Preconditions::none(),
        )
        .await
        .expect_err("an unresolved attempt must not be replayed");
    assert!(
        matches!(replay, ServiceError::InvalidArgument(_)),
        "{replay:?}"
    );

    // ...nor aborted, nor reclaimed by a sweep with an expired lease.
    assert!(service.abort_multipart(BUCKET, &key, &upload_id).is_err());
    let swept = service.sweep_expired_uploads(Duration::ZERO);
    assert_eq!(swept.reclaimed_uploads, 0);
    assert_eq!(swept.skipped_pending, 1);
    let uploads = service
        .list_uploads(BUCKET, None, None, None, 100)
        .expect("list uploads");
    assert_eq!(uploads.uploads.len(), 1);
}

#[tokio::test]
async fn a_sweep_reclaims_only_expired_uploads() {
    let service = service_with(Arc::new(ReferenceDriverFactory::new("reference"))).await;
    let key = object_key("multi/sweep.bin");
    let upload_id = start_upload(&service, &key).await;
    upload_part(&service, &key, &upload_id, 1, b"sweep me").await;

    // A live lease keeps the staged parts.
    let kept = service.sweep_expired_uploads(Duration::from_secs(3600));
    assert_eq!(kept.reclaimed_uploads, 0);
    assert_eq!(kept.reclaimed_bytes, 0);
    assert!(!service.multipart_is_idle());

    // An expired lease reclaims the parts and returns the bytes to the quota.
    let swept = service.sweep_expired_uploads(Duration::ZERO);
    assert_eq!(swept.reclaimed_uploads, 1);
    assert_eq!(swept.reclaimed_bytes, 8);
    assert!(service.multipart_is_idle());
    assert!(
        service
            .list_parts(BUCKET, &key, &upload_id, None, 100)
            .is_err()
    );
}

#[tokio::test]
async fn concurrent_completion_and_abort_are_arbitrated() {
    let service = service_with(Arc::new(ReferenceDriverFactory::new("reference"))).await;
    let key = object_key("multi/race.bin");
    let upload_id = start_upload(&service, &key).await;
    let etag = upload_part(&service, &key, &upload_id, 1, b"raced").await;
    let manifest = [(1u16, Some(etag))];

    // Two completions racing must produce exactly one publish.
    let ctx_a = ctx();
    let ctx_b = ctx();
    let (first, second) = tokio::join!(
        service.complete_multipart(
            &ctx_a,
            BUCKET,
            &key,
            &upload_id,
            &manifest,
            Preconditions::none()
        ),
        service.complete_multipart(
            &ctx_b,
            BUCKET,
            &key,
            &upload_id,
            &manifest,
            Preconditions::none()
        ),
    );
    assert!(
        first.is_ok() ^ second.is_ok(),
        "exactly one completion may win: {first:?} / {second:?}"
    );
    assert_eq!(read_object(&service, &key).await, b"raced");

    // A completion racing an abort must also produce exactly one outcome.
    let other = object_key("multi/race-2.bin");
    let upload_id = start_upload(&service, &other).await;
    let etag = upload_part(&service, &other, &upload_id, 1, b"aborted?").await;
    let manifest = [(1u16, Some(etag))];
    let ctx_c = ctx();
    let (completed, aborted) = tokio::join!(
        service.complete_multipart(
            &ctx_c,
            BUCKET,
            &other,
            &upload_id,
            &manifest,
            Preconditions::none()
        ),
        async { service.abort_multipart(BUCKET, &other, &upload_id) },
    );
    assert!(
        !(completed.is_ok() && aborted.is_ok()),
        "an abort must not succeed after a completion claimed the upload"
    );
    let published = service
        .get_object(&ctx(), BUCKET, &other, &ReadConditions::default(), None)
        .await;
    if completed.is_ok() {
        assert_eq!(read_object(&service, &other).await, b"aborted?");
    } else {
        assert!(published.is_err(), "an aborted upload must not be visible");
    }
}
