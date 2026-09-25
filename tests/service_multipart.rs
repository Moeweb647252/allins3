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
    Checksum, ChecksumAlgorithm, ChecksumType, CommitState, ContentProperties, DriverContext,
    EntityTag, NamespaceId, ObjectKey, Preconditions, UserMetadata,
};
use allins3::driver::{DriverFactory, NamespaceCapabilities, Profile, UploadHandle, UploadId};
use allins3::integrity::{base64, compute_checksum};
use allins3::service::multipart::{
    CompletionPart, DeclaredCompletionChecksum, MIN_PART_SIZE, MultipartChecksum, PartChecksums,
    PartSummary,
};
use allins3::service::{ObjectService, ReadConditions, ServiceError, TrailerChecksum};
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
            &ctx(),
            BUCKET,
            key,
            Some("tester"),
            ContentProperties::default(),
            UserMetadata::new(),
            None,
        )
        .await
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
    upload_part_bytes(service, key, upload_id, part_number, data.to_vec(), None)
        .await
        .expect("upload part")
        .etag
}

/// Uploads a part of arbitrary length, optionally declaring its checksum.
async fn upload_part_bytes(
    service: &ObjectService,
    key: &ObjectKey,
    upload_id: &str,
    part_number: u16,
    data: Vec<u8>,
    declared: Option<(ChecksumAlgorithm, Vec<u8>)>,
) -> Result<PartSummary, ServiceError> {
    service
        .upload_part(
            &ctx(),
            BUCKET,
            key,
            upload_id,
            part_number,
            PartChecksums {
                declared,
                ..Default::default()
            },
            one_shot_stream(bytes::Bytes::from(data)),
        )
        .await
}

/// Uploads a part for an arbitrary set of declared checksums.
async fn upload_part_checksums(
    service: &ObjectService,
    key: &ObjectKey,
    upload_id: &str,
    part_number: u16,
    checksums: PartChecksums,
    data: Vec<u8>,
) -> Result<PartSummary, ServiceError> {
    service
        .upload_part(
            &ctx(),
            BUCKET,
            key,
            upload_id,
            part_number,
            checksums,
            one_shot_stream(bytes::Bytes::from(data)),
        )
        .await
}

/// Starts an upload with an explicit checksum scheme.
async fn start_upload_with_checksum(
    service: &ObjectService,
    key: &ObjectKey,
    algorithm: ChecksumAlgorithm,
    checksum_type: ChecksumType,
) -> String {
    service
        .create_multipart(
            &ctx(),
            BUCKET,
            key,
            Some("tester"),
            ContentProperties::default(),
            UserMetadata::new(),
            Some(MultipartChecksum::new(algorithm, checksum_type).expect("supported scheme")),
        )
        .await
        .expect("create multipart upload")
        .upload_id
        .as_str()
        .to_string()
}

/// The raw digest of `data` under `algorithm`, as a client would compute it.
fn digest(algorithm: ChecksumAlgorithm, data: &[u8]) -> Vec<u8> {
    compute_checksum(algorithm, data).to_vec()
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
            None,
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
                None,
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
            None,
            Preconditions::none(),
        )
        .await
        .expect_err("a mismatched part ETag must be rejected");
    assert!(matches!(err, ServiceError::InvalidArgument(_)), "{err:?}");

    // A rejected completion must not consume the staged parts.
    let parts = service
        .list_parts(&ctx(), BUCKET, &key, &upload_id, None, 100)
        .await
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
            None,
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
            None,
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
            None,
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
                None,
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
            None,
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
            None,
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
async fn a_completion_that_cannot_open_the_backend_is_retryable() {
    let factory = Arc::new(FaultDriverFactory::new(
        Arc::new(ReferenceDriverFactory::new("reference")),
        FaultPlan {
            fail_first_begin_write: true,
            ..FaultPlan::none()
        },
    ));
    let service = service_with(factory).await;
    let key = object_key("multi/retry-backend.bin");
    let upload_id = start_upload(&service, &key).await;
    let etag = upload_part(&service, &key, &upload_id, 1, b"retry me").await;

    let manifest = [(1u16, Some(etag))];
    service
        .complete_multipart(
            &ctx(),
            BUCKET,
            &key,
            &upload_id,
            &manifest,
            None,
            Preconditions::none(),
        )
        .await
        .expect_err("a backend that cannot open a staged write must fail the completion");

    // Nothing was staged, so the claim must not outlive the failure: the parts
    // are still there and no attempt was recorded.
    let parts = service
        .list_parts(&ctx(), BUCKET, &key, &upload_id, None, 100)
        .await
        .expect("list parts");
    assert_eq!(parts.parts.len(), 1);
    assert!(service.latest_completion_state(&upload_id).is_none());

    // The retry is a normal completion, not one refused as "already being
    // completed" for the rest of the upload's life.
    let meta = service
        .complete_multipart(
            &ctx(),
            BUCKET,
            &key,
            &upload_id,
            &manifest,
            None,
            Preconditions::none(),
        )
        .await
        .expect("the retry must not be refused as already completing");
    assert_eq!(meta.size, 8);
    assert_eq!(read_object(&service, &key).await, b"retry me");
}

#[tokio::test]
async fn a_part_write_failure_that_aborts_finally_allows_the_retry() {
    let factory = Arc::new(FaultDriverFactory::new(
        Arc::new(ReferenceDriverFactory::new("reference")),
        FaultPlan {
            fail_first_write_chunk: true,
            ..FaultPlan::none()
        },
    ));
    let service = service_with(factory).await;
    let key = object_key("multi/retry-chunk.bin");
    let upload_id = start_upload(&service, &key).await;
    let etag = upload_part(&service, &key, &upload_id, 1, b"chunked").await;

    let manifest = [(1u16, Some(etag))];
    service
        .complete_multipart(
            &ctx(),
            BUCKET,
            &key,
            &upload_id,
            &manifest,
            None,
            Preconditions::none(),
        )
        .await
        .expect_err("a part write that fails must fail the completion");

    // The acknowledged abort proved the attempt final, so the claim must be
    // gone and the upload retryable.
    assert_eq!(
        service.latest_completion_state(&upload_id),
        Some(CommitState::NotCommittedFinal)
    );
    let meta = service
        .complete_multipart(
            &ctx(),
            BUCKET,
            &key,
            &upload_id,
            &manifest,
            None,
            Preconditions::none(),
        )
        .await
        .expect("a final non-commit must not refuse the retry");
    assert_eq!(meta.size, 7);
    assert_eq!(read_object(&service, &key).await, b"chunked");
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
            None,
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
            None,
            Preconditions::none(),
        )
        .await
        .expect_err("an unresolved attempt must not be replayed");
    assert!(
        matches!(replay, ServiceError::InvalidArgument(_)),
        "{replay:?}"
    );

    // ...nor aborted, nor reclaimed by a sweep with an expired lease.
    assert!(
        service
            .abort_multipart(&ctx(), BUCKET, &key, &upload_id)
            .await
            .is_err()
    );
    let swept = service.sweep_expired_uploads(&ctx(), Duration::ZERO).await;
    assert_eq!(swept.reclaimed_uploads, 0);
    assert_eq!(swept.skipped_pending, 1);
    let uploads = service
        .list_uploads(&ctx(), BUCKET, None, None, None, 100)
        .await
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
    let kept = service
        .sweep_expired_uploads(&ctx(), Duration::from_secs(3600))
        .await;
    assert_eq!(kept.reclaimed_uploads, 0);
    assert_eq!(kept.reclaimed_bytes, 0);
    assert!(!service.multipart_is_idle());

    // An expired lease reclaims the parts and returns the bytes to the quota.
    let swept = service.sweep_expired_uploads(&ctx(), Duration::ZERO).await;
    assert_eq!(swept.reclaimed_uploads, 1);
    assert_eq!(swept.reclaimed_bytes, 8);
    assert!(service.multipart_is_idle());
    assert!(
        service
            .list_parts(&ctx(), BUCKET, &key, &upload_id, None, 100)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn the_background_sweeper_reclaims_abandoned_staging_and_stops_on_request() {
    // Spec §10 wants every background task to have an owner, a cancel path and
    // a shutdown wait; spec §8.2 wants reclaim driven by a lease. This is the
    // task the server actually runs, not a direct call to the sweep.
    let service = service_with(Arc::new(ReferenceDriverFactory::new("reference"))).await;
    let key = object_key("multi/abandoned.bin");
    let upload_id = start_upload(&service, &key).await;
    upload_part(&service, &key, &upload_id, 1, b"abandoned").await;
    assert!(!service.multipart_is_idle());

    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    let sweeper = tokio::spawn({
        let service = service.clone();
        async move {
            service
                .run_multipart_sweeper(
                    Duration::ZERO,
                    Duration::from_millis(5),
                    async move {
                        let mut stop = stop_rx;
                        let _ = stop.changed().await;
                    },
                    |_| {},
                )
                .await;
        }
    });

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while !service.multipart_is_idle() && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(
        service.multipart_is_idle(),
        "the sweeper must reclaim an upload whose lease expired"
    );

    // The cancel path is real: the task ends when asked and joining it does not
    // hang.
    stop_tx.send(true).expect("the sweeper is still listening");
    tokio::time::timeout(Duration::from_secs(5), sweeper)
        .await
        .expect("the sweeper stops when told")
        .expect("the sweeper task did not panic");
}

#[tokio::test]
async fn a_part_upload_resets_the_reclaim_lease() {
    // The lease is measured from the upload's last activity, so an upload that
    // is still being fed parts is never reclaimed under the client. The first
    // sleep outlasts the lease from creation, so a lease measured from
    // `initiated` would reclaim here.
    const LEASE: Duration = Duration::from_millis(100);
    let service = service_with(Arc::new(ReferenceDriverFactory::new("reference"))).await;
    let key = object_key("multi/live.bin");
    let upload_id = start_upload(&service, &key).await;
    tokio::time::sleep(LEASE + Duration::from_millis(20)).await;
    upload_part(&service, &key, &upload_id, 1, b"first").await;

    let kept = service.sweep_expired_uploads(&ctx(), LEASE).await;
    assert_eq!(
        kept.reclaimed_uploads, 0,
        "a part just uploaded must keep the upload alive"
    );
    assert!(!service.multipart_is_idle());
}

#[tokio::test]
async fn an_aborted_upload_returns_its_staged_bytes_to_the_quota() {
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

    let key = object_key("multi/abort-quota.bin");
    let upload_id = start_upload(&service, &key).await;
    upload_part_bytes(
        &service,
        &key,
        &upload_id,
        1,
        vec![b'a'; 3 * 1024 * 1024],
        None,
    )
    .await
    .expect("a part inside the quota");

    service
        .abort_multipart(&ctx(), BUCKET, &key, &upload_id)
        .await
        .expect("abort the upload");
    assert!(
        service.multipart_is_idle(),
        "abort must return the staged bytes to the quota"
    );

    // The reclaimed bytes are what the next upload draws on.
    let next_key = object_key("multi/abort-quota-next.bin");
    let next_upload = start_upload(&service, &next_key).await;
    upload_part_bytes(
        &service,
        &next_key,
        &next_upload,
        1,
        vec![b'b'; 3 * 1024 * 1024],
        None,
    )
    .await
    .expect("the next upload must be able to use the reclaimed quota");
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
            None,
            Preconditions::none()
        ),
        service.complete_multipart(
            &ctx_b,
            BUCKET,
            &key,
            &upload_id,
            &manifest,
            None,
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
            None,
            Preconditions::none()
        ),
        async {
            service
                .abort_multipart(&ctx(), BUCKET, &other, &upload_id)
                .await
        },
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

#[test]
fn multipart_checksum_schemes_reject_illegal_combinations() {
    // CRC64NVME has no composite form; the SHA family has no multipart
    // full-object form (spec §6.3).
    assert!(MultipartChecksum::new(ChecksumAlgorithm::Crc64Nvme, ChecksumType::Composite).is_err());
    assert!(MultipartChecksum::new(ChecksumAlgorithm::Sha256, ChecksumType::FullObject).is_err());
    assert!(MultipartChecksum::new(ChecksumAlgorithm::Crc32, ChecksumType::Composite).is_ok());

    // A client that names only an algorithm gets that algorithm's default
    // coverage: CRC family full-object, SHA family composite.
    assert_eq!(
        MultipartChecksum::from_algorithm(ChecksumAlgorithm::Crc64Nvme)
            .expect("crc64nvme")
            .checksum_type,
        ChecksumType::FullObject
    );
    assert_eq!(
        MultipartChecksum::from_algorithm(ChecksumAlgorithm::Sha256)
            .expect("sha256")
            .checksum_type,
        ChecksumType::Composite
    );
}

#[tokio::test]
async fn multipart_full_object_checksum_covers_the_assembled_object() {
    let service = service_with(Arc::new(ReferenceDriverFactory::new("reference"))).await;
    let key = object_key("checksum/full-object.bin");
    let upload_id = start_upload_with_checksum(
        &service,
        &key,
        ChecksumAlgorithm::Crc32,
        ChecksumType::FullObject,
    )
    .await;

    let first = vec![b'a'; MIN_PART_SIZE as usize];
    let second = b"the tail after a full part".to_vec();
    let mut whole = first.clone();
    whole.extend_from_slice(&second);
    let expected = digest(ChecksumAlgorithm::Crc32, &whole);

    // The client declares each part exactly as it computed it, then the
    // full-object value over the assembled bytes.
    let one = upload_part_bytes(
        &service,
        &key,
        &upload_id,
        1,
        first.clone(),
        Some((
            ChecksumAlgorithm::Crc32,
            digest(ChecksumAlgorithm::Crc32, &first),
        )),
    )
    .await
    .expect("part one");
    let two = upload_part_bytes(
        &service,
        &key,
        &upload_id,
        2,
        second.clone(),
        Some((
            ChecksumAlgorithm::Crc32,
            digest(ChecksumAlgorithm::Crc32, &second),
        )),
    )
    .await
    .expect("part two");
    assert_eq!(
        one.checksum.as_ref().map(|c| c.value.clone()),
        Some(digest(ChecksumAlgorithm::Crc32, &first).into()),
        "the part digest must be the client's"
    );

    let manifest = [(1u16, Some(one.etag)), (2u16, Some(two.etag))];
    let declared = DeclaredCompletionChecksum {
        algorithm: Some(ChecksumAlgorithm::Crc32),
        value: Some(expected.clone()),
        checksum_type: Some(ChecksumType::FullObject),
        part_count: None,
    };
    let meta = service
        .complete_multipart(
            &ctx(),
            BUCKET,
            &key,
            &upload_id,
            &manifest,
            Some(declared),
            Preconditions::none(),
        )
        .await
        .expect("complete");

    let stored = meta.checksums.first().expect("a checksum is stored");
    assert_eq!(stored.algorithm(), ChecksumAlgorithm::Crc32);
    assert_eq!(stored.checksum_type(), ChecksumType::FullObject);
    assert_eq!(stored.value().as_ref(), expected.as_slice());
    assert_eq!(stored.part_count(), None);

    // A full-object checksum is the digest of the whole object, so it also
    // matches what a single PUT of the same bytes would store.
    assert_eq!(read_object(&service, &key).await, whole);
    let head = service
        .head_object(&ctx(), BUCKET, &key, &ReadConditions::default(), None)
        .await
        .expect("head");
    assert_eq!(
        head.metadata.checksums.first().map(|c| c.value().clone()),
        Some(expected.into())
    );
}

#[tokio::test]
async fn multipart_composite_checksum_digests_the_ordered_part_digests() {
    let service = service_with(Arc::new(ReferenceDriverFactory::new("reference"))).await;
    let key = object_key("checksum/composite.bin");
    let upload_id = start_upload_with_checksum(
        &service,
        &key,
        ChecksumAlgorithm::Sha256,
        ChecksumType::Composite,
    )
    .await;

    let first = vec![b'x'; MIN_PART_SIZE as usize];
    let second = b"composite tail".to_vec();

    let one = upload_part_bytes(&service, &key, &upload_id, 1, first.clone(), None)
        .await
        .expect("part one");
    let two = upload_part_bytes(&service, &key, &upload_id, 2, second.clone(), None)
        .await
        .expect("part two");

    // Independent expectation: SHA256 over the *concatenated raw part digests*,
    // not over the concatenated parts and not over their base64 forms.
    let mut concatenated = digest(ChecksumAlgorithm::Sha256, &first);
    concatenated.extend_from_slice(&digest(ChecksumAlgorithm::Sha256, &second));
    let expected = digest(ChecksumAlgorithm::Sha256, &concatenated);

    let manifest = [(1u16, Some(one.etag)), (2u16, Some(two.etag))];
    let declared = DeclaredCompletionChecksum {
        algorithm: Some(ChecksumAlgorithm::Sha256),
        value: Some(expected.clone()),
        checksum_type: Some(ChecksumType::Composite),
        part_count: Some(2),
    };
    let meta = service
        .complete_multipart(
            &ctx(),
            BUCKET,
            &key,
            &upload_id,
            &manifest,
            Some(declared),
            Preconditions::none(),
        )
        .await
        .expect("complete");

    let stored = meta.checksums.first().expect("a checksum is stored");
    assert_eq!(stored.checksum_type(), ChecksumType::Composite);
    assert_eq!(stored.part_count(), Some(2));
    assert_eq!(stored.value().as_ref(), expected.as_slice());

    // The composite value is not the digest of the assembled object.
    let mut whole = first.clone();
    whole.extend_from_slice(&second);
    assert_ne!(digest(ChecksumAlgorithm::Sha256, &whole), expected);
}

#[tokio::test]
async fn a_mismatched_completion_checksum_never_publishes() {
    let service = service_with(Arc::new(ReferenceDriverFactory::new("reference"))).await;
    let key = object_key("checksum/mismatch.bin");
    let upload_id = start_upload_with_checksum(
        &service,
        &key,
        ChecksumAlgorithm::Crc32,
        ChecksumType::FullObject,
    )
    .await;
    let etag = upload_part(&service, &key, &upload_id, 1, b"checked body").await;
    let manifest = [(1u16, Some(etag))];

    let wrong = DeclaredCompletionChecksum {
        algorithm: Some(ChecksumAlgorithm::Crc32),
        value: Some(vec![0, 0, 0, 0]),
        checksum_type: None,
        part_count: None,
    };
    let err = service
        .complete_multipart(
            &ctx(),
            BUCKET,
            &key,
            &upload_id,
            &manifest,
            Some(wrong),
            Preconditions::none(),
        )
        .await
        .expect_err("a wrong completion checksum must fail");
    assert!(
        matches!(err, ServiceError::BadDigest(_)),
        "a checksum mismatch is BadDigest, got {err:?}"
    );
    assert!(
        service
            .get_object(&ctx(), BUCKET, &key, &ReadConditions::default(), None)
            .await
            .is_err(),
        "a rejected completion must not publish anything"
    );

    // The upload survives the rejection, so the client can retry correctly.
    let correct = DeclaredCompletionChecksum {
        algorithm: Some(ChecksumAlgorithm::Crc32),
        value: Some(digest(ChecksumAlgorithm::Crc32, b"checked body")),
        checksum_type: Some(ChecksumType::FullObject),
        part_count: None,
    };
    service
        .complete_multipart(
            &ctx(),
            BUCKET,
            &key,
            &upload_id,
            &manifest,
            Some(correct),
            Preconditions::none(),
        )
        .await
        .expect("a corrected retry publishes");
    assert_eq!(read_object(&service, &key).await, b"checked body");
}

#[tokio::test]
async fn part_checksums_are_verified_and_reported() {
    let service = service_with(Arc::new(ReferenceDriverFactory::new("reference"))).await;
    let key = object_key("checksum/parts.bin");
    let body = b"part body".to_vec();
    let upload_id = start_upload_with_checksum(
        &service,
        &key,
        ChecksumAlgorithm::Crc32c,
        ChecksumType::FullObject,
    )
    .await;

    // A declared value that does not match the bytes is rejected and nothing is
    // staged.
    let err = upload_part_bytes(
        &service,
        &key,
        &upload_id,
        1,
        body.clone(),
        Some((ChecksumAlgorithm::Crc32c, vec![1, 2, 3, 4])),
    )
    .await
    .expect_err("a wrong part checksum must fail");
    assert!(matches!(err, ServiceError::BadDigest(_)), "got {err:?}");
    assert!(
        service
            .list_parts(&ctx(), BUCKET, &key, &upload_id, None, 100)
            .await
            .expect("list parts")
            .parts
            .is_empty(),
        "a rejected part must not be staged"
    );

    // An algorithm that does not match the upload's scheme is rejected before
    // the body is read.
    let err = upload_part_bytes(
        &service,
        &key,
        &upload_id,
        1,
        body.clone(),
        Some((
            ChecksumAlgorithm::Sha256,
            digest(ChecksumAlgorithm::Sha256, &body),
        )),
    )
    .await
    .expect_err("a mismatched algorithm must fail");
    assert!(
        matches!(err, ServiceError::InvalidRequest(_)),
        "got {err:?}"
    );

    // The correct declared value is accepted and stored with the part.
    let expected = digest(ChecksumAlgorithm::Crc32c, &body);
    let summary = upload_part_bytes(
        &service,
        &key,
        &upload_id,
        1,
        body.clone(),
        Some((ChecksumAlgorithm::Crc32c, expected.clone())),
    )
    .await
    .expect("a matching part checksum is accepted");
    assert_eq!(
        summary.checksum.as_ref().map(|c| c.value.clone()),
        Some(expected.clone().into())
    );

    let page = service
        .list_parts(&ctx(), BUCKET, &key, &upload_id, None, 100)
        .await
        .expect("list parts");
    assert_eq!(
        page.parts[0].checksum.as_ref().map(|c| c.value.clone()),
        Some(expected.into()),
        "ListParts reports the stored part checksum"
    );
}

#[tokio::test]
async fn a_part_trailer_checksum_is_verified_after_the_body() {
    let service = service_with(Arc::new(ReferenceDriverFactory::new("reference"))).await;
    let key = object_key("checksum/part-trailer.bin");
    let upload_id = start_upload_with_checksum(
        &service,
        &key,
        ChecksumAlgorithm::Crc32,
        ChecksumType::FullObject,
    )
    .await;
    let body = b"part with a trailer".to_vec();
    let expected = digest(ChecksumAlgorithm::Crc32, &body);

    // A trailer reader for one algorithm, as the protocol layer builds it.
    let trailer = |algorithm: ChecksumAlgorithm, value: Option<Vec<u8>>| {
        TrailerChecksum::new(algorithm, move || value.clone().map(|bytes| base64(&bytes)))
    };
    let stage = |checksums: PartChecksums, body: &[u8]| {
        upload_part_checksums(&service, &key, &upload_id, 1, checksums, body.to_vec())
    };

    // A matching trailer value is verified and stored with the part.
    let summary = stage(
        PartChecksums {
            trailer: Some(trailer(ChecksumAlgorithm::Crc32, Some(expected.clone()))),
            ..Default::default()
        },
        &body,
    )
    .await
    .expect("a matching trailer stages the part");
    assert_eq!(
        summary.checksum.as_ref().map(|c| c.value.clone()),
        Some(expected.clone().into())
    );

    // A trailer that disagrees with the bytes is rejected and replaces nothing.
    let err = stage(
        PartChecksums {
            trailer: Some(trailer(ChecksumAlgorithm::Crc32, Some(vec![0, 0, 0, 0]))),
            ..Default::default()
        },
        &body,
    )
    .await
    .expect_err("a mismatched trailer must fail");
    assert!(matches!(err, ServiceError::BadDigest(_)), "{err:?}");

    // A declared trailer that never arrives is a mismatch too.
    let err = stage(
        PartChecksums {
            trailer: Some(trailer(ChecksumAlgorithm::Crc32, None)),
            ..Default::default()
        },
        &body,
    )
    .await
    .expect_err("an absent trailer must fail");
    assert!(matches!(err, ServiceError::BadDigest(_)), "{err:?}");

    // A trailer algorithm that disagrees with the upload's scheme is refused
    // before any bytes are read.
    let err = stage(
        PartChecksums {
            trailer: Some(trailer(
                ChecksumAlgorithm::Sha256,
                Some(digest(ChecksumAlgorithm::Sha256, &body)),
            )),
            ..Default::default()
        },
        &body,
    )
    .await
    .expect_err("a mismatched trailer algorithm must fail");
    assert!(matches!(err, ServiceError::InvalidRequest(_)), "{err:?}");

    // Only the verified part is staged; the rejected attempts added nothing.
    let page = service
        .list_parts(&ctx(), BUCKET, &key, &upload_id, None, 100)
        .await
        .expect("list parts");
    assert_eq!(page.parts.len(), 1, "{page:?}");
    assert_eq!(
        page.parts[0].checksum.as_ref().map(|c| c.value.clone()),
        Some(expected.into())
    );
}

#[tokio::test]
async fn a_completion_that_declares_only_a_type_is_validated_against_the_scheme() {
    let service = service_with(Arc::new(ReferenceDriverFactory::new("reference"))).await;

    // A coverage-only declaration is legal: it constrains the checksum the
    // server must produce without naming a value to compare against.
    let key = object_key("checksum/type-only.bin");
    let upload_id = start_upload_with_checksum(
        &service,
        &key,
        ChecksumAlgorithm::Sha256,
        ChecksumType::Composite,
    )
    .await;
    let etag = upload_part(&service, &key, &upload_id, 1, b"type only").await;
    let manifest = [(1u16, Some(etag))];
    let expected = digest(
        ChecksumAlgorithm::Sha256,
        &digest(ChecksumAlgorithm::Sha256, b"type only"),
    );
    let meta = service
        .complete_multipart(
            &ctx(),
            BUCKET,
            &key,
            &upload_id,
            &manifest,
            Some(DeclaredCompletionChecksum {
                algorithm: None,
                value: None,
                checksum_type: Some(ChecksumType::Composite),
                part_count: None,
            }),
            Preconditions::none(),
        )
        .await
        .expect("a coverage-only declaration publishes");
    let stored = meta.checksums.first().expect("a checksum is stored");
    assert_eq!(stored.value().as_ref(), expected.as_slice());
    assert_eq!(stored.checksum_type(), ChecksumType::Composite);

    // A coverage that contradicts the upload's scheme is refused.
    let key = object_key("checksum/type-conflict.bin");
    let upload_id = start_upload_with_checksum(
        &service,
        &key,
        ChecksumAlgorithm::Crc32,
        ChecksumType::FullObject,
    )
    .await;
    let etag = upload_part(&service, &key, &upload_id, 1, b"conflict").await;
    let manifest = [(1u16, Some(etag))];
    let err = service
        .complete_multipart(
            &ctx(),
            BUCKET,
            &key,
            &upload_id,
            &manifest,
            Some(DeclaredCompletionChecksum {
                algorithm: None,
                value: None,
                checksum_type: Some(ChecksumType::Composite),
                part_count: None,
            }),
            Preconditions::none(),
        )
        .await
        .expect_err("a conflicting coverage must fail");
    assert!(matches!(err, ServiceError::InvalidRequest(_)), "{err:?}");

    // The refusal must not consume the upload: a corrected retry still works.
    let meta = service
        .complete_multipart(
            &ctx(),
            BUCKET,
            &key,
            &upload_id,
            &manifest,
            Some(DeclaredCompletionChecksum {
                algorithm: None,
                value: None,
                checksum_type: Some(ChecksumType::FullObject),
                part_count: None,
            }),
            Preconditions::none(),
        )
        .await
        .expect("a corrected declaration publishes");
    assert_eq!(
        meta.checksums.first().map(Checksum::checksum_type),
        Some(ChecksumType::FullObject)
    );

    // An upload created without a scheme needs an algorithm; a coverage alone
    // cannot supply one.
    let key = object_key("checksum/type-no-scheme.bin");
    let upload_id = start_upload(&service, &key).await;
    let etag = upload_part(&service, &key, &upload_id, 1, b"no scheme").await;
    let manifest = [(1u16, Some(etag))];
    let err = service
        .complete_multipart(
            &ctx(),
            BUCKET,
            &key,
            &upload_id,
            &manifest,
            Some(DeclaredCompletionChecksum {
                algorithm: None,
                value: None,
                checksum_type: Some(ChecksumType::FullObject),
                part_count: None,
            }),
            Preconditions::none(),
        )
        .await
        .expect_err("a coverage without a scheme must fail");
    assert!(matches!(err, ServiceError::InvalidRequest(_)), "{err:?}");
}

#[tokio::test]
async fn a_part_that_names_an_algorithm_fixes_the_upload_scheme() {
    let service = service_with(Arc::new(ReferenceDriverFactory::new("reference"))).await;
    let key = object_key("checksum/deferred-scheme.bin");
    let upload_id = start_upload(&service, &key).await;

    let body = b"scheme from the part".to_vec();
    let part_digest = digest(ChecksumAlgorithm::Crc32, &body);
    let summary = upload_part_bytes(
        &service,
        &key,
        &upload_id,
        1,
        body.clone(),
        Some((ChecksumAlgorithm::Crc32, part_digest)),
    )
    .await
    .expect("part fixes the scheme");

    // Nothing is declared on completion, so the server publishes the checksum
    // it computed under the scheme the part established.
    let manifest = [(1u16, Some(summary.etag))];
    let meta = service
        .complete_multipart(
            &ctx(),
            BUCKET,
            &key,
            &upload_id,
            &manifest,
            None,
            Preconditions::none(),
        )
        .await
        .expect("complete");
    let stored = meta.checksums.first().expect("a checksum is stored");
    assert_eq!(stored.algorithm(), ChecksumAlgorithm::Crc32);
    assert_eq!(stored.checksum_type(), ChecksumType::FullObject);
    assert_eq!(
        stored.value().as_ref(),
        digest(ChecksumAlgorithm::Crc32, &body).as_slice()
    );
}

// --- The native route ------------------------------------------------------
//
// The same operations, on a namespace whose plan selects `Multipart = Native`:
// the driver owns the parts and assembles them, while the service keeps the
// protocol semantics. The assertions mirror the coordinator's, so the two
// routes can be read side by side.

/// A service over a namespace that declares native multipart.
async fn native_service() -> ObjectService {
    let mut caps = NamespaceCapabilities::full_memory();
    caps.multipart = true;
    let factory: Arc<dyn DriverFactory> =
        Arc::new(ReferenceDriverFactory::with_capabilities("reference", caps));
    service_with(factory).await
}

async fn published_etag(service: &ObjectService, key: &ObjectKey) -> String {
    service
        .head_object(&ctx(), BUCKET, key, &ReadConditions::default(), None)
        .await
        .expect("head object")
        .metadata
        .etag
        .expect("etag")
        .as_str()
        .to_string()
}

#[tokio::test]
async fn a_native_upload_publishes_through_the_driver() {
    let service = native_service().await;
    let key = object_key("native/object.bin");
    let upload_id = start_upload(&service, &key).await;
    let first = upload_part_bytes(
        &service,
        &key,
        &upload_id,
        1,
        vec![b'a'; MIN_PART_SIZE as usize],
        None,
    )
    .await
    .expect("first part")
    .etag;
    let second = upload_part(&service, &key, &upload_id, 2, b"tail").await;

    let manifest = [(1u16, Some(first)), (2u16, Some(second))];
    let meta = service
        .complete_multipart(
            &ctx(),
            BUCKET,
            &key,
            &upload_id,
            &manifest,
            None,
            Preconditions::none(),
        )
        .await
        .expect("complete");
    assert_eq!(meta.size, MIN_PART_SIZE + 4);
    let etag = meta.etag.clone().expect("etag");
    assert!(
        etag.as_str().ends_with("-2"),
        "the multipart etag must carry the part count: {etag:?}"
    );

    let body = read_object(&service, &key).await;
    assert_eq!(body.len(), (MIN_PART_SIZE + 4) as usize);
    assert!(body.ends_with(b"tail"));
    assert!(service.multipart_is_idle());

    // The upload is consumed exactly once.
    assert!(
        service
            .complete_multipart(
                &ctx(),
                BUCKET,
                &key,
                &upload_id,
                &manifest,
                None,
                Preconditions::none()
            )
            .await
            .is_err()
    );
    assert_eq!(published_etag(&service, &key).await, etag.as_str());
}

#[tokio::test]
async fn a_native_completion_is_validated_before_it_publishes() {
    let service = native_service().await;
    let key = object_key("native/rejected.bin");
    let upload_id = start_upload(&service, &key).await;
    let etag = upload_part(&service, &key, &upload_id, 1, b"payload").await;

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
            None,
            Preconditions::none(),
        )
        .await
        .expect_err("a mismatched part ETag must be rejected");
    assert!(matches!(err, ServiceError::InvalidArgument(_)), "{err:?}");

    // Nothing was published, and the rejected attempt left the parts alone.
    assert!(
        service
            .head_object(&ctx(), BUCKET, &key, &ReadConditions::default(), None)
            .await
            .is_err()
    );
    let parts = service
        .list_parts(&ctx(), BUCKET, &key, &upload_id, None, 100)
        .await
        .expect("list parts");
    assert_eq!(parts.parts.len(), 1);
    assert!(!service.multipart_is_idle());

    let right = [(1u16, Some(etag))];
    service
        .complete_multipart(
            &ctx(),
            BUCKET,
            &key,
            &upload_id,
            &right,
            None,
            Preconditions::none(),
        )
        .await
        .expect("a corrected retry publishes");
    assert_eq!(read_object(&service, &key).await, b"payload");
}

#[tokio::test]
async fn a_native_completion_verifies_the_declared_object_checksum() {
    let service = native_service().await;
    let key = object_key("native/checksum.bin");
    let upload_id = start_upload_with_checksum(
        &service,
        &key,
        ChecksumAlgorithm::Crc32,
        ChecksumType::FullObject,
    )
    .await;
    let etag = upload_part(&service, &key, &upload_id, 1, b"assembled bytes").await;
    let manifest = [(1u16, Some(etag))];

    let wrong = DeclaredCompletionChecksum {
        algorithm: Some(ChecksumAlgorithm::Crc32),
        value: Some(vec![0, 0, 0, 0]),
        checksum_type: Some(ChecksumType::FullObject),
        part_count: None,
    };
    let err = service
        .complete_multipart(
            &ctx(),
            BUCKET,
            &key,
            &upload_id,
            &manifest,
            Some(wrong),
            Preconditions::none(),
        )
        .await
        .expect_err("a declared checksum that does not match must not publish");
    assert!(matches!(err, ServiceError::BadDigest(_)), "{err:?}");
    assert!(
        service
            .head_object(&ctx(), BUCKET, &key, &ReadConditions::default(), None)
            .await
            .is_err()
    );

    let correct = DeclaredCompletionChecksum {
        algorithm: Some(ChecksumAlgorithm::Crc32),
        value: Some(digest(ChecksumAlgorithm::Crc32, b"assembled bytes")),
        checksum_type: Some(ChecksumType::FullObject),
        part_count: None,
    };
    let meta = service
        .complete_multipart(
            &ctx(),
            BUCKET,
            &key,
            &upload_id,
            &manifest,
            Some(correct),
            Preconditions::none(),
        )
        .await
        .expect("the matching checksum publishes");
    let stored = meta.checksums.first().expect("a checksum is stored");
    assert_eq!(stored.algorithm(), ChecksumAlgorithm::Crc32);
    assert_eq!(
        stored.value().as_ref(),
        digest(ChecksumAlgorithm::Crc32, b"assembled bytes").as_slice()
    );
}

#[tokio::test]
async fn a_native_upload_abort_removes_it() {
    let service = native_service().await;
    let key = object_key("native/abort.bin");
    let upload_id = start_upload(&service, &key).await;
    upload_part(&service, &key, &upload_id, 1, b"dropped").await;

    service
        .abort_multipart(&ctx(), BUCKET, &key, &upload_id)
        .await
        .expect("abort the upload");
    assert!(service.multipart_is_idle());
    assert!(
        service
            .list_parts(&ctx(), BUCKET, &key, &upload_id, None, 100)
            .await
            .is_err()
    );
    assert!(
        service
            .head_object(&ctx(), BUCKET, &key, &ReadConditions::default(), None)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn the_native_route_keeps_its_uploads_out_of_the_coordinator() {
    let native = native_service().await;
    let coordinator = service_with(Arc::new(ReferenceDriverFactory::new("reference"))).await;
    let key = object_key("native/routing.bin");

    // The plan, not an ad-hoc handle check, decides where the upload lives: a
    // native namespace keeps it in the driver, and the sweep retires a native
    // record only once the driver no longer holds the upload, so an expired
    // lease reclaims nothing here and the client can still finish the upload...
    let native_upload = start_upload(&native, &key).await;
    upload_part(&native, &key, &native_upload, 1, b"kept").await;
    let swept = native.sweep_expired_uploads(&ctx(), Duration::ZERO).await;
    assert_eq!(
        swept.reclaimed_uploads, 0,
        "the service must not reclaim a driver-side upload"
    );
    assert_eq!(swept.skipped_pending, 0);
    assert!(
        native
            .list_parts(&ctx(), BUCKET, &key, &native_upload, None, 100)
            .await
            .is_ok(),
        "the driver-side upload must survive the sweep"
    );
    native
        .abort_multipart(&ctx(), BUCKET, &key, &native_upload)
        .await
        .expect("abort the native upload");

    // ... while the same sequence on the coordinator is the coordinator's to
    // reclaim.
    let coordinated_upload = start_upload(&coordinator, &key).await;
    upload_part(&coordinator, &key, &coordinated_upload, 1, b"kept").await;
    let swept = coordinator
        .sweep_expired_uploads(&ctx(), Duration::ZERO)
        .await;
    assert_eq!(swept.reclaimed_uploads, 1);
    assert_eq!(swept.reclaimed_bytes, 4);
}

#[tokio::test]
async fn the_sweep_retires_a_native_record_once_the_driver_forgets_the_upload() {
    let mut caps = NamespaceCapabilities::full_memory();
    caps.multipart = true;
    let factory: Arc<dyn DriverFactory> =
        Arc::new(ReferenceDriverFactory::with_capabilities("reference", caps));
    let (service, _other, driver) = fixture::two_services_over_shared_drivers(
        factory,
        BucketSpec {
            bucket: BUCKET,
            driver: DRIVER,
            namespace: "ns-a",
            profile: Profile::CoreWrite,
        },
        fixture::default_limits(),
        fixture::full_access(),
    )
    .await;

    let key = object_key("native/forgotten.bin");
    let upload_id = start_upload(&service, &key).await;
    upload_part(&service, &key, &upload_id, 1, b"orphan").await;

    // The driver still holds the upload, so an expired lease keeps the record
    // even though the service has nothing staged for it.
    let kept = service.sweep_expired_uploads(&ctx(), Duration::ZERO).await;
    assert_eq!(kept.reclaimed_uploads, 0);
    assert!(!service.multipart_is_idle());

    // The driver forgets the upload out of band (its own session lifetime, or
    // an abort from another replica). The record can no longer drive anything,
    // so the next sweep retires it and the service goes idle again.
    let namespace = driver
        .open_namespace(&ctx(), &NamespaceId::parse("ns-a").expect("namespace"))
        .await
        .expect("open namespace");
    namespace
        .multipart
        .as_ref()
        .expect("native multipart")
        .abort(
            &ctx(),
            &UploadHandle {
                upload_id: UploadId::from_raw(upload_id.clone()),
                key: key.clone(),
            },
        )
        .await
        .expect("driver-side abort");

    let swept = service.sweep_expired_uploads(&ctx(), Duration::ZERO).await;
    assert_eq!(swept.reclaimed_uploads, 1);
    assert!(service.multipart_is_idle());
    assert!(
        service
            .list_parts(&ctx(), BUCKET, &key, &upload_id, None, 100)
            .await
            .is_err(),
        "a retired native upload is no longer drivable"
    );
}

#[tokio::test]
async fn a_native_upload_another_service_created_is_adopted_and_drivable() {
    let mut caps = NamespaceCapabilities::full_memory();
    caps.multipart = true;
    let factory: Arc<dyn DriverFactory> =
        Arc::new(ReferenceDriverFactory::with_capabilities("reference", caps));
    let (creator, adopter, _driver) = fixture::two_services_over_shared_drivers(
        factory,
        BucketSpec {
            bucket: BUCKET,
            driver: DRIVER,
            namespace: "ns-a",
            profile: Profile::CoreWrite,
        },
        fixture::default_limits(),
        fixture::full_access(),
    )
    .await;

    let key = object_key("native/adopted.bin");
    let upload_id = start_upload(&creator, &key).await;
    let etag = upload_part(&creator, &key, &upload_id, 1, b"adopted").await;

    // The adopter has no session for this upload: `ListMultipartUploads` is the
    // driver's answer, and everything it reports must be drivable, so the
    // adopter takes the upload on instead of reporting a session every entry
    // point would refuse.
    let page = adopter
        .list_uploads(&ctx(), BUCKET, None, None, None, 100)
        .await
        .expect("list uploads");
    assert_eq!(page.uploads.len(), 1);
    assert_eq!(page.uploads[0].upload_id.as_str(), upload_id);
    assert!(!adopter.multipart_is_idle());

    // Drivable: the adopter can list the parts and finish the upload.
    let parts = adopter
        .list_parts(&ctx(), BUCKET, &key, &upload_id, None, 100)
        .await
        .expect("list parts");
    assert_eq!(parts.parts.len(), 1);
    assert_eq!(parts.parts[0].etag, etag);
    adopter
        .complete_multipart(
            &ctx(),
            BUCKET,
            &key,
            &upload_id,
            &[(1u16, Some(etag))],
            None,
            Preconditions::none(),
        )
        .await
        .expect("complete the adopted upload");
    assert!(adopter.multipart_is_idle());
    assert_eq!(read_object(&adopter, &key).await, b"adopted".to_vec());
}

#[tokio::test]
async fn an_empty_upload_marker_selects_from_the_beginning_on_both_routes() {
    // S3 defines the marker as a pair whose empty value means "from the start";
    // the coordinated route always read it that way, and the native route must
    // too, instead of rejecting the empty key or applying a lone marker.
    for service in [
        native_service().await,
        service_with(Arc::new(ReferenceDriverFactory::new("reference"))).await,
    ] {
        for name in ["markers/a.bin", "markers/b.bin"] {
            let key = object_key(name);
            let upload_id = start_upload(&service, &key).await;
            upload_part(&service, &key, &upload_id, 1, b"part").await;
        }
        let all = service
            .list_uploads(&ctx(), BUCKET, None, None, None, 100)
            .await
            .expect("list uploads");
        assert_eq!(all.uploads.len(), 2);
        let empty = service
            .list_uploads(&ctx(), BUCKET, None, Some(""), Some(""), 100)
            .await
            .expect("an empty marker is the start of the listing");
        assert_eq!(empty.uploads.len(), 2);
        // A lone marker is not a position: it is ignored on both routes.
        let lone = service
            .list_uploads(&ctx(), BUCKET, None, None, Some("whatever"), 100)
            .await
            .expect("a lone upload-id marker is ignored");
        assert_eq!(lone.uploads.len(), 2);
    }
}

#[tokio::test]
async fn the_native_route_and_the_coordinator_agree() {
    let native = native_service().await;
    let coordinator = service_with(Arc::new(ReferenceDriverFactory::new("reference"))).await;
    let key = object_key("parity/object.bin");

    let mut published = Vec::new();
    for service in [&native, &coordinator] {
        let upload_id = start_upload(service, &key).await;
        let first = upload_part_bytes(
            service,
            &key,
            &upload_id,
            1,
            vec![b'p'; MIN_PART_SIZE as usize],
            None,
        )
        .await
        .expect("first part")
        .etag;
        let second = upload_part(service, &key, &upload_id, 2, b"end").await;
        let manifest = [(1u16, Some(first)), (2u16, Some(second))];
        service
            .complete_multipart(
                &ctx(),
                BUCKET,
                &key,
                &upload_id,
                &manifest,
                None,
                Preconditions::none(),
            )
            .await
            .expect("complete");
        published.push((
            published_etag(service, &key).await,
            read_object(service, &key).await,
        ));
    }

    assert_eq!(
        published[0].0, published[1].0,
        "the published ETags must agree"
    );
    assert_eq!(
        published[0].1, published[1].1,
        "the published bytes must agree"
    );
}

#[tokio::test]
async fn a_part_checksum_under_another_algorithm_is_refused_on_both_routes() {
    let native = native_service().await;
    let coordinator = service_with(Arc::new(ReferenceDriverFactory::new("reference"))).await;
    let key = object_key("manifest/algorithm.bin");

    for service in [&native, &coordinator] {
        // The upload runs under CRC32, and a part digest is only computed under
        // that, so a declaration under SHA256 has nothing to be checked against
        // on either route.
        let upload_id = start_upload_with_checksum(
            service,
            &key,
            ChecksumAlgorithm::Crc32,
            ChecksumType::FullObject,
        )
        .await;
        let etag = upload_part(service, &key, &upload_id, 1, b"payload").await;

        let wrong = [CompletionPart {
            number: 1,
            etag: Some(etag.clone()),
            checksum: Some((
                ChecksumAlgorithm::Sha256,
                digest(ChecksumAlgorithm::Sha256, b"payload"),
            )),
        }];
        let err = service
            .complete_multipart(
                &ctx(),
                BUCKET,
                &key,
                &upload_id,
                &wrong,
                None,
                Preconditions::none(),
            )
            .await
            .expect_err("a part declared under another algorithm must be refused");
        assert!(matches!(err, ServiceError::InvalidRequest(_)), "{err:?}");

        // Nothing was published and the part is untouched, so the same manifest
        // under the upload's own algorithm still completes.
        assert!(
            service
                .head_object(&ctx(), BUCKET, &key, &ReadConditions::default(), None)
                .await
                .is_err()
        );
        let right = [CompletionPart {
            number: 1,
            etag: Some(etag),
            checksum: Some((
                ChecksumAlgorithm::Crc32,
                digest(ChecksumAlgorithm::Crc32, b"payload"),
            )),
        }];
        service
            .complete_multipart(
                &ctx(),
                BUCKET,
                &key,
                &upload_id,
                &right,
                None,
                Preconditions::none(),
            )
            .await
            .expect("a part declared under the upload's algorithm publishes");
        assert_eq!(read_object(service, &key).await, b"payload");
    }
}

#[tokio::test]
async fn a_zero_part_marker_is_accepted_on_both_routes() {
    let native = native_service().await;
    let coordinator = service_with(Arc::new(ReferenceDriverFactory::new("reference"))).await;
    let key = object_key("native/marker.bin");

    for service in [&native, &coordinator] {
        let upload_id = start_upload(service, &key).await;
        upload_part(service, &key, &upload_id, 1, b"one").await;
        upload_part(service, &key, &upload_id, 2, b"two").await;

        // `part-number-marker=0` asks for the first page; it is not part 0, and
        // rejecting it would make one route refuse a request the other serves.
        let page = service
            .list_parts(&ctx(), BUCKET, &key, &upload_id, Some(0), 100)
            .await
            .expect("a zero part marker must be accepted");
        assert_eq!(page.parts.len(), 2, "{page:?}");

        let page = service
            .list_parts(&ctx(), BUCKET, &key, &upload_id, Some(1), 100)
            .await
            .expect("list parts");
        assert_eq!(page.parts.len(), 1, "{page:?}");
        assert_eq!(page.parts[0].number, 2);
    }
}
