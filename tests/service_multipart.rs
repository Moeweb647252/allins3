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
    ChecksumAlgorithm, ChecksumType, CommitState, ContentProperties, DriverContext, EntityTag,
    ObjectKey, Preconditions, UserMetadata,
};
use allins3::driver::{DriverFactory, Profile};
use allins3::integrity::compute_checksum;
use allins3::service::multipart::{
    DeclaredCompletionChecksum, MIN_PART_SIZE, MultipartChecksum, PartSummary,
};
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
            None,
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
            declared,
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
            BUCKET,
            key,
            Some("tester"),
            ContentProperties::default(),
            UserMetadata::new(),
            Some(MultipartChecksum::new(algorithm, checksum_type).expect("supported scheme")),
        )
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
        algorithm: ChecksumAlgorithm::Crc32,
        value: expected.clone(),
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
        algorithm: ChecksumAlgorithm::Sha256,
        value: expected.clone(),
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
        algorithm: ChecksumAlgorithm::Crc32,
        value: vec![0, 0, 0, 0],
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
        algorithm: ChecksumAlgorithm::Crc32,
        value: digest(ChecksumAlgorithm::Crc32, b"checked body"),
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
            .list_parts(BUCKET, &key, &upload_id, None, 100)
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
        .list_parts(BUCKET, &key, &upload_id, None, 100)
        .expect("list parts");
    assert_eq!(
        page.parts[0].checksum.as_ref().map(|c| c.value.clone()),
        Some(expected.into()),
        "ListParts reports the stored part checksum"
    );
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
