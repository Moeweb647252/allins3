//! The generic multipart coordinator.
//!
//! Multipart is provided by the service layer over the driver's staged write,
//! so it works for any writable namespace without native multipart support
//! (spec §8). Sessions and staged parts live in a `MultipartStore` /
//! `StagingStore` test implementation backed by memory with a configured quota.
//!
//! This staging store is deliberately **not** durable: it survives neither a
//! restart nor a crash, which is why multipart cannot be claimed "production
//! ready" here (`INV-10`). The state machine, atomic completion, manifest
//! validation and quota accounting are real.

use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, SystemTime};

use bytes::Bytes;

use crate::domain::{
    BeginWrite, Checksum, ChecksumAlgorithm, ChecksumType, CommitAttributes, CommitState,
    ContentProperties, DriverContext, EntityTag, ObjectKey, ObjectMetadata, PartNumber,
    Preconditions, UserMetadata, WriteId,
};
use crate::driver::UploadId;
use crate::integrity::{StreamingDigest, composite_checksum, hex};

pub use super::staging::PartChecksum;
use super::staging::{StagingLedger, StoredPart};
use super::{ObjectService, ServiceError, next_chunk};

/// Minimum size of any part except the last.
pub const MIN_PART_SIZE: u64 = 5 * 1024 * 1024;

/// The checksum scheme a multipart upload runs under.
///
/// The scheme is fixed for the lifetime of the upload, because part digests can
/// only be combined by an algorithm and coverage chosen up front (spec §6.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MultipartChecksum {
    /// The algorithm every part and the finished object are hashed with.
    pub algorithm: ChecksumAlgorithm,
    /// Whether the object-level checksum digests the assembled bytes or the
    /// ordered part digests.
    pub checksum_type: ChecksumType,
}

impl MultipartChecksum {
    /// Validates an algorithm/coverage pair against the multipart support
    /// table, rejecting illegal combinations such as `CRC64NVME + COMPOSITE`.
    pub fn new(
        algorithm: ChecksumAlgorithm,
        checksum_type: ChecksumType,
    ) -> Result<Self, ServiceError> {
        if !algorithm.supports_multipart(checksum_type) {
            return Err(ServiceError::InvalidArgument(format!(
                "`{}` does not support the `{}` checksum type for multipart uploads",
                algorithm.as_str(),
                checksum_type.as_str()
            )));
        }
        Ok(Self {
            algorithm,
            checksum_type,
        })
    }

    /// The scheme a client chose when it named only an algorithm.
    pub fn from_algorithm(algorithm: ChecksumAlgorithm) -> Result<Self, ServiceError> {
        Self::new(algorithm, algorithm.default_multipart_type())
    }
}

/// The checksums a part upload declares for its bytes.
///
/// The algorithm may be named by `x-amz-sdk-checksum-algorithm`, by an
/// `x-amz-checksum-*` value or by the name the request declared as a trailer.
/// A trailer's *value* only exists once the body has been consumed, so it is
/// verified after the stream ends rather than before.
#[derive(Debug, Clone, Default)]
pub struct PartChecksums {
    /// The algorithm the request asked for, if it named one.
    pub algorithm: Option<ChecksumAlgorithm>,
    /// A value the client declared in an `x-amz-checksum-*` header.
    pub declared: Option<(ChecksumAlgorithm, Vec<u8>)>,
    /// A value the client declared in a request trailer.
    pub trailer: Option<super::TrailerChecksum>,
}

/// One entry of a completion manifest.
#[derive(Debug, Clone)]
pub struct CompletionPart {
    /// The part number.
    pub number: u16,
    /// The ETag the client expects for this part, when it sent one.
    pub etag: Option<EntityTag>,
    /// The part checksum the client declared, when it sent one.
    pub checksum: Option<(ChecksumAlgorithm, Vec<u8>)>,
}

impl From<(u16, Option<EntityTag>)> for CompletionPart {
    fn from((number, etag): (u16, Option<EntityTag>)) -> Self {
        Self {
            number,
            etag,
            checksum: None,
        }
    }
}

/// A whole-object checksum the client declared on completion.
///
/// A composite value arrives in the wire form `base64(digest)-<partcount>`, so
/// the declared digest and the declared part count are kept separately.
#[derive(Debug, Clone)]
pub struct DeclaredCompletionChecksum {
    /// The algorithm the declared value belongs to, when a value was sent.
    pub algorithm: Option<ChecksumAlgorithm>,
    /// The raw declared digest, when the client sent one. A request that sends
    /// only a checksum type declares the coverage, not a value.
    pub value: Option<Vec<u8>>,
    /// The declared coverage, when the client sent `x-amz-checksum-type`.
    pub checksum_type: Option<ChecksumType>,
    /// The declared part count, when the value carried a `-<count>` suffix.
    pub part_count: Option<u32>,
}

/// One in-progress multipart upload.
#[derive(Debug, Clone)]
pub struct UploadSession {
    /// The upload id.
    pub upload_id: UploadId,
    /// The bound bucket.
    pub bucket: String,
    /// The target key.
    pub key: ObjectKey,
    /// The principal that created it.
    pub principal: Option<String>,
    /// The binding generation it was created under.
    pub generation: u64,
    /// Content properties for the finished object.
    pub content: ContentProperties,
    /// User metadata for the finished object.
    pub user_metadata: UserMetadata,
    /// The checksum scheme the upload was created with, if any.
    pub checksum: Option<MultipartChecksum>,
    /// Creation time.
    pub initiated: SystemTime,
    /// When this upload last received a part.
    ///
    /// The reclaim lease is measured from here rather than from `initiated`, so
    /// an upload that is still being fed parts is never reclaimed under a
    /// client no matter how long it has been open.
    pub last_activity: SystemTime,
}

/// The recorded outcome of one completion attempt.
///
/// This is the multipart analogue of a driver's commit record: it exists so a
/// retried `CompleteMultipartUpload` (or an operator) can learn what happened to
/// an attempt whose reply was lost, without replaying it.
#[derive(Debug, Clone)]
pub struct CompletionRecord {
    /// The attempt identifier.
    pub write_id: WriteId,
    /// The upload the attempt belonged to.
    pub upload_id: String,
    /// The bucket the attempt targeted.
    pub bucket: String,
    /// The key the attempt targeted.
    pub key: ObjectKey,
    /// The last known state of the attempt.
    pub state: CommitState,
    /// When the state was recorded.
    pub recorded_at: SystemTime,
}

/// What a staging sweep reclaimed.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SweepOutcome {
    /// Expired uploads whose staged parts were reclaimed.
    pub reclaimed_uploads: usize,
    /// Bytes released back to the staging quota.
    pub reclaimed_bytes: u64,
    /// Expired uploads deliberately kept because a completion attempt is still
    /// in flight or has an unknown outcome.
    pub skipped_pending: usize,
}

/// Multipart state: sessions, in-flight completion claims and completion
/// records, plus the staging ledger that owns the part table and its quota.
pub(crate) struct MultipartState {
    uploads: HashMap<String, UploadSession>,
    /// Uploads with a completion attempt in flight (`upload_id -> write_id`).
    claims: HashMap<String, WriteId>,
    /// Completion records by attempt id, for recovery queries.
    completions: HashMap<String, CompletionRecord>,
    /// The part table and its byte accounting, together, so the two cannot
    /// drift apart.
    staging: StagingLedger,
}

impl MultipartState {
    /// Creates state with a staging quota in bytes.
    pub(crate) fn with_quota(quota: u64) -> Self {
        Self {
            uploads: HashMap::new(),
            claims: HashMap::new(),
            completions: HashMap::new(),
            staging: StagingLedger::new(quota),
        }
    }
}

/// The multipart state of one service.
///
/// The owner of the state, so the coordinator is the only thing that knows how
/// it is locked: every other layer reaches multipart through [`ObjectService`]'s
/// operations and never takes the lock itself.
pub(crate) struct Multipart {
    state: std::sync::Mutex<MultipartState>,
}

impl Multipart {
    /// Empty state sharing `quota` bytes of staging between all uploads.
    pub(crate) fn new(quota: u64) -> Self {
        Self {
            state: std::sync::Mutex::new(MultipartState::with_quota(quota)),
        }
    }

    /// Locks the state for one coordinator step.
    pub(crate) fn store(&self) -> std::sync::MutexGuard<'_, MultipartState> {
        crate::sync::lock(&self.state)
    }
}

/// The staging quota charged for one part body while it is being received.
///
/// The charge is taken per chunk, before the chunk is buffered, so a part the
/// quota cannot hold is refused mid-body instead of after the whole body has
/// been read into memory (spec §9.3). It is released however the read ends —
/// staged, refused, cancelled or failed — so a refused part never holds quota
/// that another uploader could have used.
struct StagingReservation<'a> {
    service: &'a ObjectService,
    bytes: u64,
}

impl<'a> StagingReservation<'a> {
    fn new(service: &'a ObjectService) -> Self {
        Self { service, bytes: 0 }
    }

    /// Charges one more received chunk.
    fn grow(&mut self, bytes: u64) {
        self.bytes = self.bytes.saturating_add(bytes);
    }

    /// Releases the charge early, before the part is staged: staged bytes are
    /// counted by `staged_bytes`, and counting both would charge one part
    /// twice against the quota.
    fn release(&mut self) {
        let bytes = std::mem::take(&mut self.bytes);
        if bytes > 0 {
            self.service.release_staging(bytes);
        }
    }
}

impl Drop for StagingReservation<'_> {
    fn drop(&mut self) {
        self.release();
    }
}

/// The target upload and the source object of an `UploadPartCopy`.
#[derive(Debug, Clone)]
pub(crate) struct UploadPartCopyRequest {
    /// The upload's bucket.
    pub bucket: String,
    /// The upload's target key.
    pub key: ObjectKey,
    /// The upload id.
    pub upload_id: String,
    /// The destination part number.
    pub part_number: u16,
    /// The bucket holding the source object.
    pub source_bucket: String,
    /// The source object key.
    pub source_key: ObjectKey,
    /// The source byte range, absent for the whole object.
    pub range: Option<crate::domain::RangeSpec>,
}

/// A part summary returned to the protocol layer.
#[derive(Debug, Clone)]
pub struct PartSummary {
    /// Part number.
    pub number: u16,
    /// Size in bytes.
    pub size: u64,
    /// Part ETag.
    pub etag: EntityTag,
    /// The part checksum under the upload's scheme, when it has one.
    pub checksum: Option<PartChecksum>,
    /// Last modification time.
    pub last_modified: SystemTime,
}

/// A page of parts.
#[derive(Debug, Clone)]
pub struct PartListPage {
    /// Parts in ascending order.
    pub parts: Vec<PartSummary>,
    /// Marker for the next page.
    pub next_marker: Option<u16>,
    /// Whether more parts follow.
    pub is_truncated: bool,
}

/// A page of in-progress uploads.
#[derive(Debug, Clone)]
pub struct UploadListPage {
    /// Uploads in marker order.
    pub uploads: Vec<UploadSession>,
    /// Next key marker.
    pub next_key_marker: Option<ObjectKey>,
    /// Next upload-id marker.
    pub next_upload_id_marker: Option<UploadId>,
    /// Whether more uploads follow.
    pub is_truncated: bool,
}

/// How many completion records are retained for recovery queries.
const COMPLETION_RECORD_LIMIT: usize = 4096;

impl ObjectService {
    fn multipart(&self) -> std::sync::MutexGuard<'_, MultipartState> {
        self.inner.multipart.store()
    }

    /// Creates a multipart upload without touching any existing object.
    pub fn create_multipart(
        &self,
        bucket: &str,
        key: &ObjectKey,
        principal: Option<&str>,
        content: ContentProperties,
        user_metadata: UserMetadata,
        checksum: Option<MultipartChecksum>,
    ) -> Result<UploadSession, ServiceError> {
        let binding = self.binding(bucket)?;
        self.require_writable(&binding)?;
        let upload_id = UploadId::generate();
        let session = UploadSession {
            upload_id: upload_id.clone(),
            bucket: bucket.to_string(),
            key: key.clone(),
            principal: principal.map(str::to_string),
            generation: binding.generation,
            content,
            user_metadata,
            checksum,
            initiated: SystemTime::now(),
            last_activity: SystemTime::now(),
        };
        let mut state = self.multipart();
        state
            .uploads
            .insert(upload_id.as_str().to_string(), session.clone());
        Ok(session)
    }

    fn require_session(
        &self,
        state: &MultipartState,
        bucket: &str,
        key: &ObjectKey,
        upload_id: &str,
    ) -> Result<UploadSession, ServiceError> {
        let session =
            state.uploads.get(upload_id).cloned().ok_or_else(|| {
                ServiceError::InvalidArgument("unknown multipart upload id".into())
            })?;
        // The upload id is bound to its namespace and key; a mismatched use
        // must fail rather than touch another object.
        if session.bucket != bucket || session.key != *key {
            return Err(ServiceError::InvalidArgument(
                "upload id does not match this bucket/key".into(),
            ));
        }
        Ok(session)
    }

    /// Uploads one part, replacing any previous part with the same number only
    /// after the new part is fully received.
    ///
    /// When the upload has a checksum scheme (or the part declares an algorithm
    /// that fixes one), every part is hashed with it and the digest is stored
    /// with the part so completion can combine them without re-reading.
    // Each argument is the part's address, its identity or a distinct input the
    // caller already has; grouping them would only relabel them.
    #[allow(clippy::too_many_arguments)]
    pub async fn upload_part(
        &self,
        ctx: &DriverContext,
        bucket: &str,
        key: &ObjectKey,
        upload_id: &str,
        part_number: u16,
        checksums: PartChecksums,
        body: crate::driver::ObjectByteStream,
    ) -> Result<PartSummary, ServiceError> {
        // A part upload buffers up to `max_single_write_size` of request body, so
        // it takes the same operation slot as any other write: a saturated
        // server or driver instance answers `SlowDown` instead of queueing, and a
        // graceful drain waits for a part still in flight instead of exiting
        // under it. Admitting happens here, once for the whole operation;
        // `upload_part_copy` admits at its own entry instead of through this.
        let (_binding, _slot) = self.binding_slot(bucket)?;
        self.upload_part_in(ctx, bucket, key, upload_id, part_number, checksums, body)
            .await
    }

    /// The staging itself, for a caller that already admitted the operation.
    ///
    /// A copy reads its source and stages the part as one span, so it admits
    /// once at its own entry rather than through each step here.
    #[allow(clippy::too_many_arguments)]
    async fn upload_part_in(
        &self,
        ctx: &DriverContext,
        bucket: &str,
        key: &ObjectKey,
        upload_id: &str,
        part_number: u16,
        checksums: PartChecksums,
        mut body: crate::driver::ObjectByteStream,
    ) -> Result<PartSummary, ServiceError> {
        PartNumber::new(part_number).map_err(|e| ServiceError::InvalidArgument(e.to_string()))?;

        // Every algorithm this request names - the requested one, the header
        // value's or the trailer's - must agree with the upload's scheme.
        // Resolving before the body is consumed rejects a mismatch without
        // reading a single byte.
        let part_algorithm = checksums
            .algorithm
            .or_else(|| checksums.declared.as_ref().map(|(algorithm, _)| *algorithm))
            .or_else(|| {
                checksums
                    .trailer
                    .as_ref()
                    .map(|trailer| trailer.algorithm())
            });
        let (scheme, replaced) = {
            let state = self.multipart();
            let session = self.require_session(&state, bucket, key, upload_id)?;
            // Receiving a replacement for a staged part needs no room for both
            // copies at once: the staged one is released in the same step that
            // stages the new bytes, so the quota is charged for the difference.
            let replaced = state
                .staging
                .part(&session.upload_id, part_number)
                .map_or(0, |part| part.size);
            let scheme = match (session.checksum, part_algorithm) {
                (Some(scheme), Some(algorithm)) if scheme.algorithm != algorithm => {
                    return Err(ServiceError::InvalidRequest(format!(
                        "the part checksum algorithm `{}` does not match the upload's `{}`",
                        algorithm.as_str(),
                        scheme.algorithm.as_str()
                    )));
                }
                (Some(scheme), _) => Some(scheme),
                // A part that names an algorithm on an upload created without
                // one fixes the scheme for the remaining parts.
                (None, Some(algorithm)) => Some(MultipartChecksum::from_algorithm(algorithm)?),
                (None, None) => None,
            };
            (scheme, replaced)
        };

        let mut buffer: Vec<u8> = Vec::new();
        // The quota is charged per chunk, before a chunk is buffered, so a part
        // the staging quota cannot hold is refused while it is still streaming
        // rather than after the whole body has been read into memory.
        let mut reservation = StagingReservation::new(self);
        let mut md5 = StreamingDigest::new(ChecksumAlgorithm::Md5);
        let mut digest = scheme.map(|scheme| StreamingDigest::new(scheme.algorithm));
        let limit = self.inner.limits.max_single_write_size;
        loop {
            // A cancelled or timed-out request must stop consuming the body and
            // must not stage a partial part.
            if ctx.is_done() {
                return Err(ServiceError::InvalidRequest(
                    "upload cancelled before the part was fully received".into(),
                ));
            }
            match next_chunk(&mut body).await {
                None => break,
                Some(Ok(chunk)) => {
                    if buffer.len() as u64 + chunk.len() as u64 > limit {
                        return Err(ServiceError::InvalidRequest(
                            "part exceeds the configured maximum size".into(),
                        ));
                    }
                    self.reserve_staging(chunk.len() as u64, replaced)?;
                    reservation.grow(chunk.len() as u64);
                    md5.update(&chunk);
                    if let Some(hasher) = digest.as_mut() {
                        hasher.update(&chunk);
                    }
                    buffer.extend_from_slice(&chunk);
                }
                Some(Err(e)) => return Err(ServiceError::from_driver(&e, true)),
            }
        }

        // From here the received bytes are counted as staged, so the
        // reservation must stop counting them as in-flight.
        reservation.release();

        let data = Bytes::from(buffer);
        let size = data.len() as u64;
        let md5_bytes = md5.finalize();
        let mut md5_digest = [0u8; 16];
        md5_digest.copy_from_slice(&md5_bytes);

        let checksum = match (digest, scheme) {
            (Some(hasher), Some(scheme)) => {
                let value = hasher.finalize();
                // A value declared in a header, or in a trailer that only became
                // readable once the body was consumed, must match the bytes
                // received; a part is never staged when it does not. A trailer
                // the client declared but never sent is a mismatch too. When a
                // request carries both, the trailer is the declaration that was
                // verified: it is the one the body framing carries, and the
                // header value only selected the algorithm.
                let declared = match &checksums.trailer {
                    None => checksums.declared.clone(),
                    Some(trailer) => match trailer.value() {
                        Some(value) => Some((scheme.algorithm, value)),
                        None => {
                            return Err(ServiceError::BadDigest(
                                "the checksum you declared in a trailer was not sent".into(),
                            ));
                        }
                    },
                };
                if let Some((_, expected)) = &declared
                    && value.as_ref() != expected.as_slice()
                {
                    return Err(ServiceError::BadDigest(
                        "the checksum you specified did not match what we received".into(),
                    ));
                }
                Some(PartChecksum {
                    algorithm: scheme.algorithm,
                    value,
                })
            }
            _ => None,
        };

        let mut state = self.multipart();
        let session = self.require_session(&state, bucket, key, upload_id)?;

        // A scheme an earlier part adopted fixes the upload, so a part whose
        // digest was computed under a different one can no longer be staged.
        match (session.checksum, scheme) {
            (Some(existing), Some(ours)) if existing.algorithm != ours.algorithm => {
                return Err(ServiceError::InvalidRequest(format!(
                    "the part checksum algorithm `{}` does not match the upload's `{}`",
                    ours.algorithm.as_str(),
                    existing.algorithm.as_str()
                )));
            }
            (None, Some(ours)) => {
                if let Some(session) = state.uploads.get_mut(upload_id) {
                    session.checksum = Some(ours);
                }
            }
            _ => {}
        }
        // The part is about to be staged, so the upload's reclaim lease resets.
        if let Some(session) = state.uploads.get_mut(upload_id) {
            session.last_activity = SystemTime::now();
        }

        // The ledger enforces the part-count bound and the quota, releasing the
        // replaced part in the same step it stages the new one.
        state.staging.stage(
            &session.upload_id,
            StoredPart {
                number: part_number,
                size,
                data,
                md5: md5_digest,
                checksum: checksum.clone(),
            },
        )?;

        Ok(PartSummary {
            number: part_number,
            size,
            etag: EntityTag::new(hex(&md5_digest)).expect("hex"),
            checksum,
            last_modified: SystemTime::now(),
        })
    }

    /// Charges `bytes` of a part body that is being received to the staging
    /// quota, refusing the chunk when the quota cannot hold it.
    ///
    /// The charge is taken before the bytes are buffered, so the refusal
    /// reaches the client while the body is still streaming and the resident
    /// buffer never grows past what the quota allows. `credit` is the staged
    /// size of the part this attempt replaces: staging the replacement releases
    /// the old part in the same step, so only the difference is charged.
    fn reserve_staging(&self, bytes: u64, credit: u64) -> Result<(), ServiceError> {
        let mut state = self.multipart();
        state.staging.reserve(bytes, credit)
    }

    /// Returns bytes previously charged by [`ObjectService::reserve_staging`].
    fn release_staging(&self, bytes: u64) {
        let mut state = self.multipart();
        state.staging.release(bytes);
    }

    /// Lists parts of an upload.
    pub fn list_parts(
        &self,
        bucket: &str,
        key: &ObjectKey,
        upload_id: &str,
        marker: Option<u16>,
        limit: u32,
    ) -> Result<PartListPage, ServiceError> {
        let state = self.multipart();
        let session = self.require_session(&state, bucket, key, upload_id)?;
        let empty = BTreeMap::new();
        let parts = state.staging.parts_of(&session.upload_id).unwrap_or(&empty);

        let start = marker.unwrap_or(0);
        let collected: Vec<&StoredPart> = parts.values().filter(|p| p.number > start).collect();
        let limit = limit.max(1) as usize;
        let truncated = collected.len() > limit;
        let page = collected
            .into_iter()
            .take(limit)
            .map(|p| PartSummary {
                number: p.number,
                size: p.size,
                etag: p.etag(),
                checksum: p.checksum.clone(),
                last_modified: SystemTime::now(),
            })
            .collect::<Vec<_>>();
        let next_marker = truncated.then(|| page.last().map(|p| p.number).unwrap_or(0));
        Ok(PartListPage {
            parts: page,
            next_marker,
            is_truncated: truncated,
        })
    }

    /// Lists in-progress uploads for a bucket.
    pub fn list_uploads(
        &self,
        bucket: &str,
        prefix: Option<&str>,
        key_marker: Option<&str>,
        upload_id_marker: Option<&str>,
        limit: u32,
    ) -> Result<UploadListPage, ServiceError> {
        let binding = self.binding(bucket)?;
        let state = self.multipart();
        let mut all: Vec<UploadSession> = state
            .uploads
            .values()
            .filter(|s| s.bucket == bucket && s.generation == binding.generation)
            .filter(|s| prefix.is_none_or(|p| s.key.as_str().starts_with(p)))
            .cloned()
            .collect();
        all.sort_by(|a, b| {
            a.key
                .as_str()
                .cmp(b.key.as_str())
                .then(a.upload_id.as_str().cmp(b.upload_id.as_str()))
        });

        let filtered: Vec<UploadSession> = all
            .into_iter()
            .filter(|s| match (key_marker, upload_id_marker) {
                (Some(km), Some(um)) => (s.key.as_str(), s.upload_id.as_str()) > (km, um),
                _ => true,
            })
            .collect();

        let limit = limit.max(1) as usize;
        let truncated = filtered.len() > limit;
        let page: Vec<UploadSession> = filtered.into_iter().take(limit).collect();
        let (next_key_marker, next_upload_id_marker) = if truncated {
            page.last()
                .map(|s| (Some(s.key.clone()), Some(s.upload_id.clone())))
                .unwrap_or((None, None))
        } else {
            (None, None)
        };

        Ok(UploadListPage {
            uploads: page,
            next_key_marker,
            next_upload_id_marker,
            is_truncated: truncated,
        })
    }

    /// Completes an upload from an exact manifest, atomically publishing the
    /// assembled object.
    ///
    /// The object-level checksum is computed from the manifest's parts according
    /// to the upload's scheme: a `FULL_OBJECT` one digests the assembled bytes,
    /// a `COMPOSITE` one digests the ordered part digests (spec §6.3). A
    /// mismatch with a declared completion checksum aborts before anything is
    /// written to the target.
    // As in `upload_part`, the arguments are the upload's address plus the
    // manifest, the declared checksum and the write conditions.
    #[allow(clippy::too_many_arguments)]
    pub async fn complete_multipart<M>(
        &self,
        ctx: &DriverContext,
        bucket: &str,
        key: &ObjectKey,
        upload_id: &str,
        manifest: &[M],
        declared: Option<DeclaredCompletionChecksum>,
        preconditions: Preconditions,
    ) -> Result<ObjectMetadata, ServiceError>
    where
        M: Into<CompletionPart> + Clone,
    {
        let (binding, _slot) = self.binding_slot(bucket)?;
        self.require_writable(&binding)?;
        let manifest: Vec<CompletionPart> = manifest.iter().cloned().map(Into::into).collect();

        // Snapshot and validate the manifest while holding the lock, then
        // release it before the (awaiting) publish.
        let (session, ordered, etag, checksum, write_id) = {
            let mut state = self.multipart();
            let session = self.require_session(&state, bucket, key, upload_id)?;
            let empty = BTreeMap::new();
            let parts = state.staging.parts_of(&session.upload_id).unwrap_or(&empty);

            if manifest.is_empty() {
                return Err(ServiceError::MalformedXml(
                    "the completion manifest must contain at least one part".into(),
                ));
            }

            let mut ordered: Vec<StoredPart> = Vec::with_capacity(manifest.len());
            let mut seen = std::collections::HashSet::new();
            for entry in &manifest {
                let number = entry.number;
                if !seen.insert(number) {
                    return Err(ServiceError::MalformedXml(
                        "the completion manifest contains duplicate part numbers".into(),
                    ));
                }
                let part = parts.get(&number).ok_or_else(|| {
                    ServiceError::InvalidArgument(format!(
                        "part {number} was not uploaded for this upload"
                    ))
                })?;
                if let Some(expected) = &entry.etag
                    && part.etag() != *expected
                {
                    return Err(ServiceError::InvalidArgument(format!(
                        "part {number} ETag does not match the uploaded part"
                    )));
                }
                // A declared part checksum must match the received bytes; an
                // unverified part is never assembled into the object.
                if let Some((algorithm, expected)) = &entry.checksum
                    && part_checksum(part, *algorithm).as_ref() != expected.as_slice()
                {
                    return Err(ServiceError::BadDigest(format!(
                        "the checksum you specified for part {number} did not match what we received"
                    )));
                }
                ordered.push(part.clone());
            }

            // Enforce ordering and the minimum size of every non-final part.
            for window in ordered.windows(2) {
                if window[0].number >= window[1].number {
                    return Err(ServiceError::InvalidArgument(
                        "parts must be listed in ascending part-number order".into(),
                    ));
                }
            }
            for part in &ordered[..ordered.len() - 1] {
                if part.size < MIN_PART_SIZE {
                    return Err(ServiceError::InvalidArgument(
                        "each part except the last must be at least 5 MiB".into(),
                    ));
                }
            }

            let md5s: Vec<[u8; 16]> = ordered.iter().map(|p| p.md5).collect();
            let etag = crate::integrity::etag_for_multipart(&md5s);

            // Resolve the checksum scheme: the upload's own, or one this
            // completion request fixes for an upload created without one.
            let scheme = match (session.checksum, &declared) {
                (Some(scheme), Some(declared)) => {
                    // A declaration constrains the upload's scheme: the value's
                    // algorithm and any coverage must agree with it.
                    if let Some(algorithm) = declared.algorithm
                        && algorithm != scheme.algorithm
                    {
                        return Err(ServiceError::InvalidRequest(format!(
                            "the completion checksum algorithm `{}` does not match the upload's `{}`",
                            algorithm.as_str(),
                            scheme.algorithm.as_str()
                        )));
                    }
                    if let Some(declared_type) = declared.checksum_type
                        && declared_type != scheme.checksum_type
                    {
                        return Err(ServiceError::InvalidRequest(format!(
                            "the completion checksum type `{}` does not match the upload's `{}`",
                            declared_type.as_str(),
                            scheme.checksum_type.as_str()
                        )));
                    }
                    Some(scheme)
                }
                (Some(scheme), None) => Some(scheme),
                // An upload created without a scheme needs an algorithm to
                // compute one; a coverage alone cannot supply it.
                (None, Some(declared)) => {
                    let Some(algorithm) = declared.algorithm else {
                        return Err(ServiceError::InvalidRequest(
                            "a completion checksum type requires a checksum algorithm".into(),
                        ));
                    };
                    Some(MultipartChecksum::new(
                        algorithm,
                        declared
                            .checksum_type
                            .unwrap_or_else(|| algorithm.default_multipart_type()),
                    )?)
                }
                (None, None) => None,
            };

            let checksum = match scheme {
                None => None,
                Some(scheme) => {
                    let value = match scheme.checksum_type {
                        // The assembled object is the parts in manifest order, so
                        // hashing them in order is hashing the object.
                        ChecksumType::FullObject => {
                            let mut hasher = StreamingDigest::new(scheme.algorithm);
                            for part in &ordered {
                                hasher.update(&part.data);
                            }
                            hasher.finalize()
                        }
                        ChecksumType::Composite => {
                            let digests: Vec<Bytes> = ordered
                                .iter()
                                .map(|part| part_checksum(part, scheme.algorithm))
                                .collect();
                            composite_checksum(scheme.algorithm, &digests)
                        }
                    };
                    if let Some(declared) = &declared {
                        if let Some(expected) = &declared.value
                            && expected.as_slice() != value.as_ref()
                        {
                            return Err(ServiceError::BadDigest(
                                "the checksum you specified did not match what we received".into(),
                            ));
                        }
                        if let Some(count) = declared.part_count
                            && count != ordered.len() as u32
                        {
                            return Err(ServiceError::BadDigest(format!(
                                "the checksum you specified covers {count} parts but the manifest has {}",
                                ordered.len()
                            )));
                        }
                    }
                    let checksum = match scheme.checksum_type {
                        ChecksumType::FullObject => {
                            Checksum::new(scheme.algorithm, ChecksumType::FullObject, value)
                        }
                        ChecksumType::Composite => {
                            Checksum::composite(scheme.algorithm, value, ordered.len() as u32)
                        }
                    };
                    Some(checksum.map_err(|e| ServiceError::InvalidArgument(e.to_string()))?)
                }
            };

            // Claim the completion exactly once. A second concurrent (or
            // retried) completion of the same upload must not publish again and
            // overwrite whatever the first one produced.
            if state.claims.contains_key(upload_id) {
                return Err(ServiceError::InvalidArgument(
                    "this multipart upload is already being completed".into(),
                ));
            }
            let write_id = WriteId::generate();
            state.claims.insert(upload_id.to_string(), write_id.clone());
            (session, ordered, etag, checksum, write_id)
        };

        let total: u64 = ordered.iter().map(|p| p.size).sum();
        // The three steps that can fail before a single part byte reaches the
        // backend - opening the namespace, finding a writable handle, starting
        // the staged write - leave nothing that could publish later. A failure
        // there must therefore release the claim, or a retry with the same
        // upload id would be refused forever even though the first attempt
        // never had a publish path.
        let handle = match self.inner.handles.get(&binding, ctx).await {
            Ok(handle) => handle,
            Err(e) => {
                self.release_claim(upload_id, &write_id);
                return Err(e);
            }
        };
        let Some(write) = handle.write.as_ref() else {
            self.release_claim(upload_id, &write_id);
            return Err(ServiceError::NotImplemented(
                "bucket is not writable".into(),
            ));
        };

        let mut staged = match write
            .begin_write(
                ctx,
                BeginWrite {
                    key: key.clone(),
                    expected_length: Some(total),
                    preconditions,
                    write_id: write_id.clone(),
                    content: session.content.clone(),
                    user_metadata: session.user_metadata.clone(),
                    checksum_algorithm: None,
                },
            )
            .await
        {
            Ok(staged) => staged,
            Err(e) => {
                self.release_claim(upload_id, &write_id);
                return Err(ServiceError::from_driver(&e, true));
            }
        };

        for part in &ordered {
            if let Err(e) = staged.write_chunk(ctx, part.data.clone()).await {
                // A failed part write never published anything; the attempt is
                // final as soon as the abort is acknowledged.
                let aborted = staged.abort(ctx).await;
                let attempt_state = if aborted.is_ok() {
                    CommitState::NotCommittedFinal
                } else {
                    CommitState::Unknown
                };
                // A proven final non-commit allows a replacement attempt, so it
                // must not leave the claim behind: an unresolved outcome keeps
                // it, because a late publish is still possible.
                if attempt_state.permits_replacement_attempt() {
                    self.release_claim(upload_id, &write_id);
                }
                self.record_completion(&write_id, &session, attempt_state, upload_id);
                return Err(ServiceError::from_driver(&e, true));
            }
        }

        // The object-level checksum was computed from the parts above; the ETag
        // is the multipart `hash-partcount` form.
        let checksums: Vec<Checksum> = checksum.into_iter().collect();
        let meta = match staged
            .commit(
                ctx,
                CommitAttributes {
                    length: total,
                    etag: Some(etag),
                    checksums,
                },
            )
            .await
        {
            Ok(meta) => meta,
            Err(e) => {
                // `commit` consumed the staged write, so the outcome must come
                // from the error itself, and an unresolved outcome must be
                // reconciled with the driver's commit record.
                let mut attempt_state = e.commit_state_or_infer();
                if matches!(attempt_state, CommitState::Unknown | CommitState::InFlight)
                    && let Some(recovery) = &handle.recovery
                    && let Ok(observed) = recovery.query(ctx, &write_id).await
                {
                    attempt_state = observed;
                }

                if attempt_state == CommitState::Committed {
                    // The object was published; only the reply was lost. The
                    // original response is restored from the published
                    // revision and nothing is published again.
                    if let Ok(meta) = handle.read.stat(ctx, key).await {
                        self.finish_upload(&write_id, &session, upload_id);
                        return Ok(meta);
                    }
                }

                // Only `NotCommittedFinal` allows a retry with the same upload
                // id; every other state keeps the claim so the attempt can
                // never be replayed.
                if attempt_state.permits_replacement_attempt() {
                    self.release_claim(upload_id, &write_id);
                }
                self.record_completion(&write_id, &session, attempt_state, upload_id);
                return Err(ServiceError::from_driver(&e, true));
            }
        };

        // Reclaim staging only after a successful publish.
        self.finish_upload(&write_id, &session, upload_id);
        Ok(meta)
    }

    /// Records a committed upload and reclaims its staging.
    fn finish_upload(&self, write_id: &WriteId, session: &UploadSession, upload_id: &str) {
        let mut state = self.multipart();
        self.record_completion_in(
            &mut state,
            write_id,
            session,
            CommitState::Committed,
            upload_id,
        );
        state.claims.remove(upload_id);
        state.staging.take(&session.upload_id);
        state.uploads.remove(upload_id);
    }

    fn release_claim(&self, upload_id: &str, write_id: &WriteId) {
        let mut state = self.multipart();
        if state.claims.get(upload_id) == Some(write_id) {
            state.claims.remove(upload_id);
        }
    }

    fn record_completion(
        &self,
        write_id: &WriteId,
        session: &UploadSession,
        state: CommitState,
        upload_id: &str,
    ) {
        let mut guard = self.multipart();
        self.record_completion_in(&mut guard, write_id, session, state, upload_id);
    }

    fn record_completion_in(
        &self,
        guard: &mut MultipartState,
        write_id: &WriteId,
        session: &UploadSession,
        state: CommitState,
        upload_id: &str,
    ) {
        while guard.completions.len() >= COMPLETION_RECORD_LIMIT {
            // Drop the oldest record; a missing record is never proof of failure.
            let Some(oldest) = guard
                .completions
                .iter()
                .min_by_key(|(_, r)| r.recorded_at)
                .map(|(k, _)| k.clone())
            else {
                break;
            };
            guard.completions.remove(&oldest);
        }
        guard.completions.insert(
            write_id.to_string(),
            CompletionRecord {
                write_id: write_id.clone(),
                upload_id: upload_id.to_string(),
                bucket: session.bucket.clone(),
                key: session.key.clone(),
                state,
                recorded_at: SystemTime::now(),
            },
        );
    }

    /// All recorded completion attempts of one upload, oldest first.
    pub fn completion_records_for_upload(&self, upload_id: &str) -> Vec<CompletionRecord> {
        let state = self.multipart();
        let mut records: Vec<CompletionRecord> = state
            .completions
            .values()
            .filter(|r| r.upload_id == upload_id)
            .cloned()
            .collect();
        records.sort_by_key(|r| r.recorded_at);
        records
    }

    /// The latest recorded state of an upload's completion attempts.
    pub fn latest_completion_state(&self, upload_id: &str) -> Option<CommitState> {
        self.completion_records_for_upload(upload_id)
            .last()
            .map(|r| r.state)
    }

    /// Reclaims staged parts of uploads whose lease expired.
    ///
    /// Parts of an upload with an in-flight or unknown completion attempt are
    /// kept: reclaiming them could destroy a part that a late publish still
    /// needs, and a published object is never touched by a sweep.
    pub fn sweep_expired_uploads(&self, lease: Duration) -> SweepOutcome {
        let mut state = self.multipart();
        let now = SystemTime::now();
        let mut outcome = SweepOutcome::default();

        let expired: Vec<UploadId> = state
            .uploads
            .iter()
            .filter(|(_, s)| {
                now.duration_since(s.last_activity)
                    .is_ok_and(|age| age >= lease)
            })
            .map(|(id, _)| UploadId::from_raw(id.as_str()))
            .collect();

        for upload_id in expired {
            if state.claims.contains_key(upload_id.as_str()) {
                outcome.skipped_pending += 1;
                continue;
            }
            // An `Unknown` attempt on this upload must survive until recovery.
            let unresolved = state
                .completions
                .values()
                .any(|r| r.upload_id == upload_id.as_str() && r.state == CommitState::Unknown);
            if unresolved {
                outcome.skipped_pending += 1;
                continue;
            }
            outcome.reclaimed_bytes += state.staging.take(&upload_id);
            state.uploads.remove(upload_id.as_str());
            state.claims.remove(upload_id.as_str());
            outcome.reclaimed_uploads += 1;
        }

        outcome
    }

    /// Aborts an upload and reclaims its staged parts.
    pub fn abort_multipart(
        &self,
        bucket: &str,
        key: &ObjectKey,
        upload_id: &str,
    ) -> Result<(), ServiceError> {
        let mut state = self.multipart();
        let session = self.require_session(&state, bucket, key, upload_id)?;
        // A completion that already claimed this upload wins the race.
        if state.claims.contains_key(upload_id) {
            return Err(ServiceError::InvalidArgument(
                "this multipart upload is being completed".into(),
            ));
        }
        state.staging.take(&session.upload_id);
        state.uploads.remove(upload_id);
        Ok(())
    }

    /// Reads a source range and stores it as a part (`UploadPartCopy`).
    pub(crate) async fn upload_part_copy(
        &self,
        ctx: &DriverContext,
        copy: UploadPartCopyRequest,
    ) -> Result<(PartSummary, ObjectMetadata), ServiceError> {
        // Like `copy_object`, one slot admits the whole copy-part: the source
        // read and the staging below must not each claim a share of the
        // destination's scope, or a limit of one would stall the operation.
        let (_binding, _slot) = self.binding_slot(&copy.bucket)?;
        let source_binding = self.binding(&copy.source_bucket)?;
        let source = self
            .get_object_in(
                &source_binding,
                ctx,
                &copy.source_key,
                &super::ReadConditions::default(),
                copy.range,
            )
            .await?;
        let metadata = source.metadata.clone();
        let summary = self
            .upload_part_in(
                ctx,
                &copy.bucket,
                &copy.key,
                &copy.upload_id,
                copy.part_number,
                PartChecksums::default(),
                source.stream,
            )
            .await?;
        Ok((summary, metadata))
    }

    /// Whether multipart state holds no sessions (used by tests).
    pub fn multipart_is_idle(&self) -> bool {
        let state = self.multipart();
        state.uploads.is_empty() && state.staging.is_idle()
    }

    /// Reclaims expired multipart staging on a fixed interval until `shutdown`
    /// resolves.
    ///
    /// This is the sweeper's whole body, kept here rather than inline in the
    /// server so its cancel path and reclaim behaviour can be tested (spec
    /// §10: a background task has one owner, a cancel path and a shutdown wait;
    /// spec §8.2: a sweeper reclaims by lease, never by file age).
    pub async fn run_multipart_sweeper<F, S>(
        &self,
        lease: Duration,
        interval: Duration,
        shutdown: S,
        mut on_sweep: F,
    ) where
        F: FnMut(SweepOutcome),
        S: std::future::Future<Output = ()>,
    {
        let mut ticker = tokio::time::interval(interval);
        // A sweep that runs late must not then fire a burst of catch-up sweeps.
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        tokio::pin!(shutdown);
        loop {
            tokio::select! {
                _ = &mut shutdown => return,
                _ = ticker.tick() => on_sweep(self.sweep_expired_uploads(lease)),
            }
        }
    }
}

/// The checksum of one stored part for `algorithm`: the digest computed when
/// the part was uploaded, or a freshly computed one for a part that predates the
/// upload's scheme (or was replaced under a different one).
fn part_checksum(part: &StoredPart, algorithm: ChecksumAlgorithm) -> Bytes {
    match &part.checksum {
        Some(checksum) if checksum.algorithm == algorithm => checksum.value.clone(),
        _ => crate::integrity::compute_checksum(algorithm, &part.data),
    }
}
