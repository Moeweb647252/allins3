//! Multipart: the generic coordinator and the native route.
//!
//! Which one serves an upload is decided by the namespace's capability plan,
//! once, when the upload is created (spec §3.3): a namespace whose plan selects
//! `Multipart = Native` keeps its session, its parts and its completion in the
//! driver through [`MultipartNamespace`], and everything else is coordinated
//! here over the driver's staged write (spec §8), so any writable namespace can
//! serve multipart.
//!
//! The coordinator's state has a durable mirror (spec §8.1, `INV-10`): every
//! mutation queues an operation on [`store::MultipartStore`], and an operation
//! that must be acknowledged flushes them before its response is written, so a
//! part the client was told is staged really is. In a development deployment
//! the mirror is the no-op in-memory store; in production it is the file-backed
//! one under `state_dir`. The state machine, atomic completion, manifest
//! validation and quota accounting are the same either way.
//!
//! Both routes share this module's protocol semantics: the manifest is
//! validated, the scheme is fixed for the upload's lifetime, and completion
//! publishes exactly once. Only *where* the parts live and who assembles them
//! differs, so the two routes can be compared on the same behaviour.

use std::collections::{BTreeMap, HashMap};
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use bytes::Bytes;

use crate::domain::{
    BeginWrite, Checksum, ChecksumAlgorithm, ChecksumType, CommitAttributes, CommitState,
    ContentProperties, DriverContext, DriverErrorKind, EntityTag, ObjectKey, ObjectMetadata,
    PartNumber, Preconditions, RequestId, UserMetadata, WriteId,
};
use crate::driver::{
    CompletedPart, CreateUploadRequest, MultipartChecksumScheme, MultipartNamespace, PartInfo,
    UploadHandle, UploadId,
};
use crate::integrity::{StreamingDigest, composite_checksum, hex};

use super::handles::Namespace;
pub use super::staging::PartChecksum;
use super::staging::{StagingLedger, StoredPart};
use super::{ObjectService, ServiceError, next_chunk};

mod rules;
mod store;

pub(crate) use store::{DiskStore, MemoryStore, MultipartStore};

/// Builds the coordinator's durable store for a deployment.
///
/// A configured directory makes the coordinator's state durable; without one
/// the state is process-local, which is what a development deployment and every
/// test want (`ADR-0014`).
pub(crate) fn store_for(state_dir: Option<std::path::PathBuf>) -> Arc<dyn MultipartStore> {
    match state_dir {
        Some(dir) => Arc::new(DiskStore::new(dir)),
        None => Arc::new(MemoryStore),
    }
}

use rules::{
    finish_part_checksum, part_checksum, resolve_completion_scheme, upload_marker,
    validate_manifest,
};

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
#[derive(Debug, Clone, PartialEq)]
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
    /// The driver-side upload handle when the namespace's plan selected the
    /// native route, and `None` when the coordinator owns the parts.
    ///
    /// The decision is recorded on the session rather than re-derived per
    /// operation, so a later call cannot route an upload differently from the
    /// one that created it.
    pub(crate) native: Option<UploadHandle>,
}

/// Where one upload's parts and completion live.
enum UploadRoute {
    /// The driver owns the session, its parts and the completion.
    Native {
        /// The open namespace, for the read/recovery handles the native
        /// completion needs when it reconciles a lost reply.
        namespace: Arc<Namespace>,
        multipart: Arc<dyn MultipartNamespace>,
        upload: UploadHandle,
    },
    /// The coordinator owns them over the driver's staged write.
    Emulated,
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

/// One completion attempt that is still unresolved.
///
/// A pending attempt is one an operator must eventually settle: it either
/// published (the reply was lost) or it can be retried, but the record alone
/// does not say which. Retrying `CompleteMultipartUpload` re-reads the backend's
/// commit record and resolves it online (ADR-0016); this view is what an
/// operator inspects to find one.
#[derive(Debug, Clone)]
pub struct PendingCompletion {
    /// The upload the attempt belonged to.
    pub upload_id: String,
    /// The attempt that is still unresolved, when it holds a claim.
    pub write_id: Option<WriteId>,
    /// The bucket the upload targets.
    pub bucket: String,
    /// The key the upload targets.
    pub key: ObjectKey,
    /// The last recorded state of the attempt.
    pub state: CommitState,
    /// When that state was recorded, when a record exists.
    pub recorded_at: Option<SystemTime>,
}

/// What reconciling an unresolved completion attempt concluded.
///
/// Internal to the coordinator: the retry path turns it into a response or an
/// error, and never exposes the mechanism to a client.
enum PendingOutcome {
    /// The attempt had published; the object is returned as its completion.
    Published(Box<ObjectMetadata>),
    /// The attempt was proven not to have published; the claim was released
    /// and a replacement attempt may proceed.
    Retryable,
    /// The outcome is still unknown; the attempt is retained and never replayed.
    Unresolved,
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

/// What startup recovery restored and reconciled (spec §7.2).
///
/// Reported rather than logged from here: the service has no logger, and the
/// process that owns the log is the one that reads this after composition.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RecoveryReport {
    /// Sessions restored from the durable mirror.
    pub sessions_restored: usize,
    /// Staged parts restored from the durable mirror.
    pub parts_restored: usize,
    /// Interrupted attempts found to have published; their records were
    /// restored and nothing was published again.
    pub committed: usize,
    /// Interrupted attempts proven not to have published; they may be retried.
    pub retryable: usize,
    /// Interrupted attempts whose outcome is unknown; retained, never replayed.
    pub pending: usize,
    /// Uploads the initial sweep reclaimed.
    pub reclaimed_uploads: usize,
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
    /// Durable-mirror work queued by the mutations above, applied at the
    /// operation boundary ([`ObjectService::flush_pending`]).
    pending: Vec<PersistOp>,
}

impl MultipartState {
    /// Creates state with a staging quota in bytes.
    pub(crate) fn with_quota(quota: u64) -> Self {
        Self {
            uploads: HashMap::new(),
            claims: HashMap::new(),
            completions: HashMap::new(),
            staging: StagingLedger::new(quota),
            pending: Vec::new(),
        }
    }
}

/// One queued durable-mirror operation.
///
/// The coordinator queues these as it mutates its working set — cheap and
/// synchronous, under the same lock as the mutation — and applies them after
/// releasing it, where a file write can await without holding the lock. Each
/// carries its own payload, captured when the mutation happened, so a later
/// removal of the record cannot race the write that records it.
#[derive(Debug, Clone)]
enum PersistOp {
    /// Record (or replace) a session. Boxed because a session is much larger
    /// than the other operations, which are ids or small records.
    Session(Box<UploadSession>),
    /// Forget a session.
    ForgetSession(String),
    /// Record (or replace) a staged part and its bytes.
    Part(UploadId, StoredPart),
    /// Forget every staged part of one upload.
    ForgetStaging(UploadId),
    /// Record a completion attempt in flight.
    Claim(String, WriteId),
    /// Forget a completion attempt.
    ForgetClaim(String),
    /// Record (or replace) a completion record.
    Completion(CompletionRecord),
    /// Forget a completion record the coordinator evicted.
    ForgetCompletion(WriteId),
}

/// The multipart state of one service.
///
/// The owner of the state, so the coordinator is the only thing that knows how
/// it is locked: every other layer reaches multipart through [`ObjectService`]'s
/// operations and never takes the lock itself.
pub(crate) struct Multipart {
    state: std::sync::Mutex<MultipartState>,
    durable: Arc<dyn MultipartStore>,
}

impl Multipart {
    /// Empty state sharing `quota` bytes of staging between all uploads,
    /// mirrored through `durable`.
    pub(crate) fn new(quota: u64, durable: Arc<dyn MultipartStore>) -> Self {
        Self {
            state: std::sync::Mutex::new(MultipartState::with_quota(quota)),
            durable,
        }
    }

    /// Locks the state for one coordinator step.
    pub(crate) fn store(&self) -> std::sync::MutexGuard<'_, MultipartState> {
        crate::sync::lock(&self.state)
    }

    /// The durable mirror every queued operation is applied to.
    pub(crate) fn durable(&self) -> &Arc<dyn MultipartStore> {
        &self.durable
    }
}

/// Applies one queued operation to the durable mirror.
async fn apply_persist(store: &dyn MultipartStore, op: &PersistOp) -> Result<(), ServiceError> {
    match op {
        PersistOp::Session(session) => store.write_session(session.as_ref()).await,
        PersistOp::ForgetSession(upload_id) => store.delete_session(upload_id).await,
        PersistOp::Part(upload_id, part) => store.write_part(upload_id.as_str(), part).await,
        PersistOp::ForgetStaging(upload_id) => store.delete_upload(upload_id.as_str()).await,
        PersistOp::Claim(upload_id, write_id) => store.write_claim(upload_id, write_id).await,
        PersistOp::ForgetClaim(upload_id) => store.delete_claim(upload_id).await,
        PersistOp::Completion(record) => store.write_completion(record).await,
        PersistOp::ForgetCompletion(write_id) => store.delete_completion(write_id).await,
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

    /// Applies the durable-mirror work the last mutation queued.
    ///
    /// Called at an operation boundary — never while the state lock is held,
    /// because the mirror writes files — and before an acknowledgement is
    /// written, so a `200` implies the record reached the store (`INV-10`). A
    /// failed write re-queues itself and everything after it ahead of any work
    /// queued meanwhile, so one transient fault cannot silently drop durability
    /// for later operations.
    async fn flush_pending(&self) -> Result<(), ServiceError> {
        let pending: Vec<PersistOp> = {
            let mut state = self.multipart();
            std::mem::take(&mut state.pending)
        };
        let store = self.inner.multipart.durable().clone();
        for (index, op) in pending.iter().enumerate() {
            if let Err(error) = apply_persist(&*store, op).await {
                let mut state = self.multipart();
                let mut requeued: Vec<PersistOp> = pending[index..].to_vec();
                requeued.append(&mut state.pending);
                state.pending = requeued;
                return Err(error);
            }
        }
        Ok(())
    }

    /// Flushes queued durability work, preferring the caller's own outcome when
    /// both fail: a client must learn what the operation did, not that the
    /// mirror failed afterwards.
    async fn settle<T>(&self, outcome: Result<T, ServiceError>) -> Result<T, ServiceError> {
        match (outcome, self.flush_pending().await) {
            (Ok(value), Ok(())) => Ok(value),
            (Ok(_), Err(error)) => Err(error),
            (Err(error), _) => Err(error),
        }
    }

    /// Restores the coordinator's durable state and reconciles interrupted
    /// completion attempts (spec §7.2, `INV-09`, `INV-10`).
    ///
    /// Runs after `warmup` and before readiness, so a replica that has not yet
    /// accounted for what its predecessor left behind never serves traffic. It
    /// is read-only with respect to user objects: an attempt that published is
    /// recorded, never replayed ([`CommitRecovery`] and ADR-0003). A recovery
    /// failure fails startup rather than starting with unreconciled state.
    pub(crate) async fn recover(
        &self,
        ctx: &DriverContext,
        lease: Duration,
    ) -> Result<RecoveryReport, ServiceError> {
        let loaded = self.inner.multipart.durable().load().await?;
        let mut report = RecoveryReport::default();
        {
            let mut state = self.multipart();
            for session in loaded.sessions {
                state
                    .uploads
                    .insert(session.upload_id.as_str().to_string(), session);
                report.sessions_restored += 1;
            }
            for (upload_id, part) in loaded.parts {
                // Restoring a part the configured quota cannot hold is a
                // deployment mistake, not something to paper over: the ledger
                // refuses it and recovery fails closed.
                state.staging.stage(&upload_id, part)?;
                report.parts_restored += 1;
            }
            for record in loaded.completions {
                state
                    .completions
                    .insert(record.write_id.to_string(), record);
            }
            for (upload_id, write_id) in loaded.claims {
                state.claims.insert(upload_id, write_id);
            }
        }

        // Reconcile every interrupted attempt against the driver's commit
        // record. A claim whose session is gone is dropped: there is nothing
        // left for it to describe.
        for (upload_id, write_id) in self.claim_snapshot() {
            match self.reconcile_claim(ctx, &upload_id, &write_id).await {
                Some(CommitState::Committed) => report.committed += 1,
                Some(CommitState::NotCommittedFinal) => report.retryable += 1,
                // Never replayed, and never reclaimed: an unresolved attempt
                // may still publish, so the upload is retained for an operator
                // rather than silently overwritten.
                Some(_) => report.pending += 1,
                None => {}
            }
        }
        self.flush_pending().await?;

        // One initial sweep, so an upload whose lease already expired while the
        // process was down is reclaimed by the running sweeper's own rule.
        let swept = self.sweep_expired_uploads(ctx, lease).await;
        report.reclaimed_uploads = swept.reclaimed_uploads;
        // Recorded once, so the process that owns the log can report it after
        // composition without recovery having to return it up the call chain.
        let _ = self.inner.recovery.set(report);
        Ok(report)
    }

    /// Applies what a reconciliation outcome implies, returning it.
    ///
    /// `Committed` retires the upload as published and restores its record;
    /// `NotCommittedFinal` records the proven non-commit and releases the claim
    /// so a replacement attempt may run; anything unresolved is recorded as
    /// unknown and retained, never replayed.
    fn settle_attempt(
        &self,
        upload_id: &str,
        session: &UploadSession,
        write_id: &WriteId,
        observed: CommitState,
    ) -> CommitState {
        match observed {
            CommitState::Committed => {
                self.finish_upload(write_id, session, upload_id);
                CommitState::Committed
            }
            CommitState::NotCommittedFinal => {
                self.record_completion(
                    write_id,
                    session,
                    CommitState::NotCommittedFinal,
                    upload_id,
                );
                self.release_claim(upload_id, write_id);
                CommitState::NotCommittedFinal
            }
            CommitState::InFlight | CommitState::Unknown => {
                self.record_completion(write_id, session, CommitState::Unknown, upload_id);
                CommitState::Unknown
            }
        }
    }

    /// Snapshots the outstanding completion claims.
    ///
    /// Taken under the lock and returned owned, so the reconciliation loops can
    /// await the backend without holding it.
    fn claim_snapshot(&self) -> Vec<(String, WriteId)> {
        let state = self.multipart();
        state
            .claims
            .iter()
            .map(|(upload_id, write_id)| (upload_id.clone(), write_id.clone()))
            .collect()
    }

    /// Reconciles one persisted claim against the backend's commit record and
    /// applies the result, without counting it.
    ///
    /// This is the single path shared by startup recovery and the running
    /// sweeper, so the two can never disagree about what a given state implies
    /// (`ADR-0016`). Returns the applied state, or `None` when the claim's
    /// session is already gone — a claim with no session describes nothing, so
    /// it is dropped and there is no outcome to count.
    async fn reconcile_claim(
        &self,
        ctx: &DriverContext,
        upload_id: &str,
        write_id: &WriteId,
    ) -> Option<CommitState> {
        let session = {
            let state = self.multipart();
            state.uploads.get(upload_id).cloned()
        };
        let Some(session) = session else {
            let mut state = self.multipart();
            state.claims.remove(upload_id);
            state
                .pending
                .push(PersistOp::ForgetClaim(upload_id.to_string()));
            return None;
        };
        let observed = self.reconcile_attempt(ctx, &session, write_id).await;
        Some(self.settle_attempt(upload_id, &session, write_id, observed))
    }

    /// Reconciles every unresolved completion attempt against the backend's
    /// commit record, applying whatever is now provable (`ADR-0016`).
    ///
    /// This is the running counterpart of startup recovery: an attempt whose
    /// record was not yet definitive when it failed — or when the process last
    /// started — is retired as published or released as retryable as soon as
    /// the backend can answer, without an operator editing `state_dir` by hand.
    /// It is read-only toward user objects: it never publishes, and a record it
    /// cannot read keeps the attempt pending.
    async fn reconcile_pending_completions(&self, ctx: &DriverContext) {
        for (upload_id, write_id) in self.claim_snapshot() {
            self.reconcile_claim(ctx, &upload_id, &write_id).await;
        }
        // A released claim or a retired upload must be durable before the
        // sweeper treats the upload as resolvable.
        self.flush_pending().await.ok();
    }

    /// Resolves one upload's outstanding completion claim against the backend's
    /// commit record, applying a definitive outcome online.
    ///
    /// `expected_etag` is the object a retry would publish, when the caller
    /// knows it. A backend record that is not definitive is then checked against
    /// the object itself: a target that already carries exactly that ETag is
    /// proof the earlier attempt published (the manual's conservative read of an
    /// `InFlight` or missing record). Nothing is ever published here, and an
    /// attempt that cannot be proven either way stays pending.
    async fn resolve_pending_claim(
        &self,
        ctx: &DriverContext,
        session: &UploadSession,
        upload_id: &str,
        write_id: &WriteId,
        expected_etag: Option<&EntityTag>,
    ) -> PendingOutcome {
        match self.reconcile_attempt(ctx, session, write_id).await {
            CommitState::Committed => match self.published_object(ctx, session).await {
                Some(meta) => {
                    self.finish_upload(write_id, session, upload_id);
                    PendingOutcome::Published(Box::new(meta))
                }
                // The record says published but the object cannot be read;
                // retaining the attempt is the only safe reading.
                None => PendingOutcome::Unresolved,
            },
            CommitState::NotCommittedFinal => {
                self.settle_attempt(upload_id, session, write_id, CommitState::NotCommittedFinal);
                PendingOutcome::Retryable
            }
            CommitState::InFlight | CommitState::Unknown => {
                if let Some(expected) = expected_etag
                    && let Some(meta) = self.published_object(ctx, session).await
                    && meta.etag.as_ref() == Some(expected)
                {
                    self.finish_upload(write_id, session, upload_id);
                    return PendingOutcome::Published(Box::new(meta));
                }
                PendingOutcome::Unresolved
            }
        }
    }

    /// Reads the target object of a session, if it is there.
    async fn published_object(
        &self,
        ctx: &DriverContext,
        session: &UploadSession,
    ) -> Option<ObjectMetadata> {
        let binding = self.binding(&session.bucket).ok()?;
        let namespace = self.inner.handles.get(&binding, ctx).await.ok()?;
        namespace.read.stat(ctx, &session.key).await.ok()
    }

    /// Asks the driver what became of one interrupted attempt.
    ///
    /// A namespace that declares no `recovery` cannot answer, and any error is
    /// treated as an unknown outcome rather than as a proven non-commit: the
    /// safe reading of "we could not find out" is "it may have published".
    async fn reconcile_attempt(
        &self,
        ctx: &DriverContext,
        session: &UploadSession,
        write_id: &WriteId,
    ) -> CommitState {
        let Ok(binding) = self.binding(&session.bucket) else {
            return CommitState::Unknown;
        };
        let Ok(namespace) = self.inner.handles.get(&binding, ctx).await else {
            return CommitState::Unknown;
        };
        match &namespace.recovery {
            Some(recovery) => recovery
                .query(ctx, write_id)
                .await
                .unwrap_or(CommitState::Unknown),
            None => CommitState::Unknown,
        }
    }

    /// Resolves where an existing upload's parts and completion live.
    ///
    /// This is the single place the "a native upload's namespace no longer
    /// offers native multipart" condition is turned into an error: every
    /// operation on an existing upload routes through here, so none of them can
    /// re-derive the rule differently.
    async fn upload_route(
        &self,
        binding: &crate::catalog::BucketBinding,
        ctx: &DriverContext,
        session: &UploadSession,
    ) -> Result<UploadRoute, ServiceError> {
        let Some(upload) = session.native.clone() else {
            return Ok(UploadRoute::Emulated);
        };
        let namespace = self.inner.handles.get(binding, ctx).await?;
        let multipart = namespace.native_multipart().cloned().ok_or_else(|| {
            ServiceError::Internal(format!(
                "upload `{}` was created natively but bucket `{}` no longer offers native multipart",
                upload.upload_id.as_str(),
                binding.bucket
            ))
        })?;
        Ok(UploadRoute::Native {
            namespace,
            multipart,
            upload,
        })
    }

    /// Creates a multipart upload without touching any existing object.
    ///
    /// The namespace's capability plan decides where the upload lives: a native
    /// namespace starts it in the driver and the driver's upload id is what the
    /// client sees, otherwise the coordinator starts one here.
    // Each argument is the upload's address, its owner or a distinct input the
    // caller already has; grouping them would only relabel them.
    #[allow(clippy::too_many_arguments)]
    pub async fn create_multipart(
        &self,
        ctx: &DriverContext,
        bucket: &str,
        key: &ObjectKey,
        principal: Option<&str>,
        content: ContentProperties,
        user_metadata: UserMetadata,
        checksum: Option<MultipartChecksum>,
    ) -> Result<UploadSession, ServiceError> {
        let binding = self.binding(bucket)?;
        self.require_writable(&binding)?;
        let namespace = self.inner.handles.get(&binding, ctx).await?;
        let native = match namespace.native_multipart() {
            Some(multipart) => Some(
                multipart
                    .create_upload(
                        ctx,
                        CreateUploadRequest {
                            key: key.clone(),
                            content: content.clone(),
                            user_metadata: user_metadata.clone(),
                            checksum: checksum.map(|scheme| MultipartChecksumScheme {
                                algorithm: scheme.algorithm,
                                checksum_type: scheme.checksum_type,
                            }),
                        },
                    )
                    .await
                    .map_err(|e| ServiceError::from_driver(&e, true))?,
            ),
            None => None,
        };
        let upload_id = native
            .as_ref()
            .map(|upload| upload.upload_id.clone())
            .unwrap_or_else(UploadId::generate);
        let session = UploadSession {
            upload_id,
            bucket: bucket.to_string(),
            key: key.clone(),
            principal: principal.map(str::to_string),
            generation: binding.generation,
            content,
            user_metadata,
            checksum,
            initiated: SystemTime::now(),
            last_activity: SystemTime::now(),
            native,
        };
        {
            let mut state = self.multipart();
            state
                .uploads
                .insert(session.upload_id.as_str().to_string(), session.clone());
            state
                .pending
                .push(PersistOp::Session(Box::new(session.clone())));
        }
        // The upload id is only meaningful once it is durable: a client that
        // was handed one must be able to use it after a restart.
        self.flush_pending().await?;
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
        let (binding, _slot) = self.binding_slot(bucket)?;
        self.route_upload_part(
            ctx,
            &binding,
            bucket,
            key,
            upload_id,
            part_number,
            checksums,
            body,
        )
        .await
    }

    /// Stages one part on the route its upload was created with.
    // Each argument is the part's address, its identity or a distinct input the
    // caller already has; grouping them would only relabel them.
    #[allow(clippy::too_many_arguments)]
    async fn route_upload_part(
        &self,
        ctx: &DriverContext,
        binding: &crate::catalog::BucketBinding,
        bucket: &str,
        key: &ObjectKey,
        upload_id: &str,
        part_number: u16,
        checksums: PartChecksums,
        body: crate::driver::ObjectByteStream,
    ) -> Result<PartSummary, ServiceError> {
        let session = {
            let state = self.multipart();
            self.require_session(&state, bucket, key, upload_id)?
        };
        match self.upload_route(binding, ctx, &session).await? {
            UploadRoute::Native {
                multipart, upload, ..
            } => {
                self.upload_part_native(
                    ctx,
                    &session,
                    multipart.as_ref(),
                    &upload,
                    part_number,
                    checksums,
                    body,
                )
                .await
            }
            UploadRoute::Emulated => {
                self.upload_part_in(ctx, bucket, key, upload_id, part_number, checksums, body)
                    .await
            }
        }
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

        let checksum = finish_part_checksum(digest, scheme, &checksums)?;

        // The lock is taken and released inside this block, so no guard can
        // live across the flush below.
        let summary = {
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
                    if let Some(record) = state.uploads.get_mut(upload_id) {
                        record.checksum = Some(ours);
                    }
                }
                _ => {}
            }
            // The part is about to be staged, so the upload's reclaim lease resets.
            if let Some(record) = state.uploads.get_mut(upload_id) {
                record.last_activity = SystemTime::now();
            }

            // The ledger enforces the part-count bound and the quota, releasing
            // the replaced part in the same step it stages the new one.
            let part = StoredPart {
                number: part_number,
                size,
                data,
                md5: md5_digest,
                checksum: checksum.clone(),
            };
            state.staging.stage(&session.upload_id, part.clone())?;
            // The checksum an earlier part adopted and the lease reset above are
            // on the current record, so mirror that rather than the pre-update
            // clone; the session is recorded before the part that belongs to it.
            if let Some(current) = state.uploads.get(upload_id).cloned() {
                state.pending.push(PersistOp::Session(Box::new(current)));
            }
            state
                .pending
                .push(PersistOp::Part(session.upload_id.clone(), part));

            PartSummary {
                number: part_number,
                size,
                etag: EntityTag::new(hex(&md5_digest)).expect("hex"),
                checksum,
                last_modified: SystemTime::now(),
            }
        };

        // The part is only acknowledged once it is durable (`INV-10`).
        self.flush_pending().await?;
        Ok(summary)
    }

    /// Uploads one part on the native route: the driver owns the bytes.
    ///
    /// The service still digests the received bytes and refuses a part whose
    /// declared checksum does not match *before* committing it, so a part the
    /// client cannot vouch for never replaces a good one. The object-level
    /// checksum is the driver's to compute at completion, where it assembles
    /// the parts.
    // Each argument is the part's address, its identity or a distinct input the
    // caller already has; grouping them would only relabel them.
    #[allow(clippy::too_many_arguments)]
    async fn upload_part_native(
        &self,
        ctx: &DriverContext,
        session: &UploadSession,
        multipart: &dyn MultipartNamespace,
        upload: &UploadHandle,
        part_number: u16,
        checksums: PartChecksums,
        mut body: crate::driver::ObjectByteStream,
    ) -> Result<PartSummary, ServiceError> {
        let number = PartNumber::new(part_number)
            .map_err(|e| ServiceError::InvalidArgument(e.to_string()))?;

        // Every algorithm the request names must agree with the upload's scheme;
        // resolving before the body is consumed rejects a mismatch without
        // reading a byte.
        let part_algorithm = checksums
            .algorithm
            .or_else(|| checksums.declared.as_ref().map(|(algorithm, _)| *algorithm))
            .or_else(|| {
                checksums
                    .trailer
                    .as_ref()
                    .map(|trailer| trailer.algorithm())
            });
        let scheme = match (session.checksum, part_algorithm) {
            (Some(scheme), Some(algorithm)) if scheme.algorithm != algorithm => {
                return Err(ServiceError::InvalidRequest(format!(
                    "the part checksum algorithm `{}` does not match the upload's `{}`",
                    algorithm.as_str(),
                    scheme.algorithm.as_str()
                )));
            }
            (Some(scheme), _) => Some(scheme),
            (None, Some(algorithm)) => Some(MultipartChecksum::from_algorithm(algorithm)?),
            (None, None) => None,
        };

        let limit = self.inner.limits.max_single_write_size;
        // The part's length is not known before its body is received, so the
        // driver is told to discover it; the maximum is enforced below, chunk by
        // chunk, exactly as the coordinator enforces it while buffering.
        let mut staged = multipart
            .begin_part(ctx, upload, number, None)
            .await
            .map_err(|e| ServiceError::from_driver(&e, true))?;

        let mut md5 = StreamingDigest::new(ChecksumAlgorithm::Md5);
        let mut digest = scheme.map(|scheme| StreamingDigest::new(scheme.algorithm));
        let mut length = 0u64;
        loop {
            if ctx.is_done() {
                let _ = staged.abort(ctx).await;
                return Err(ServiceError::InvalidRequest(
                    "upload cancelled before the part was fully received".into(),
                ));
            }
            match next_chunk(&mut body).await {
                None => break,
                Some(Ok(chunk)) => {
                    if length + chunk.len() as u64 > limit {
                        let _ = staged.abort(ctx).await;
                        return Err(ServiceError::InvalidRequest(
                            "part exceeds the configured maximum size".into(),
                        ));
                    }
                    md5.update(&chunk);
                    if let Some(hasher) = digest.as_mut() {
                        hasher.update(&chunk);
                    }
                    length += chunk.len() as u64;
                    // The part is streamed straight into the driver's staged
                    // part: the coordinator's buffer and staging quota exist to
                    // hold parts the service owns, which these are not.
                    if let Err(e) = staged.write_chunk(ctx, chunk).await {
                        let _ = staged.abort(ctx).await;
                        return Err(ServiceError::from_driver(&e, true));
                    }
                }
                Some(Err(e)) => {
                    let _ = staged.abort(ctx).await;
                    return Err(ServiceError::from_driver(&e, true));
                }
            }
        }

        let md5_bytes = md5.finalize();
        let mut md5_digest = [0u8; 16];
        md5_digest.copy_from_slice(&md5_bytes);
        let checksum = match finish_part_checksum(digest, scheme, &checksums) {
            Ok(checksum) => checksum,
            // A part whose declared digest does not match the bytes is
            // discarded rather than committed.
            Err(e) => {
                let _ = staged.abort(ctx).await;
                return Err(e);
            }
        };

        // A part that named an algorithm may have fixed the scheme while this
        // body streamed; a concurrent part under a different one can no longer
        // join this upload, and must not be committed.
        if let Some(ours) = scheme {
            // Read and release the lock before touching the driver again: a
            // guard must not live across an await.
            let existing = {
                let state = self.multipart();
                state
                    .uploads
                    .get(upload.upload_id.as_str())
                    .and_then(|record| record.checksum)
                    .or(session.checksum)
            };
            if let Some(existing) = existing
                && existing.algorithm != ours.algorithm
            {
                let _ = staged.abort(ctx).await;
                return Err(ServiceError::InvalidRequest(format!(
                    "the part checksum algorithm `{}` does not match the upload's `{}`",
                    ours.algorithm.as_str(),
                    existing.algorithm.as_str()
                )));
            }
        }

        let etag = EntityTag::new(hex(&md5_digest)).expect("hex");
        let meta = match staged
            .commit(
                ctx,
                CommitAttributes {
                    length,
                    etag: Some(etag.clone()),
                    checksums: Vec::new(),
                },
            )
            .await
        {
            Ok(meta) => meta,
            Err(e) => return Err(ServiceError::from_driver(&e, true)),
        };

        // The upload's reclaim lease resets on the activity the driver has now
        // recorded, and an upload created without a scheme adopts the one this
        // part named.
        {
            let mut state = self.multipart();
            if let Some(record) = state.uploads.get_mut(upload.upload_id.as_str()) {
                record.last_activity = SystemTime::now();
                if let (None, Some(ours)) = (record.checksum, scheme) {
                    record.checksum = Some(ours);
                }
                let current = record.clone();
                state.pending.push(PersistOp::Session(Box::new(current)));
            }
        }
        // The driver owns the part's bytes; the session's lease and adopted
        // scheme are the service's, and they are what a restart must see.
        self.flush_pending().await?;

        Ok(PartSummary {
            number: part_number,
            size: length,
            etag: meta.etag.unwrap_or(etag),
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

    /// Lists parts of an upload on the route it was created with.
    pub async fn list_parts(
        &self,
        ctx: &DriverContext,
        bucket: &str,
        key: &ObjectKey,
        upload_id: &str,
        marker: Option<u16>,
        limit: u32,
    ) -> Result<PartListPage, ServiceError> {
        let binding = self.binding(bucket)?;
        let session = {
            let state = self.multipart();
            self.require_session(&state, bucket, key, upload_id)?
        };
        match self.upload_route(&binding, ctx, &session).await? {
            UploadRoute::Native {
                multipart, upload, ..
            } => {
                // `part-number-marker=0` is the client asking for the first
                // page, which is how the coordinator reads it. Part numbers
                // start at 1, so 0 is not a part to parse and reject.
                let marker = marker
                    .filter(|marker| *marker > 0)
                    .map(PartNumber::new)
                    .transpose()
                    .map_err(|e| ServiceError::InvalidArgument(e.to_string()))?;
                let limit = NonZeroU32::new(limit.max(1)).expect("part page limit is non-zero");
                let page = multipart
                    .list_parts(ctx, &upload, marker, limit)
                    .await
                    .map_err(|e| ServiceError::from_driver(&e, true))?;
                let parts = page
                    .parts
                    .into_iter()
                    .map(|part| {
                        // A part the driver lists is an uploaded part, so a
                        // missing ETag is a contract violation, not an empty
                        // field to paper over.
                        let Some(etag) = part.etag else {
                            return Err(ServiceError::Internal(format!(
                                "the driver listed part {} without an ETag",
                                part.part_number.get()
                            )));
                        };
                        Ok(PartSummary {
                            number: part.part_number.get(),
                            size: part.size,
                            etag,
                            checksum: part.checksum.map(|checksum| PartChecksum {
                                algorithm: checksum.algorithm(),
                                value: checksum.value().clone(),
                            }),
                            last_modified: part.last_modified,
                        })
                    })
                    .collect::<Result<Vec<_>, ServiceError>>()?;
                Ok(PartListPage {
                    parts,
                    next_marker: page.next_marker.map(PartNumber::get),
                    is_truncated: page.is_truncated,
                })
            }
            UploadRoute::Emulated => {
                self.list_parts_emulated(bucket, key, upload_id, marker, limit)
            }
        }
    }

    /// Lists parts the coordinator holds.
    fn list_parts_emulated(
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

    /// Lists in-progress uploads for a bucket, on the route its namespace uses.
    ///
    /// A native namespace's uploads are the driver's to enumerate (it owns the
    /// sessions), so the page comes from there. Everything the page reports must
    /// be drivable, so a driver-reported upload the service has no record of is
    /// *adopted* into the session table rather than returned as a session every
    /// entry point would refuse (its checksum, content and metadata stay empty,
    /// because the driver holds them for an upload the service did not create).
    pub async fn list_uploads(
        &self,
        ctx: &DriverContext,
        bucket: &str,
        prefix: Option<&str>,
        key_marker: Option<&str>,
        upload_id_marker: Option<&str>,
        limit: u32,
    ) -> Result<UploadListPage, ServiceError> {
        let binding = self.binding(bucket)?;
        // The marker is a pair and its meaning is the service's to fix, so both
        // routes read it the same way before either page is produced.
        let marker = upload_marker(key_marker, upload_id_marker)?;
        let namespace = self.inner.handles.get(&binding, ctx).await?;
        let Some(multipart) = namespace.native_multipart().cloned() else {
            return self.list_uploads_emulated(bucket, prefix, marker, limit);
        };

        let (key_marker, upload_id_marker) = match marker {
            Some((key, upload_id)) => (Some(key), Some(upload_id)),
            None => (None, None),
        };
        let limit = NonZeroU32::new(limit.max(1)).expect("upload page limit is non-zero");
        let page = multipart
            .list_uploads(ctx, &binding.namespace, key_marker, upload_id_marker, limit)
            .await
            .map_err(|e| ServiceError::from_driver(&e, true))?;

        let uploads: Vec<UploadSession> = {
            let mut state = self.multipart();
            page.uploads
                .into_iter()
                // The driver's page implements the marker semantics; the prefix is
                // the service's to apply, because the driver contract does not carry
                // one (`MultipartNamespace::list_uploads`).
                .filter(|upload| {
                    prefix.is_none_or(|prefix| upload.key.as_str().starts_with(prefix))
                })
                .map(
                    |upload| match state.uploads.get(upload.upload_id.as_str()).cloned() {
                        Some(record) => record,
                        None => {
                            // Adopt the upload so the page it appeared on is a page
                            // of uploads the client can drive. `last_activity` starts
                            // now so the sweep does not immediately retire a record
                            // it just learned about; the driver decides when the
                            // upload is gone.
                            let session = UploadSession {
                                upload_id: upload.upload_id.clone(),
                                bucket: bucket.to_string(),
                                key: upload.key.clone(),
                                principal: None,
                                generation: binding.generation,
                                content: ContentProperties::default(),
                                user_metadata: UserMetadata::default(),
                                checksum: None,
                                initiated: upload.initiated,
                                last_activity: SystemTime::now(),
                                native: Some(UploadHandle {
                                    upload_id: upload.upload_id.clone(),
                                    key: upload.key.clone(),
                                }),
                            };
                            state
                                .uploads
                                .insert(upload.upload_id.as_str().to_string(), session.clone());
                            // An adopted upload is one the client can drive, so
                            // the record is durable like any other session.
                            state
                                .pending
                                .push(PersistOp::Session(Box::new(session.clone())));
                            session
                        }
                    },
                )
                .collect()
        };
        self.flush_pending().await?;

        Ok(UploadListPage {
            uploads,
            next_key_marker: page.next_key_marker,
            next_upload_id_marker: page.next_upload_id_marker,
            is_truncated: page.is_truncated,
        })
    }

    /// Lists uploads the coordinator holds.
    fn list_uploads_emulated(
        &self,
        bucket: &str,
        prefix: Option<&str>,
        marker: Option<(ObjectKey, UploadId)>,
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
            .filter(|s| match &marker {
                Some((key, upload_id)) => {
                    (s.key.as_str(), s.upload_id.as_str()) > (key.as_str(), upload_id.as_str())
                }
                None => true,
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

        // The route was decided when the upload was created: a native upload
        // keeps its parts and its assembly in the driver.
        let session = {
            let state = self.multipart();
            self.require_session(&state, bucket, key, upload_id)?
        };
        if let UploadRoute::Native {
            namespace,
            multipart,
            upload,
        } = self.upload_route(&binding, ctx, &session).await?
        {
            return self
                .complete_multipart_native(
                    ctx,
                    key,
                    upload_id,
                    &session,
                    &namespace,
                    &multipart,
                    &upload,
                    &manifest,
                    declared,
                    preconditions,
                )
                .await;
        }

        // Snapshot and validate the manifest while holding the lock, then
        // release it before the (awaiting) publish.
        let (session, ordered, etag, checksum) = {
            let state = self.multipart();
            let session = self.require_session(&state, bucket, key, upload_id)?;
            let empty = BTreeMap::new();
            let parts = state.staging.parts_of(&session.upload_id).unwrap_or(&empty);

            // Resolve the checksum scheme before the manifest is checked: it
            // names the only algorithm a part digest is computed under, which
            // the check below needs. The upload's own scheme, or one this
            // completion request fixes for an upload created without one.
            let scheme = resolve_completion_scheme(session.checksum, &declared)?;

            // The manifest and the parts it names are checked by the code both
            // routes run, so neither route accepts a manifest the other
            // refuses. Staging keeps the parts afterwards, so the validated
            // ones are cloned out here rather than borrowed past the lock.
            let ordered: Vec<StoredPart> = validate_manifest(&manifest, scheme, parts)?
                .into_iter()
                .cloned()
                .collect();

            let md5s: Vec<[u8; 16]> = ordered.iter().map(|p| p.md5).collect();
            let etag = crate::integrity::etag_for_multipart(&md5s);

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

            (session, ordered, etag, checksum)
        };

        // A claim means an earlier attempt may still be resolving. Before this
        // retry is refused, reconcile that attempt against the backend's commit
        // record: a record that now proves publication recovers the lost reply,
        // and one that proves the attempt never published releases the claim so
        // this attempt may proceed. This is the online counterpart of startup
        // recovery (`ADR-0016`), and it needs no operator-side file surgery.
        if let Some(pending) = {
            let state = self.multipart();
            state.claims.get(upload_id).cloned()
        } {
            match self
                .resolve_pending_claim(ctx, &session, upload_id, &pending, Some(&etag))
                .await
            {
                PendingOutcome::Published(meta) => return self.settle(Ok(*meta)).await,
                PendingOutcome::Retryable => {}
                PendingOutcome::Unresolved => {
                    return Err(ServiceError::InvalidArgument(
                        "this multipart upload is already being completed".into(),
                    ));
                }
            }
        }

        // Claim the completion exactly once. A second concurrent attempt must
        // not publish again and overwrite whatever the first one produced.
        let write_id = {
            let mut state = self.multipart();
            if state.claims.contains_key(upload_id) {
                return Err(ServiceError::InvalidArgument(
                    "this multipart upload is already being completed".into(),
                ));
            }
            let write_id = WriteId::generate();
            state.claims.insert(upload_id.to_string(), write_id.clone());
            state
                .pending
                .push(PersistOp::Claim(upload_id.to_string(), write_id.clone()));
            write_id
        };
        // The claim is durable *before* the attempt starts: if the process dies
        // mid-publish, the write id it may have published under is on disk, and
        // startup can reconcile it (`ADR-0013`).
        self.flush_pending().await?;

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
                return self.settle(Err(e)).await;
            }
        };
        let Some(write) = handle.writer() else {
            self.release_claim(upload_id, &write_id);
            return self
                .settle(Err(ServiceError::NotImplemented(
                    "bucket is not writable".into(),
                )))
                .await;
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
                return self.settle(Err(ServiceError::from_driver(&e, true))).await;
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
                return self.settle(Err(ServiceError::from_driver(&e, true))).await;
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
                        return self.settle(Ok(meta)).await;
                    }
                }

                // Only `NotCommittedFinal` allows a retry with the same upload
                // id; every other state keeps the claim so the attempt can
                // never be replayed.
                if attempt_state.permits_replacement_attempt() {
                    self.release_claim(upload_id, &write_id);
                }
                self.record_completion(&write_id, &session, attempt_state, upload_id);
                return self.settle(Err(ServiceError::from_driver(&e, true))).await;
            }
        };

        // Reclaim staging only after a successful publish.
        self.finish_upload(&write_id, &session, upload_id);
        self.settle(Ok(meta)).await
    }

    /// Completes an upload whose parts the driver owns.
    ///
    /// The service still owns the protocol semantics: it validates the manifest
    /// against the parts the driver reports (existence, ETag, per-part digest,
    /// ascending order and the minimum size of every non-final part), resolves
    /// the checksum scheme, arbitrates the single completion, and hands the
    /// declared object checksum to the driver. The driver assembles, verifies and
    /// publishes atomically, so every rejection above happens before anything is
    /// published.
    // The arguments are the upload's address, the session, the driver handle
    // and the manifest with its declared checksum and conditions.
    #[allow(clippy::too_many_arguments)]
    async fn complete_multipart_native(
        &self,
        ctx: &DriverContext,
        key: &ObjectKey,
        upload_id: &str,
        session: &UploadSession,
        namespace: &Arc<Namespace>,
        multipart: &Arc<dyn MultipartNamespace>,
        upload: &UploadHandle,
        manifest: &[CompletionPart],
        declared: Option<DeclaredCompletionChecksum>,
        preconditions: Preconditions,
    ) -> Result<ObjectMetadata, ServiceError> {
        let scheme = resolve_completion_scheme(session.checksum, &declared)?;

        // What can be assembled is what the driver reports, so the manifest is
        // checked against that, not against a service-side part table.
        let mut parts: BTreeMap<u16, PartInfo> = BTreeMap::new();
        let mut marker = None;
        loop {
            let limit = NonZeroU32::new(1000).expect("part page limit is non-zero");
            let page = multipart
                .list_parts(ctx, upload, marker, limit)
                .await
                .map_err(|e| ServiceError::from_driver(&e, true))?;
            let truncated = page.is_truncated;
            marker = page.next_marker;
            for part in page.parts {
                parts.insert(part.part_number.get(), part);
            }
            if !truncated || marker.is_none() {
                break;
            }
        }

        // The manifest checks the coordinator runs, over the parts the driver
        // reports: the two routes cannot disagree about what a manifest means.
        let ordered = validate_manifest(manifest, scheme, &parts)?;

        // A declared composite checksum names the part count it covers; the
        // manifest must be able to satisfy it, or the completion cannot be
        // accepted at all.
        if let Some(count) = declared.as_ref().and_then(|declared| declared.part_count)
            && count != ordered.len() as u32
        {
            return Err(ServiceError::BadDigest(format!(
                "the checksum you specified covers {count} parts but the manifest has {}",
                ordered.len()
            )));
        }

        // A claim means an earlier attempt may still be resolving; reconcile it
        // against the backend's commit record before refusing the retry
        // (`ADR-0016`). A native upload's expected object is the driver's, so
        // only a definitive record resolves it here.
        if let Some(pending) = {
            let state = self.multipart();
            state.claims.get(upload_id).cloned()
        } {
            match self
                .resolve_pending_claim(ctx, session, upload_id, &pending, None)
                .await
            {
                PendingOutcome::Published(meta) => return self.settle(Ok(*meta)).await,
                PendingOutcome::Retryable => {}
                PendingOutcome::Unresolved => {
                    return Err(ServiceError::InvalidArgument(
                        "this multipart upload is already being completed".into(),
                    ));
                }
            }
        }

        // Claim the completion exactly once: a concurrent (or retried)
        // completion of the same upload must not publish a second time.
        let write_id = {
            let mut state = self.multipart();
            if state.claims.contains_key(upload_id) {
                return Err(ServiceError::InvalidArgument(
                    "this multipart upload is already being completed".into(),
                ));
            }
            let write_id = WriteId::generate();
            state.claims.insert(upload_id.to_string(), write_id.clone());
            state
                .pending
                .push(PersistOp::Claim(upload_id.to_string(), write_id.clone()));
            write_id
        };
        // Durable before the attempt, so a crash mid-publish leaves the write id
        // startup must reconcile (`ADR-0013`).
        self.flush_pending().await?;

        let driver_parts: Vec<CompletedPart> = manifest
            .iter()
            .map(|entry| CompletedPart {
                part_number: PartNumber::new(entry.number)
                    .expect("manifest part numbers were validated"),
                etag: entry.etag.clone(),
            })
            .collect();
        // The declared value is handed to the driver so it can verify the
        // assembled object *before* publishing; reconstructing it as a typed
        // checksum is what lets the driver compare like with like.
        let expected: Vec<Checksum> = match (&declared, scheme) {
            (Some(declared), Some(scheme)) => match declared.value.clone() {
                None => Vec::new(),
                Some(value) => {
                    let coverage = declared.checksum_type.unwrap_or(scheme.checksum_type);
                    let built = match coverage {
                        ChecksumType::Composite => Checksum::composite(
                            scheme.algorithm,
                            value,
                            declared.part_count.unwrap_or(manifest.len() as u32),
                        ),
                        ChecksumType::FullObject => {
                            Checksum::new(scheme.algorithm, ChecksumType::FullObject, value)
                        }
                    };
                    vec![built.map_err(|e| ServiceError::InvalidArgument(e.to_string()))?]
                }
            },
            _ => Vec::new(),
        };
        let driver_scheme = scheme.map(|scheme| MultipartChecksumScheme {
            algorithm: scheme.algorithm,
            checksum_type: scheme.checksum_type,
        });

        let meta = match multipart
            .complete(
                ctx,
                upload,
                driver_parts,
                driver_scheme,
                expected,
                preconditions,
            )
            .await
        {
            Ok(meta) => meta,
            Err(e) => {
                // An unresolved outcome must be reconciled with the driver's
                // commit record before the attempt is judged.
                let mut attempt_state = e.commit_state_or_infer();
                if matches!(attempt_state, CommitState::Unknown | CommitState::InFlight)
                    && let Some(recovery) = &namespace.recovery
                    && let Ok(observed) = recovery.query(ctx, &write_id).await
                {
                    attempt_state = observed;
                }
                if attempt_state == CommitState::Committed {
                    // The object was published; only the reply was lost.
                    if let Ok(meta) = namespace.read.stat(ctx, key).await {
                        self.finish_upload(&write_id, session, upload_id);
                        return self.settle(Ok(meta)).await;
                    }
                }
                if attempt_state.permits_replacement_attempt() {
                    self.release_claim(upload_id, &write_id);
                }
                self.record_completion(&write_id, session, attempt_state, upload_id);
                return self.settle(Err(ServiceError::from_driver(&e, true))).await;
            }
        };

        self.finish_upload(&write_id, session, upload_id);
        self.settle(Ok(meta)).await
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
        // The reclamation above is only durable once it is mirrored: a stale
        // record would make startup re-reconcile a finished upload.
        state
            .pending
            .push(PersistOp::ForgetClaim(upload_id.to_string()));
        state
            .pending
            .push(PersistOp::ForgetStaging(session.upload_id.clone()));
        state
            .pending
            .push(PersistOp::ForgetSession(upload_id.to_string()));
    }

    fn release_claim(&self, upload_id: &str, write_id: &WriteId) {
        let mut state = self.multipart();
        if state.claims.get(upload_id) == Some(write_id) {
            state.claims.remove(upload_id);
            state
                .pending
                .push(PersistOp::ForgetClaim(upload_id.to_string()));
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
        // Reconciliation runs on every sweep, so recording the same outcome
        // again must not churn the durable mirror. A record is keyed by attempt
        // id, and an unchanged state is a no-op.
        if guard
            .completions
            .get(write_id.as_str())
            .is_some_and(|existing| existing.state == state)
        {
            return;
        }
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
            // The evicted record's durable copy goes too: the mirror must stay
            // bounded exactly as the retained set is.
            guard
                .pending
                .push(PersistOp::ForgetCompletion(WriteId::from_raw(oldest)));
        }
        let record = CompletionRecord {
            write_id: write_id.clone(),
            upload_id: upload_id.to_string(),
            bucket: session.bucket.clone(),
            key: session.key.clone(),
            state,
            recorded_at: SystemTime::now(),
        };
        guard
            .completions
            .insert(write_id.to_string(), record.clone());
        guard.pending.push(PersistOp::Completion(record));
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

    /// The completion attempts that are still unresolved.
    ///
    /// An upload is pending when it holds a claim (an attempt that may still
    /// publish) or its latest completion record is unknown. A client can resolve
    /// one by retrying `CompleteMultipartUpload`: the service re-reads the
    /// backend's commit record and either restores the published object or
    /// releases the claim for a replacement attempt (`ADR-0016`). This is the
    /// list an operator inspects to find one to settle.
    pub fn pending_completions(&self) -> Vec<PendingCompletion> {
        let state = self.multipart();
        let mut pending: Vec<PendingCompletion> = state
            .uploads
            .values()
            .filter_map(|session| {
                let upload_id = session.upload_id.as_str();
                let claim = state.claims.get(upload_id).cloned();
                let latest = state
                    .completions
                    .values()
                    .filter(|record| record.upload_id == upload_id)
                    .max_by_key(|record| record.recorded_at);
                let unresolved = claim.is_some()
                    || latest.is_some_and(|record| record.state == CommitState::Unknown);
                if !unresolved {
                    return None;
                }
                Some(PendingCompletion {
                    upload_id: upload_id.to_string(),
                    write_id: claim,
                    bucket: session.bucket.clone(),
                    key: session.key.clone(),
                    // A claim with no record yet is an attempt in flight.
                    state: latest
                        .map(|record| record.state)
                        .unwrap_or(CommitState::InFlight),
                    recorded_at: latest.map(|record| record.recorded_at),
                })
            })
            .collect();
        pending.sort_by(|a, b| a.upload_id.cmp(&b.upload_id));
        pending
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
    ///
    /// A native upload's parts live in the driver, which owns its own session
    /// lifetime, so its record here is retired only once the driver no longer
    /// knows the upload: dropping it while the driver still holds it would take
    /// away the record of an upload a client can still finish, while keeping it
    /// forever would leak the session and never let the service go idle. The
    /// driver's own store bounds such a session; the record is a binding check,
    /// not a copy of the upload.
    pub async fn sweep_expired_uploads(
        &self,
        ctx: &DriverContext,
        lease: Duration,
    ) -> SweepOutcome {
        // Resolve what can be resolved before reclaiming: a pending completion
        // is not reclaimable, but once the backend's commit record is
        // definitive it is retired (published) or released (retryable) by the
        // running service, without waiting for a client or a restart.
        self.reconcile_pending_completions(ctx).await;

        let now = SystemTime::now();
        // Pick the expired uploads under the lock, then release it: confirming a
        // native upload awaits the driver, and a guard must not live across an
        // await, so every mutation below re-checks the record it is about to
        // retire.
        let expired: Vec<(UploadId, String, Option<UploadHandle>)> = {
            let state = self.multipart();
            state
                .uploads
                .iter()
                .filter(|(_, s)| {
                    now.duration_since(s.last_activity)
                        .is_ok_and(|age| age >= lease)
                })
                .map(|(id, s)| {
                    (
                        UploadId::from_raw(id.as_str()),
                        s.bucket.clone(),
                        s.native.clone(),
                    )
                })
                .collect()
        };

        let mut outcome = SweepOutcome::default();
        for (upload_id, bucket, native) in expired {
            // The claim and unresolved-attempt gates, plus a re-check of the
            // lease: a candidate awaited earlier may have gained a claim, an
            // unresolved attempt or fresh activity meanwhile.
            let (has_claim, unresolved, present_and_expired) = {
                let state = self.multipart();
                (
                    state.claims.contains_key(upload_id.as_str()),
                    state.completions.values().any(|r| {
                        r.upload_id == upload_id.as_str() && r.state == CommitState::Unknown
                    }),
                    state.uploads.get(upload_id.as_str()).is_some_and(|s| {
                        now.duration_since(s.last_activity)
                            .is_ok_and(|age| age >= lease)
                    }),
                )
            };
            if has_claim || unresolved {
                outcome.skipped_pending += 1;
                continue;
            }
            if !present_and_expired {
                // Already retired, or the upload saw activity while an earlier
                // candidate was awaited: nothing to reclaim.
                continue;
            }
            if let Some(upload) = native
                && self.driver_still_holds(ctx, &bucket, &upload).await
            {
                // The driver is the authority on a native upload's lifetime:
                // the record is kept until the driver forgets the upload.
                continue;
            }

            {
                let mut state = self.multipart();
                outcome.reclaimed_bytes += state.staging.take(&upload_id);
                state.uploads.remove(upload_id.as_str());
                state.claims.remove(upload_id.as_str());
                state
                    .pending
                    .push(PersistOp::ForgetStaging(upload_id.clone()));
                state
                    .pending
                    .push(PersistOp::ForgetSession(upload_id.as_str().to_string()));
                state
                    .pending
                    .push(PersistOp::ForgetClaim(upload_id.as_str().to_string()));
                outcome.reclaimed_uploads += 1;
            }
        }

        // A reclaimed upload must not come back on restart, so the removals are
        // flushed before the sweep reports them.
        self.flush_pending().await.ok();
        outcome
    }

    /// Whether the driver still knows a native upload.
    ///
    /// The driver owns a native upload's lifetime, so the service retires its
    /// record only once the driver reports the upload gone (`NotFound`). Any
    /// other outcome — including a transient driver error — counts as "still
    /// there", so a failed probe keeps the record for the next sweep rather
    /// than orphaning an upload the driver can still complete.
    async fn driver_still_holds(
        &self,
        ctx: &DriverContext,
        bucket: &str,
        upload: &UploadHandle,
    ) -> bool {
        let Ok(binding) = self.binding(bucket) else {
            return true;
        };
        let Ok(namespace) = self.inner.handles.get(&binding, ctx).await else {
            return true;
        };
        let Some(multipart) = namespace.native_multipart().cloned() else {
            // The namespace no longer offers native multipart, so the record can
            // no longer drive anything: let it go.
            return false;
        };
        let limit = NonZeroU32::new(1).expect("part page limit is non-zero");
        !matches!(
            multipart.list_parts(ctx, upload, None, limit).await,
            Err(error) if error.kind() == DriverErrorKind::NotFound
        )
    }

    /// Aborts an upload and reclaims its parts, on the route it was created
    /// with.
    pub async fn abort_multipart(
        &self,
        ctx: &DriverContext,
        bucket: &str,
        key: &ObjectKey,
        upload_id: &str,
    ) -> Result<(), ServiceError> {
        let binding = self.binding(bucket)?;
        let session = {
            let state = self.multipart();
            self.require_session(&state, bucket, key, upload_id)?
        };
        // A completion that already claimed this upload wins the race; checking
        // before the driver is touched keeps an abort from racing a publish.
        if self.multipart().claims.contains_key(upload_id) {
            return Err(ServiceError::InvalidArgument(
                "this multipart upload is being completed".into(),
            ));
        }

        if let UploadRoute::Native {
            multipart, upload, ..
        } = self.upload_route(&binding, ctx, &session).await?
        {
            multipart
                .abort(ctx, &upload)
                .await
                .map_err(|e| ServiceError::from_driver(&e, true))?;
        }

        {
            let mut state = self.multipart();
            state.staging.take(&session.upload_id);
            state.uploads.remove(upload_id);
            state
                .pending
                .push(PersistOp::ForgetStaging(session.upload_id.clone()));
            state
                .pending
                .push(PersistOp::ForgetSession(upload_id.to_string()));
        }
        // The abort is acknowledged, so the durable record must go with it.
        self.flush_pending().await?;
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
        let (binding, _slot) = self.binding_slot(&copy.bucket)?;
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
            .route_upload_part(
                ctx,
                &binding,
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

    /// Whether an in-progress upload was created on the native route.
    ///
    /// The route is recorded on the session when the upload is created and is
    /// never re-derived, so this is the decision a later operation follows.
    /// `None` means the upload is not held here (completed, aborted or unknown),
    /// which is not the same as "coordinated".
    pub fn upload_is_native(&self, upload_id: &str) -> Option<bool> {
        let state = self.multipart();
        state
            .uploads
            .get(upload_id)
            .map(|session| session.native.is_some())
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
                _ = ticker.tick() => {
                    // The sweep asks the driver about native uploads, and a
                    // driver that never answers must not hold the sweeper (and
                    // its shutdown) open, so the probe is bounded by the sweep
                    // interval. A probe that times out keeps the record for the
                    // next tick rather than reclaiming under a live client.
                    let ctx = DriverContext::new(RequestId::new("multipart-sweep"))
                        .with_timeout(interval);
                    on_sweep(self.sweep_expired_uploads(&ctx, lease).await);
                }
            }
        }
    }
}
