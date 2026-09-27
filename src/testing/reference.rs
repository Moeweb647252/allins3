//! The in-memory reference driver.
//!
//! This is a test tool, **not** a production backend and **not** the selected
//! storage product. It stores each namespace in a `BTreeMap`, which gives byte
//! order for free, and emulates atomic publish with a single mutex so it can
//! be used to validate the interface and the contract tests.
//!
//! Commit-outcome tracking is real: every attempt is recorded as in-flight,
//! committed or finally-not-committed, and a fenced attempt can never publish
//! (see `docs/implementation-spec.md` §9.2).

use std::collections::{BTreeMap, HashMap};
use std::num::NonZeroU32;
use std::ops::Bound;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use async_trait::async_trait;
use bytes::Bytes;

use crate::domain::{
    BeginWrite, Checksum, ChecksumAlgorithm, ChecksumType, CommitAttributes, CommitState,
    ContentProperties, DeleteRequest, DeleteResult, DriverContext, DriverError, DriverErrorKind,
    EntityTag, ListCursor, ListPage, ListRequest, NamespaceId, ObjectKey, ObjectMetadata,
    ObjectRevision, PartNumber, Preconditions, UserMetadata, WriteId,
};
use crate::driver::{
    CommitRecovery, CompletedPart, CopyRequest, CreateUploadRequest, DriverDescriptor,
    DriverFactory, DriverHealth, DriverKind, FactoryConfig, MultipartChecksumScheme,
    MultipartNamespace, MultipartUpload, NamespaceCapabilities, NamespaceHandle, NativeCopy,
    PartInfo, PartPage, ReadNamespace, ReadRequest, ReadResult, SecretResolver, StagedWrite,
    StorageDriver, UploadHandle, UploadId, UploadPage, WriteNamespace,
};

use super::support::one_shot_stream;

/// A stored object: its metadata and its bytes.
#[derive(Clone)]
pub(crate) struct StoredObject {
    pub(crate) metadata: ObjectMetadata,
    pub(crate) content: Bytes,
}

#[derive(Default)]
pub(crate) struct NamespaceState {
    pub(crate) objects: BTreeMap<String, StoredObject>,
    pub(crate) recovery: HashMap<String, CommitState>,
    /// In-progress native multipart uploads, keyed by upload id.
    pub(crate) uploads: BTreeMap<String, NativeUpload>,
    next_revision: u64,
}

/// One in-progress native multipart upload.
#[derive(Clone)]
pub(crate) struct NativeUpload {
    key: ObjectKey,
    content: ContentProperties,
    user_metadata: UserMetadata,
    checksum: Option<MultipartChecksumScheme>,
    initiated: SystemTime,
    parts: BTreeMap<u16, NativePart>,
}

/// One staged native part: its bytes and the identity of those bytes.
#[derive(Clone)]
struct NativePart {
    data: Bytes,
    etag: EntityTag,
    checksum: Option<Checksum>,
}

impl NamespaceState {
    pub(crate) fn next_revision(&mut self) -> ObjectRevision {
        self.next_revision += 1;
        ObjectRevision::new(format!("rev-{}", self.next_revision))
    }
}

/// The get-or-create namespace table shared by the in-memory test drivers.
///
/// Only the plumbing lives here. Each driver keeps its own state type and its
/// own state machine: [`super::second`] exists to be an *independent*
/// implementation (`INV-01`), so its staging and commit logic is deliberately
/// not shared with the reference driver — sharing it would defeat the point of
/// having two implementations to compare.
///
/// A poisoned table is recovered rather than unwrapped, so one panicked request
/// cannot turn every later request into a panic (spec §10).
pub(crate) struct NamespaceTable<S> {
    namespaces: Mutex<HashMap<NamespaceId, Arc<Mutex<S>>>>,
}

impl<S: Default> NamespaceTable<S> {
    /// An empty table.
    pub(crate) fn new() -> Self {
        Self {
            namespaces: Mutex::new(HashMap::new()),
        }
    }

    /// The state of `namespace`, created on first use.
    pub(crate) fn get(&self, namespace: &NamespaceId) -> Arc<Mutex<S>> {
        crate::sync::lock(&self.namespaces)
            .entry(namespace.clone())
            .or_default()
            .clone()
    }
}

/// Factory for [`ReferenceDriver`] instances.
pub struct ReferenceDriverFactory {
    kind: DriverKind,
    capabilities: NamespaceCapabilities,
}

impl ReferenceDriverFactory {
    /// A factory for a fully-featured in-memory namespace.
    pub fn new(kind: impl Into<String>) -> Self {
        Self {
            kind: DriverKind::new(kind),
            capabilities: NamespaceCapabilities::full_memory(),
        }
    }

    /// A factory with explicit (possibly weak) namespace capabilities. This is
    /// how contract tests exercise capability rejection.
    pub fn with_capabilities(kind: impl Into<String>, capabilities: NamespaceCapabilities) -> Self {
        Self {
            kind: DriverKind::new(kind),
            capabilities,
        }
    }

    /// A factory whose namespace provides native multipart.
    ///
    /// [`NamespaceCapabilities::full_memory`] emulates multipart with the
    /// service coordinator; declaring it native lets the native route be driven
    /// end to end — including over HTTP, where the demonstrator otherwise never
    /// reaches it.
    pub fn native_multipart(kind: impl Into<String>) -> Self {
        let mut capabilities = NamespaceCapabilities::full_memory();
        capabilities.multipart = true;
        Self::with_capabilities(kind, capabilities)
    }
}

#[async_trait]
impl DriverFactory for ReferenceDriverFactory {
    fn kind(&self) -> DriverKind {
        self.kind.clone()
    }

    async fn validate(
        &self,
        _ctx: &DriverContext,
        _config: &FactoryConfig,
        _secrets: &dyn SecretResolver,
    ) -> Result<(), DriverError> {
        Ok(())
    }

    async fn create(
        &self,
        _ctx: &DriverContext,
        config: &FactoryConfig,
        _secrets: &dyn SecretResolver,
    ) -> Result<Arc<dyn StorageDriver>, DriverError> {
        Ok(Arc::new(ReferenceDriver {
            descriptor: DriverDescriptor {
                kind: self.kind.clone(),
                instance_id: config.instance_id.clone(),
                display_name: "in-memory reference driver".into(),
                version: Some(env!("CARGO_PKG_VERSION").into()),
            },
            capabilities: self.capabilities.clone(),
            namespaces: NamespaceTable::new(),
        }))
    }
}

/// The in-memory reference driver.
pub struct ReferenceDriver {
    descriptor: DriverDescriptor,
    capabilities: NamespaceCapabilities,
    namespaces: NamespaceTable<NamespaceState>,
}

#[async_trait]
impl StorageDriver for ReferenceDriver {
    fn descriptor(&self) -> DriverDescriptor {
        self.descriptor.clone()
    }

    async fn health(&self, _ctx: &DriverContext) -> Result<DriverHealth, DriverError> {
        Ok(DriverHealth::Healthy)
    }

    async fn shutdown(&self, _ctx: &DriverContext) -> Result<(), DriverError> {
        Ok(())
    }

    async fn open_namespace(
        &self,
        _ctx: &DriverContext,
        namespace: &NamespaceId,
    ) -> Result<NamespaceHandle, DriverError> {
        let state = self.namespaces.get(namespace);
        let caps = self.capabilities.clone();

        let read = Arc::new(RefRead {
            state: state.clone(),
        });
        let write = caps.write.then(|| {
            Arc::new(RefWrite {
                state: state.clone(),
            }) as Arc<dyn WriteNamespace>
        });
        let native_copy = caps.native_copy.then(|| {
            Arc::new(RefCopy {
                namespace: namespace.clone(),
                state: state.clone(),
            }) as Arc<dyn NativeCopy>
        });
        let recovery = caps.recovery.then(|| {
            Arc::new(RefRecovery {
                state: state.clone(),
            }) as Arc<dyn CommitRecovery>
        });
        let multipart = caps.multipart.then(|| {
            Arc::new(RefMultipart {
                namespace: namespace.clone(),
                state: state.clone(),
            }) as Arc<dyn MultipartNamespace>
        });

        let handle = NamespaceHandle {
            capabilities: caps,
            read,
            write,
            multipart,
            native_copy,
            recovery,
        };
        debug_assert!(
            handle.is_self_consistent(),
            "capabilities and handles diverged"
        );
        Ok(handle)
    }
}

struct RefRead {
    state: Arc<Mutex<NamespaceState>>,
}

#[async_trait]
impl ReadNamespace for RefRead {
    async fn stat(
        &self,
        ctx: &DriverContext,
        key: &ObjectKey,
    ) -> Result<ObjectMetadata, DriverError> {
        check_ctx(ctx)?;
        let state = self.state.lock().map_err(poisoned)?;
        state
            .objects
            .get(key.as_str())
            .map(|o| o.metadata.clone())
            .ok_or_else(|| DriverError::not_found(format!("key `{key}` not found")))
    }

    async fn read(&self, ctx: &DriverContext, req: ReadRequest) -> Result<ReadResult, DriverError> {
        check_ctx(ctx)?;
        let state = self.state.lock().map_err(poisoned)?;
        let stored = state
            .objects
            .get(req.key.as_str())
            .ok_or_else(|| DriverError::not_found(format!("key `{}` not found", req.key)))?;

        if stored.metadata.revision != req.expected_revision {
            return Err(DriverError::revision_unavailable(format!(
                "revision `{}` is no longer readable",
                req.expected_revision
            )));
        }

        let content = match req.range {
            None => stored.content.clone(),
            Some(range) => slice_range(&stored.content, range),
        };

        Ok(ReadResult {
            metadata: stored.metadata.clone(),
            stream: one_shot_stream(content),
        })
    }

    async fn list(&self, ctx: &DriverContext, req: ListRequest) -> Result<ListPage, DriverError> {
        check_ctx(ctx)?;
        let state = self.state.lock().map_err(poisoned)?;
        Ok(page_for(&state, &req))
    }
}

pub(crate) fn poisoned<T>(_e: std::sync::PoisonError<T>) -> DriverError {
    DriverError::internal("namespace state lock poisoned")
}

pub(crate) fn check_ctx(ctx: &DriverContext) -> Result<(), DriverError> {
    if ctx.is_done() {
        let kind = if ctx.is_cancelled() {
            DriverErrorKind::Cancelled
        } else {
            DriverErrorKind::TemporarilyUnavailable
        };
        return Err(DriverError::new(kind, "request cancelled or past deadline"));
    }
    Ok(())
}

fn slice_range(content: &Bytes, range: crate::domain::ResolvedRange) -> Bytes {
    let start = range.start() as usize;
    let len = range.len() as usize;
    let end = start.saturating_add(len).min(content.len());
    if start >= content.len() {
        return Bytes::new();
    }
    content.slice(start..end)
}

/// An entry in an ordered listing. The object metadata is boxed to keep the
/// enum small; its key is available as `metadata.key`.
enum Entry {
    Object(Box<ObjectMetadata>),
    Prefix(String),
}

impl Entry {
    fn sort_key(&self) -> &str {
        match self {
            Entry::Object(m) => m.key.as_str(),
            Entry::Prefix(p) => p,
        }
    }
}

/// Builds an ordered page, folding common prefixes.
///
/// Pagination uses an opaque index cursor (`i:<n>`). For a static data set this
/// guarantees no missing or duplicated entries and no common prefix repeated
/// across pages. Under concurrent mutation the index may shift; the driver does
/// not promise a global snapshot across pages (spec §4.3), and the service
/// layer re-authenticates and rebinds the token on every page.
pub(crate) fn page_for(state: &NamespaceState, req: &ListRequest) -> ListPage {
    let prefix = req.prefix().unwrap_or("");
    let delimiter = req.delimiter();

    let mut entries: Vec<Entry> = Vec::new();
    let mut last_prefix: Option<String> = None;

    for (key, stored) in state.objects.iter() {
        let Some(remainder) = key.strip_prefix(prefix) else {
            continue;
        };
        match delimiter.and_then(|d| remainder.find(d)) {
            Some(idx) => {
                let folded = format!("{prefix}{}", &remainder[..=idx]);
                if last_prefix.as_deref() != Some(folded.as_str()) {
                    entries.push(Entry::Prefix(folded.clone()));
                    last_prefix = Some(folded);
                }
            }
            None => {
                entries.push(Entry::Object(Box::new(stored.metadata.clone())));
                last_prefix = None;
            }
        }
    }

    let start = match req.cursor() {
        Some(cursor) => parse_index_cursor(cursor).unwrap_or(usize::MAX),
        None => match req.start_after() {
            Some(marker) => entries
                .iter()
                .position(|e| e.sort_key() > marker.as_str())
                .unwrap_or(entries.len()),
            None => 0,
        },
    };
    let start = start.min(entries.len());

    let limit = req.limit().get() as usize;
    let end = start.saturating_add(limit).min(entries.len());
    let is_truncated = end < entries.len();

    let mut objects = Vec::new();
    let mut common_prefixes = Vec::new();
    for entry in entries.into_iter().skip(start).take(limit) {
        match entry {
            Entry::Object(meta) => objects.push(*meta),
            Entry::Prefix(p) => common_prefixes.push(p),
        }
    }

    ListPage {
        objects,
        common_prefixes,
        next_cursor: is_truncated.then(|| ListCursor::new(format!("i:{end}"))),
        is_truncated,
    }
}

fn parse_index_cursor(cursor: &ListCursor) -> Option<usize> {
    cursor.as_str().strip_prefix("i:")?.parse().ok()
}

struct RefWrite {
    state: Arc<Mutex<NamespaceState>>,
}

#[async_trait]
impl WriteNamespace for RefWrite {
    async fn begin_write(
        &self,
        ctx: &DriverContext,
        req: BeginWrite,
    ) -> Result<Box<dyn StagedWrite>, DriverError> {
        check_ctx(ctx)?;
        {
            let mut state = self.state.lock().map_err(poisoned)?;
            state
                .recovery
                .insert(req.write_id.to_string(), CommitState::InFlight);
        }
        Ok(Box::new(RefStagedWrite {
            state: self.state.clone(),
            req,
            buffer: Vec::new(),
        }))
    }

    async fn delete(
        &self,
        ctx: &DriverContext,
        req: DeleteRequest,
    ) -> Result<DeleteResult, DriverError> {
        delete_object(&self.state, ctx, req).await
    }
}

/// Atomically deletes a key, honouring preconditions.
///
/// Shared by the in-memory test drivers, so the conditional-delete rules exist
/// in one place.
pub(crate) async fn delete_object(
    state: &Mutex<NamespaceState>,
    ctx: &DriverContext,
    req: DeleteRequest,
) -> Result<DeleteResult, DriverError> {
    check_ctx(ctx)?;
    let mut state = state.lock().map_err(poisoned)?;
    let existing = state.objects.get(req.key.as_str()).cloned();

    if let Some(tag) = &req.preconditions.if_match
        && existing.as_ref().and_then(|o| o.metadata.etag.as_ref()) != Some(tag)
    {
        return Err(DriverError::precondition_failed(
            "If-Match did not match the current object",
        ));
    }
    if req.preconditions.if_none_match && existing.is_some() {
        return Err(DriverError::precondition_failed(
            "If-None-Match: * but the object exists",
        ));
    }

    let removed = state.objects.remove(req.key.as_str());
    Ok(DeleteResult {
        deleted: removed.is_some(),
        removed_revision: removed.map(|o| o.metadata.revision),
    })
}

struct RefStagedWrite {
    state: Arc<Mutex<NamespaceState>>,
    req: BeginWrite,
    buffer: Vec<u8>,
}

#[async_trait]
impl StagedWrite for RefStagedWrite {
    async fn write_chunk(&mut self, ctx: &DriverContext, bytes: Bytes) -> Result<(), DriverError> {
        check_ctx(ctx)?;
        if let Some(expected) = self.req.expected_length
            && (self.buffer.len() + bytes.len()) as u64 > expected
        {
            return Err(DriverError::invalid_argument(
                "payload exceeds the declared content length",
            ));
        }
        self.buffer.extend_from_slice(&bytes);
        Ok(())
    }

    async fn commit(
        mut self: Box<Self>,
        ctx: &DriverContext,
        attrs: CommitAttributes,
    ) -> Result<ObjectMetadata, DriverError> {
        check_ctx(ctx)?;
        let mut state = self.state.lock().map_err(poisoned)?;

        // Fencing: a write id that has reached the final not-committed state can
        // never publish, even if a late call reaches this point.
        match state.recovery.get(self.req.write_id.as_str()) {
            Some(CommitState::NotCommittedFinal) => {
                return Err(DriverError::internal("write attempt is fenced")
                    .with_commit_state(CommitState::NotCommittedFinal));
            }
            Some(CommitState::Committed) => {
                return Err(DriverError::internal("write attempt already committed")
                    .with_commit_state(CommitState::Committed));
            }
            _ => {}
        }

        if let Some(expected) = self.req.expected_length
            && (attrs.length != expected || attrs.length != self.buffer.len() as u64)
        {
            state.recovery.insert(
                self.req.write_id.to_string(),
                CommitState::NotCommittedFinal,
            );
            return Err(DriverError::invalid_argument(
                "decoded length did not match the declared content length",
            )
            .with_commit_state(CommitState::NotCommittedFinal));
        }

        // Preconditions are evaluated in the same critical section as publish.
        let existing_etag = state
            .objects
            .get(self.req.key.as_str())
            .and_then(|o| o.metadata.etag.clone());
        if let Some(tag) = &self.req.preconditions.if_match
            && existing_etag.as_ref() != Some(tag)
        {
            state.recovery.insert(
                self.req.write_id.to_string(),
                CommitState::NotCommittedFinal,
            );
            return Err(DriverError::precondition_failed(
                "If-Match did not match the current object",
            )
            .with_commit_state(CommitState::NotCommittedFinal));
        }
        if self.req.preconditions.if_none_match && existing_etag.is_some() {
            state.recovery.insert(
                self.req.write_id.to_string(),
                CommitState::NotCommittedFinal,
            );
            return Err(
                DriverError::precondition_failed("If-None-Match: * but the object exists")
                    .with_commit_state(CommitState::NotCommittedFinal),
            );
        }

        let revision = state.next_revision();
        let metadata = ObjectMetadata {
            key: self.req.key.clone(),
            size: attrs.length,
            etag: attrs.etag,
            last_modified: SystemTime::now(),
            content: self.req.content.clone(),
            user_metadata: self.req.user_metadata.clone(),
            checksums: attrs.checksums,
            revision,
        };

        let content = Bytes::from(std::mem::take(&mut self.buffer));
        state.objects.insert(
            self.req.key.to_string(),
            StoredObject {
                metadata: metadata.clone(),
                content,
            },
        );
        state
            .recovery
            .insert(self.req.write_id.to_string(), CommitState::Committed);
        Ok(metadata)
    }

    async fn abort(self: Box<Self>, _ctx: &DriverContext) -> Result<(), DriverError> {
        let mut state = self.state.lock().map_err(poisoned)?;
        state.recovery.insert(
            self.req.write_id.to_string(),
            CommitState::NotCommittedFinal,
        );
        Ok(())
    }
}

struct RefCopy {
    namespace: NamespaceId,
    state: Arc<Mutex<NamespaceState>>,
}

#[async_trait]
impl NativeCopy for RefCopy {
    async fn copy(
        &self,
        ctx: &DriverContext,
        req: CopyRequest,
    ) -> Result<ObjectMetadata, DriverError> {
        check_ctx(ctx)?;
        if req.source.namespace != self.namespace {
            return Err(DriverError::invalid_argument(
                "native copy source is outside this namespace",
            ));
        }
        let mut state = self.state.lock().map_err(poisoned)?;

        let source = state
            .objects
            .get(req.source.key.as_str())
            .cloned()
            .ok_or_else(|| DriverError::not_found("copy source not found"))?;
        if source.metadata.revision != req.source.revision {
            return Err(DriverError::revision_unavailable(
                "copy source revision is no longer readable",
            ));
        }

        let existing_etag = state
            .objects
            .get(req.destination.as_str())
            .and_then(|o| o.metadata.etag.clone());
        if let Some(tag) = &req.preconditions.if_match
            && existing_etag.as_ref() != Some(tag)
        {
            return Err(DriverError::precondition_failed(
                "If-Match did not match the destination object",
            ));
        }
        if req.preconditions.if_none_match && existing_etag.is_some() {
            return Err(DriverError::precondition_failed(
                "If-None-Match: * but the destination exists",
            ));
        }

        // `Replace` must take the caller's content properties as well as the
        // caller's user metadata; keeping the source properties would silently
        // ignore half of the replacement request.
        let (content, source_content, user_metadata) = match req.metadata_directive {
            crate::driver::MetadataDirective::Copy => (
                source.content.clone(),
                source.metadata.content.clone(),
                source.metadata.user_metadata.clone(),
            ),
            crate::driver::MetadataDirective::Replace => (
                source.content.clone(),
                req.content.clone(),
                req.user_metadata.clone(),
            ),
        };
        let revision = state.next_revision();
        let metadata = ObjectMetadata {
            key: req.destination.clone(),
            size: content.len() as u64,
            etag: source.metadata.etag.clone(),
            last_modified: SystemTime::now(),
            content: source_content,
            user_metadata,
            checksums: source.metadata.checksums.clone(),
            revision,
        };
        state.objects.insert(
            req.destination.to_string(),
            StoredObject {
                metadata: metadata.clone(),
                content,
            },
        );
        Ok(metadata)
    }
}

struct RefRecovery {
    state: Arc<Mutex<NamespaceState>>,
}

#[async_trait]
impl CommitRecovery for RefRecovery {
    async fn query(
        &self,
        ctx: &DriverContext,
        write_id: &WriteId,
    ) -> Result<CommitState, DriverError> {
        check_ctx(ctx)?;
        let state = self.state.lock().map_err(poisoned)?;
        // A missing record is never proof of final non-commit.
        Ok(state
            .recovery
            .get(write_id.as_str())
            .copied()
            .unwrap_or(CommitState::Unknown))
    }
}

/// The reference driver's native multipart.
///
/// The upload session, its parts and the assembly live here rather than in the
/// service, so this is the implementation the native route is compared against:
/// the coordinator route over a staged write must behave the same.
struct RefMultipart {
    namespace: NamespaceId,
    state: Arc<Mutex<NamespaceState>>,
}

impl RefMultipart {
    fn upload<'a>(
        &self,
        state: &'a NamespaceState,
        id: &UploadId,
    ) -> Result<&'a NativeUpload, DriverError> {
        state
            .uploads
            .get(id.as_str())
            .ok_or_else(|| DriverError::not_found(format!("upload `{}` not found", id.as_str())))
    }
}

#[async_trait]
impl MultipartNamespace for RefMultipart {
    async fn create_upload(
        &self,
        ctx: &DriverContext,
        req: CreateUploadRequest,
    ) -> Result<UploadHandle, DriverError> {
        check_ctx(ctx)?;
        let mut state = self.state.lock().map_err(poisoned)?;
        let upload_id = UploadId::generate();
        state.uploads.insert(
            upload_id.as_str().to_string(),
            NativeUpload {
                key: req.key.clone(),
                content: req.content,
                user_metadata: req.user_metadata,
                checksum: req.checksum,
                initiated: SystemTime::now(),
                parts: BTreeMap::new(),
            },
        );
        Ok(UploadHandle {
            upload_id,
            key: req.key,
        })
    }

    async fn begin_part(
        &self,
        ctx: &DriverContext,
        upload: &UploadHandle,
        part_number: PartNumber,
        expected_length: Option<u64>,
    ) -> Result<Box<dyn StagedWrite>, DriverError> {
        check_ctx(ctx)?;
        // The upload must exist when the part starts; the part itself is staged
        // off to the side and replaces the stored one only at commit.
        let scheme = {
            let state = self.state.lock().map_err(poisoned)?;
            self.upload(&state, &upload.upload_id)?.checksum
        };
        Ok(Box::new(RefPartWrite {
            state: self.state.clone(),
            upload_id: upload.upload_id.clone(),
            part_number: part_number.get(),
            expected_length,
            scheme,
            buffer: Vec::new(),
        }))
    }

    async fn list_parts(
        &self,
        ctx: &DriverContext,
        upload: &UploadHandle,
        marker: Option<PartNumber>,
        limit: NonZeroU32,
    ) -> Result<PartPage, DriverError> {
        check_ctx(ctx)?;
        let state = self.state.lock().map_err(poisoned)?;
        let stored = self.upload(&state, &upload.upload_id)?;
        let start = marker.map(|marker| marker.get()).unwrap_or(0);
        let collected: Vec<(u16, NativePart)> = stored
            .parts
            .range((Bound::Excluded(start), Bound::Unbounded))
            .map(|(number, part)| (*number, part.clone()))
            .collect();

        let limit = limit.get() as usize;
        let is_truncated = collected.len() > limit;
        let page: Vec<(u16, NativePart)> = collected.into_iter().take(limit).collect();
        let next_marker = is_truncated
            .then(|| page.last().map(|(number, _)| *number))
            .flatten()
            .map(|number| PartNumber::new(number).expect("stored part numbers are valid"));
        let parts = page
            .into_iter()
            .map(|(number, part)| PartInfo {
                part_number: PartNumber::new(number).expect("stored part numbers are valid"),
                size: part.data.len() as u64,
                etag: Some(part.etag),
                checksum: part.checksum,
                last_modified: SystemTime::now(),
            })
            .collect();
        Ok(PartPage {
            parts,
            next_marker,
            is_truncated,
        })
    }

    async fn list_uploads(
        &self,
        ctx: &DriverContext,
        namespace: &NamespaceId,
        key_marker: Option<ObjectKey>,
        upload_id_marker: Option<UploadId>,
        limit: NonZeroU32,
    ) -> Result<UploadPage, DriverError> {
        check_ctx(ctx)?;
        if namespace != &self.namespace {
            return Err(DriverError::not_found("namespace is outside this instance"));
        }
        let state = self.state.lock().map_err(poisoned)?;
        let mut uploads: Vec<(ObjectKey, UploadId, SystemTime)> = state
            .uploads
            .iter()
            .map(|(id, upload)| {
                (
                    upload.key.clone(),
                    UploadId::from_raw(id.as_str()),
                    upload.initiated,
                )
            })
            .collect();
        uploads.sort_by(|a, b| {
            a.0.as_str()
                .cmp(b.0.as_str())
                .then(a.1.as_str().cmp(b.1.as_str()))
        });

        let filtered: Vec<(ObjectKey, UploadId, SystemTime)> = uploads
            .into_iter()
            .filter(|(key, id, _)| match (&key_marker, &upload_id_marker) {
                (Some(key_marker), Some(upload_id_marker)) => {
                    (key.as_str(), id.as_str()) > (key_marker.as_str(), upload_id_marker.as_str())
                }
                _ => true,
            })
            .collect();

        let limit = limit.get() as usize;
        let is_truncated = filtered.len() > limit;
        let page: Vec<(ObjectKey, UploadId, SystemTime)> =
            filtered.into_iter().take(limit).collect();
        let (next_key_marker, next_upload_id_marker) = if is_truncated {
            page.last()
                .map(|(key, id, _)| (Some(key.clone()), Some(id.clone())))
                .unwrap_or((None, None))
        } else {
            (None, None)
        };
        let uploads = page
            .into_iter()
            .map(|(key, upload_id, initiated)| MultipartUpload {
                upload_id,
                key,
                initiated,
            })
            .collect();
        Ok(UploadPage {
            uploads,
            next_key_marker,
            next_upload_id_marker,
            is_truncated,
        })
    }

    async fn complete(
        &self,
        ctx: &DriverContext,
        upload: &UploadHandle,
        parts: Vec<CompletedPart>,
        scheme: Option<MultipartChecksumScheme>,
        expected: Vec<Checksum>,
        preconditions: Preconditions,
    ) -> Result<ObjectMetadata, DriverError> {
        check_ctx(ctx)?;
        let mut state = self.state.lock().map_err(poisoned)?;
        let session = self.upload(&state, &upload.upload_id)?.clone();

        // The manifest is validated against the stored parts before a byte is
        // assembled, and the assembly itself is validated before anything is
        // published.
        let mut seen = std::collections::HashSet::new();
        let mut ordered: Vec<(u16, NativePart)> = Vec::with_capacity(parts.len());
        for entry in &parts {
            let number = entry.part_number.get();
            if !seen.insert(number) {
                return Err(DriverError::invalid_argument(
                    "the manifest contains duplicate part numbers",
                ));
            }
            let part = session.parts.get(&number).ok_or_else(|| {
                DriverError::invalid_argument(format!("part {number} was not uploaded"))
            })?;
            if let Some(tag) = &entry.etag
                && &part.etag != tag
            {
                return Err(DriverError::invalid_argument(format!(
                    "part {number} ETag does not match the uploaded part"
                )));
            }
            ordered.push((number, part.clone()));
        }
        for window in ordered.windows(2) {
            if window[0].0 >= window[1].0 {
                return Err(DriverError::invalid_argument(
                    "parts must be listed in ascending part-number order",
                ));
            }
        }

        let mut assembled: Vec<u8> =
            Vec::with_capacity(ordered.iter().map(|(_, part)| part.data.len()).sum());
        for (_, part) in &ordered {
            assembled.extend_from_slice(&part.data);
        }
        let assembled = Bytes::from(assembled);

        let effective = scheme.or(session.checksum);
        let checksums = match effective {
            None => Vec::new(),
            Some(scheme) => {
                let digest = match scheme.checksum_type {
                    ChecksumType::FullObject => {
                        crate::integrity::compute_checksum(scheme.algorithm, &assembled)
                    }
                    ChecksumType::Composite => {
                        let digests: Vec<Bytes> = ordered
                            .iter()
                            .map(|(_, part)| {
                                crate::integrity::compute_checksum(scheme.algorithm, &part.data)
                            })
                            .collect();
                        crate::integrity::composite_checksum(scheme.algorithm, &digests)
                    }
                };
                let checksum = match scheme.checksum_type {
                    ChecksumType::FullObject => {
                        Checksum::new(scheme.algorithm, ChecksumType::FullObject, digest)
                    }
                    ChecksumType::Composite => {
                        Checksum::composite(scheme.algorithm, digest, ordered.len() as u32)
                    }
                }
                .map_err(|e| DriverError::internal(e.to_string()))?;
                vec![checksum]
            }
        };

        // The client's declared checksum is verified here, before anything is
        // published: a rejected completion does not consume the upload.
        for declared in &expected {
            let matches = checksums.iter().any(|actual| {
                actual.algorithm() == declared.algorithm()
                    && actual.checksum_type() == declared.checksum_type()
                    && actual.value() == declared.value()
                    && actual.part_count() == declared.part_count()
            });
            if !matches {
                return Err(DriverError::payload_digest_mismatch(
                    "the declared checksum did not match the assembled object",
                ));
            }
        }

        let existing_etag = state
            .objects
            .get(session.key.as_str())
            .and_then(|object| object.metadata.etag.clone());
        if let Some(tag) = &preconditions.if_match
            && existing_etag.as_ref() != Some(tag)
        {
            return Err(DriverError::precondition_failed(
                "If-Match did not match the current object",
            ));
        }
        if preconditions.if_none_match && existing_etag.is_some() {
            return Err(DriverError::precondition_failed(
                "If-None-Match: * but the object exists",
            ));
        }

        let md5s: Vec<[u8; 16]> = ordered
            .iter()
            .map(|(_, part)| {
                let digest = crate::integrity::compute_checksum(ChecksumAlgorithm::Md5, &part.data);
                let mut md5 = [0u8; 16];
                md5.copy_from_slice(&digest);
                md5
            })
            .collect();
        let etag = crate::integrity::etag_for_multipart(&md5s);

        let revision = state.next_revision();
        let metadata = ObjectMetadata {
            key: session.key.clone(),
            size: assembled.len() as u64,
            etag: Some(etag),
            last_modified: SystemTime::now(),
            content: session.content.clone(),
            user_metadata: session.user_metadata.clone(),
            checksums,
            revision,
        };
        state.objects.insert(
            session.key.to_string(),
            StoredObject {
                metadata: metadata.clone(),
                content: assembled,
            },
        );
        // The upload is consumed exactly once: a replay finds no session and
        // cannot publish again.
        state.uploads.remove(upload.upload_id.as_str());
        Ok(metadata)
    }

    async fn abort(&self, ctx: &DriverContext, upload: &UploadHandle) -> Result<(), DriverError> {
        check_ctx(ctx)?;
        let mut state = self.state.lock().map_err(poisoned)?;
        state
            .uploads
            .remove(upload.upload_id.as_str())
            .map(|_| ())
            .ok_or_else(|| {
                DriverError::not_found(format!("upload `{}` not found", upload.upload_id.as_str()))
            })
    }
}

/// One staged native part, held outside the upload until it commits.
struct RefPartWrite {
    state: Arc<Mutex<NamespaceState>>,
    upload_id: UploadId,
    part_number: u16,
    expected_length: Option<u64>,
    scheme: Option<MultipartChecksumScheme>,
    buffer: Vec<u8>,
}

#[async_trait]
impl StagedWrite for RefPartWrite {
    async fn write_chunk(&mut self, ctx: &DriverContext, bytes: Bytes) -> Result<(), DriverError> {
        check_ctx(ctx)?;
        if let Some(expected) = self.expected_length
            && (self.buffer.len() + bytes.len()) as u64 > expected
        {
            return Err(DriverError::invalid_argument(
                "payload exceeds the declared content length",
            ));
        }
        self.buffer.extend_from_slice(&bytes);
        Ok(())
    }

    async fn commit(
        mut self: Box<Self>,
        ctx: &DriverContext,
        attrs: CommitAttributes,
    ) -> Result<ObjectMetadata, DriverError> {
        check_ctx(ctx)?;
        let data = Bytes::from(std::mem::take(&mut self.buffer));
        let length = data.len() as u64;
        if let Some(expected) = self.expected_length
            && attrs.length != expected
        {
            return Err(DriverError::invalid_argument(
                "decoded length did not match the declared content length",
            ));
        }

        let md5 = crate::integrity::compute_checksum(ChecksumAlgorithm::Md5, &data);
        let mut md5_digest = [0u8; 16];
        md5_digest.copy_from_slice(&md5);
        let etag = attrs
            .etag
            .clone()
            .unwrap_or_else(|| EntityTag::new(crate::integrity::hex(&md5_digest)).expect("hex"));
        let checksum = match self.scheme {
            Some(scheme) => Some(
                Checksum::new(
                    scheme.algorithm,
                    scheme.checksum_type,
                    crate::integrity::compute_checksum(scheme.algorithm, &data),
                )
                .map_err(|e| DriverError::internal(e.to_string()))?,
            ),
            None => None,
        };

        let mut state = self.state.lock().map_err(poisoned)?;
        let revision = state.next_revision();
        let upload = state
            .uploads
            .get_mut(self.upload_id.as_str())
            .ok_or_else(|| {
                DriverError::not_found(format!("upload `{}` not found", self.upload_id.as_str()))
            })?;
        // The stored part is replaced only now, so an aborted or failed part
        // never disturbs the valid one.
        upload.parts.insert(
            self.part_number,
            NativePart {
                data,
                etag: etag.clone(),
                checksum: checksum.clone(),
            },
        );
        Ok(ObjectMetadata {
            key: upload.key.clone(),
            size: length,
            etag: Some(etag),
            last_modified: SystemTime::now(),
            content: upload.content.clone(),
            user_metadata: upload.user_metadata.clone(),
            checksums: checksum.into_iter().collect(),
            revision,
        })
    }

    async fn abort(self: Box<Self>, _ctx: &DriverContext) -> Result<(), DriverError> {
        // Nothing was ever published, so dropping the buffer is the whole abort.
        Ok(())
    }
}
