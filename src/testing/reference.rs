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
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use async_trait::async_trait;
use bytes::Bytes;

use crate::domain::{
    BeginWrite, CommitAttributes, CommitState, DeleteRequest, DeleteResult, DriverContext,
    DriverError, DriverErrorKind, DriverInstanceId, ListCursor, ListPage, ListRequest, NamespaceId,
    ObjectKey, ObjectMetadata, ObjectRevision, WriteId,
};
use crate::driver::{
    CommitRecovery, CopyRequest, DriverDescriptor, DriverFactory, DriverHealth, DriverKind,
    FactoryConfig, MultipartNamespace, NamespaceCapabilities, NamespaceHandle, NativeCopy,
    ReadNamespace, ReadRequest, ReadResult, SecretResolver, StagedWrite, StorageDriver,
    WriteNamespace,
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
    next_revision: u64,
}

impl NamespaceState {
    pub(crate) fn next_revision(&mut self) -> ObjectRevision {
        self.next_revision += 1;
        ObjectRevision::new(format!("rev-{}", self.next_revision))
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
}

#[async_trait]
impl DriverFactory for ReferenceDriverFactory {
    fn kind(&self) -> DriverKind {
        self.kind.clone()
    }

    async fn validate(
        &self,
        _ctx: &DriverContext,
        _secrets: &dyn SecretResolver,
    ) -> Result<(), DriverError> {
        Ok(())
    }

    async fn create(
        &self,
        _ctx: &DriverContext,
        instance_id: &DriverInstanceId,
        _secrets: &dyn SecretResolver,
    ) -> Result<Arc<dyn StorageDriver>, DriverError> {
        Ok(Arc::new(ReferenceDriver {
            descriptor: DriverDescriptor {
                kind: self.kind.clone(),
                instance_id: instance_id.clone(),
                display_name: "in-memory reference driver".into(),
                version: Some(env!("CARGO_PKG_VERSION").into()),
            },
            capabilities: self.capabilities.clone(),
            namespaces: Mutex::new(HashMap::new()),
        }))
    }
}

/// The in-memory reference driver.
pub struct ReferenceDriver {
    descriptor: DriverDescriptor,
    capabilities: NamespaceCapabilities,
    namespaces: Mutex<HashMap<NamespaceId, Arc<Mutex<NamespaceState>>>>,
}

impl ReferenceDriver {
    fn state(&self, namespace: &NamespaceId) -> Arc<Mutex<NamespaceState>> {
        let mut map = self.namespaces.lock().expect("namespace map poisoned");
        map.entry(namespace.clone())
            .or_insert_with(|| Arc::new(Mutex::new(NamespaceState::default())))
            .clone()
    }
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
        let state = self.state(namespace);
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

        let handle = NamespaceHandle {
            capabilities: caps,
            read,
            write,
            multipart: None::<Arc<dyn MultipartNamespace>>,
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

fn poisoned<T>(_e: std::sync::PoisonError<T>) -> DriverError {
    DriverError::internal("namespace state lock poisoned")
}

fn check_ctx(ctx: &DriverContext) -> Result<(), DriverError> {
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
fn page_for(state: &NamespaceState, req: &ListRequest) -> ListPage {
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
        check_ctx(ctx)?;
        let mut state = self.state.lock().map_err(poisoned)?;
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

        let (content, user_metadata) = match req.metadata_directive {
            crate::driver::MetadataDirective::Copy => (
                source.content.clone(),
                source.metadata.user_metadata.clone(),
            ),
            crate::driver::MetadataDirective::Replace => {
                (source.content.clone(), req.user_metadata.clone())
            }
        };
        let revision = state.next_revision();
        let metadata = ObjectMetadata {
            key: req.destination.clone(),
            size: content.len() as u64,
            etag: source.metadata.etag.clone(),
            last_modified: SystemTime::now(),
            content: source.metadata.content.clone(),
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

// Suppress unused warnings for items kept for API symmetry.
#[allow(dead_code)]
fn _unused(_: &FactoryConfig, _: NonZeroU32) {}
