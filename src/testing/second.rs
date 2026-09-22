//! A second, independent in-memory test driver.
//!
//! Where the reference driver uses a `BTreeMap` and index cursors, this one
//! keeps a sorted `Vec` of entries, resolves keys with binary search, sorts the
//! listing explicitly, and stores commit records separately. It shares no
//! implementation code with the reference driver, which is the point: adding it
//! required no change to `domain` or `driver` (`INV-01`).
//!
//! It deliberately does **not** implement native copy, so the capability
//! planner must route copy to the emulated path.

use std::collections::HashMap;
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
    CommitRecovery, DriverDescriptor, DriverFactory, DriverHealth, DriverKind,
    NamespaceCapabilities, NamespaceHandle, NativeCopy, ReadNamespace, ReadRequest, ReadResult,
    SecretResolver, StagedWrite, StorageDriver, WriteNamespace,
};

use super::reference::StoredObject;
use super::support::one_shot_stream;

#[derive(Default)]
struct VecState {
    entries: Vec<(String, StoredObject)>,
    recovery: HashMap<String, CommitState>,
    revision_counter: u64,
}

impl VecState {
    fn locate(&self, key: &str) -> Result<usize, usize> {
        self.entries.binary_search_by(|(k, _)| k.as_str().cmp(key))
    }

    fn get(&self, key: &str) -> Option<&StoredObject> {
        self.locate(key).ok().map(|i| &self.entries[i].1)
    }

    fn revision(&mut self) -> ObjectRevision {
        self.revision_counter += 1;
        ObjectRevision::new(format!("v{}", self.revision_counter))
    }
}

/// Factory for [`VecDriver`] instances.
pub struct VecDriverFactory {
    kind: DriverKind,
}

impl VecDriverFactory {
    /// Creates a factory for the given kind name.
    pub fn new(kind: impl Into<String>) -> Self {
        Self {
            kind: DriverKind::new(kind),
        }
    }
}

fn vec_capabilities() -> NamespaceCapabilities {
    let mut caps = NamespaceCapabilities::full_memory();
    caps.native_copy = false;
    caps
}

#[async_trait]
impl DriverFactory for VecDriverFactory {
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
        Ok(Arc::new(VecDriver {
            descriptor: DriverDescriptor {
                kind: self.kind.clone(),
                instance_id: instance_id.clone(),
                display_name: "sorted-vec test driver".into(),
                version: Some(env!("CARGO_PKG_VERSION").into()),
            },
            namespaces: Mutex::new(HashMap::new()),
        }))
    }
}

/// A second in-memory driver backed by a sorted vector.
pub struct VecDriver {
    descriptor: DriverDescriptor,
    namespaces: Mutex<HashMap<NamespaceId, Arc<Mutex<VecState>>>>,
}

impl VecDriver {
    fn state(&self, namespace: &NamespaceId) -> Arc<Mutex<VecState>> {
        let mut map = self.namespaces.lock().expect("namespace map poisoned");
        map.entry(namespace.clone())
            .or_insert_with(|| Arc::new(Mutex::new(VecState::default())))
            .clone()
    }
}

fn io_err<T>(_e: std::sync::PoisonError<T>) -> DriverError {
    DriverError::internal("namespace state lock poisoned")
}

fn guard(ctx: &DriverContext) -> Result<(), DriverError> {
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

#[async_trait]
impl StorageDriver for VecDriver {
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
        let read = Arc::new(VecRead {
            state: state.clone(),
        });
        let write = Some(Arc::new(VecWrite {
            state: state.clone(),
        }) as Arc<dyn WriteNamespace>);
        let recovery = Some(Arc::new(VecRecovery { state }) as Arc<dyn CommitRecovery>);
        Ok(NamespaceHandle {
            capabilities: vec_capabilities(),
            read,
            write,
            multipart: None,
            native_copy: None::<Arc<dyn NativeCopy>>,
            recovery,
        })
    }
}

struct VecRead {
    state: Arc<Mutex<VecState>>,
}

#[async_trait]
impl ReadNamespace for VecRead {
    async fn stat(
        &self,
        ctx: &DriverContext,
        key: &ObjectKey,
    ) -> Result<ObjectMetadata, DriverError> {
        guard(ctx)?;
        let state = self.state.lock().map_err(io_err)?;
        state
            .get(key.as_str())
            .map(|o| o.metadata.clone())
            .ok_or_else(|| DriverError::not_found(format!("key `{key}` not found")))
    }

    async fn read(&self, ctx: &DriverContext, req: ReadRequest) -> Result<ReadResult, DriverError> {
        guard(ctx)?;
        let state = self.state.lock().map_err(io_err)?;
        let stored = state
            .get(req.key.as_str())
            .ok_or_else(|| DriverError::not_found(format!("key `{}` not found", req.key)))?;
        if stored.metadata.revision != req.expected_revision {
            return Err(DriverError::revision_unavailable(
                "the checked revision is no longer readable",
            ));
        }
        let content = match req.range {
            None => stored.content.clone(),
            Some(range) => {
                let start = (range.start() as usize).min(stored.content.len());
                let end = (range.end_inclusive() as usize + 1).min(stored.content.len());
                stored.content.slice(start..end)
            }
        };
        Ok(ReadResult {
            metadata: stored.metadata.clone(),
            stream: one_shot_stream(content),
        })
    }

    async fn list(&self, ctx: &DriverContext, req: ListRequest) -> Result<ListPage, DriverError> {
        guard(ctx)?;
        let state = self.state.lock().map_err(io_err)?;
        Ok(build_page(&state, &req))
    }
}

/// A different listing implementation: collect and sort explicitly, then fold.
fn build_page(state: &VecState, req: &ListRequest) -> ListPage {
    let prefix = req.prefix().unwrap_or("");
    let delimiter = req.delimiter();

    let mut keys: Vec<&str> = state
        .entries
        .iter()
        .map(|(k, _)| k.as_str())
        .filter(|k| k.starts_with(prefix))
        .collect();
    keys.sort_unstable();

    let mut objects: Vec<ObjectMetadata> = Vec::new();
    let mut prefixes: Vec<String> = Vec::new();
    let mut seen_prefix: Option<String> = None;

    for key in keys {
        let remainder = &key[prefix.len()..];
        match delimiter.and_then(|d| remainder.find(d)) {
            Some(idx) => {
                let folded = format!("{prefix}{}", &remainder[..=idx]);
                if seen_prefix.as_deref() != Some(folded.as_str()) {
                    prefixes.push(folded.clone());
                    seen_prefix = Some(folded);
                }
            }
            None => {
                if let Some(stored) = state.get(key) {
                    objects.push(stored.metadata.clone());
                }
                seen_prefix = None;
            }
        }
    }

    // Iterate entries in merged order.
    enum Item<'a> {
        Obj(&'a ObjectMetadata),
        Pref(&'a str),
    }
    let mut merged: Vec<Item<'_>> = Vec::new();
    {
        let mut oi = 0usize;
        let mut pi = 0usize;
        while oi < objects.len() || pi < prefixes.len() {
            let take_prefix = match (objects.get(oi), prefixes.get(pi)) {
                (Some(o), Some(p)) => p.as_str() < o.key.as_str(),
                (None, Some(_)) => true,
                _ => false,
            };
            if take_prefix {
                merged.push(Item::Pref(&prefixes[pi]));
                pi += 1;
            } else {
                merged.push(Item::Obj(&objects[oi]));
                oi += 1;
            }
        }
    }

    let start = match req.cursor() {
        Some(cursor) => cursor
            .as_str()
            .strip_prefix("n:")
            .and_then(|n| n.parse::<usize>().ok())
            .unwrap_or(usize::MAX),
        None => match req.start_after() {
            Some(marker) => merged
                .iter()
                .position(|item| match item {
                    Item::Obj(o) => o.key.as_str() > marker.as_str(),
                    Item::Pref(p) => *p > marker.as_str(),
                })
                .unwrap_or(merged.len()),
            None => 0,
        },
    };
    let start = start.min(merged.len());
    let total = merged.len();
    let limit = req.limit().get() as usize;
    let end = start.saturating_add(limit).min(total);

    let mut out_objects = Vec::new();
    let mut out_prefixes = Vec::new();
    for item in merged.into_iter().skip(start).take(limit) {
        match item {
            Item::Obj(o) => out_objects.push(o.clone()),
            Item::Pref(p) => out_prefixes.push(p.to_string()),
        }
    }

    ListPage {
        objects: out_objects,
        common_prefixes: out_prefixes,
        next_cursor: (end < total).then(|| ListCursor::new(format!("n:{end}"))),
        is_truncated: end < total,
    }
}

struct VecWrite {
    state: Arc<Mutex<VecState>>,
}

#[async_trait]
impl WriteNamespace for VecWrite {
    async fn begin_write(
        &self,
        ctx: &DriverContext,
        req: BeginWrite,
    ) -> Result<Box<dyn StagedWrite>, DriverError> {
        guard(ctx)?;
        self.state
            .lock()
            .map_err(io_err)?
            .recovery
            .insert(req.write_id.to_string(), CommitState::InFlight);
        Ok(Box::new(VecStaged {
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
        guard(ctx)?;
        let mut state = self.state.lock().map_err(io_err)?;
        let existing_ci = state.locate(req.key.as_str());
        let existing_etag = existing_ci
            .ok()
            .and_then(|i| state.entries[i].1.metadata.etag.clone());
        if let Some(tag) = &req.preconditions.if_match
            && existing_etag.as_ref() != Some(tag)
        {
            return Err(DriverError::precondition_failed("If-Match did not match"));
        }
        if req.preconditions.if_none_match && existing_ci.is_ok() {
            return Err(DriverError::precondition_failed("object exists"));
        }
        match existing_ci {
            Ok(index) => {
                let (_, removed) = state.entries.remove(index);
                Ok(DeleteResult {
                    deleted: true,
                    removed_revision: Some(removed.metadata.revision),
                })
            }
            Err(_) => Ok(DeleteResult::default()),
        }
    }
}

struct VecStaged {
    state: Arc<Mutex<VecState>>,
    req: BeginWrite,
    buffer: Vec<u8>,
}

#[async_trait]
impl StagedWrite for VecStaged {
    async fn write_chunk(&mut self, ctx: &DriverContext, bytes: Bytes) -> Result<(), DriverError> {
        guard(ctx)?;
        if let Some(expected) = self.req.expected_length
            && (self.buffer.len() + bytes.len()) as u64 > expected
        {
            return Err(DriverError::invalid_argument("payload too long"));
        }
        self.buffer.extend_from_slice(&bytes);
        Ok(())
    }

    async fn commit(
        mut self: Box<Self>,
        ctx: &DriverContext,
        attrs: CommitAttributes,
    ) -> Result<ObjectMetadata, DriverError> {
        guard(ctx)?;
        let mut state = self.state.lock().map_err(io_err)?;

        if matches!(
            state.recovery.get(self.req.write_id.as_str()),
            Some(CommitState::NotCommittedFinal)
        ) {
            return Err(DriverError::internal("attempt is fenced")
                .with_commit_state(CommitState::NotCommittedFinal));
        }

        if let Some(expected) = self.req.expected_length
            && (attrs.length != expected || attrs.length != self.buffer.len() as u64)
        {
            state.recovery.insert(
                self.req.write_id.to_string(),
                CommitState::NotCommittedFinal,
            );
            return Err(DriverError::invalid_argument("length mismatch")
                .with_commit_state(CommitState::NotCommittedFinal));
        }

        let existing_etag = state
            .get(self.req.key.as_str())
            .and_then(|o| o.metadata.etag.clone());
        if let Some(tag) = &self.req.preconditions.if_match
            && existing_etag.as_ref() != Some(tag)
        {
            state.recovery.insert(
                self.req.write_id.to_string(),
                CommitState::NotCommittedFinal,
            );
            return Err(DriverError::precondition_failed("If-Match did not match")
                .with_commit_state(CommitState::NotCommittedFinal));
        }
        if self.req.preconditions.if_none_match && existing_etag.is_some() {
            state.recovery.insert(
                self.req.write_id.to_string(),
                CommitState::NotCommittedFinal,
            );
            return Err(DriverError::precondition_failed("object exists")
                .with_commit_state(CommitState::NotCommittedFinal));
        }

        let revision = state.revision();
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
        let entry = (
            self.req.key.to_string(),
            StoredObject {
                metadata: metadata.clone(),
                content,
            },
        );
        match state.locate(self.req.key.as_str()) {
            Ok(index) => state.entries[index] = entry,
            Err(index) => state.entries.insert(index, entry),
        }
        state
            .recovery
            .insert(self.req.write_id.to_string(), CommitState::Committed);
        Ok(metadata)
    }

    async fn abort(self: Box<Self>, _ctx: &DriverContext) -> Result<(), DriverError> {
        self.state.lock().map_err(io_err)?.recovery.insert(
            self.req.write_id.to_string(),
            CommitState::NotCommittedFinal,
        );
        Ok(())
    }
}

struct VecRecovery {
    state: Arc<Mutex<VecState>>,
}

#[async_trait]
impl CommitRecovery for VecRecovery {
    async fn query(
        &self,
        ctx: &DriverContext,
        write_id: &WriteId,
    ) -> Result<CommitState, DriverError> {
        guard(ctx)?;
        let state = self.state.lock().map_err(io_err)?;
        Ok(state
            .recovery
            .get(write_id.as_str())
            .copied()
            .unwrap_or(CommitState::Unknown))
    }
}
