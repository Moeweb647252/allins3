//! A counting sink: a write path that measures a body instead of storing it.
//!
//! This is a **measurement instrument**, not a backend. It exists so the
//! resource check (P5-06) can push object sizes through the real service
//! pipeline without holding them: every chunk is counted and dropped, and only
//! the published metadata is retained.
//!
//! Because no bytes are kept, `read` refuses rather than serving an empty body
//! that would look like a successful read. `stat` and `list` are real, so a
//! caller can still inspect what a write published.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use async_trait::async_trait;
use bytes::Bytes;

use crate::domain::{
    BeginWrite, CommitAttributes, DeleteRequest, DeleteResult, DriverContext, DriverError,
    DriverErrorKind, DriverInstanceId, ListCursor, ListPage, ListRequest, NamespaceId, ObjectKey,
    ObjectMetadata, ObjectRevision,
};
use crate::driver::{
    ConditionalSupport, Consistency, DriverDescriptor, DriverFactory, DriverHealth, DriverKind,
    DriverLimits, NamespaceCapabilities, NamespaceHandle, Persistence, RangeSupport, ReadNamespace,
    ReadRequest, ReadResult, SecretResolver, StagedWrite, StorageDriver, WriteNamespace,
};

/// The counters a counting sink accumulates across writes.
#[derive(Debug, Default)]
pub struct Sink {
    received: AtomicU64,
    chunks: AtomicU64,
    commits: AtomicU64,
    aborts: AtomicU64,
}

impl Sink {
    /// Bytes written through the sink, counted as they arrive.
    pub fn received(&self) -> u64 {
        self.received.load(Ordering::SeqCst)
    }

    /// The number of chunks the sink received.
    pub fn chunks(&self) -> u64 {
        self.chunks.load(Ordering::SeqCst)
    }

    /// The number of staged writes that committed.
    pub fn commits(&self) -> u64 {
        self.commits.load(Ordering::SeqCst)
    }

    /// The number of staged writes that were aborted.
    pub fn aborts(&self) -> u64 {
        self.aborts.load(Ordering::SeqCst)
    }
}

#[derive(Default)]
struct SinkState {
    objects: BTreeMap<String, ObjectMetadata>,
    next_revision: u64,
}

impl SinkState {
    fn next_revision(&mut self) -> ObjectRevision {
        self.next_revision += 1;
        ObjectRevision::new(format!("sink-{}", self.next_revision))
    }
}

/// Factory for [`CountingDriver`] instances.
///
/// The [`Sink`] is created up front and shared with every instance this factory
/// builds, so a test can read the counters after the service has published.
pub struct CountingDriverFactory {
    kind: DriverKind,
    sink: Arc<Sink>,
}

impl CountingDriverFactory {
    /// A factory whose counters are readable through [`Self::sink`].
    pub fn new(kind: impl Into<String>) -> Self {
        Self {
            kind: DriverKind::new(kind),
            sink: Arc::new(Sink::default()),
        }
    }

    /// The shared counters.
    pub fn sink(&self) -> Arc<Sink> {
        self.sink.clone()
    }
}

#[async_trait]
impl DriverFactory for CountingDriverFactory {
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
        Ok(Arc::new(CountingDriver {
            descriptor: DriverDescriptor {
                kind: self.kind.clone(),
                instance_id: instance_id.clone(),
                display_name: "counting sink driver".into(),
                version: Some(env!("CARGO_PKG_VERSION").into()),
            },
            sink: self.sink.clone(),
            namespaces: Mutex::new(BTreeMap::new()),
        }))
    }
}

/// The counting sink driver.
pub struct CountingDriver {
    descriptor: DriverDescriptor,
    sink: Arc<Sink>,
    namespaces: Mutex<BTreeMap<NamespaceId, Arc<Mutex<SinkState>>>>,
}

impl CountingDriver {
    fn state(&self, namespace: &NamespaceId) -> Arc<Mutex<SinkState>> {
        let mut map = self.namespaces.lock().expect("namespace map poisoned");
        map.entry(namespace.clone()).or_default().clone()
    }
}

#[async_trait]
impl StorageDriver for CountingDriver {
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
        let handle = NamespaceHandle {
            capabilities: NamespaceCapabilities {
                read: true,
                // Listing is metadata-only here, but it is real: callers can
                // still page over what the sink measured.
                list: true,
                write: true,
                multipart: false,
                native_copy: false,
                recovery: false,
                range: RangeSupport::Unsupported,
                conditional: ConditionalSupport::Native,
                consistency: Consistency::Strong,
                persistence: Persistence::Volatile,
                limits: DriverLimits {
                    max_object_size: None,
                    max_single_write_size: None,
                    max_multipart_parts: None,
                    min_part_size: None,
                    max_metadata_count: None,
                    max_metadata_bytes: None,
                },
                exclusive_writer: true,
            },
            read: Arc::new(SinkRead {
                state: state.clone(),
            }),
            write: Some(Arc::new(SinkWrite {
                state: state.clone(),
                sink: self.sink.clone(),
            })),
            multipart: None,
            native_copy: None,
            recovery: None,
        };
        debug_assert!(
            handle.is_self_consistent(),
            "capabilities and handles diverged"
        );
        Ok(handle)
    }
}

fn poisoned<T>(_e: std::sync::PoisonError<T>) -> DriverError {
    DriverError::internal("sink state lock poisoned")
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

struct SinkRead {
    state: Arc<Mutex<SinkState>>,
}

#[async_trait]
impl ReadNamespace for SinkRead {
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
            .cloned()
            .ok_or_else(|| DriverError::not_found(format!("no object at {}", key.as_str())))
    }

    async fn read(
        &self,
        _ctx: &DriverContext,
        req: ReadRequest,
    ) -> Result<ReadResult, DriverError> {
        // Refusing is the honest answer: the sink never kept the bytes.
        Err(DriverError::not_supported(format!(
            "the counting sink stores no bytes for {}",
            req.key.as_str()
        )))
    }

    async fn list(&self, ctx: &DriverContext, req: ListRequest) -> Result<ListPage, DriverError> {
        check_ctx(ctx)?;
        let state = self.state.lock().map_err(poisoned)?;

        let prefix = req.prefix().unwrap_or("");
        let delimiter = req.delimiter();
        let mut objects = Vec::new();
        let mut common_prefixes = Vec::new();
        let mut last_prefix: Option<String> = None;

        for (key, metadata) in state.objects.iter() {
            let Some(remainder) = key.strip_prefix(prefix) else {
                continue;
            };
            match delimiter.and_then(|d| remainder.find(d)) {
                Some(idx) => {
                    let folded = format!("{prefix}{}", &remainder[..=idx]);
                    if last_prefix.as_deref() != Some(folded.as_str()) {
                        common_prefixes.push(folded.clone());
                        last_prefix = Some(folded);
                    }
                }
                None => {
                    last_prefix = None;
                    let after_cursor = req.cursor().is_none_or(|c| key.as_str() > c.as_str());
                    let after_marker = req.start_after().is_none_or(|m| key.as_str() > m.as_str());
                    if after_cursor && after_marker {
                        objects.push(metadata.clone());
                    }
                }
            }
        }

        let limit = req.limit().get() as usize;
        let truncated = objects.len() > limit;
        objects.truncate(limit);
        common_prefixes.truncate(limit);
        let next_cursor = truncated
            .then(|| objects.last().map(|m| ListCursor::new(m.key.to_string())))
            .flatten();

        Ok(ListPage {
            objects,
            common_prefixes,
            next_cursor,
            is_truncated: truncated,
        })
    }
}

struct SinkWrite {
    state: Arc<Mutex<SinkState>>,
    sink: Arc<Sink>,
}

#[async_trait]
impl WriteNamespace for SinkWrite {
    async fn begin_write(
        &self,
        ctx: &DriverContext,
        req: BeginWrite,
    ) -> Result<Box<dyn StagedWrite>, DriverError> {
        check_ctx(ctx)?;
        Ok(Box::new(SinkStaged {
            state: self.state.clone(),
            sink: self.sink.clone(),
            req,
            received: 0,
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
            && existing.as_ref().and_then(|m| m.etag.as_ref()) != Some(tag)
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
            removed_revision: removed.map(|m| m.revision),
        })
    }
}

struct SinkStaged {
    state: Arc<Mutex<SinkState>>,
    sink: Arc<Sink>,
    req: BeginWrite,
    received: u64,
}

#[async_trait]
impl StagedWrite for SinkStaged {
    async fn write_chunk(&mut self, ctx: &DriverContext, bytes: Bytes) -> Result<(), DriverError> {
        check_ctx(ctx)?;
        let received = self.received + bytes.len() as u64;
        if let Some(expected) = self.req.expected_length
            && received > expected
        {
            return Err(DriverError::invalid_argument(
                "payload exceeds the declared content length",
            ));
        }
        self.received = received;
        // The bytes are counted and dropped: this is the whole point of the
        // sink, and why an object of any size costs one chunk of memory.
        self.sink
            .received
            .fetch_add(bytes.len() as u64, Ordering::SeqCst);
        self.sink.chunks.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn commit(
        self: Box<Self>,
        ctx: &DriverContext,
        attrs: CommitAttributes,
    ) -> Result<ObjectMetadata, DriverError> {
        check_ctx(ctx)?;
        if let Some(expected) = self.req.expected_length
            && (attrs.length != expected || attrs.length != self.received)
        {
            return Err(DriverError::invalid_argument(
                "decoded length did not match the declared content length",
            ));
        }

        let mut state = self.state.lock().map_err(poisoned)?;
        let existing_etag = state
            .objects
            .get(self.req.key.as_str())
            .and_then(|m| m.etag.clone());
        if let Some(tag) = &self.req.preconditions.if_match
            && existing_etag.as_ref() != Some(tag)
        {
            return Err(DriverError::precondition_failed(
                "If-Match did not match the current object",
            ));
        }
        if self.req.preconditions.if_none_match && existing_etag.is_some() {
            return Err(DriverError::precondition_failed(
                "If-None-Match: * but the object exists",
            ));
        }

        let metadata = ObjectMetadata {
            key: self.req.key.clone(),
            size: attrs.length,
            etag: attrs.etag,
            last_modified: SystemTime::now(),
            content: self.req.content.clone(),
            user_metadata: self.req.user_metadata.clone(),
            checksums: attrs.checksums,
            revision: state.next_revision(),
        };
        state
            .objects
            .insert(self.req.key.as_str().to_string(), metadata.clone());
        self.sink.commits.fetch_add(1, Ordering::SeqCst);
        Ok(metadata)
    }

    async fn abort(self: Box<Self>, _ctx: &DriverContext) -> Result<(), DriverError> {
        self.sink.aborts.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
