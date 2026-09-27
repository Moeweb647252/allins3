//! The filesystem namespace: reads, staged writes, commits, deletes, recovery
//! records and ordered listing.
//!
//! Every publish is a single atomic `rename` of one object file, and the file
//! is fsynced before the rename, so a reader never observes a half-written
//! object and a crash never leaves metadata and content disagreeing.
//!
//! ## The registry audit log a commit keeps
//!
//! The service decides whether an in-flight write may be replaced by asking
//! [`FsRecovery::query`]. This driver writes one record per attempt: `InFlight`
//! when the write starts, `NotCommittedFinal` on abort or on a validation
//! failure before publish, `Committed` after the rename. A record that cannot
//! be read — including one lost to a crash between the rename and the record
//! write — is reported as [`CommitState::Unknown`], never as a final failure
//! (`ADR-0003`).

use std::io::SeekFrom;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use bytes::Bytes;
use futures_core::Stream;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio_util::io::ReaderStream;

use crate::domain::{
    BeginWrite, CommitAttributes, CommitState, DeleteRequest, DeleteResult, DriverContext,
    DriverError, DriverErrorKind, ListCursor, ListPage, ListRequest, NamespaceId, ObjectKey,
    ObjectMetadata, ObjectRevision, Preconditions, WriteId,
};
use crate::driver::{
    BlockingExecutor, CommitRecovery, ReadNamespace, ReadRequest, ReadResult, WriteNamespace,
};

use super::meta::{self, StoredMetadata};
use super::{check_ctx, io_error};

/// The byte length of the little-endian metadata header.
const HEADER_LEN: u64 = 8;

/// Namespace directory layout.
#[derive(Debug, Clone)]
pub(crate) struct Layout {
    namespace_dir: PathBuf,
}

impl Layout {
    /// The layout for `namespace` under `root`.
    pub(crate) fn new(root: &Path, namespace: &NamespaceId) -> Self {
        Self {
            namespace_dir: root.join(format!("ns-{}", hex(namespace.as_str().as_bytes()))),
        }
    }

    pub(crate) fn objects_dir(&self) -> PathBuf {
        self.namespace_dir.join("objects")
    }

    pub(crate) fn staging_dir(&self) -> PathBuf {
        self.namespace_dir.join("staging")
    }

    pub(crate) fn recovery_dir(&self) -> PathBuf {
        self.namespace_dir.join("recovery")
    }

    fn object_path(&self, key: &str) -> PathBuf {
        self.objects_dir().join(hex(key.as_bytes()))
    }

    fn object_tmp_path(&self, key: &str) -> PathBuf {
        self.objects_dir()
            .join(format!("{}.tmp", hex(key.as_bytes())))
    }

    fn staging_path(&self, write_id: &WriteId) -> PathBuf {
        self.staging_dir().join(hex(write_id.as_str().as_bytes()))
    }

    fn recovery_path(&self, write_id: &WriteId) -> PathBuf {
        self.recovery_dir().join(hex(write_id.as_str().as_bytes()))
    }
}

/// State shared by every handle of one namespace on one driver.
pub(crate) struct NamespaceShared {
    pub(crate) layout: Layout,
    pub(crate) max_object_size: Option<u64>,
    pub(crate) max_single_write_size: Option<u64>,
    pub(crate) blocking: BlockingExecutor,
    /// Serialises commits and deletes of this namespace within the process, so
    /// a conditional check and its publish are one critical section. Declaring
    /// `exclusive_writer` is what makes an in-process lock sufficient.
    pub(crate) lock: Arc<tokio::sync::Mutex<()>>,
    /// Revision counter, shared across namespaces so two revisions minted in
    /// the same nanosecond still differ.
    pub(crate) revision_seq: Arc<AtomicU64>,
}

/// The read and write face of one filesystem namespace.
pub(crate) struct FsNamespace {
    pub(crate) shared: Arc<NamespaceShared>,
}

/// The commit-outcome query face of one filesystem namespace.
pub(crate) struct FsRecovery {
    pub(crate) shared: Arc<NamespaceShared>,
}

#[async_trait]
impl ReadNamespace for FsNamespace {
    async fn stat(
        &self,
        ctx: &DriverContext,
        key: &ObjectKey,
    ) -> Result<ObjectMetadata, DriverError> {
        check_ctx(ctx)?;
        let path = self.shared.layout.object_path(key.as_str());
        read_header(&path)
            .await?
            .map(|header| header.metadata)
            .ok_or_else(|| DriverError::not_found(format!("key `{key}` not found")))
    }

    async fn read(&self, ctx: &DriverContext, req: ReadRequest) -> Result<ReadResult, DriverError> {
        check_ctx(ctx)?;
        let path = self.shared.layout.object_path(req.key.as_str());
        let header = read_header(&path)
            .await?
            .ok_or_else(|| DriverError::not_found(format!("key `{}` not found", req.key)))?;

        // A read is pinned to the revision an earlier `stat` captured; falling
        // back to whatever is current would silently serve the wrong bytes.
        if header.metadata.revision != req.expected_revision {
            return Err(DriverError::revision_unavailable(format!(
                "revision `{}` is no longer readable",
                req.expected_revision
            )));
        }

        let (offset, length) = match req.range {
            None => (header.content_offset, header.metadata.size),
            Some(range) => (
                header.content_offset + range.start(),
                range
                    .len()
                    .min(header.metadata.size.saturating_sub(range.start())),
            ),
        };

        let mut file = tokio::fs::File::open(&path)
            .await
            .map_err(|e| io_error("open an object for reading", &path, e))?;
        file.seek(SeekFrom::Start(offset))
            .await
            .map_err(|e| io_error("seek an object", &path, e))?;

        let stream: crate::driver::ObjectByteStream =
            Box::pin(MappedReader(ReaderStream::new(file.take(length))));
        Ok(ReadResult {
            metadata: header.metadata,
            stream,
        })
    }

    async fn list(&self, ctx: &DriverContext, req: ListRequest) -> Result<ListPage, DriverError> {
        check_ctx(ctx)?;
        let objects_dir = self.shared.layout.objects_dir();
        let mut dir = tokio::fs::read_dir(&objects_dir)
            .await
            .map_err(|e| io_error("list a namespace", &objects_dir, e))?;

        let mut entries: Vec<(String, ObjectMetadata)> = Vec::new();
        while let Some(entry) = dir
            .next_entry()
            .await
            .map_err(|e| io_error("list a namespace", &objects_dir, e))?
        {
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            if name.ends_with(".tmp") {
                continue;
            }
            let Some(bytes) = unhex(name) else { continue };
            let Ok(key) = String::from_utf8(bytes) else {
                continue;
            };
            // A concurrently deleted object is simply absent from the page.
            if let Some(header) = read_header(&entry.path()).await? {
                entries.push((key, header.metadata));
            }
        }

        // Sorting and folding is CPU over the whole namespace, so it runs on
        // the shared blocking executor rather than a runtime worker thread.
        let blocking = self.shared.blocking.clone();
        blocking.run(move || paginate(entries, &req)).await
    }
}

#[async_trait]
impl WriteNamespace for FsNamespace {
    async fn begin_write(
        &self,
        ctx: &DriverContext,
        req: BeginWrite,
    ) -> Result<Box<dyn crate::driver::StagedWrite>, DriverError> {
        check_ctx(ctx)?;
        let staging_path = self.shared.layout.staging_path(&req.write_id);
        let file = tokio::fs::File::create(&staging_path)
            .await
            .map_err(|e| io_error("stage a write", &staging_path, e))?;
        // Record the attempt before any byte is accepted, so a crash cannot
        // leave an attempt with no discoverable identity.
        self.shared.write_record(&req.write_id, "InFlight").await?;
        Ok(Box::new(FsStagedWrite {
            shared: self.shared.clone(),
            req,
            file: Some(file),
            written: 0,
        }))
    }

    async fn delete(
        &self,
        ctx: &DriverContext,
        req: DeleteRequest,
    ) -> Result<DeleteResult, DriverError> {
        check_ctx(ctx)?;
        let _guard = self.shared.lock.lock().await;
        let path = self.shared.layout.object_path(req.key.as_str());
        let existing = read_header(&path).await?.map(|header| header.metadata);

        check_preconditions(
            &req.preconditions,
            existing.as_ref().and_then(|m| m.etag.as_ref()),
        )?;

        if existing.is_none() {
            // Deleting an absent key is a success with `deleted = false`.
            return Ok(DeleteResult {
                deleted: false,
                removed_revision: None,
            });
        }
        let removed_revision = existing.map(|metadata| metadata.revision);
        tokio::fs::remove_file(&path)
            .await
            .map_err(|e| io_error("delete an object", &path, e))?;
        Ok(DeleteResult {
            deleted: true,
            removed_revision,
        })
    }
}

#[async_trait]
impl CommitRecovery for FsRecovery {
    async fn query(
        &self,
        ctx: &DriverContext,
        write_id: &WriteId,
    ) -> Result<CommitState, DriverError> {
        check_ctx(ctx)?;
        let path = self.shared.layout.recovery_path(write_id);
        match tokio::fs::read_to_string(&path).await {
            Ok(text) => Ok(match text.trim() {
                "Committed" => CommitState::Committed,
                "NotCommittedFinal" => CommitState::NotCommittedFinal,
                "InFlight" => CommitState::InFlight,
                // An unreadable record is not proof of anything.
                _ => CommitState::Unknown,
            }),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(CommitState::Unknown),
            Err(e) => Err(io_error("read a commit record", &path, e)),
        }
    }
}

impl NamespaceShared {
    async fn write_record(&self, write_id: &WriteId, state: &str) -> Result<(), DriverError> {
        let path = self.layout.recovery_path(write_id);
        tokio::fs::write(&path, state)
            .await
            .map_err(|e| io_error("write a commit record", &path, e))
    }

    fn next_revision(&self) -> ObjectRevision {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let seq = self.revision_seq.fetch_add(1, Ordering::Relaxed);
        ObjectRevision::new(format!("fs-{nanos:032x}-{seq:016x}"))
    }
}

/// One in-progress staged write.
struct FsStagedWrite {
    shared: Arc<NamespaceShared>,
    req: BeginWrite,
    file: Option<tokio::fs::File>,
    written: u64,
}

#[async_trait]
impl crate::driver::StagedWrite for FsStagedWrite {
    async fn write_chunk(&mut self, ctx: &DriverContext, bytes: Bytes) -> Result<(), DriverError> {
        check_ctx(ctx)?;
        if let Some(expected) = self.req.expected_length
            && self.written + bytes.len() as u64 > expected
        {
            return Err(DriverError::invalid_argument(
                "payload exceeds the declared content length",
            ));
        }
        let file = self
            .file
            .as_mut()
            .ok_or_else(|| DriverError::internal("staged write is already closed"))?;
        file.write_all(&bytes)
            .await
            .map_err(|e| DriverError::internal(format!("staged write failed: {e}")))?;
        self.written += bytes.len() as u64;
        Ok(())
    }

    async fn commit(
        mut self: Box<Self>,
        ctx: &DriverContext,
        attrs: CommitAttributes,
    ) -> Result<ObjectMetadata, DriverError> {
        check_ctx(ctx)?;

        if let Some(mut file) = self.file.take() {
            file.flush()
                .await
                .map_err(|e| DriverError::internal(format!("staged write flush failed: {e}")))?;
        }

        // A declared length that disagrees with what arrived is a client error
        // *before* publish: nothing is visible and the attempt is final.
        let declared_ok = self
            .req
            .expected_length
            .is_none_or(|expected| expected == attrs.length);
        if !declared_ok || attrs.length != self.written {
            self.shared
                .write_record(&self.req.write_id, "NotCommittedFinal")
                .await?;
            return Err(DriverError::invalid_argument(
                "decoded length did not match the declared content length",
            )
            .with_commit_state(CommitState::NotCommittedFinal));
        }

        let _guard = self.shared.lock.lock().await;

        match self.shared.read_record(&self.req.write_id).await? {
            // Fencing: a fenced attempt can never publish, even a late one.
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

        let path = self.shared.layout.object_path(self.req.key.as_str());
        let existing = read_header(&path).await?.map(|header| header.metadata);
        if let Err(e) = check_preconditions(
            &self.req.preconditions,
            existing.as_ref().and_then(|m| m.etag.as_ref()),
        ) {
            self.shared
                .write_record(&self.req.write_id, "NotCommittedFinal")
                .await?;
            return Err(e.with_commit_state(CommitState::NotCommittedFinal));
        }

        if let Some(max) = self.shared.max_object_size
            && attrs.length > max
        {
            self.shared
                .write_record(&self.req.write_id, "NotCommittedFinal")
                .await?;
            return Err(DriverError::resource_exhausted(format!(
                "object exceeds the driver's declared maximum of {max} bytes"
            ))
            .with_commit_state(CommitState::NotCommittedFinal));
        }
        if let Some(max) = self.shared.max_single_write_size
            && attrs.length > max
        {
            self.shared
                .write_record(&self.req.write_id, "NotCommittedFinal")
                .await?;
            return Err(DriverError::resource_exhausted(format!(
                "write exceeds the driver's declared single-write maximum of {max} bytes"
            ))
            .with_commit_state(CommitState::NotCommittedFinal));
        }

        let metadata = ObjectMetadata {
            key: self.req.key.clone(),
            size: attrs.length,
            etag: attrs.etag.clone(),
            last_modified: SystemTime::now(),
            content: self.req.content.clone(),
            user_metadata: self.req.user_metadata.clone(),
            checksums: attrs.checksums.clone(),
            revision: self.shared.next_revision(),
        };

        // Assemble the whole object as one file so the publish is one rename.
        let tmp = self.shared.layout.object_tmp_path(self.req.key.as_str());
        write_object_file(
            &tmp,
            &self.shared.layout.staging_path(&self.req.write_id),
            &metadata,
        )
        .await?;
        tokio::fs::rename(&tmp, &path)
            .await
            .map_err(|e| io_error("publish an object", &path, e))?;

        // The staged copy is superseded by the published file.
        let staging = self.shared.layout.staging_path(&self.req.write_id);
        let _ = tokio::fs::remove_file(&staging).await;

        // Recorded after the rename: a crash before this leaves `InFlight`,
        // which the service treats as reconcile-needed rather than as proof of
        // failure (`ADR-0003`).
        self.shared
            .write_record(&self.req.write_id, "Committed")
            .await?;
        Ok(metadata)
    }

    async fn abort(self: Box<Self>, _ctx: &DriverContext) -> Result<(), DriverError> {
        // Marking first makes a late commit impossible; then reclaim the bytes.
        self.shared
            .write_record(&self.req.write_id, "NotCommittedFinal")
            .await?;
        let staging = self.shared.layout.staging_path(&self.req.write_id);
        let _ = tokio::fs::remove_file(&staging).await;
        Ok(())
    }
}

impl NamespaceShared {
    async fn read_record(&self, write_id: &WriteId) -> Result<Option<CommitState>, DriverError> {
        let path = self.layout.recovery_path(write_id);
        match tokio::fs::read_to_string(&path).await {
            Ok(text) => Ok(Some(match text.trim() {
                "Committed" => CommitState::Committed,
                "NotCommittedFinal" => CommitState::NotCommittedFinal,
                "InFlight" => CommitState::InFlight,
                _ => CommitState::Unknown,
            })),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(io_error("read a commit record", &path, e)),
        }
    }
}

/// A parsed object header.
struct Header {
    metadata: ObjectMetadata,
    content_offset: u64,
}

/// Reads only an object's metadata, never its bytes.
async fn read_header(path: &Path) -> Result<Option<Header>, DriverError> {
    let mut file = match tokio::fs::File::open(path).await {
        Ok(file) => file,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(io_error("open an object", path, e)),
    };

    let mut len_buf = [0u8; HEADER_LEN as usize];
    file.read_exact(&mut len_buf)
        .await
        .map_err(|e| io_error("read an object header", path, e))?;
    let meta_len = u64::from_le_bytes(len_buf);

    let mut meta_buf = vec![0u8; meta_len as usize];
    file.read_exact(&mut meta_buf)
        .await
        .map_err(|e| io_error("read object metadata", path, e))?;
    let text = String::from_utf8(meta_buf)
        .map_err(|e| DriverError::internal(format!("stored metadata is not utf-8: {e}")))?;
    let stored: StoredMetadata = toml::from_str(&text)
        .map_err(|e| DriverError::internal(format!("stored metadata is invalid: {e}")))?;
    let metadata = meta::decode(stored)?;

    Ok(Some(Header {
        metadata,
        content_offset: HEADER_LEN + meta_len,
    }))
}

/// Writes `metadata` and the staged content into one file and fsyncs it.
///
/// The caller renames it into place, which is the publish point.
async fn write_object_file(
    target: &Path,
    staging: &Path,
    metadata: &ObjectMetadata,
) -> Result<(), DriverError> {
    let text = toml::to_string(&meta::encode(metadata))
        .map_err(|e| DriverError::internal(format!("cannot encode metadata: {e}")))?;
    let meta_bytes = text.into_bytes();

    let mut out = tokio::fs::File::create(target)
        .await
        .map_err(|e| io_error("create an object file", target, e))?;
    out.write_all(&(meta_bytes.len() as u64).to_le_bytes())
        .await
        .map_err(|e| io_error("write an object header", target, e))?;
    out.write_all(&meta_bytes)
        .await
        .map_err(|e| io_error("write object metadata", target, e))?;

    let mut staged = tokio::fs::File::open(staging)
        .await
        .map_err(|e| io_error("read a staged write", staging, e))?;
    tokio::io::copy(&mut staged, &mut out)
        .await
        .map_err(|e| io_error("assemble an object", target, e))?;

    out.flush()
        .await
        .map_err(|e| io_error("flush an object", target, e))?;
    out.sync_all()
        .await
        .map_err(|e| io_error("fsync an object", target, e))?;
    Ok(())
}

/// Evaluates the atomic preconditions against the current object.
fn check_preconditions(
    preconditions: &Preconditions,
    current_etag: Option<&crate::domain::EntityTag>,
) -> Result<(), DriverError> {
    if let Some(tag) = &preconditions.if_match
        && current_etag != Some(tag)
    {
        return Err(DriverError::precondition_failed(
            "If-Match did not match the current object",
        ));
    }
    if preconditions.if_none_match && current_etag.is_some() {
        return Err(DriverError::precondition_failed(
            "If-None-Match: * but the object exists",
        ));
    }
    Ok(())
}

/// A byte stream that maps filesystem errors onto [`DriverError`].
struct MappedReader(ReaderStream<tokio::io::Take<tokio::fs::File>>);

impl Stream for MappedReader {
    type Item = Result<Bytes, DriverError>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        // `ReaderStream` is `Unpin`, so the projection is safe.
        let inner = &mut self.get_mut().0;
        match Pin::new(inner).poll_next(cx) {
            std::task::Poll::Ready(Some(Ok(bytes))) => std::task::Poll::Ready(Some(Ok(bytes))),
            std::task::Poll::Ready(Some(Err(e))) => std::task::Poll::Ready(Some(Err(
                DriverError::new(DriverErrorKind::Internal, "object read failed").with_cause(e),
            ))),
            std::task::Poll::Ready(None) => std::task::Poll::Ready(None),
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}

// --- listing --------------------------------------------------------------

enum Entry {
    Object(Box<ObjectMetadata>),
    Prefix(String),
}

impl Entry {
    fn sort_key(&self) -> &str {
        match self {
            Entry::Object(metadata) => metadata.key.as_str(),
            Entry::Prefix(prefix) => prefix,
        }
    }
}

/// Folds common prefixes and paginates, exactly as the reference driver does.
///
/// Uses the same opaque index cursor (`i:<n>`), so for a static data set no
/// entry is missing or repeated across pages and no common prefix repeats.
fn paginate(entries: Vec<(String, ObjectMetadata)>, req: &ListRequest) -> ListPage {
    let mut ordered = entries;
    ordered.sort_by(|a, b| a.0.cmp(&b.0));

    let prefix = req.prefix().unwrap_or("");
    let delimiter = req.delimiter();

    let mut folded: Vec<Entry> = Vec::new();
    let mut last_prefix: Option<String> = None;
    for (key, metadata) in ordered {
        let Some(remainder) = key.strip_prefix(prefix) else {
            continue;
        };
        match delimiter.and_then(|d| remainder.find(d)) {
            Some(idx) => {
                let common = format!("{prefix}{}", &remainder[..=idx]);
                if last_prefix.as_deref() != Some(common.as_str()) {
                    folded.push(Entry::Prefix(common.clone()));
                    last_prefix = Some(common);
                }
            }
            None => {
                folded.push(Entry::Object(Box::new(metadata)));
                last_prefix = None;
            }
        }
    }

    let start = match req.cursor() {
        Some(cursor) => parse_index_cursor(cursor).unwrap_or(usize::MAX),
        None => match req.start_after() {
            Some(marker) => folded
                .iter()
                .position(|e| e.sort_key() > marker.as_str())
                .unwrap_or(folded.len()),
            None => 0,
        },
    };
    let start = start.min(folded.len());

    let limit = req.limit().get() as usize;
    let end = start.saturating_add(limit).min(folded.len());
    let is_truncated = end < folded.len();

    let mut objects = Vec::new();
    let mut common_prefixes = Vec::new();
    for entry in folded.into_iter().skip(start).take(limit) {
        match entry {
            Entry::Object(metadata) => objects.push(*metadata),
            Entry::Prefix(prefix) => common_prefixes.push(prefix),
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

// --- encoding -------------------------------------------------------------

fn hex(bytes: &[u8]) -> String {
    crate::integrity::hex(bytes)
}

fn unhex(value: &str) -> Option<Vec<u8>> {
    if !value.len().is_multiple_of(2) {
        return None;
    }
    let mut out = Vec::with_capacity(value.len() / 2);
    let bytes = value.as_bytes();
    for pair in bytes.chunks(2) {
        let hi = hex_digit(pair[0])?;
        let lo = hex_digit(pair[1])?;
        out.push((hi << 4) | lo);
    }
    Some(out)
}

fn hex_digit(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    use super::*;

    #[test]
    fn hex_round_trips_arbitrary_bytes() {
        let raw = b"dir/a b+c\x00\xff";
        let decoded = unhex(&hex(raw)).expect("decodes");
        assert_eq!(decoded, raw);
        assert!(unhex("abc").is_none());
        assert!(unhex("zz").is_none());
    }

    fn metadata(key: &str) -> ObjectMetadata {
        ObjectMetadata {
            key: ObjectKey::parse(key).unwrap(),
            size: 1,
            etag: None,
            last_modified: UNIX_EPOCH,
            content: Default::default(),
            user_metadata: Default::default(),
            checksums: Vec::new(),
            revision: ObjectRevision::new("r"),
        }
    }

    #[test]
    fn pagination_folds_prefixes_and_is_stable_across_pages() {
        let entries = vec![
            ("a/1".to_string(), metadata("a/1")),
            ("a/2".to_string(), metadata("a/2")),
            ("b".to_string(), metadata("b")),
            ("c".to_string(), metadata("c")),
        ];
        let req = ListRequest::new(NonZeroU32::new(10).unwrap()).with_delimiter('/');
        let page = paginate(entries.clone(), &req);
        assert_eq!(page.common_prefixes, vec!["a/"]);
        assert_eq!(
            page.objects
                .iter()
                .map(|m| m.key.as_str())
                .collect::<Vec<_>>(),
            vec!["b", "c"]
        );
        assert!(!page.is_truncated);
    }

    #[test]
    fn pagination_resumes_from_its_own_cursor() {
        let entries: Vec<(String, ObjectMetadata)> = (0..5)
            .map(|i| {
                let key = format!("k{i}");
                (key.clone(), metadata(&key))
            })
            .collect();
        let first = paginate(
            entries.clone(),
            &ListRequest::new(NonZeroU32::new(2).unwrap()),
        );
        assert!(first.is_truncated);
        assert_eq!(first.objects.len(), 2);
        let cursor = first.next_cursor.clone().expect("cursor");
        let second = paginate(
            entries,
            &ListRequest::new(NonZeroU32::new(10).unwrap())
                .with_cursor(cursor)
                .expect("cursor"),
        );
        assert_eq!(second.objects[0].key.as_str(), "k2");
        assert!(!second.is_truncated);
    }

    #[test]
    fn pagination_start_after_is_exclusive() {
        let entries: Vec<(String, ObjectMetadata)> = ["a", "b", "c"]
            .iter()
            .map(|k| (k.to_string(), metadata(k)))
            .collect();
        let req = ListRequest::new(NonZeroU32::new(10).unwrap())
            .with_start_after(ObjectKey::parse("a").unwrap())
            .expect("start-after");
        let page = paginate(entries, &req);
        assert_eq!(
            page.objects
                .iter()
                .map(|m| m.key.as_str())
                .collect::<Vec<_>>(),
            vec!["b", "c"]
        );
    }
}
