//! The read-only 139 namespace.
//!
//! A namespace is one drive folder. Every file below it becomes an object whose
//! key is its path relative to that folder, so the drive is exposed as-is: real
//! file names, real sizes, the drive's own sha256 digest as the ETag, and its
//! revision token as the driver revision. Nested folders become key prefixes.
//!
//! There is no write face. The drive exposes no hidden staging, no atomic
//! publish and no conditional write, so the namespace declares read-only
//! capabilities and the service never routes a mutation here.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::UNIX_EPOCH;

use async_trait::async_trait;

use crate::domain::{
    ContentProperties, DriverContext, DriverError, EntityTag, ListPage, ListRequest, ObjectKey,
    ObjectMetadata, ObjectRevision, UserMetadata,
};
use crate::driver::{BlockingExecutor, ReadNamespace, ReadRequest, ReadResult};

use super::check_ctx;
use super::client::{FileItem, Yun139Client};
use super::stamp;

/// The read face of one drive folder.
pub(crate) struct Yun139Namespace {
    pub(crate) client: Arc<Yun139Client>,
    pub(crate) blocking: BlockingExecutor,
    /// The drive folder id that backs this namespace.
    pub(crate) folder_id: String,
    /// Ceiling on entries enumerated for one operation.
    pub(crate) max_objects: u64,
}

#[async_trait]
impl ReadNamespace for Yun139Namespace {
    async fn stat(
        &self,
        ctx: &DriverContext,
        key: &ObjectKey,
    ) -> Result<ObjectMetadata, DriverError> {
        check_ctx(ctx)?;
        let item = self
            .resolve(ctx, key.as_str())
            .await?
            .ok_or_else(|| DriverError::not_found(format!("key `{key}` not found")))?;
        metadata_for(key, &item)
    }

    async fn read(&self, ctx: &DriverContext, req: ReadRequest) -> Result<ReadResult, DriverError> {
        check_ctx(ctx)?;
        let item = self
            .resolve(ctx, req.key.as_str())
            .await?
            .ok_or_else(|| DriverError::not_found(format!("key `{}` not found", req.key)))?;

        let revision = revision_for(&item);
        if revision != req.expected_revision {
            return Err(DriverError::revision_unavailable(format!(
                "revision `{}` is no longer readable",
                req.expected_revision
            )));
        }

        // The download URL is minted for whatever revision is current now, so a
        // change between the check above and this call must be detected rather
        // than silently served.
        let info = self.client.download_info(&item.file_id).await?;
        if let Some(rid) = info
            .revision_id
            .as_deref()
            .filter(|value| !value.is_empty())
            && ObjectRevision::new(rid) != req.expected_revision
        {
            return Err(DriverError::revision_unavailable(format!(
                "revision `{}` changed before it could be read",
                req.expected_revision
            )));
        }

        let metadata = metadata_for(&req.key, &item)?;
        let stream = self.client.download_stream(&info.url, req.range).await?;
        Ok(ReadResult { metadata, stream })
    }

    async fn list(&self, ctx: &DriverContext, req: ListRequest) -> Result<ListPage, DriverError> {
        check_ctx(ctx)?;
        let entries = self.walk_all(ctx).await?;
        // Sorting and folding is CPU over the whole namespace; it runs on the
        // shared blocking executor rather than a runtime worker, as the
        // filesystem driver does for the same reason.
        let blocking = self.blocking.clone();
        let request = req.clone();
        blocking.run(move || paginate(entries, &request)).await
    }
}

impl Yun139Namespace {
    /// Every object below the namespace folder, as `(key, metadata)`.
    async fn walk_all(
        &self,
        ctx: &DriverContext,
    ) -> Result<Vec<(String, ObjectMetadata)>, DriverError> {
        let mut out = Vec::new();
        let mut stack: Vec<(String, String)> = vec![(self.folder_id.clone(), String::new())];
        // A folder id may reappear only through a cycle, which the drive does
        // not create; the set makes that explicit rather than looping forever.
        let mut visited: HashSet<String> = HashSet::new();
        let mut seen: u64 = 0;

        while let Some((folder, prefix)) = stack.pop() {
            if !visited.insert(folder.clone()) {
                continue;
            }
            for item in self.children(ctx, &folder).await? {
                seen += 1;
                if seen > self.max_objects {
                    return Err(DriverError::resource_exhausted(format!(
                        "the namespace lists more than {} entries; narrow the deployment or raise `max_objects`",
                        self.max_objects
                    )));
                }
                if item.is_folder() {
                    stack.push((item.file_id.clone(), join(&prefix, &item.name)));
                } else {
                    let key = join(&prefix, &item.name);
                    out.push((key.clone(), metadata_for_key(&key, &item)?));
                }
            }
        }
        Ok(out)
    }

    /// Every child of one folder, following the cursor to the last page.
    async fn children(
        &self,
        ctx: &DriverContext,
        folder_id: &str,
    ) -> Result<Vec<FileItem>, DriverError> {
        let mut items = Vec::new();
        let mut cursor: Option<String> = None;
        loop {
            check_ctx(ctx)?;
            let page = self
                .client
                .list_children(folder_id, self.client.page_size(), cursor.as_deref())
                .await?;
            items.extend(page.items);
            if items.len() as u64 > self.max_objects {
                return Err(DriverError::resource_exhausted(format!(
                    "a drive folder lists more than {} entries",
                    self.max_objects
                )));
            }
            match page.next_page_cursor {
                Some(next) => cursor = Some(next),
                None => break,
            }
        }
        Ok(items)
    }

    /// Resolves a key to its drive entry, walking one folder per path segment.
    async fn resolve(
        &self,
        ctx: &DriverContext,
        key: &str,
    ) -> Result<Option<FileItem>, DriverError> {
        let segments: Vec<&str> = key.split('/').collect();
        if segments.is_empty() || segments.iter().any(|segment| segment.is_empty()) {
            return Ok(None);
        }
        let (last, directories) = segments.split_last().expect("non-empty");

        let mut current = self.folder_id.clone();
        for directory in directories {
            match self.find_child(ctx, &current, directory).await? {
                Some(item) if item.is_folder() => current = item.file_id,
                _ => return Ok(None),
            }
        }
        match self.find_child(ctx, &current, last).await? {
            Some(item) if !item.is_folder() => Ok(Some(item)),
            _ => Ok(None),
        }
    }

    async fn find_child(
        &self,
        ctx: &DriverContext,
        folder_id: &str,
        name: &str,
    ) -> Result<Option<FileItem>, DriverError> {
        Ok(self
            .children(ctx, folder_id)
            .await?
            .into_iter()
            .find(|item| item.name == name))
    }
}

fn join(prefix: &str, name: &str) -> String {
    if prefix.is_empty() {
        name.to_string()
    } else {
        format!("{prefix}/{name}")
    }
}

fn metadata_for_key(key: &str, item: &FileItem) -> Result<ObjectMetadata, DriverError> {
    let key = ObjectKey::parse(key.to_string()).map_err(|e| {
        DriverError::internal(format!("a drive entry produced an invalid object key: {e}"))
    })?;
    metadata_for(&key, item)
}

fn metadata_for(key: &ObjectKey, item: &FileItem) -> Result<ObjectMetadata, DriverError> {
    // The drive's own digest is a stable, quote-free token suitable as an ETag;
    // the revision below, not this, is what a read is pinned to.
    let etag = item
        .content_hash
        .as_deref()
        .filter(|hash| !hash.is_empty())
        .and_then(|hash| EntityTag::new(hash).ok());
    let last_modified = item
        .updated_at
        .as_deref()
        .and_then(stamp::parse_iso8601)
        .map(stamp::to_system_time)
        .unwrap_or(UNIX_EPOCH);
    Ok(ObjectMetadata {
        key: key.clone(),
        size: item.size.unwrap_or(0),
        etag,
        last_modified,
        content: ContentProperties::default(),
        user_metadata: UserMetadata::new(),
        checksums: Vec::new(),
        revision: revision_for(item),
    })
}

fn revision_for(item: &FileItem) -> ObjectRevision {
    let token = item
        .revision_id
        .as_deref()
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| {
            format!(
                "{}@{}",
                item.file_id,
                item.updated_at.as_deref().unwrap_or("")
            )
        });
    ObjectRevision::new(token)
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

/// Folds common prefixes and paginates, with the same opaque index cursor the
/// reference and filesystem drivers use.
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
        next_cursor: is_truncated.then(|| crate::domain::ListCursor::new(format!("i:{end}"))),
        is_truncated,
    }
}

fn parse_index_cursor(cursor: &crate::domain::ListCursor) -> Option<usize> {
    cursor.as_str().strip_prefix("i:")?.parse().ok()
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;
    use std::time::UNIX_EPOCH;

    use super::*;

    fn file_item(name: &str, size: u64, revision: &str) -> FileItem {
        FileItem {
            file_id: format!("id-{name}"),
            name: name.to_string(),
            kind: "file".to_string(),
            size: Some(size),
            content_hash: Some("deadbeef".to_string()),
            revision_id: Some(revision.to_string()),
            updated_at: Some("2026-04-06T15:27:23.787+08:00".to_string()),
        }
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
    fn join_builds_paths() {
        assert_eq!(join("", "a"), "a");
        assert_eq!(join("a", "b"), "a/b");
        assert_eq!(join("a/b", "c"), "a/b/c");
    }

    #[test]
    fn metadata_maps_the_drive_fields() {
        let item = file_item("a.bin", 42, "rev-1");
        let key = ObjectKey::parse("a.bin").unwrap();
        let meta = metadata_for(&key, &item).unwrap();
        assert_eq!(meta.size, 42);
        assert_eq!(meta.revision, ObjectRevision::new("rev-1"));
        assert_eq!(meta.etag.as_ref().map(|t| t.as_str()), Some("deadbeef"));
        assert_ne!(meta.last_modified, UNIX_EPOCH);
    }

    #[test]
    fn a_missing_revision_falls_back_to_identity_fields() {
        let mut item = file_item("a.bin", 1, "ignored");
        item.revision_id = None;
        let revision = revision_for(&item);
        assert!(revision.as_str().starts_with("id-a.bin@"));
    }

    #[test]
    fn pagination_folds_prefixes_and_resumes() {
        let entries = vec![
            ("a/1".to_string(), metadata("a/1")),
            ("a/2".to_string(), metadata("a/2")),
            ("b".to_string(), metadata("b")),
            ("c".to_string(), metadata("c")),
        ];
        let page = paginate(
            entries.clone(),
            &ListRequest::new(NonZeroU32::new(10).unwrap()).with_delimiter('/'),
        );
        assert_eq!(page.common_prefixes, vec!["a/"]);
        assert_eq!(
            page.objects
                .iter()
                .map(|m| m.key.as_str())
                .collect::<Vec<_>>(),
            vec!["b", "c"]
        );
        assert!(!page.is_truncated);

        let first = paginate(entries, &ListRequest::new(NonZeroU32::new(2).unwrap()));
        assert!(first.is_truncated);
        assert_eq!(first.objects.len(), 2);
        let cursor = first.next_cursor.clone().unwrap();
        let second = paginate(
            vec![
                ("a/1".to_string(), metadata("a/1")),
                ("a/2".to_string(), metadata("a/2")),
                ("b".to_string(), metadata("b")),
                ("c".to_string(), metadata("c")),
            ],
            &ListRequest::new(NonZeroU32::new(10).unwrap())
                .with_cursor(cursor)
                .unwrap(),
        );
        assert_eq!(second.objects[0].key.as_str(), "b");
    }
}
