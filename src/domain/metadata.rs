//! Object metadata, revisions, content properties and user metadata.

use std::collections::BTreeMap;
use std::fmt;
use std::time::SystemTime;

use thiserror::Error;

use super::checksum::Checksum;
use super::key::ObjectKey;

/// Maximum number of user metadata entries the service accepts.
pub const MAX_METADATA_COUNT: usize = 100;
/// Maximum total bytes of user metadata keys and values the service accepts.
pub const MAX_METADATA_BYTES: usize = 8 * 1024;
/// Maximum byte length of a single user metadata key.
pub const MAX_USER_METADATA_KEY_LEN: usize = 128;

/// An opaque, driver-defined revision identifier.
///
/// A revision binds one concrete (content, metadata) pair. It is **not** an
/// ETag and **not** an S3 `versionId`. A metadata-only update must yield a new
/// revision even when the ETag is unchanged.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ObjectRevision(String);

impl ObjectRevision {
    /// Wraps a driver-defined revision token.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Borrows the opaque token.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ObjectRevision {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// An HTTP entity tag, stored without the surrounding quotes.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct EntityTag(String);

impl EntityTag {
    /// Builds an entity tag from its unquoted value.
    pub fn new(value: impl Into<String>) -> Result<Self, MetadataError> {
        let value = value.into();
        if value.is_empty() {
            return Err(MetadataError::EmptyEntityTag);
        }
        if value.contains('"') {
            return Err(MetadataError::EntityTagContainsQuote);
        }
        Ok(Self(value))
    }

    /// The unquoted tag value.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The wire form, including quotes.
    pub fn quoted(&self) -> String {
        format!("\"{}\"", self.0)
    }
}

impl fmt::Display for EntityTag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.quoted())
    }
}

/// HTTP content properties preserved per object.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ContentProperties {
    /// `Content-Type`.
    pub content_type: Option<String>,
    /// `Content-Encoding` after transfer decoding; never `aws-chunked`.
    pub content_encoding: Option<String>,
    /// `Content-Language`.
    pub content_language: Option<String>,
    /// `Content-Disposition`.
    pub content_disposition: Option<String>,
    /// `Cache-Control`.
    pub cache_control: Option<String>,
    /// `Expires`, at second granularity.
    pub expires: Option<SystemTime>,
}

/// Case-insensitive user metadata (`x-amz-meta-*`).
///
/// Keys are canonicalised to lowercase so lookups are consistent. Values are
/// stored verbatim; they are never truncated to fake success.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct UserMetadata(BTreeMap<String, String>);

impl UserMetadata {
    /// Creates an empty set.
    pub fn new() -> Self {
        Self::default()
    }

    /// Inserts a key/value pair, enforcing the count and size limits.
    pub fn insert(
        &mut self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Result<(), MetadataError> {
        let key = key.into().to_ascii_lowercase();
        let value = value.into();
        if key.is_empty() {
            return Err(MetadataError::EmptyMetadataKey);
        }
        if key.len() > MAX_USER_METADATA_KEY_LEN {
            return Err(MetadataError::MetadataKeyTooLong);
        }
        if key.chars().any(char::is_control) || value.chars().any(char::is_control) {
            return Err(MetadataError::MetadataContainsControlCharacter);
        }

        // Replacing an existing key does not increase the count.
        if !self.0.contains_key(&key) && self.0.len() >= MAX_METADATA_COUNT {
            return Err(MetadataError::TooManyMetadataEntries);
        }
        let prospective = self.byte_len_with(&key, &value);
        if prospective > MAX_METADATA_BYTES {
            return Err(MetadataError::MetadataTooLarge);
        }
        self.0.insert(key, value);
        Ok(())
    }

    /// Looks up a value by key, case-insensitively.
    pub fn get(&self, key: &str) -> Option<&str> {
        self.0.get(&key.to_ascii_lowercase()).map(String::as_str)
    }

    /// Iterates over canonicalised (lowercase) keys and values.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.0.iter().map(|(k, v)| (k.as_str(), v.as_str()))
    }

    /// Number of entries.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Whether there are no entries.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    fn byte_len_with(&self, key: &str, value: &str) -> usize {
        self.0
            .iter()
            .filter(|(k, _)| k.as_str() != key)
            .map(|(k, v)| k.len() + v.len())
            .sum::<usize>()
            + key.len()
            + value.len()
    }
}

/// Complete metadata for one object revision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectMetadata {
    /// The object key.
    pub key: ObjectKey,
    /// Object length in bytes.
    pub size: u64,
    /// ETag, if the backing store exposes one. Never used as the revision.
    pub etag: Option<EntityTag>,
    /// Last modification time.
    pub last_modified: SystemTime,
    /// HTTP content properties.
    pub content: ContentProperties,
    /// Case-insensitive user metadata.
    pub user_metadata: UserMetadata,
    /// Integrity checksums stored with this revision.
    pub checksums: Vec<Checksum>,
    /// The driver-internal revision binding content and metadata.
    pub revision: ObjectRevision,
}

/// Metadata could not be accepted.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum MetadataError {
    /// A metadata key was empty.
    #[error("metadata key must not be empty")]
    EmptyMetadataKey,
    /// A metadata key exceeded the per-key limit.
    #[error("metadata key exceeds {MAX_USER_METADATA_KEY_LEN} bytes")]
    MetadataKeyTooLong,
    /// The metadata count limit was exceeded.
    #[error("too many user metadata entries (max {MAX_METADATA_COUNT})")]
    TooManyMetadataEntries,
    /// The total metadata size limit was exceeded.
    #[error("user metadata exceeds {MAX_METADATA_BYTES} bytes")]
    MetadataTooLarge,
    /// A metadata key or value contained a control character.
    #[error("metadata must not contain control characters")]
    MetadataContainsControlCharacter,
    /// An entity tag was empty.
    #[error("entity tag must not be empty")]
    EmptyEntityTag,
    /// An entity tag contained a quote.
    #[error("entity tag must not contain a quote character")]
    EntityTagContainsQuote,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta(pairs: &[(&str, &str)]) -> UserMetadata {
        let mut m = UserMetadata::new();
        for (k, v) in pairs {
            m.insert(*k, *v).unwrap();
        }
        m
    }

    #[test]
    fn metadata_keys_are_case_insensitive() {
        let m = meta(&[("Foo", "1")]);
        assert_eq!(m.get("foo"), Some("1"));
        assert_eq!(m.get("FOO"), Some("1"));
    }

    #[test]
    fn replacing_a_key_does_not_grow_count() {
        let mut m = meta(&[("foo", "1")]);
        m.insert("FOO", "2").unwrap();
        assert_eq!(m.len(), 1);
        assert_eq!(m.get("foo"), Some("2"));
    }

    #[test]
    fn enforces_count_limit() {
        let mut m = UserMetadata::new();
        for i in 0..MAX_METADATA_COUNT {
            m.insert(format!("k{i}"), "v").unwrap();
        }
        assert_eq!(
            m.insert("overflow", "v"),
            Err(MetadataError::TooManyMetadataEntries)
        );
    }

    #[test]
    fn enforces_size_limit() {
        let mut m = UserMetadata::new();
        let big = "v".repeat(MAX_METADATA_BYTES);
        assert_eq!(m.insert("k", big), Err(MetadataError::MetadataTooLarge));
    }

    #[test]
    fn entity_tag_quoting() {
        let t = EntityTag::new("abc").unwrap();
        assert_eq!(t.as_str(), "abc");
        assert_eq!(t.to_string(), "\"abc\"");
        assert_eq!(EntityTag::new(""), Err(MetadataError::EmptyEntityTag));
        assert_eq!(
            EntityTag::new("a\"b"),
            Err(MetadataError::EntityTagContainsQuote)
        );
    }

    #[test]
    fn revision_distinguishes_equal_etag() {
        let a = ObjectRevision::new("rev-1");
        let b = ObjectRevision::new("rev-2");
        assert_ne!(a, b);
    }
}
