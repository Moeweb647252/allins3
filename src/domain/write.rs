//! Write, delete and commit request/response types.

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use thiserror::Error;

use super::checksum::{Checksum, ChecksumAlgorithm};
use super::key::ObjectKey;
use super::metadata::{ContentProperties, EntityTag, UserMetadata};

/// Maximum part number for multipart uploads.
pub const MAX_PART_NUMBER: u16 = 10_000;

/// A part number in the range 1..=10,000.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PartNumber(u16);

impl PartNumber {
    /// Validates a part number.
    pub fn new(value: u16) -> Result<Self, PartNumberError> {
        if value == 0 || value > MAX_PART_NUMBER {
            return Err(PartNumberError(value));
        }
        Ok(Self(value))
    }

    /// The numeric part number.
    pub fn get(self) -> u16 {
        self.0
    }
}

/// A part number was outside 1..=10,000.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("part number {0} is outside 1..={MAX_PART_NUMBER}")]
pub struct PartNumberError(pub u16);

/// Preconditions evaluated atomically with the mutation.
///
/// A driver must evaluate these in the same atomic operation as the publish or
/// delete; a prior `HEAD` followed by an unconditional mutation is not
/// acceptable.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Preconditions {
    /// `If-Match`: the current ETag must equal this value.
    pub if_match: Option<EntityTag>,
    /// `If-None-Match: *`: the object must not exist.
    pub if_none_match: bool,
}

impl Preconditions {
    /// No preconditions.
    pub fn none() -> Self {
        Self::default()
    }

    /// Requires the object to not exist.
    pub fn must_not_exist() -> Self {
        Self {
            if_match: None,
            if_none_match: true,
        }
    }

    /// Requires the current ETag to match.
    pub fn if_match(tag: EntityTag) -> Self {
        Self {
            if_match: Some(tag),
            if_none_match: false,
        }
    }

    /// Whether any precondition is set.
    pub fn is_empty(&self) -> bool {
        self.if_match.is_none() && !self.if_none_match
    }
}

/// A non-reusable identifier for one commit attempt.
///
/// Scoped to a driver instance and namespace. Each new attempt at the same
/// logical operation must use a fresh `WriteId`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct WriteId(String);

impl WriteId {
    /// Wraps an existing identifier (for recovery of persisted records).
    pub fn from_raw(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Borrows the identifier.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Generates a process-unique identifier for a new attempt.
    pub fn generate() -> Self {
        use std::collections::hash_map::RandomState;
        use std::hash::{BuildHasher, Hasher};

        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        // A per-process random seed so two processes cannot mint the same id
        // even if their clocks and counters collide.
        let mut hasher = RandomState::new().build_hasher();
        hasher.write_u64(n);
        let seed = hasher.finish();
        Self(format!("{seed:016x}{nanos:032x}{n:016x}"))
    }
}

impl fmt::Display for WriteId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Everything a driver needs to start a staged write.
#[derive(Debug, Clone)]
pub struct BeginWrite {
    /// The target key.
    pub key: ObjectKey,
    /// Expected decoded length, when known from the request.
    pub expected_length: Option<u64>,
    /// Atomic preconditions.
    pub preconditions: Preconditions,
    /// The attempt identifier.
    pub write_id: WriteId,
    /// HTTP content properties to store.
    pub content: ContentProperties,
    /// User metadata to store.
    pub user_metadata: UserMetadata,
    /// Requested checksum algorithm, if the client asked for one.
    pub checksum_algorithm: Option<ChecksumAlgorithm>,
}

/// Final attributes supplied at commit time.
#[derive(Debug, Clone)]
pub struct CommitAttributes {
    /// Actual decoded length.
    pub length: u64,
    /// Final ETag, when the driver has one.
    pub etag: Option<EntityTag>,
    /// Verified checksums.
    pub checksums: Vec<Checksum>,
}

/// An atomic delete request.
#[derive(Debug, Clone)]
pub struct DeleteRequest {
    /// The target key.
    pub key: ObjectKey,
    /// Atomic preconditions for conditional delete.
    pub preconditions: Preconditions,
}

/// The result of a delete.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DeleteResult {
    /// Whether an object was removed. Deleting an absent object is a success
    /// with `deleted = false`.
    pub deleted: bool,
    /// The revision that was removed, when known.
    pub removed_revision: Option<super::metadata::ObjectRevision>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_ids_are_unique() {
        let a = WriteId::generate();
        let b = WriteId::generate();
        assert_ne!(a, b);
    }

    #[test]
    fn part_number_bounds() {
        assert!(PartNumber::new(0).is_err());
        assert!(PartNumber::new(1).is_ok());
        assert!(PartNumber::new(10_000).is_ok());
        assert!(PartNumber::new(10_001).is_err());
    }

    #[test]
    fn preconditions_constructors() {
        assert!(Preconditions::none().is_empty());
        assert!(!Preconditions::must_not_exist().is_empty());
        let tag = EntityTag::new("e").unwrap();
        assert_eq!(Preconditions::if_match(tag.clone()).if_match, Some(tag));
    }
}
