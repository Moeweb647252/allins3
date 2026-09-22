//! Ordered listing requests, pages and cursors.

use std::num::NonZeroU32;

use thiserror::Error;

use super::key::ObjectKey;
use super::metadata::ObjectMetadata;

/// Maximum byte length of a listing prefix.
pub const MAX_LIST_PREFIX_LEN: usize = 1024;

/// An opaque, driver-defined continuation cursor.
///
/// The token that travels to the client is built and authenticated by the
/// service layer, not by the driver; the driver only ever sees this opaque
/// value.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ListCursor(String);

impl ListCursor {
    /// Wraps a driver-defined cursor.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Borrows the opaque cursor.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A request for one ordered page of objects.
///
/// `start_after` and `cursor` are mutually exclusive by construction: a valid
/// continuation token decides where to resume, and `start-after` only controls
/// a first request without a token. There is no way to build a `ListRequest`
/// that sets both, so a driver can never "fall back" from an invalid token to
/// `start-after` and silently rescan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListRequest {
    prefix: Option<String>,
    delimiter: Option<char>,
    start_after: Option<ObjectKey>,
    cursor: Option<ListCursor>,
    limit: NonZeroU32,
}

impl ListRequest {
    /// Creates a request for `limit` entries with no prefix or delimiter.
    pub fn new(limit: NonZeroU32) -> Self {
        Self {
            prefix: None,
            delimiter: None,
            start_after: None,
            cursor: None,
            limit,
        }
    }

    /// Sets the key prefix.
    pub fn with_prefix(mut self, prefix: impl Into<String>) -> Result<Self, ListRequestError> {
        let prefix = prefix.into();
        if prefix.len() > MAX_LIST_PREFIX_LEN {
            return Err(ListRequestError::PrefixTooLong);
        }
        self.prefix = Some(prefix);
        Ok(self)
    }

    /// Sets the delimiter used to fold common prefixes.
    pub fn with_delimiter(mut self, delimiter: char) -> Self {
        self.delimiter = Some(delimiter);
        self
    }

    /// Sets the first-request start position.
    pub fn with_start_after(mut self, key: ObjectKey) -> Result<Self, ListRequestError> {
        if self.cursor.is_some() {
            return Err(ListRequestError::StartAfterAndCursorMutuallyExclusive);
        }
        self.start_after = Some(key);
        Ok(self)
    }

    /// Resumes from an opaque cursor.
    pub fn with_cursor(mut self, cursor: ListCursor) -> Result<Self, ListRequestError> {
        if self.start_after.is_some() {
            return Err(ListRequestError::StartAfterAndCursorMutuallyExclusive);
        }
        self.cursor = Some(cursor);
        Ok(self)
    }

    /// The key prefix, if any.
    pub fn prefix(&self) -> Option<&str> {
        self.prefix.as_deref()
    }

    /// The delimiter, if any.
    pub fn delimiter(&self) -> Option<char> {
        self.delimiter
    }

    /// The start-after key, if this is a first request.
    pub fn start_after(&self) -> Option<&ObjectKey> {
        self.start_after.as_ref()
    }

    /// The continuation cursor, if this is a resumed request.
    pub fn cursor(&self) -> Option<&ListCursor> {
        self.cursor.as_ref()
    }

    /// The page size limit.
    pub fn limit(&self) -> NonZeroU32 {
        self.limit
    }
}

/// One page of an ordered listing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListPage {
    /// Objects in ascending key order, sharing no prefix with a folded entry.
    pub objects: Vec<ObjectMetadata>,
    /// Folded common prefixes, in ascending order.
    pub common_prefixes: Vec<String>,
    /// Cursor for the next page, absent when the listing is complete.
    pub next_cursor: Option<ListCursor>,
    /// Whether more entries follow.
    pub is_truncated: bool,
}

impl ListPage {
    /// An empty, complete page.
    pub fn empty() -> Self {
        Self {
            objects: Vec::new(),
            common_prefixes: Vec::new(),
            next_cursor: None,
            is_truncated: false,
        }
    }

    /// The number of quota slots consumed (objects plus folded prefixes).
    pub fn entry_count(&self) -> usize {
        self.objects.len() + self.common_prefixes.len()
    }
}

/// A listing request could not be built.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ListRequestError {
    /// `start-after` and a continuation cursor were both supplied.
    #[error("`start-after` and a continuation cursor are mutually exclusive")]
    StartAfterAndCursorMutuallyExclusive,
    /// The prefix was too long.
    #[error("listing prefix exceeds {MAX_LIST_PREFIX_LEN} bytes")]
    PrefixTooLong,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limit(n: u32) -> NonZeroU32 {
        NonZeroU32::new(n).unwrap()
    }

    #[test]
    fn start_after_and_cursor_are_mutually_exclusive() {
        let key = ObjectKey::parse("a").unwrap();
        let cursor = ListCursor::new("c1");
        let err = ListRequest::new(limit(10))
            .with_start_after(key)
            .unwrap()
            .with_cursor(cursor);
        assert_eq!(
            err,
            Err(ListRequestError::StartAfterAndCursorMutuallyExclusive)
        );

        let key = ObjectKey::parse("a").unwrap();
        let err = ListRequest::new(limit(10))
            .with_cursor(ListCursor::new("c1"))
            .unwrap()
            .with_start_after(key);
        assert_eq!(
            err,
            Err(ListRequestError::StartAfterAndCursorMutuallyExclusive)
        );
    }

    #[test]
    fn prefix_length_limit() {
        let long = "x".repeat(MAX_LIST_PREFIX_LEN + 1);
        assert_eq!(
            ListRequest::new(limit(10)).with_prefix(long),
            Err(ListRequestError::PrefixTooLong)
        );
    }

    #[test]
    fn entry_count_counts_prefixes() {
        let page = ListPage {
            objects: Vec::new(),
            common_prefixes: vec!["a/".into(), "b/".into()],
            next_cursor: None,
            is_truncated: false,
        };
        assert_eq!(page.entry_count(), 2);
    }
}
