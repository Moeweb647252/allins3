//! Validated object keys.
//!
//! [`ObjectKey`] is deliberately distinct from any local path type. S3 is a
//! flat object space: a trailing `/` is an ordinary key byte, `a`, `a/` and
//! `a//b` are different keys, and no URI/dot-segment normalisation is applied.

use std::fmt;

use thiserror::Error;

/// Maximum accepted object key length in bytes (spec §4.2).
pub const MAX_KEY_LEN: usize = 1024;

/// An object key could not be accepted as-is.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum KeyError {
    /// The key was empty.
    #[error("object key must not be empty")]
    Empty,
    /// The key exceeded [`MAX_KEY_LEN`] bytes.
    #[error("object key exceeds {MAX_KEY_LEN} bytes")]
    TooLong,
}

/// A validated UTF-8 object key.
///
/// The key is stored exactly as received; no percent-decoding, `//` merging or
/// dot-segment resolution happens here. Decoding is the protocol layer's job and
/// must preserve the original bytes.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ObjectKey(String);

impl ObjectKey {
    /// Validates and wraps an object key.
    pub fn parse(value: impl Into<String>) -> Result<Self, KeyError> {
        let value = value.into();
        if value.is_empty() {
            return Err(KeyError::Empty);
        }
        if value.len() > MAX_KEY_LEN {
            return Err(KeyError::TooLong);
        }
        Ok(Self(value))
    }

    /// Borrows the key as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Borrows the key as raw bytes. Length is measured in UTF-8 bytes.
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }

    /// Returns the key length in bytes.
    pub fn byte_len(&self) -> usize {
        self.0.len()
    }
}

impl fmt::Display for ObjectKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::str::FromStr for ObjectKey {
    type Err = KeyError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preserves_significant_bytes() {
        assert_ne!(
            ObjectKey::parse("a").unwrap(),
            ObjectKey::parse("a/").unwrap()
        );
        assert_ne!(
            ObjectKey::parse("a/b").unwrap(),
            ObjectKey::parse("a//b").unwrap()
        );
        assert_eq!(ObjectKey::parse("a+b").unwrap().as_str(), "a+b");
    }

    #[test]
    fn rejects_empty_and_overlong() {
        assert_eq!(ObjectKey::parse(""), Err(KeyError::Empty));
        assert_eq!(
            ObjectKey::parse("x".repeat(MAX_KEY_LEN + 1)),
            Err(KeyError::TooLong)
        );
        assert!(ObjectKey::parse("x".repeat(MAX_KEY_LEN)).is_ok());
    }

    #[test]
    fn counts_utf8_bytes() {
        let key = ObjectKey::parse("é").unwrap();
        assert_eq!(key.as_str().chars().count(), 1);
        assert_eq!(key.byte_len(), 2);
    }

    #[test]
    fn accepts_control_and_newline() {
        assert!(ObjectKey::parse("a\nb").is_ok());
        assert!(ObjectKey::parse("a\tb").is_ok());
    }
}
