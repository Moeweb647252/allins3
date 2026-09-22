//! Strongly-typed identifiers.
//!
//! `BucketId`, `DriverInstanceId` and `NamespaceId` are distinct types so a raw
//! string cannot be passed where a specific kind of identifier is expected.
//! None of them are assumed to be a directory, bucket name or database table.

use std::fmt;

use thiserror::Error;

/// Maximum byte length accepted for any identifier.
const MAX_ID_LEN: usize = 255;

/// An identifier was empty, too long or contained a forbidden byte.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum IdError {
    /// The identifier was empty.
    #[error("identifier must not be empty")]
    Empty,
    /// The identifier exceeded [`MAX_ID_LEN`] bytes.
    #[error("identifier exceeds {MAX_ID_LEN} bytes")]
    TooLong,
    /// The identifier contained an ASCII control character.
    #[error("identifier must not contain control characters")]
    ControlCharacter,
}

impl IdError {
    fn check(value: &str) -> Result<(), IdError> {
        if value.is_empty() {
            return Err(IdError::Empty);
        }
        if value.len() > MAX_ID_LEN {
            return Err(IdError::TooLong);
        }
        if value.chars().any(char::is_control) {
            return Err(IdError::ControlCharacter);
        }
        Ok(())
    }
}

macro_rules! id_type {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub struct $name(String);

        impl $name {
            /// Parses and validates an identifier.
            pub fn parse(value: impl Into<String>) -> Result<Self, IdError> {
                let value = value.into();
                IdError::check(&value)?;
                Ok(Self(value))
            }

            /// Borrows the identifier as a string slice.
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl std::str::FromStr for $name {
            type Err = IdError;
            fn from_str(s: &str) -> Result<Self, Self::Err> {
                Self::parse(s)
            }
        }
    };
}

id_type! {
    /// Logical bucket name as exposed to S3 clients.
    BucketId
}

id_type! {
    /// One configured instance of a driver kind, including its connection
    /// resources and isolation space.
    DriverInstanceId
}

id_type! {
    /// An isolation space inside a driver instance. Opaque to the core layer.
    NamespaceId
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_empty() {
        assert_eq!(BucketId::parse(""), Err(IdError::Empty));
    }

    #[test]
    fn rejects_control_characters() {
        assert_eq!(BucketId::parse("a\nb"), Err(IdError::ControlCharacter));
    }

    #[test]
    fn distinguishes_id_types() {
        // Compile-time distinction is the point; runtime round-trip is checked
        // here as a smoke test.
        let b = BucketId::parse("example-a").unwrap();
        assert_eq!(b.as_str(), "example-a");
        assert_eq!(b.to_string(), "example-a");
    }

    #[test]
    fn rejects_overlong() {
        let long = "x".repeat(MAX_ID_LEN + 1);
        assert_eq!(BucketId::parse(long), Err(IdError::TooLong));
    }
}
