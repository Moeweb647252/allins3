//! Checksum algorithms and values.
//!
//! ETag, the integrity checksum and the internal revision are three distinct
//! fields (spec §5.4). This module models the checksum only.
//!
//! Legal algorithm/type combinations follow spec §6.3: `CRC64NVME` has no
//! composite form, and `Content-MD5` is not a general checksum algorithm.

use bytes::Bytes;
use thiserror::Error;

/// A checksum algorithm supported by the first compatibility tier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ChecksumAlgorithm {
    /// `Content-MD5`; only valid for single `PUT`/`UploadPart` and control requests.
    Md5,
    /// CRC32.
    Crc32,
    /// CRC32C.
    Crc32c,
    /// CRC64NVME.
    Crc64Nvme,
    /// SHA-1.
    Sha1,
    /// SHA-256.
    Sha256,
}

impl ChecksumAlgorithm {
    /// The canonical lowercase name, matching the `x-amz-checksum-*` suffix.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Md5 => "md5",
            Self::Crc32 => "crc32",
            Self::Crc32c => "crc32c",
            Self::Crc64Nvme => "crc64nvme",
            Self::Sha1 => "sha1",
            Self::Sha256 => "sha256",
        }
    }

    /// Whether `Content-MD5` semantics apply (not a general checksum).
    pub fn is_content_md5(self) -> bool {
        matches!(self, Self::Md5)
    }
}

/// How a checksum covers a multipart object.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ChecksumType {
    /// A single digest over the whole decoded object.
    FullObject,
    /// A digest derived from ordered part digests.
    Composite,
}

/// A checksum value together with its algorithm and coverage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Checksum {
    algorithm: ChecksumAlgorithm,
    checksum_type: ChecksumType,
    value: Bytes,
}

impl Checksum {
    /// Builds a checksum, rejecting illegal algorithm/type combinations.
    pub fn new(
        algorithm: ChecksumAlgorithm,
        checksum_type: ChecksumType,
        value: impl Into<Bytes>,
    ) -> Result<Self, ChecksumError> {
        let value = value.into();
        if value.is_empty() {
            return Err(ChecksumError::Empty);
        }
        if algorithm.is_content_md5() {
            return Err(ChecksumError::ContentMd5IsNotAChecksum);
        }
        match (algorithm, checksum_type) {
            (ChecksumAlgorithm::Crc64Nvme, ChecksumType::Composite) => {
                Err(ChecksumError::IllegalCombination {
                    algorithm,
                    checksum_type,
                })
            }
            _ => Ok(Self {
                algorithm,
                checksum_type,
                value,
            }),
        }
    }

    /// The algorithm.
    pub fn algorithm(&self) -> ChecksumAlgorithm {
        self.algorithm
    }

    /// The coverage.
    pub fn checksum_type(&self) -> ChecksumType {
        self.checksum_type
    }

    /// The raw checksum bytes.
    pub fn value(&self) -> &Bytes {
        &self.value
    }
}

/// A checksum could not be constructed.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ChecksumError {
    /// The value was empty.
    #[error("checksum value must not be empty")]
    Empty,
    /// `Content-MD5` is not a general checksum algorithm.
    #[error("Content-MD5 is not a general checksum algorithm")]
    ContentMd5IsNotAChecksum,
    /// The algorithm has no such coverage form.
    #[error("illegal checksum combination: {algorithm:?} + {checksum_type:?}")]
    IllegalCombination {
        /// The requested algorithm.
        algorithm: ChecksumAlgorithm,
        /// The requested coverage.
        checksum_type: ChecksumType,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_crc64nvme_composite() {
        let err = Checksum::new(
            ChecksumAlgorithm::Crc64Nvme,
            ChecksumType::Composite,
            vec![1],
        );
        assert!(matches!(err, Err(ChecksumError::IllegalCombination { .. })));
    }

    #[test]
    fn accepts_crc64nvme_full_object() {
        assert!(
            Checksum::new(
                ChecksumAlgorithm::Crc64Nvme,
                ChecksumType::FullObject,
                vec![1]
            )
            .is_ok()
        );
    }

    #[test]
    fn rejects_md5_as_checksum() {
        let err = Checksum::new(ChecksumAlgorithm::Md5, ChecksumType::FullObject, vec![1]);
        assert_eq!(err, Err(ChecksumError::ContentMd5IsNotAChecksum));
    }

    #[test]
    fn rejects_empty_value() {
        let err = Checksum::new(ChecksumAlgorithm::Crc32, ChecksumType::FullObject, vec![]);
        assert_eq!(err, Err(ChecksumError::Empty));
    }
}
