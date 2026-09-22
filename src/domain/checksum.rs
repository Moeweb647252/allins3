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

    /// Whether this algorithm has a checksum of the given coverage (spec §6.3).
    ///
    /// `CRC64NVME` is full-object only; `Content-MD5` is never a general
    /// checksum, so it supports neither form.
    pub fn supports(self, checksum_type: ChecksumType) -> bool {
        !matches!(
            (self, checksum_type),
            (Self::Md5, _) | (Self::Crc64Nvme, ChecksumType::Composite)
        )
    }

    /// Whether this algorithm may cover a *multipart* object with this coverage
    /// (spec §6.3).
    ///
    /// The multipart table is narrower than the single-object one: the CRC
    /// family may cover either form, while the SHA family is composite-only.
    pub fn supports_multipart(self, checksum_type: ChecksumType) -> bool {
        match checksum_type {
            ChecksumType::FullObject => {
                matches!(self, Self::Crc32 | Self::Crc32c | Self::Crc64Nvme)
            }
            ChecksumType::Composite => {
                matches!(self, Self::Crc32 | Self::Crc32c | Self::Sha1 | Self::Sha256)
            }
        }
    }

    /// The default coverage for a multipart upload that names no explicit type.
    ///
    /// The CRC family defaults to a single digest over the assembled object;
    /// the SHA family defaults to a digest of the ordered part digests.
    pub fn default_multipart_type(self) -> ChecksumType {
        match self {
            Self::Md5 | Self::Crc32 | Self::Crc32c | Self::Crc64Nvme => ChecksumType::FullObject,
            Self::Sha1 | Self::Sha256 => ChecksumType::Composite,
        }
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

impl ChecksumType {
    /// The wire name used by `x-amz-checksum-type`.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::FullObject => "FULL_OBJECT",
            Self::Composite => "COMPOSITE",
        }
    }
}

/// A checksum value together with its algorithm and coverage.
///
/// `value` is always the raw digest bytes. A composite checksum additionally
/// carries the part count, because its wire form is `base64(digest)-<parts>`;
/// storing the count separately keeps "what was hashed" distinct from "how it
/// is printed", which spec §6.3 requires.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Checksum {
    algorithm: ChecksumAlgorithm,
    checksum_type: ChecksumType,
    value: Bytes,
    part_count: Option<u32>,
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
        if !algorithm.supports(checksum_type) {
            return Err(ChecksumError::IllegalCombination {
                algorithm,
                checksum_type,
            });
        }
        Ok(Self {
            algorithm,
            checksum_type,
            value,
            part_count: None,
        })
    }

    /// Builds a multipart composite checksum: a digest of the ordered part
    /// digests, identified on the wire by the number of parts.
    pub fn composite(
        algorithm: ChecksumAlgorithm,
        value: impl Into<Bytes>,
        part_count: u32,
    ) -> Result<Self, ChecksumError> {
        if part_count == 0 {
            return Err(ChecksumError::ZeroPartCount);
        }
        let mut checksum = Self::new(algorithm, ChecksumType::Composite, value)?;
        checksum.part_count = Some(part_count);
        Ok(checksum)
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

    /// The number of parts a composite checksum covers, if it is composite.
    pub fn part_count(&self) -> Option<u32> {
        self.part_count
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
    /// A composite checksum covered zero parts.
    #[error("a composite checksum must cover at least one part")]
    ZeroPartCount,
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

    #[test]
    fn composite_checksums_carry_their_part_count() {
        let checksum =
            Checksum::composite(ChecksumAlgorithm::Crc32, vec![1, 2, 3, 4], 3).expect("composite");
        assert_eq!(checksum.checksum_type(), ChecksumType::Composite);
        assert_eq!(checksum.part_count(), Some(3));
        assert_eq!(checksum.value().as_ref(), &[1, 2, 3, 4]);

        assert_eq!(
            Checksum::composite(ChecksumAlgorithm::Crc32, vec![1], 0),
            Err(ChecksumError::ZeroPartCount)
        );
        assert!(matches!(
            Checksum::composite(ChecksumAlgorithm::Crc64Nvme, vec![1], 2),
            Err(ChecksumError::IllegalCombination { .. })
        ));
        assert_eq!(
            Checksum::new(ChecksumAlgorithm::Crc32, ChecksumType::FullObject, vec![1])
                .expect("full object")
                .part_count(),
            None
        );
    }

    #[test]
    fn multipart_support_follows_spec_6_3() {
        // CRC may cover either form; the SHA family is composite-only.
        assert!(ChecksumAlgorithm::Crc32.supports_multipart(ChecksumType::FullObject));
        assert!(ChecksumAlgorithm::Crc32c.supports_multipart(ChecksumType::Composite));
        assert!(ChecksumAlgorithm::Crc64Nvme.supports_multipart(ChecksumType::FullObject));
        assert!(ChecksumAlgorithm::Sha256.supports_multipart(ChecksumType::Composite));
        assert!(!ChecksumAlgorithm::Crc64Nvme.supports_multipart(ChecksumType::Composite));
        assert!(!ChecksumAlgorithm::Sha1.supports_multipart(ChecksumType::FullObject));
        assert!(!ChecksumAlgorithm::Md5.supports_multipart(ChecksumType::FullObject));
    }

    #[test]
    fn checksum_type_names_its_wire_form() {
        assert_eq!(ChecksumType::FullObject.as_str(), "FULL_OBJECT");
        assert_eq!(ChecksumType::Composite.as_str(), "COMPOSITE");
    }

    #[test]
    fn multipart_defaults_follow_the_crc_and_sha_families() {
        assert_eq!(
            ChecksumAlgorithm::Crc32.default_multipart_type(),
            ChecksumType::FullObject
        );
        assert_eq!(
            ChecksumAlgorithm::Crc64Nvme.default_multipart_type(),
            ChecksumType::FullObject
        );
        assert_eq!(
            ChecksumAlgorithm::Sha256.default_multipart_type(),
            ChecksumType::Composite
        );
        assert!(ChecksumAlgorithm::Crc64Nvme.supports(ChecksumType::FullObject));
        assert!(!ChecksumAlgorithm::Crc64Nvme.supports(ChecksumType::Composite));
        assert!(!ChecksumAlgorithm::Md5.supports(ChecksumType::FullObject));
    }
}
