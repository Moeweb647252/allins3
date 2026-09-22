//! Integrity: ETag policy and checksum computation.
//!
//! ETag, integrity checksum and internal revision are three different things
//! (spec §5.4). This module owns the service's ETag policy and the byte-level
//! checksum algorithms.

use bytes::Bytes;
use md5::{Digest as _, Md5};
use sha1::Sha1;
use sha2::Sha256;

use crate::domain::{ChecksumAlgorithm, EntityTag};

/// The unified ETag for a plain (non-multipart) write: MD5 of the decoded
/// object content, hex-encoded and unquoted.
pub fn etag_for_write(content: &[u8]) -> EntityTag {
    let mut hasher = Md5::new();
    hasher.update(content);
    EntityTag::new(hex(&hasher.finalize())).expect("hex is a valid tag")
}

/// The multipart ETag: MD5 of the concatenated part MD5s, suffixed with the
/// part count (`hash-partcount`), per spec §5.4.
pub fn etag_for_multipart(part_md5s: &[[u8; 16]]) -> EntityTag {
    let mut hasher = Md5::new();
    for digest in part_md5s {
        hasher.update(digest);
    }
    let tag = format!("{}-{}", hex(&hasher.finalize()), part_md5s.len());
    EntityTag::new(tag).expect("multipart tag is well formed")
}

/// The raw MD5 digest of a byte slice.
pub fn md5_digest(content: &[u8]) -> [u8; 16] {
    let mut hasher = Md5::new();
    hasher.update(content);
    hasher.finalize().into()
}

/// Computes the raw composite digest for a multipart object.
///
/// A composite checksum is a digest of the ordered **raw part digests**, not a
/// concatenation of their encoded forms (spec §6.3). The caller obtains each
/// part digest with [`compute_checksum`] using the same algorithm, and is
/// responsible for the `-<partcount>` suffix that identifies the wire form.
pub fn composite_checksum(algorithm: ChecksumAlgorithm, part_digests: &[Bytes]) -> Bytes {
    let mut hasher = StreamingDigest::new(algorithm);
    for digest in part_digests {
        hasher.update(digest);
    }
    hasher.finalize()
}

/// Computes the raw digest for a checksum algorithm over a whole object.
pub fn compute_checksum(algorithm: ChecksumAlgorithm, content: &[u8]) -> Bytes {
    match algorithm {
        ChecksumAlgorithm::Md5 => Bytes::copy_from_slice(&md5_digest(content)),
        ChecksumAlgorithm::Crc32 => Bytes::copy_from_slice(&crc32(data(content)).to_be_bytes()),
        ChecksumAlgorithm::Crc32c => Bytes::copy_from_slice(&crc32c(content).to_be_bytes()),
        ChecksumAlgorithm::Crc64Nvme => Bytes::copy_from_slice(&crc64_nvme(content).to_be_bytes()),
        ChecksumAlgorithm::Sha1 => {
            let mut h = Sha1::new();
            h.update(content);
            Bytes::copy_from_slice(&h.finalize())
        }
        ChecksumAlgorithm::Sha256 => {
            let mut h = Sha256::new();
            h.update(content);
            Bytes::copy_from_slice(&h.finalize())
        }
    }
}

fn data(content: &[u8]) -> &[u8] {
    content
}

/// Standard CRC-32 (IEEE).
pub fn crc32(content: &[u8]) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(content);
    hasher.finalize()
}

/// CRC-32C (Castagnoli), reflected polynomial `0x82F63B78`.
pub fn crc32c(content: &[u8]) -> u32 {
    crc_reflected(content, 0x82F6_3B78, 0xFFFF_FFFF, 0xFFFF_FFFF)
}

/// CRC-64/NVME, reflected polynomial `0x9A6C9329AC4BC9B5`.
pub fn crc64_nvme(content: &[u8]) -> u64 {
    crc_reflected_u64(content, 0x9A6C_9329_AC4B_C9B5, u64::MAX, u64::MAX)
}

fn crc_reflected(data: &[u8], poly: u32, init: u32, xorout: u32) -> u32 {
    let mut crc = init;
    for &byte in data {
        crc ^= u32::from(byte);
        for _ in 0..8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (poly & mask);
        }
    }
    crc ^ xorout
}

fn crc_reflected_u64(data: &[u8], poly: u64, init: u64, xorout: u64) -> u64 {
    let mut crc = init;
    for &byte in data {
        crc ^= u64::from(byte);
        for _ in 0..8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (poly & mask);
        }
    }
    crc ^ xorout
}

/// Incremental CRC-32C state.
#[derive(Debug, Clone)]
pub struct Crc32cState {
    crc: u32,
}

impl Default for Crc32cState {
    fn default() -> Self {
        Self { crc: 0xFFFF_FFFF }
    }
}

impl Crc32cState {
    /// Creates a fresh state.
    pub fn new() -> Self {
        Self::default()
    }

    /// Feeds more bytes.
    pub fn update(&mut self, data: &[u8]) {
        let mut crc = self.crc;
        for &byte in data {
            crc ^= u32::from(byte);
            for _ in 0..8 {
                let mask = (crc & 1).wrapping_neg();
                crc = (crc >> 1) ^ (0x82F6_3B78 & mask);
            }
        }
        self.crc = crc;
    }

    /// Returns the final digest.
    pub fn finalize(self) -> u32 {
        self.crc ^ 0xFFFF_FFFF
    }
}

/// Incremental CRC-64/NVME state.
#[derive(Debug, Clone)]
pub struct Crc64NvmeState {
    crc: u64,
}

impl Default for Crc64NvmeState {
    fn default() -> Self {
        Self { crc: u64::MAX }
    }
}

impl Crc64NvmeState {
    /// Creates a fresh state.
    pub fn new() -> Self {
        Self::default()
    }

    /// Feeds more bytes.
    pub fn update(&mut self, data: &[u8]) {
        let mut crc = self.crc;
        for &byte in data {
            crc ^= u64::from(byte);
            for _ in 0..8 {
                let mask = (crc & 1).wrapping_neg();
                crc = (crc >> 1) ^ (0x9A6C_9329_AC4B_C9B5 & mask);
            }
        }
        self.crc = crc;
    }

    /// Returns the final digest.
    pub fn finalize(self) -> u64 {
        self.crc ^ u64::MAX
    }
}

/// A streaming digest for one checksum algorithm.
#[derive(Debug)]
pub enum StreamingDigest {
    /// MD5.
    Md5(Md5),
    /// CRC-32.
    Crc32(crc32fast::Hasher),
    /// CRC-32C.
    Crc32c(Crc32cState),
    /// CRC-64/NVME.
    Crc64Nvme(Crc64NvmeState),
    /// SHA-1.
    Sha1(Sha1),
    /// SHA-256.
    Sha256(Sha256),
}

impl StreamingDigest {
    /// Creates a digest for `algorithm`.
    pub fn new(algorithm: ChecksumAlgorithm) -> Self {
        match algorithm {
            ChecksumAlgorithm::Md5 => Self::Md5(Md5::new()),
            ChecksumAlgorithm::Crc32 => Self::Crc32(crc32fast::Hasher::new()),
            ChecksumAlgorithm::Crc32c => Self::Crc32c(Crc32cState::new()),
            ChecksumAlgorithm::Crc64Nvme => Self::Crc64Nvme(Crc64NvmeState::new()),
            ChecksumAlgorithm::Sha1 => Self::Sha1(Sha1::new()),
            ChecksumAlgorithm::Sha256 => Self::Sha256(Sha256::new()),
        }
    }

    /// Feeds more bytes.
    pub fn update(&mut self, data: &[u8]) {
        match self {
            Self::Md5(h) => h.update(data),
            Self::Crc32(h) => h.update(data),
            Self::Crc32c(h) => h.update(data),
            Self::Crc64Nvme(h) => h.update(data),
            Self::Sha1(h) => h.update(data),
            Self::Sha256(h) => h.update(data),
        }
    }

    /// Returns the raw digest bytes.
    pub fn finalize(self) -> Bytes {
        match self {
            Self::Md5(h) => Bytes::copy_from_slice(&h.finalize()),
            Self::Crc32(h) => Bytes::copy_from_slice(&h.finalize().to_be_bytes()),
            Self::Crc32c(h) => Bytes::copy_from_slice(&h.finalize().to_be_bytes()),
            Self::Crc64Nvme(h) => Bytes::copy_from_slice(&h.finalize().to_be_bytes()),
            Self::Sha1(h) => Bytes::copy_from_slice(&h.finalize()),
            Self::Sha256(h) => Bytes::copy_from_slice(&h.finalize()),
        }
    }
}

/// Hex-encodes bytes.
pub fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(out, "{b:02x}");
    }
    out
}

/// Base64-encodes bytes (standard alphabet, padded) as used by S3 checksum
/// headers.
pub fn base64(bytes: &[u8]) -> String {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

/// Decodes the base64 form S3 uses for checksum values.
pub fn decode_base64(value: &str) -> Result<Vec<u8>, String> {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD
        .decode(value)
        .map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn md5_etag_matches_known_vector() {
        // MD5("hello") = 5d41402abc4b2a76b9719d911017c592
        assert_eq!(
            etag_for_write(b"hello").as_str(),
            "5d41402abc4b2a76b9719d911017c592"
        );
    }

    #[test]
    fn multipart_etag_has_partcount_suffix() {
        let parts = [md5_digest(b"a"), md5_digest(b"b")];
        let tag = etag_for_multipart(&parts);
        assert!(tag.as_str().ends_with("-2"), "got {tag}");
    }

    #[test]
    fn crc32_matches_known_vector() {
        // CRC-32("123456789") = 0xCBF43926
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
    }

    #[test]
    fn crc32c_matches_known_vector() {
        // CRC-32C("123456789") = 0xE3069283
        assert_eq!(crc32c(b"123456789"), 0xE306_9283);
    }

    #[test]
    fn crc64nvme_matches_known_vector() {
        // CRC-64/NVME("123456789") = 0xAE8B14860A799888
        assert_eq!(crc64_nvme(b"123456789"), 0xAE8B_1486_0A79_9888);
    }

    #[test]
    fn composite_checksum_hashes_the_ordered_part_digests() {
        let parts: [&[u8]; 3] = [b"part-one", b"part-two", b"part-three"];
        for algorithm in [
            ChecksumAlgorithm::Crc32,
            ChecksumAlgorithm::Crc32c,
            ChecksumAlgorithm::Sha1,
            ChecksumAlgorithm::Sha256,
        ] {
            let digests: Vec<Bytes> = parts
                .iter()
                .map(|part| compute_checksum(algorithm, part))
                .collect();

            // Independent expectation: the same algorithm over the concatenated
            // raw part digests (no base64, no separators, no truncation).
            let mut concatenated = Vec::new();
            for digest in &digests {
                concatenated.extend_from_slice(digest);
            }
            assert_eq!(
                composite_checksum(algorithm, &digests),
                compute_checksum(algorithm, &concatenated),
                "{algorithm:?} composite mismatch"
            );

            // Order and membership must matter.
            let reversed: Vec<Bytes> = digests.iter().rev().cloned().collect();
            assert_ne!(
                composite_checksum(algorithm, &digests),
                composite_checksum(algorithm, &reversed)
            );
        }
    }

    #[test]
    fn sha256_known_vector() {
        // SHA-256("") = e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
        let digest = compute_checksum(ChecksumAlgorithm::Sha256, b"");
        assert_eq!(
            hex(&digest),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }
}
