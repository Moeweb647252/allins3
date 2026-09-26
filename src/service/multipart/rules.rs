//! The rules both multipart routes run.
//!
//! The coordinator keeps its parts in the staging ledger and a native upload
//! keeps them in the driver, but the *meaning* of a completion manifest, a
//! checksum scheme and a `ListMultipartUploads` marker must not depend on which
//! route served the upload. Everything here is shared by both: a marker parser,
//! the scheme resolver, per-part digest handling, and the one manifest
//! validation. A rule in this file cannot be applied on one route and forgotten
//! on the other.

use std::collections::BTreeMap;

use bytes::Bytes;

use crate::domain::{ChecksumAlgorithm, EntityTag, ObjectKey};
use crate::driver::{PartInfo, UploadId};
use crate::integrity::StreamingDigest;
use crate::service::ServiceError;
use crate::service::staging::StoredPart;

use super::{
    CompletionPart, DeclaredCompletionChecksum, MIN_PART_SIZE, MultipartChecksum, PartChecksum,
    PartChecksums,
};

/// The `(key, upload id)` a `ListMultipartUploads` page continues from.
///
/// S3 defines the marker as a pair: the upload-id marker only means anything
/// together with its key marker, and an empty value means "from the start".
/// The service resolves that rule once, here, so the coordinated and native
/// routes cannot disagree about what a partial or empty marker selects.
pub(super) fn upload_marker(
    key_marker: Option<&str>,
    upload_id_marker: Option<&str>,
) -> Result<Option<(ObjectKey, UploadId)>, ServiceError> {
    let (Some(key), Some(upload_id)) = (key_marker, upload_id_marker) else {
        return Ok(None);
    };
    if key.is_empty() || upload_id.is_empty() {
        return Ok(None);
    }
    let key = ObjectKey::parse(key).map_err(|e| ServiceError::InvalidArgument(e.to_string()))?;
    Ok(Some((key, UploadId::from_raw(upload_id))))
}

/// Resolves the checksum scheme a completion publishes under.
///
/// The upload's own scheme, unless the upload was created without one and the
/// completion request names an algorithm. A declaration can constrain the
/// upload's scheme — its algorithm and coverage must agree — but never change
/// it, and a coverage alone cannot supply an algorithm. Both routes resolve it
/// here, so they accept and reject the same declarations.
pub(super) fn resolve_completion_scheme(
    upload: Option<MultipartChecksum>,
    declared: &Option<DeclaredCompletionChecksum>,
) -> Result<Option<MultipartChecksum>, ServiceError> {
    match (upload, declared) {
        (Some(scheme), Some(declared)) => {
            if let Some(algorithm) = declared.algorithm
                && algorithm != scheme.algorithm
            {
                return Err(ServiceError::InvalidRequest(format!(
                    "the completion checksum algorithm `{}` does not match the upload's `{}`",
                    algorithm.as_str(),
                    scheme.algorithm.as_str()
                )));
            }
            if let Some(declared_type) = declared.checksum_type
                && declared_type != scheme.checksum_type
            {
                return Err(ServiceError::InvalidRequest(format!(
                    "the completion checksum type `{}` does not match the upload's `{}`",
                    declared_type.as_str(),
                    scheme.checksum_type.as_str()
                )));
            }
            Ok(Some(scheme))
        }
        (Some(scheme), None) => Ok(Some(scheme)),
        (None, Some(declared)) => {
            let Some(algorithm) = declared.algorithm else {
                return Err(ServiceError::InvalidRequest(
                    "a completion checksum type requires a checksum algorithm".into(),
                ));
            };
            Ok(Some(MultipartChecksum::new(
                algorithm,
                declared
                    .checksum_type
                    .unwrap_or_else(|| algorithm.default_multipart_type()),
            )?))
        }
        (None, None) => Ok(None),
    }
}

/// Finalises a part's digest under `scheme` and verifies any value the client
/// declared for it.
///
/// A value declared in a header, or in a trailer that only became readable once
/// the body was consumed, must match the bytes received; a part is never staged
/// when it does not. A trailer the client declared but never sent is a mismatch
/// too. When a request carries both, the trailer is the declaration that was
/// verified: it is the one the body framing carries, and the header value only
/// selected the algorithm. Both routes share this so a part cannot be verified
/// one way natively and another through the coordinator.
pub(super) fn finish_part_checksum(
    digest: Option<StreamingDigest>,
    scheme: Option<MultipartChecksum>,
    checksums: &PartChecksums,
) -> Result<Option<PartChecksum>, ServiceError> {
    let (Some(hasher), Some(scheme)) = (digest, scheme) else {
        return Ok(None);
    };
    let value = hasher.finalize();
    let declared = match &checksums.trailer {
        None => checksums.declared.clone(),
        Some(trailer) => match trailer.value() {
            Some(value) => Some((scheme.algorithm, value)),
            None => {
                return Err(ServiceError::BadDigest(
                    "the checksum you declared in a trailer was not sent".into(),
                ));
            }
        },
    };
    if let Some((_, expected)) = &declared
        && value.as_ref() != expected.as_slice()
    {
        return Err(ServiceError::BadDigest(
            "the checksum you specified did not match what we received".into(),
        ));
    }
    Ok(Some(PartChecksum {
        algorithm: scheme.algorithm,
        value,
    }))
}

/// The checksum of one stored part for `algorithm`: the digest computed when
/// the part was uploaded, or a freshly computed one for a part that predates the
/// upload's scheme (or was replaced under a different one).
pub(super) fn part_checksum(part: &StoredPart, algorithm: ChecksumAlgorithm) -> Bytes {
    match &part.checksum {
        Some(checksum) if checksum.algorithm == algorithm => checksum.value.clone(),
        _ => crate::integrity::compute_checksum(algorithm, &part.data),
    }
}

/// One part an upload holds, as a manifest check needs to see it.
///
/// The two routes keep their parts in different places — the coordinator's
/// staging ledger, the driver's upload session — so the checks below read them
/// through this view. Producing the map is the only thing a route has to do;
/// the validation itself is shared, so a manifest cannot be accepted by one
/// route and refused by the other.
pub(super) trait UploadedPart {
    /// The number the upload stored the part under.
    fn part_number(&self) -> u16;
    /// The part's ETag, when the upload has one.
    fn part_etag(&self) -> Option<EntityTag>;
    /// The part's size in bytes.
    fn part_size(&self) -> u64;
    /// The digest of the part's bytes under `algorithm`, when this route can
    /// produce one.
    fn part_digest(&self, algorithm: ChecksumAlgorithm) -> Option<Bytes>;
}

impl UploadedPart for StoredPart {
    fn part_number(&self) -> u16 {
        self.number
    }

    fn part_etag(&self) -> Option<EntityTag> {
        Some(StoredPart::etag(self))
    }

    fn part_size(&self) -> u64 {
        self.size
    }

    fn part_digest(&self, algorithm: ChecksumAlgorithm) -> Option<Bytes> {
        // Staging holds the bytes, so any algorithm can be answered: from the
        // digest taken when the part was received, or by digesting those bytes
        // now, which is what a part predating the upload's scheme needs.
        Some(part_checksum(self, algorithm))
    }
}

impl UploadedPart for PartInfo {
    fn part_number(&self) -> u16 {
        self.part_number.get()
    }

    fn part_etag(&self) -> Option<EntityTag> {
        self.etag.clone()
    }

    fn part_size(&self) -> u64 {
        self.size
    }

    fn part_digest(&self, algorithm: ChecksumAlgorithm) -> Option<Bytes> {
        // A driver digests its parts under the upload's scheme, so that is the
        // only algorithm it reports a digest for; any other leaves nothing to
        // compare a declaration against.
        self.checksum
            .as_ref()
            .filter(|checksum| checksum.algorithm() == algorithm)
            .map(|checksum| checksum.value().clone())
    }
}

/// Validates a completion manifest against the parts an upload actually holds,
/// and returns those parts in manifest order.
///
/// This is the single manifest check both routes run before either assembles
/// anything: every part the client named must exist and carry the ETag it named,
/// no part may be listed twice, the list must be in ascending part-number order,
/// every part but the last must meet [`MIN_PART_SIZE`], and a declared per-part
/// checksum must match that part's digest. `scheme` is the algorithm the upload
/// runs under — fixed when it was created, or by this completion — and a part
/// digest is only ever computed under it, so a declaration under another
/// algorithm (or on an upload with no scheme) is refused rather than checked
/// against a value that is not the upload's.
pub(super) fn validate_manifest<'a, P: UploadedPart>(
    manifest: &[CompletionPart],
    scheme: Option<MultipartChecksum>,
    parts: &'a BTreeMap<u16, P>,
) -> Result<Vec<&'a P>, ServiceError> {
    if manifest.is_empty() {
        return Err(ServiceError::MalformedXml(
            "the completion manifest must contain at least one part".into(),
        ));
    }

    let mut seen = std::collections::HashSet::new();
    let mut ordered: Vec<&P> = Vec::with_capacity(manifest.len());
    for entry in manifest {
        let number = entry.number;
        if !seen.insert(number) {
            return Err(ServiceError::MalformedXml(
                "the completion manifest contains duplicate part numbers".into(),
            ));
        }
        let part = parts.get(&number).ok_or_else(|| {
            ServiceError::InvalidArgument(format!("part {number} was not uploaded for this upload"))
        })?;
        if let Some(expected) = &entry.etag
            && part.part_etag().as_ref() != Some(expected)
        {
            return Err(ServiceError::InvalidArgument(format!(
                "part {number} ETag does not match the uploaded part"
            )));
        }
        if let Some((algorithm, expected)) = &entry.checksum {
            if !scheme.is_some_and(|scheme| scheme.algorithm == *algorithm) {
                return Err(ServiceError::InvalidRequest(format!(
                    "the part checksum algorithm `{}` does not match the upload's",
                    algorithm.as_str()
                )));
            }
            match part.part_digest(*algorithm) {
                // A declared value must match the bytes that were received; an
                // unverified part is never assembled into the object.
                Some(actual) if actual.as_ref() == expected.as_slice() => {}
                Some(_) => {
                    return Err(ServiceError::BadDigest(format!(
                        "the checksum you specified for part {number} did not match what we received"
                    )));
                }
                // Only a route that cannot digest the part reaches this: the
                // driver is required to report each part's digest under the
                // upload's scheme, so a missing one leaves nothing to verify.
                None => {
                    return Err(ServiceError::InvalidRequest(format!(
                        "the driver reported no checksum for part {number}"
                    )));
                }
            }
        }
        ordered.push(part);
    }

    for window in ordered.windows(2) {
        if window[0].part_number() >= window[1].part_number() {
            return Err(ServiceError::InvalidArgument(
                "parts must be listed in ascending part-number order".into(),
            ));
        }
    }
    for part in &ordered[..ordered.len() - 1] {
        if part.part_size() < MIN_PART_SIZE {
            return Err(ServiceError::InvalidArgument(
                "each part except the last must be at least 5 MiB".into(),
            ));
        }
    }

    Ok(ordered)
}
