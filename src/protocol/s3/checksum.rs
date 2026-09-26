//! S3 checksum headers: parsing what a request declares, and rendering what an
//! object carries.
//!
//! Spec §6.3: a composite checksum is `base64(digest)-<partcount>`, a
//! full-object one has no suffix, and the coverage is published alongside so a
//! client can tell them apart instead of guessing from the algorithm. Every
//! `x-amz-checksum-*` header is decoded here, so the service never has to know
//! the wire spelling of an algorithm or a coverage.

use crate::domain::{ChecksumAlgorithm, ChecksumType};
use crate::integrity::base64;
use crate::service::multipart::{DeclaredCompletionChecksum, MultipartChecksum};
use crate::service::{ServiceError, TrailerChecksum};

use super::map_error;

/// The wire form of an object's checksums (spec §6.3).
///
/// A composite checksum is `base64(digest)-<partcount>`; a full-object one has
/// no suffix. The coverage is published alongside so a client can tell them
/// apart instead of guessing from the algorithm.
#[derive(Default)]
pub(super) struct ChecksumHeaders {
    pub(super) md5: Option<String>,
    pub(super) crc32: Option<String>,
    pub(super) crc32c: Option<String>,
    pub(super) crc64nvme: Option<String>,
    pub(super) sha1: Option<String>,
    pub(super) sha256: Option<String>,
    pub(super) checksum_type: Option<s3s::dto::ChecksumType>,
}

impl ChecksumHeaders {
    pub(super) fn from_checksums(checksums: &[crate::domain::Checksum]) -> Self {
        use crate::domain::ChecksumAlgorithm as Alg;
        let mut headers = Self::default();
        for checksum in checksums {
            let mut value = base64(checksum.value());
            if let Some(count) = checksum.part_count() {
                value = format!("{value}-{count}");
            }
            match checksum.algorithm() {
                Alg::Md5 => headers.md5 = Some(value),
                Alg::Crc32 => headers.crc32 = Some(value),
                Alg::Crc32c => headers.crc32c = Some(value),
                Alg::Crc64Nvme => headers.crc64nvme = Some(value),
                Alg::Sha1 => headers.sha1 = Some(value),
                Alg::Sha256 => headers.sha256 = Some(value),
            }
            headers.checksum_type = Some(s3s::dto::ChecksumType::from_static(
                checksum.checksum_type().as_str(),
            ));
        }
        headers
    }
}

/// The checksum a request declares it will send in a trailer.
///
/// `s3s` parses `x-amz-trailer` and exposes the trailing headers, but leaves
/// their verification to the handler, so the service is given a reader and
/// re-checks the digest once the body has been consumed (spec §6.3). A request
/// that declares a trailer must deliver it: a body that never produces one (no
/// trailing-headers handle, or no value) is reported as an absent declaration,
/// not skipped.
pub(super) fn request_trailer_checksum<T>(
    request: &s3s::S3Request<T>,
    request_id: &str,
) -> s3s::S3Result<Option<TrailerChecksum>> {
    let Some(name) = declared_trailer_checksum(&request.headers) else {
        return Ok(None);
    };
    let handle = request.trailing_headers.clone();
    let Some(algorithm) = checksum_algorithm_wire_name(&name) else {
        return Err(map_error(
            ServiceError::NotImplemented(format!("unsupported checksum trailer `{name}`")),
            request_id,
        ));
    };
    let header = match hyper::header::HeaderName::from_bytes(name.as_bytes()) {
        Ok(header) => header,
        Err(_) => {
            return Err(map_error(
                ServiceError::InvalidArgument(format!("invalid trailer name `{name}`")),
                request_id,
            ));
        }
    };
    Ok(Some(TrailerChecksum::new(algorithm, move || {
        handle.as_ref()?.read(|headers| {
            headers
                .get(&header)
                .and_then(|value| value.to_str().ok())
                .map(str::to_string)
        })?
    })))
}

/// The `x-amz-checksum-*` name listed in `x-amz-trailer`, if any.
fn declared_trailer_checksum(headers: &hyper::HeaderMap) -> Option<String> {
    let value = headers.get("x-amz-trailer")?.to_str().ok()?;
    value
        .split(',')
        .map(str::trim)
        .find(|name| name.to_ascii_lowercase().starts_with("x-amz-checksum-"))
        .map(|name| name.to_ascii_lowercase())
}

/// The algorithm a `x-amz-checksum-*` header name selects.
fn checksum_algorithm_wire_name(name: &str) -> Option<ChecksumAlgorithm> {
    match name.strip_prefix("x-amz-checksum-")? {
        "crc32" => Some(ChecksumAlgorithm::Crc32),
        "crc32c" => Some(ChecksumAlgorithm::Crc32c),
        "crc64nvme" => Some(ChecksumAlgorithm::Crc64Nvme),
        "sha1" => Some(ChecksumAlgorithm::Sha1),
        "sha256" => Some(ChecksumAlgorithm::Sha256),
        _ => None,
    }
}

/// A checksum algorithm together with the raw value the client declared for it.
pub(super) type DeclaredChecksum = (ChecksumAlgorithm, Vec<u8>);

/// The `x-amz-checksum-*` header fields shared by part-level requests, in the
/// order they are checked.
pub(super) fn part_checksum_fields<'a>(
    crc32: Option<&'a str>,
    crc32c: Option<&'a str>,
    crc64nvme: Option<&'a str>,
    sha1: Option<&'a str>,
    sha256: Option<&'a str>,
) -> [(ChecksumAlgorithm, Option<&'a str>); 5] {
    [
        (ChecksumAlgorithm::Crc32, crc32),
        (ChecksumAlgorithm::Crc32c, crc32c),
        (ChecksumAlgorithm::Crc64Nvme, crc64nvme),
        (ChecksumAlgorithm::Sha1, sha1),
        (ChecksumAlgorithm::Sha256, sha256),
    ]
}

/// Finds the one checksum the client declared, rejecting algorithms this server
/// does not implement rather than silently ignoring them.
pub(super) fn declared_checksum_header<'a>(
    declared: &[(ChecksumAlgorithm, Option<&'a str>)],
    unsupported: &[Option<&'a str>],
    request_id: &str,
) -> s3s::S3Result<Option<(ChecksumAlgorithm, &'a str)>> {
    if unsupported.iter().any(Option::is_some) {
        return Err(map_error(
            ServiceError::NotImplemented("unsupported checksum algorithm".into()),
            request_id,
        ));
    }
    Ok(declared
        .iter()
        .find_map(|(algorithm, value)| value.map(|value| (*algorithm, value))))
}

/// Decodes a declared checksum value (standard base64, no part-count suffix).
pub(super) fn decode_checksum(
    algorithm: ChecksumAlgorithm,
    value: &str,
    request_id: &str,
) -> s3s::S3Result<DeclaredChecksum> {
    let bytes = crate::integrity::decode_base64(value).map_err(|e| {
        map_error(
            ServiceError::InvalidArgument(format!("invalid checksum: {e}")),
            request_id,
        )
    })?;
    Ok((algorithm, bytes))
}

/// Builds the multipart checksum scheme a create request asked for.
pub(super) fn checksum_scheme(
    algorithm: Option<&s3s::dto::ChecksumAlgorithm>,
    checksum_type: Option<&s3s::dto::ChecksumType>,
    request_id: &str,
) -> s3s::S3Result<Option<MultipartChecksum>> {
    let Some(algorithm) = algorithm else {
        if checksum_type.is_some() {
            return Err(map_error(
                ServiceError::InvalidArgument(
                    "a checksum type requires a checksum algorithm".into(),
                ),
                request_id,
            ));
        }
        return Ok(None);
    };
    let algorithm = checksum_algorithm_from(algorithm.as_str(), request_id)?;
    let checksum_type = match checksum_type {
        Some(value) => checksum_type_from(value.as_str(), request_id)?,
        None => algorithm.default_multipart_type(),
    };
    MultipartChecksum::new(algorithm, checksum_type)
        .map(Some)
        .map_err(|e| map_error(e, request_id))
}

/// The whole-object checksum a completion request declared.
///
/// A composite value arrives as `base64(digest)-<partcount>`, so the count is
/// split off before the digest is decoded; the digest is what gets compared.
pub(super) fn complete_checksum(
    input: &s3s::dto::CompleteMultipartUploadInput,
    request_id: &str,
) -> s3s::S3Result<Option<DeclaredCompletionChecksum>> {
    let found = declared_checksum_header(
        &part_checksum_fields(
            input.checksum_crc32.as_deref(),
            input.checksum_crc32c.as_deref(),
            input.checksum_crc64nvme.as_deref(),
            input.checksum_sha1.as_deref(),
            input.checksum_sha256.as_deref(),
        ),
        &[
            input.checksum_sha512.as_deref(),
            input.checksum_xxhash128.as_deref(),
            input.checksum_xxhash3.as_deref(),
            input.checksum_xxhash64.as_deref(),
        ],
        request_id,
    )?;
    let checksum_type = input
        .checksum_type
        .as_ref()
        .map(|value| checksum_type_from(value.as_str(), request_id))
        .transpose()?;
    // A request may declare only the coverage; that constrains the checksum the
    // service must produce without naming a value to compare against.
    let (raw_algorithm, raw_value) = match found {
        Some(found) => found,
        None => {
            return Ok(
                checksum_type.map(|checksum_type| DeclaredCompletionChecksum {
                    algorithm: None,
                    value: None,
                    checksum_type: Some(checksum_type),
                    part_count: None,
                }),
            );
        }
    };
    let (value, part_count) = split_part_count(raw_value);
    let (algorithm, value) = decode_checksum(raw_algorithm, value, request_id)?;
    Ok(Some(DeclaredCompletionChecksum {
        algorithm: Some(algorithm),
        value: Some(value),
        checksum_type,
        part_count,
    }))
}

/// Splits the `-<partcount>` suffix off a composite checksum value.
fn split_part_count(value: &str) -> (&str, Option<u32>) {
    match value.rsplit_once('-') {
        Some((digest, count)) => match count.parse::<u32>() {
            Ok(count) => (digest, Some(count)),
            Err(_) => (value, None),
        },
        None => (value, None),
    }
}

/// The wire name of a checksum algorithm.
pub(super) fn checksum_algorithm_wire(algorithm: ChecksumAlgorithm) -> &'static str {
    match algorithm {
        ChecksumAlgorithm::Md5 => "MD5",
        ChecksumAlgorithm::Crc32 => "CRC32",
        ChecksumAlgorithm::Crc32c => "CRC32C",
        ChecksumAlgorithm::Crc64Nvme => "CRC64NVME",
        ChecksumAlgorithm::Sha1 => "SHA1",
        ChecksumAlgorithm::Sha256 => "SHA256",
    }
}

fn checksum_type_from(value: &str, request_id: &str) -> s3s::S3Result<ChecksumType> {
    match value {
        "COMPOSITE" => Ok(ChecksumType::Composite),
        "FULL_OBJECT" => Ok(ChecksumType::FullObject),
        other => Err(map_error(
            ServiceError::InvalidArgument(format!("unsupported checksum type `{other}`")),
            request_id,
        )),
    }
}

/// Extracts a requested checksum algorithm and any client-declared value.
pub(super) fn parse_checksums(
    input: &s3s::dto::PutObjectInput,
    request_id: &str,
) -> s3s::S3Result<(Option<ChecksumAlgorithm>, Option<DeclaredChecksum>)> {
    let algorithm = input
        .checksum_algorithm
        .as_ref()
        .map(|a| checksum_algorithm_from(a.as_str(), request_id))
        .transpose()?;

    let declared = declared_checksum_header(
        &part_checksum_fields(
            input.checksum_crc32.as_deref(),
            input.checksum_crc32c.as_deref(),
            input.checksum_crc64nvme.as_deref(),
            input.checksum_sha1.as_deref(),
            input.checksum_sha256.as_deref(),
        ),
        &[
            input.checksum_sha512.as_deref(),
            input.checksum_xxhash128.as_deref(),
            input.checksum_xxhash3.as_deref(),
            input.checksum_xxhash64.as_deref(),
        ],
        request_id,
    )?
    .map(|(algorithm, value)| decode_checksum(algorithm, value, request_id))
    .transpose()?;

    Ok((algorithm, declared))
}

pub(super) fn checksum_algorithm_from(
    value: &str,
    request_id: &str,
) -> s3s::S3Result<ChecksumAlgorithm> {
    use crate::domain::ChecksumAlgorithm as Alg;
    match value {
        "CRC32" => Ok(Alg::Crc32),
        "CRC32C" => Ok(Alg::Crc32c),
        "CRC64NVME" => Ok(Alg::Crc64Nvme),
        "SHA1" => Ok(Alg::Sha1),
        "SHA256" => Ok(Alg::Sha256),
        other => Err(map_error(
            ServiceError::NotImplemented(format!("unsupported checksum algorithm `{other}`")),
            request_id,
        )),
    }
}
