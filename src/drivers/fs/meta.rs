//! The on-disk metadata codec for the filesystem driver.
//!
//! Domain types carry no `serde` derives on purpose — they are the interfaces'
//! vocabulary, not a storage format. So the driver keeps its own explicit DTOs
//! and converts at the boundary. A field added to `ObjectMetadata` is a compile
//! error here until it is persisted, which is exactly the review the metadata
//! ownership rule (`ADR-0002`) wants: the driver owns the durable form of every
//! field it returns.
//!
//! Times are stored as nanoseconds since the Unix epoch, so a metadata
//! round-trip is exact. A pre-epoch `SystemTime` is unrepresentable in this
//! format; the service never produces one, and the decoder maps the sentinel
//! back to the epoch rather than failing a whole object over a timestamp.

use std::collections::BTreeMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::Engine as _;
use serde::{Deserialize, Serialize};

use crate::domain::{
    Checksum, ChecksumAlgorithm, ChecksumType, ContentProperties, DriverError, EntityTag,
    ObjectKey, ObjectMetadata, ObjectRevision, UserMetadata,
};

/// The durable form of one object's metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct StoredMetadata {
    pub(crate) key: String,
    pub(crate) size: u64,
    pub(crate) etag: Option<String>,
    pub(crate) last_modified_nanos: u64,
    pub(crate) content_type: Option<String>,
    pub(crate) content_encoding: Option<String>,
    pub(crate) content_language: Option<String>,
    pub(crate) content_disposition: Option<String>,
    pub(crate) cache_control: Option<String>,
    pub(crate) expires_nanos: Option<u64>,
    pub(crate) user_metadata: BTreeMap<String, String>,
    pub(crate) checksums: Vec<StoredChecksum>,
    pub(crate) revision: String,
}

/// The durable form of one checksum.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct StoredChecksum {
    algorithm: String,
    checksum_type: String,
    /// Base64 of the raw digest bytes.
    value: String,
    part_count: Option<u32>,
}

/// Converts domain metadata into its durable form.
pub(crate) fn encode(metadata: &ObjectMetadata) -> StoredMetadata {
    StoredMetadata {
        key: metadata.key.to_string(),
        size: metadata.size,
        etag: metadata.etag.as_ref().map(|tag| tag.as_str().to_string()),
        last_modified_nanos: to_nanos(metadata.last_modified),
        content_type: metadata.content.content_type.clone(),
        content_encoding: metadata.content.content_encoding.clone(),
        content_language: metadata.content.content_language.clone(),
        content_disposition: metadata.content.content_disposition.clone(),
        cache_control: metadata.content.cache_control.clone(),
        expires_nanos: metadata.content.expires.map(to_nanos),
        user_metadata: metadata
            .user_metadata
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
        checksums: metadata.checksums.iter().map(encode_checksum).collect(),
        revision: metadata.revision.as_str().to_string(),
    }
}

fn encode_checksum(checksum: &Checksum) -> StoredChecksum {
    StoredChecksum {
        algorithm: checksum.algorithm().as_str().to_string(),
        checksum_type: checksum.checksum_type().as_str().to_string(),
        value: base64::engine::general_purpose::STANDARD.encode(checksum.value()),
        part_count: checksum.part_count(),
    }
}

/// Rebuilds domain metadata from its durable form.
///
/// A corrupt or unreadable stored record is reported as an internal error
/// rather than a `NotFound`: the object file exists, so pretending it does not
/// would silently hide data loss.
pub(crate) fn decode(stored: StoredMetadata) -> Result<ObjectMetadata, DriverError> {
    let key = ObjectKey::parse(stored.key.clone())
        .map_err(|e| DriverError::internal(format!("stored key is invalid: {e}")))?;
    let etag = match stored.etag {
        Some(value) => Some(
            EntityTag::new(value)
                .map_err(|e| DriverError::internal(format!("stored etag is invalid: {e}")))?,
        ),
        None => None,
    };

    let mut user_metadata = UserMetadata::new();
    for (k, v) in stored.user_metadata {
        user_metadata
            .insert(k, v)
            .map_err(|e| DriverError::internal(format!("stored user metadata is invalid: {e}")))?;
    }

    let mut checksums = Vec::with_capacity(stored.checksums.len());
    for entry in stored.checksums {
        checksums.push(decode_checksum(entry)?);
    }

    Ok(ObjectMetadata {
        key,
        size: stored.size,
        etag,
        last_modified: from_nanos(stored.last_modified_nanos),
        content: ContentProperties {
            content_type: stored.content_type,
            content_encoding: stored.content_encoding,
            content_language: stored.content_language,
            content_disposition: stored.content_disposition,
            cache_control: stored.cache_control,
            expires: stored.expires_nanos.map(from_nanos),
        },
        user_metadata,
        checksums,
        revision: ObjectRevision::new(stored.revision),
    })
}

fn decode_checksum(stored: StoredChecksum) -> Result<Checksum, DriverError> {
    let algorithm = parse_algorithm(&stored.algorithm)?;
    let checksum_type = parse_checksum_type(&stored.checksum_type)?;
    let value = crate::integrity::decode_base64(&stored.value)
        .map_err(|e| DriverError::internal(format!("stored checksum value is invalid: {e}")))?;
    match (checksum_type, stored.part_count) {
        (ChecksumType::Composite, Some(part_count)) => {
            Checksum::composite(algorithm, value, part_count)
                .map_err(|e| DriverError::internal(format!("stored checksum is invalid: {e}")))
        }
        (ChecksumType::Composite, None) => Err(DriverError::internal(
            "stored composite checksum has no part count",
        )),
        (ChecksumType::FullObject, _) => Checksum::new(algorithm, ChecksumType::FullObject, value)
            .map_err(|e| DriverError::internal(format!("stored checksum is invalid: {e}"))),
    }
}

fn parse_algorithm(value: &str) -> Result<ChecksumAlgorithm, DriverError> {
    ChecksumAlgorithm::parse(value).ok_or_else(|| {
        DriverError::internal(format!("stored checksum algorithm `{value}` is unknown"))
    })
}

fn parse_checksum_type(value: &str) -> Result<ChecksumType, DriverError> {
    ChecksumType::parse(value)
        .ok_or_else(|| DriverError::internal(format!("stored checksum type `{value}` is unknown")))
}

fn to_nanos(time: SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

fn from_nanos(nanos: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_nanos(nanos)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::SystemTime;

    fn sample() -> ObjectMetadata {
        let mut user_metadata = UserMetadata::new();
        user_metadata.insert("colour", "green").unwrap();
        ObjectMetadata {
            key: ObjectKey::parse("dir/a b+c").unwrap(),
            size: 42,
            etag: Some(EntityTag::new("deadbeef").unwrap()),
            last_modified: UNIX_EPOCH + Duration::from_secs(1_700_000_000),
            content: ContentProperties {
                content_type: Some("text/plain".into()),
                content_language: Some("en".into()),
                expires: Some(UNIX_EPOCH + Duration::from_secs(1_800_000_000)),
                ..ContentProperties::default()
            },
            user_metadata,
            checksums: vec![
                Checksum::new(
                    ChecksumAlgorithm::Crc32,
                    ChecksumType::FullObject,
                    vec![1, 2, 3, 4],
                )
                .unwrap(),
            ],
            revision: ObjectRevision::new("r-1"),
        }
    }

    #[test]
    fn round_trips_every_field() {
        let original = sample();
        let restored = decode(encode(&original)).expect("decodes");
        assert_eq!(restored, original);
    }

    #[test]
    fn round_trips_a_composite_checksum_with_its_part_count() {
        let mut original = sample();
        original.checksums =
            vec![Checksum::composite(ChecksumAlgorithm::Sha256, vec![9; 32], 3).unwrap()];
        let restored = decode(encode(&original)).expect("decodes");
        assert_eq!(restored.checksums[0].part_count(), Some(3));
        assert_eq!(restored, original);
    }

    #[test]
    fn survives_a_pre_epoch_last_modified_as_the_epoch() {
        let mut original = sample();
        original.last_modified = UNIX_EPOCH - Duration::from_secs(5);
        let restored = decode(encode(&original)).expect("decodes");
        assert_eq!(restored.last_modified, UNIX_EPOCH);
    }

    #[test]
    fn an_unknown_stored_algorithm_is_an_internal_error_not_a_missing_object() {
        let mut stored = encode(&sample());
        stored.checksums[0].algorithm = "rot13".into();
        assert!(decode(stored).is_err());
    }

    #[test]
    fn the_dto_serialises_as_toml() {
        // The durable form must be text TOML, which is what the driver writes.
        let text = toml::to_string(&encode(&sample())).expect("serialises");
        let parsed: StoredMetadata = toml::from_str(&text).expect("parses");
        assert_eq!(parsed.key, "dir/a b+c");
    }

    #[test]
    fn last_modified_round_trips_to_the_nanosecond() {
        let t = SystemTime::UNIX_EPOCH + Duration::from_nanos(12_345_678_901);
        assert_eq!(from_nanos(to_nanos(t)), t);
    }
}
