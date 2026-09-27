//! The durable home of the coordinator's multipart state.
//!
//! A client that received `200` for an `UploadPart` must still be able to
//! `ListParts` and `Complete` that upload after the process restarts or is
//! killed (`INV-10`, spec §8.1). The coordinator's sessions, staged parts,
//! claims and completion records therefore have a durable mirror, and this
//! module is its one owner.
//!
//! The coordinator keeps its working set in memory and mirrors every mutation
//! through [`MultipartStore`]; a mutation that must be acknowledged is flushed
//! before the response is written, so an acknowledged part is on disk. Two
//! implementations make the choice explicit:
//!
//! - [`MemoryStore`] persists nothing. It is the default for a development
//!   deployment and for tests, where volatile state is the point.
//! - [`DiskStore`] mirrors each record as its own file under one root. It uses
//!   no embedded database: a record is `tmp → fsync → rename`, so a partial
//!   write is never mistaken for a complete one, and a crash leaves at worst an
//!   orphan that startup ignores.
//!
//! One decision the layout fixes: a part's bytes and the record that describes
//! them are two files, and the record is written *second*. A crash between them
//! leaves data with no record, which load treats as absent — the part was never
//! acknowledged, so that is the honest reading.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::domain::{ChecksumAlgorithm, ChecksumType, CommitState, ObjectKey, WriteId};
use crate::driver::{UploadHandle, UploadId};
use crate::service::ServiceError;
use crate::service::staging::{PartChecksum, StoredPart};

use super::{CompletionRecord, MultipartChecksum, UploadSession};

/// Everything the coordinator had durably recorded when it last ran.
#[derive(Debug, Default)]
pub(crate) struct PersistedState {
    /// In-progress sessions, keyed nowhere: the coordinator inserts by id.
    pub(crate) sessions: Vec<UploadSession>,
    /// Staged parts, each tagged with its upload.
    pub(crate) parts: Vec<(UploadId, StoredPart)>,
    /// Completion records, for retried completions and recovery queries.
    pub(crate) completions: Vec<CompletionRecord>,
    /// Completion attempts that were in flight when the process stopped.
    pub(crate) claims: Vec<(String, WriteId)>,
}

/// The durable mirror of the coordinator's state.
///
/// Every method is an operation on one record, never on the whole state: a
/// write-through mirror must not rewrite every session to record one part.
#[async_trait]
pub(crate) trait MultipartStore: Send + Sync + std::fmt::Debug {
    /// Loads everything recorded so far. An unreadable or corrupt record is an
    /// error, never silently skipped: a lost session is returned as a startup
    /// failure rather than as a `NoSuchUpload` a client cannot explain.
    async fn load(&self) -> Result<PersistedState, ServiceError>;

    /// Records (or replaces) a session.
    async fn write_session(&self, session: &UploadSession) -> Result<(), ServiceError>;

    /// Forgets a session.
    async fn delete_session(&self, upload_id: &str) -> Result<(), ServiceError>;

    /// Records (or replaces) one staged part and its bytes.
    async fn write_part(&self, upload_id: &str, part: &StoredPart) -> Result<(), ServiceError>;

    /// Forgets every staged part of one upload.
    async fn delete_upload(&self, upload_id: &str) -> Result<(), ServiceError>;

    /// Records a completion attempt that is in flight.
    async fn write_claim(&self, upload_id: &str, write_id: &WriteId) -> Result<(), ServiceError>;

    /// Forgets a completion attempt.
    async fn delete_claim(&self, upload_id: &str) -> Result<(), ServiceError>;

    /// Records (or replaces) a completion record.
    async fn write_completion(&self, record: &CompletionRecord) -> Result<(), ServiceError>;

    /// Forgets a completion record the coordinator no longer retains.
    async fn delete_completion(&self, write_id: &WriteId) -> Result<(), ServiceError>;
}

/// A store that keeps nothing: the coordinator's state stays in memory.
#[derive(Debug, Default)]
pub(crate) struct MemoryStore;

#[async_trait]
impl MultipartStore for MemoryStore {
    async fn load(&self) -> Result<PersistedState, ServiceError> {
        Ok(PersistedState::default())
    }

    async fn write_session(&self, _session: &UploadSession) -> Result<(), ServiceError> {
        Ok(())
    }

    async fn delete_session(&self, _upload_id: &str) -> Result<(), ServiceError> {
        Ok(())
    }

    async fn write_part(&self, _upload_id: &str, _part: &StoredPart) -> Result<(), ServiceError> {
        Ok(())
    }

    async fn delete_upload(&self, _upload_id: &str) -> Result<(), ServiceError> {
        Ok(())
    }

    async fn write_claim(&self, _upload_id: &str, _write_id: &WriteId) -> Result<(), ServiceError> {
        Ok(())
    }

    async fn delete_claim(&self, _upload_id: &str) -> Result<(), ServiceError> {
        Ok(())
    }

    async fn write_completion(&self, _record: &CompletionRecord) -> Result<(), ServiceError> {
        Ok(())
    }

    async fn delete_completion(&self, _write_id: &WriteId) -> Result<(), ServiceError> {
        Ok(())
    }
}

/// The file-backed mirror. One record is one file, written atomically.
#[derive(Debug)]
pub(crate) struct DiskStore {
    root: PathBuf,
}

impl DiskStore {
    /// A store rooted at `root`; directories are created on first write.
    pub(crate) fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    fn dir(&self, kind: &str) -> PathBuf {
        self.root.join(kind)
    }

    fn session_path(&self, upload_id: &str) -> PathBuf {
        self.dir("sessions")
            .join(format!("{}.toml", encode_name(upload_id)))
    }

    fn claim_path(&self, upload_id: &str) -> PathBuf {
        self.dir("claims")
            .join(format!("{}.toml", encode_name(upload_id)))
    }

    fn completion_path(&self, write_id: &WriteId) -> PathBuf {
        self.dir("completions")
            .join(format!("{}.toml", encode_name(write_id.as_str())))
    }

    fn upload_dir(&self, upload_id: &str) -> PathBuf {
        self.dir("parts").join(encode_name(upload_id))
    }
}

#[async_trait]
impl MultipartStore for DiskStore {
    async fn load(&self) -> Result<PersistedState, ServiceError> {
        let mut state = PersistedState::default();

        for path in list_dir(&self.dir("sessions")).await? {
            let text = read_text(&path).await?;
            let dto: SessionDto = parse(&path, &text)?;
            state.sessions.push(decode_session(dto)?);
        }
        for path in list_dir(&self.dir("claims")).await? {
            let text = read_text(&path).await?;
            let dto: ClaimDto = parse(&path, &text)?;
            let write_id = WriteId::from_raw(dto.write_id);
            state.claims.push((dto.upload_id, write_id));
        }
        for path in list_dir(&self.dir("completions")).await? {
            let text = read_text(&path).await?;
            let dto: CompletionDto = parse(&path, &text)?;
            state.completions.push(decode_completion(dto)?);
        }
        for upload_dir in list_dir(&self.dir("parts")).await? {
            for path in list_dir(&upload_dir).await? {
                let name = path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .unwrap_or_default();
                // Only the record commits a part; raw bytes with no record were
                // never acknowledged, so they are not loaded (and the next
                // write of that part overwrites them).
                if !(name.starts_with("meta-") && name.ends_with(".toml")) {
                    continue;
                }
                let text = read_text(&path).await?;
                let dto: PartDto = parse(&path, &text)?;
                let data_path = path.with_file_name(format!("data-{:05}", dto.number));
                let data = tokio::fs::read(&data_path)
                    .await
                    .map_err(|e| storage_error(&data_path, e))?;
                state.parts.push(decode_part(dto, data)?);
            }
        }
        Ok(state)
    }

    async fn write_session(&self, session: &UploadSession) -> Result<(), ServiceError> {
        let path = self.session_path(session.upload_id.as_str());
        let text = toml::to_string(&encode_session(session))
            .map_err(|e| ServiceError::Internal(format!("cannot encode multipart session: {e}")))?;
        write_atomic(&path, text.as_bytes()).await
    }

    async fn delete_session(&self, upload_id: &str) -> Result<(), ServiceError> {
        remove_file(&self.session_path(upload_id)).await
    }

    async fn write_part(&self, upload_id: &str, part: &StoredPart) -> Result<(), ServiceError> {
        let dir = self.upload_dir(upload_id);
        tokio::fs::create_dir_all(&dir)
            .await
            .map_err(|e| storage_error(&dir, e))?;
        // Bytes first, record second: the record is the commit marker, so a
        // crash between the two leaves an unacknowledged part, not a part whose
        // bytes are missing.
        let data_path = dir.join(format!("data-{:05}", part.number));
        write_atomic(&data_path, &part.data).await?;
        let meta_path = dir.join(format!("meta-{:05}.toml", part.number));
        let text = toml::to_string(&encode_part(upload_id, part))
            .map_err(|e| ServiceError::Internal(format!("cannot encode multipart part: {e}")))?;
        write_atomic(&meta_path, text.as_bytes()).await
    }

    async fn delete_upload(&self, upload_id: &str) -> Result<(), ServiceError> {
        let dir = self.upload_dir(upload_id);
        match tokio::fs::remove_dir_all(&dir).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(storage_error(&dir, e)),
        }
    }

    async fn write_claim(&self, upload_id: &str, write_id: &WriteId) -> Result<(), ServiceError> {
        let path = self.claim_path(upload_id);
        let dto = ClaimDto {
            upload_id: upload_id.to_string(),
            write_id: write_id.as_str().to_string(),
        };
        let text = toml::to_string(&dto)
            .map_err(|e| ServiceError::Internal(format!("cannot encode claim: {e}")))?;
        write_atomic(&path, text.as_bytes()).await
    }

    async fn delete_claim(&self, upload_id: &str) -> Result<(), ServiceError> {
        remove_file(&self.claim_path(upload_id)).await
    }

    async fn write_completion(&self, record: &CompletionRecord) -> Result<(), ServiceError> {
        let path = self.completion_path(&record.write_id);
        let text = toml::to_string(&encode_completion(record))
            .map_err(|e| ServiceError::Internal(format!("cannot encode completion: {e}")))?;
        write_atomic(&path, text.as_bytes()).await
    }

    async fn delete_completion(&self, write_id: &WriteId) -> Result<(), ServiceError> {
        remove_file(&self.completion_path(write_id)).await
    }
}

// --- Record shapes ---------------------------------------------------------

/// The durable form of a session.
///
/// Explicit fields rather than a `serde` derive on `UploadSession`, so a field
/// added to the session is a compile error here until it is persisted: the
/// store owns the durable form of everything it is handed.
#[derive(Debug, Serialize, Deserialize)]
struct SessionDto {
    upload_id: String,
    bucket: String,
    key: String,
    principal: Option<String>,
    generation: u64,
    content_type: Option<String>,
    content_encoding: Option<String>,
    content_language: Option<String>,
    content_disposition: Option<String>,
    cache_control: Option<String>,
    expires_nanos: Option<u64>,
    user_metadata: BTreeMap<String, String>,
    checksum_algorithm: Option<String>,
    checksum_type: Option<String>,
    initiated_nanos: u64,
    last_activity_nanos: u64,
    native_upload_id: Option<String>,
}

/// The durable form of one part's record; its bytes live beside it.
#[derive(Debug, Serialize, Deserialize)]
struct PartDto {
    upload_id: String,
    number: u16,
    size: u64,
    /// Hex MD5, the part ETag.
    md5: String,
    checksum_algorithm: Option<String>,
    /// Base64 of the raw digest.
    checksum_value: Option<String>,
}

/// The durable form of a completion attempt marker.
#[derive(Debug, Serialize, Deserialize)]
struct ClaimDto {
    upload_id: String,
    write_id: String,
}

/// The durable form of a completion record.
#[derive(Debug, Serialize, Deserialize)]
struct CompletionDto {
    write_id: String,
    upload_id: String,
    bucket: String,
    key: String,
    state: String,
    recorded_at_nanos: u64,
}

// --- Codecs ----------------------------------------------------------------

fn encode_session(session: &UploadSession) -> SessionDto {
    SessionDto {
        upload_id: session.upload_id.as_str().to_string(),
        bucket: session.bucket.clone(),
        key: session.key.as_str().to_string(),
        principal: session.principal.clone(),
        generation: session.generation,
        content_type: session.content.content_type.clone(),
        content_encoding: session.content.content_encoding.clone(),
        content_language: session.content.content_language.clone(),
        content_disposition: session.content.content_disposition.clone(),
        cache_control: session.content.cache_control.clone(),
        expires_nanos: session.content.expires.map(to_nanos),
        user_metadata: session
            .user_metadata
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
        checksum_algorithm: session
            .checksum
            .map(|scheme| scheme.algorithm.as_str().to_string()),
        checksum_type: session
            .checksum
            .map(|scheme| scheme.checksum_type.as_str().to_string()),
        initiated_nanos: to_nanos(session.initiated),
        last_activity_nanos: to_nanos(session.last_activity),
        native_upload_id: session
            .native
            .as_ref()
            .map(|upload| upload.upload_id.as_str().to_string()),
    }
}

fn decode_session(dto: SessionDto) -> Result<UploadSession, ServiceError> {
    let key = ObjectKey::parse(dto.key.clone())
        .map_err(|e| ServiceError::Internal(format!("stored session key `{}`: {e}", dto.key)))?;
    let mut user_metadata = crate::domain::UserMetadata::new();
    for (k, v) in dto.user_metadata {
        user_metadata.insert(k, v).map_err(|e| {
            ServiceError::Internal(format!("stored session user metadata is invalid: {e}"))
        })?;
    }
    let checksum = match (dto.checksum_algorithm, dto.checksum_type) {
        (Some(algorithm), Some(checksum_type)) => Some(MultipartChecksum::new(
            parse_algorithm(&algorithm)?,
            parse_checksum_type(&checksum_type)?,
        )?),
        (None, None) => None,
        _ => {
            return Err(ServiceError::Internal(
                "stored session has a checksum algorithm without its type".into(),
            ));
        }
    };
    // A native upload's driver handle is reconstructed from the id and the
    // target key it was bound to; the driver owns the rest of its state.
    let native = dto.native_upload_id.map(|upload_id| UploadHandle {
        upload_id: UploadId::from_raw(upload_id),
        key: key.clone(),
    });
    Ok(UploadSession {
        upload_id: UploadId::from_raw(dto.upload_id),
        bucket: dto.bucket,
        key,
        principal: dto.principal,
        generation: dto.generation,
        content: crate::domain::ContentProperties {
            content_type: dto.content_type,
            content_encoding: dto.content_encoding,
            content_language: dto.content_language,
            content_disposition: dto.content_disposition,
            cache_control: dto.cache_control,
            expires: dto.expires_nanos.map(from_nanos),
        },
        user_metadata,
        checksum,
        initiated: from_nanos(dto.initiated_nanos),
        last_activity: from_nanos(dto.last_activity_nanos),
        native,
    })
}

fn encode_part(upload_id: &str, part: &StoredPart) -> PartDto {
    PartDto {
        upload_id: upload_id.to_string(),
        number: part.number,
        size: part.size,
        md5: crate::integrity::hex(&part.md5),
        checksum_algorithm: part
            .checksum
            .as_ref()
            .map(|checksum| checksum.algorithm.as_str().to_string()),
        checksum_value: part.checksum.as_ref().map(|checksum| {
            use base64::Engine as _;
            base64::engine::general_purpose::STANDARD.encode(&checksum.value)
        }),
    }
}

fn decode_part(dto: PartDto, data: Vec<u8>) -> Result<(UploadId, StoredPart), ServiceError> {
    let md5_bytes = crate::integrity::decode_hex(&dto.md5)
        .ok_or_else(|| ServiceError::Internal("stored part has a non-hex MD5".into()))?;
    let mut md5 = [0u8; 16];
    if md5_bytes.len() != md5.len() {
        return Err(ServiceError::Internal(
            "stored part has an MD5 of the wrong length".into(),
        ));
    }
    md5.copy_from_slice(&md5_bytes);
    let checksum = match (dto.checksum_algorithm, dto.checksum_value) {
        (Some(algorithm), Some(value)) => {
            let value = crate::integrity::decode_base64(&value).map_err(|e| {
                ServiceError::Internal(format!("stored part checksum is not base64: {e}"))
            })?;
            Some(PartChecksum {
                algorithm: parse_algorithm(&algorithm)?,
                value: Bytes::from(value),
            })
        }
        (None, None) => None,
        _ => {
            return Err(ServiceError::Internal(
                "stored part has a checksum algorithm or value without the other".into(),
            ));
        }
    };
    // The record's size is authoritative for the ledger, and it must match the
    // bytes actually stored, or the quota accounting would be a fiction.
    if dto.size != data.len() as u64 {
        return Err(ServiceError::Internal(format!(
            "stored part {} records {} bytes but holds {}",
            dto.number,
            dto.size,
            data.len()
        )));
    }
    Ok((
        UploadId::from_raw(dto.upload_id),
        StoredPart {
            number: dto.number,
            size: dto.size,
            data: Bytes::from(data),
            md5,
            checksum,
        },
    ))
}

fn encode_completion(record: &CompletionRecord) -> CompletionDto {
    CompletionDto {
        write_id: record.write_id.as_str().to_string(),
        upload_id: record.upload_id.clone(),
        bucket: record.bucket.clone(),
        key: record.key.as_str().to_string(),
        state: encode_state(record.state).to_string(),
        recorded_at_nanos: to_nanos(record.recorded_at),
    }
}

fn decode_completion(dto: CompletionDto) -> Result<CompletionRecord, ServiceError> {
    Ok(CompletionRecord {
        write_id: WriteId::from_raw(dto.write_id),
        upload_id: dto.upload_id,
        bucket: dto.bucket,
        key: ObjectKey::parse(dto.key.clone()).map_err(|e| {
            ServiceError::Internal(format!("stored completion key `{}`: {e}", dto.key))
        })?,
        state: decode_state(&dto.state)?,
        recorded_at: from_nanos(dto.recorded_at_nanos),
    })
}

fn parse_algorithm(value: &str) -> Result<ChecksumAlgorithm, ServiceError> {
    ChecksumAlgorithm::parse(value)
        .ok_or_else(|| ServiceError::Internal(format!("stored checksum algorithm `{value}`")))
}

fn parse_checksum_type(value: &str) -> Result<ChecksumType, ServiceError> {
    ChecksumType::parse(value)
        .ok_or_else(|| ServiceError::Internal(format!("stored checksum type `{value}`")))
}

/// The durable spelling of a commit state.
fn encode_state(state: CommitState) -> &'static str {
    match state {
        CommitState::Committed => "committed",
        CommitState::NotCommittedFinal => "not_committed_final",
        CommitState::InFlight => "in_flight",
        CommitState::Unknown => "unknown",
    }
}

fn decode_state(value: &str) -> Result<CommitState, ServiceError> {
    match value {
        "committed" => Ok(CommitState::Committed),
        "not_committed_final" => Ok(CommitState::NotCommittedFinal),
        "in_flight" => Ok(CommitState::InFlight),
        "unknown" => Ok(CommitState::Unknown),
        other => Err(ServiceError::Internal(format!(
            "stored completion state `{other}` is unknown"
        ))),
    }
}

// --- Filesystem helpers ----------------------------------------------------

/// Makes an id safe as one path component.
///
/// Upload and write ids are generated hex, but they arrive from a persisted
/// file, so a record that named `../../etc` must not become a path. Every
/// character outside the id alphabet is percent-escaped, so the mapping is
/// total and injective rather than lossy.
fn encode_name(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_' {
            out.push(byte as char);
        } else {
            out.push('%');
            out.push_str(&format!("{byte:02x}"));
        }
    }
    out
}

async fn list_dir(dir: &Path) -> Result<Vec<PathBuf>, ServiceError> {
    let mut entries = match tokio::fs::read_dir(dir).await {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(storage_error(dir, e)),
    };
    let mut paths = Vec::new();
    while let Some(entry) = entries
        .next_entry()
        .await
        .map_err(|e| storage_error(dir, e))?
    {
        paths.push(entry.path());
    }
    Ok(paths)
}

async fn read_text(path: &Path) -> Result<String, ServiceError> {
    tokio::fs::read_to_string(path)
        .await
        .map_err(|e| storage_error(path, e))
}

fn parse<T: serde::de::DeserializeOwned>(path: &Path, text: &str) -> Result<T, ServiceError> {
    toml::from_str(text).map_err(|e| {
        ServiceError::Internal(format!(
            "stored record {} cannot be parsed: {e}",
            path.display()
        ))
    })
}

async fn remove_file(path: &Path) -> Result<(), ServiceError> {
    match tokio::fs::remove_file(path).await {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(storage_error(path, e)),
    }
}

/// Writes `bytes` to `path` so that a reader ever sees the old file or the
/// whole new one, never a partial write: write a temporary sibling, fsync it,
/// rename it over the target, then fsync the directory entry.
///
/// The temporary name is unique per call, so two concurrent writers for one
/// record cannot corrupt each other's staging file.
async fn write_atomic(path: &Path, bytes: &[u8]) -> Result<(), ServiceError> {
    let dir = path.parent().unwrap_or_else(|| Path::new("."));
    tokio::fs::create_dir_all(dir)
        .await
        .map_err(|e| storage_error(dir, e))?;
    let tmp = dir.join(format!(
        ".{}.{}.tmp",
        path.file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("record"),
        WriteId::generate().as_str()
    ));
    let result = async {
        tokio::fs::write(&tmp, bytes)
            .await
            .map_err(|e| storage_error(&tmp, e))?;
        let file = tokio::fs::OpenOptions::new()
            .write(true)
            .open(&tmp)
            .await
            .map_err(|e| storage_error(&tmp, e))?;
        file.sync_all().await.map_err(|e| storage_error(&tmp, e))?;
        tokio::fs::rename(&tmp, path)
            .await
            .map_err(|e| storage_error(path, e))?;
        // Best effort: a directory fsync is what makes the rename itself
        // durable, and not every platform allows opening a directory.
        if let Ok(dir_file) = tokio::fs::File::open(dir).await {
            let _ = dir_file.sync_all().await;
        }
        Ok(())
    }
    .await;
    if result.is_err() {
        let _ = tokio::fs::remove_file(&tmp).await;
    }
    result
}

fn storage_error(path: &Path, error: std::io::Error) -> ServiceError {
    ServiceError::Internal(format!(
        "multipart state {} is unavailable: {error}",
        path.display()
    ))
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
    use crate::domain::{ChecksumType, ContentProperties, UserMetadata};
    use crate::service::multipart::MultipartChecksum;

    fn upload(id: &str) -> UploadId {
        UploadId::from_raw(id)
    }

    fn session(id: &str) -> UploadSession {
        let mut user_metadata = UserMetadata::new();
        user_metadata.insert("colour", "green").unwrap();
        UploadSession {
            upload_id: upload(id),
            bucket: "bucket-a".into(),
            key: ObjectKey::parse("dir/a b+c").unwrap(),
            principal: Some("tester".into()),
            generation: 7,
            content: ContentProperties {
                content_type: Some("text/plain".into()),
                ..ContentProperties::default()
            },
            user_metadata,
            checksum: Some(
                MultipartChecksum::new(ChecksumAlgorithm::Crc32, ChecksumType::FullObject).unwrap(),
            ),
            initiated: UNIX_EPOCH + Duration::from_secs(1_700_000_000),
            last_activity: UNIX_EPOCH + Duration::from_nanos(1_700_000_000_123_456_789),
            native: None,
        }
    }

    fn part(number: u16, data: &[u8]) -> StoredPart {
        let mut md5 = [0u8; 16];
        md5[..data.len().min(16)].copy_from_slice(&data[..data.len().min(16)]);
        StoredPart {
            number,
            size: data.len() as u64,
            data: Bytes::copy_from_slice(data),
            md5,
            checksum: Some(PartChecksum {
                algorithm: ChecksumAlgorithm::Crc32,
                value: Bytes::from_static(&[1, 2, 3, 4]),
            }),
        }
    }

    fn completion() -> CompletionRecord {
        CompletionRecord {
            write_id: WriteId::from_raw("write-1"),
            upload_id: "upload-1".into(),
            bucket: "bucket-a".into(),
            key: ObjectKey::parse("k").unwrap(),
            state: CommitState::Unknown,
            recorded_at: UNIX_EPOCH + Duration::from_secs(42),
        }
    }

    fn temp_root(tag: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "allins3-store-{tag}-{}",
            WriteId::generate().as_str()
        ));
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    #[tokio::test]
    async fn a_disk_store_round_trips_every_record() {
        let root = temp_root("roundtrip");
        let store = DiskStore::new(&root);
        let session = session("upload-1");
        store.write_session(&session).await.unwrap();
        store
            .write_part("upload-1", &part(1, b"hello world"))
            .await
            .unwrap();
        store
            .write_claim("upload-1", &WriteId::from_raw("w"))
            .await
            .unwrap();
        store.write_completion(&completion()).await.unwrap();

        let loaded = store.load().await.unwrap();
        assert_eq!(loaded.sessions, vec![session]);
        assert_eq!(loaded.parts.len(), 1);
        assert_eq!(loaded.parts[0].1.data.as_ref(), b"hello world");
        assert_eq!(loaded.parts[0].1.md5, part(1, b"hello world").md5);
        assert_eq!(loaded.claims.len(), 1);
        assert_eq!(loaded.completions.len(), 1);
        assert_eq!(loaded.completions[0].state, CommitState::Unknown);

        // A replaced part is one record, not a second copy.
        store.write_part("upload-1", &part(1, b"x")).await.unwrap();
        let loaded = store.load().await.unwrap();
        assert_eq!(loaded.parts.len(), 1);
        assert_eq!(loaded.parts[0].1.data.as_ref(), b"x");
        std::fs::remove_dir_all(&root).ok();
    }

    #[tokio::test]
    async fn forgetting_a_upload_removes_its_parts() {
        let root = temp_root("forget");
        let store = DiskStore::new(&root);
        store.write_part("u", &part(1, b"a")).await.unwrap();
        store.delete_upload("u").await.unwrap();
        let loaded = store.load().await.unwrap();
        assert!(loaded.parts.is_empty());
        // Deleting twice is not an error: cleanup is idempotent.
        store.delete_upload("u").await.unwrap();
        std::fs::remove_dir_all(&root).ok();
    }

    #[tokio::test]
    async fn a_part_without_its_record_is_not_loaded() {
        let root = temp_root("orphan");
        let store = DiskStore::new(&root);
        store.write_part("u", &part(1, b"a")).await.unwrap();
        // Delete the record, keeping the bytes: an unacknowledged part.
        std::fs::remove_file(root.join("parts").join("u").join("meta-00001.toml")).unwrap();
        let loaded = store.load().await.unwrap();
        assert!(loaded.parts.is_empty());
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn an_id_is_a_single_safe_path_component() {
        assert_eq!(encode_name("abc-123_x"), "abc-123_x");
        assert_eq!(encode_name("../../etc"), "%2e%2e%2f%2e%2e%2fetc");
        // Injective: two different ids never collide.
        assert_ne!(encode_name("a/b"), encode_name("a%2fb"));
    }

    #[tokio::test]
    async fn a_memory_store_persists_nothing() {
        let store = MemoryStore;
        store.write_session(&session("u")).await.unwrap();
        assert!(store.load().await.unwrap().sessions.is_empty());
    }

    #[test]
    fn a_session_round_trips_through_its_durable_form() {
        let original = session("upload-1");
        let restored = decode_session(encode_session(&original)).unwrap();
        assert_eq!(restored, original);
    }

    #[test]
    fn a_corrupt_record_is_an_error_not_a_missing_session() {
        let mut dto = encode_session(&session("u"));
        dto.key = String::new();
        assert!(decode_session(dto).is_err());
    }

    #[test]
    fn a_part_whose_record_disagrees_with_its_bytes_is_an_error() {
        let dto = encode_part("u", &part(1, b"abcd"));
        assert!(decode_part(dto, b"abc".to_vec()).is_err());
    }
}
