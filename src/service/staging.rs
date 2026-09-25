//! The staging ledger: one owner for the multipart staging quota and the part
//! table.
//!
//! Before this module the part table, the staged byte count, the in-flight byte
//! count and the quota were four loose fields on `MultipartState`, and three
//! call sites (a finished upload, a swept upload, an aborted upload) each
//! removed the parts and subtracted the freed bytes by hand. The invariant that
//! matters — staged bytes plus bytes still being received never exceed the
//! quota, and dropping a part returns its bytes in the same step — is now stated
//! and asserted here, and nowhere else.
//!
//! The ledger is deliberately not durable and not shared: it belongs to one
//! `Multipart`, so tests reason about a ledger they built (spec §8).

use std::collections::{BTreeMap, HashMap};

use bytes::Bytes;

use crate::domain::{ChecksumAlgorithm, EntityTag};
use crate::driver::UploadId;
use crate::integrity::hex;

use super::ServiceError;

/// The most parts one upload may hold. Replacing an existing part is always
/// allowed, so this bounds the table, not the client's ability to correct a
/// part.
const MAX_PARTS_PER_UPLOAD: u16 = 10_000;

/// The checksum computed for one part under the upload's scheme.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartChecksum {
    /// The algorithm the value was computed with.
    pub algorithm: ChecksumAlgorithm,
    /// The raw digest bytes (never base64).
    pub value: Bytes,
}

/// A staged part.
#[derive(Debug, Clone)]
pub(crate) struct StoredPart {
    /// The part number.
    pub(crate) number: u16,
    /// Size in bytes.
    pub(crate) size: u64,
    /// Part bytes (staged).
    pub(crate) data: Bytes,
    /// MD5 of the part.
    pub(crate) md5: [u8; 16],
    /// The part checksum under the upload's scheme, when it has one.
    pub(crate) checksum: Option<PartChecksum>,
}

impl StoredPart {
    /// The part ETag.
    pub(crate) fn etag(&self) -> EntityTag {
        EntityTag::new(hex(&self.md5)).expect("md5 hex is a valid tag")
    }
}

/// The staging quota, the staged bytes, the bytes currently being received and
/// the part table — one owner, so the accounting cannot drift from the table.
pub(crate) struct StagingLedger {
    parts: HashMap<UploadId, BTreeMap<u16, StoredPart>>,
    quota: u64,
    /// Bytes of parts already staged.
    staged_bytes: u64,
    /// Bytes of part bodies currently being received, charged before they are
    /// buffered so a part the quota cannot hold is refused mid-body.
    reserved_bytes: u64,
}

impl StagingLedger {
    /// A ledger that will hold `quota` bytes of staged and in-flight parts.
    pub(crate) fn new(quota: u64) -> Self {
        Self {
            parts: HashMap::new(),
            quota,
            staged_bytes: 0,
            reserved_bytes: 0,
        }
    }

    /// Charges `bytes` of a body being received, crediting `credit` bytes of the
    /// part this attempt replaces.
    ///
    /// The credit matters because staging the replacement releases the old part
    /// in the same step, so only the difference is ever charged: an upload can
    /// replace its last part without room for both copies at once.
    pub(crate) fn reserve(&mut self, bytes: u64, credit: u64) -> Result<(), ServiceError> {
        let used = self
            .staged_bytes
            .saturating_sub(credit)
            .saturating_add(self.reserved_bytes);
        if used.saturating_add(bytes) > self.quota {
            return Err(ServiceError::SlowDown);
        }
        self.reserved_bytes = self.reserved_bytes.saturating_add(bytes);
        Ok(())
    }

    /// Returns bytes previously charged by [`StagingLedger::reserve`].
    ///
    /// Called when the received bytes become staged, and however a reception
    /// ends — staged, refused, cancelled or failed — so a refused part never
    /// holds quota another uploader could have used.
    pub(crate) fn release(&mut self, bytes: u64) {
        self.reserved_bytes = self.reserved_bytes.saturating_sub(bytes);
    }

    /// Stages `part`, replacing any part with the same number.
    ///
    /// Refuses a new part once the upload holds [`MAX_PARTS_PER_UPLOAD`] parts,
    /// and refuses any part whose staged bytes would push the quota over.
    pub(crate) fn stage(
        &mut self,
        upload_id: &UploadId,
        part: StoredPart,
    ) -> Result<(), ServiceError> {
        let staged = self.parts.get(upload_id);
        let replacing = staged.is_some_and(|parts| parts.contains_key(&part.number));
        let count = staged.map_or(0, BTreeMap::len);
        if !replacing && count >= usize::from(MAX_PARTS_PER_UPLOAD) {
            return Err(ServiceError::InvalidArgument(format!(
                "a multipart upload may have at most {MAX_PARTS_PER_UPLOAD} parts"
            )));
        }
        let previous = staged
            .and_then(|parts| parts.get(&part.number))
            .map_or(0, |existing| existing.size);
        let new_total = self
            .staged_bytes
            .saturating_sub(previous)
            .saturating_add(part.size);
        if new_total > self.quota {
            return Err(ServiceError::SlowDown);
        }
        self.staged_bytes = new_total;
        self.parts
            .entry(upload_id.clone())
            .or_default()
            .insert(part.number, part);
        Ok(())
    }

    /// Drops every part of `upload_id` and returns the bytes freed.
    ///
    /// The table and the byte count move in one step, so no caller can free one
    /// without the other.
    pub(crate) fn take(&mut self, upload_id: &UploadId) -> u64 {
        let Some(parts) = self.parts.remove(upload_id) else {
            return 0;
        };
        let freed: u64 = parts.values().map(|part| part.size).sum();
        self.staged_bytes = self.staged_bytes.saturating_sub(freed);
        freed
    }

    /// The staged part `number` of `upload_id`, if that part exists.
    pub(crate) fn part(&self, upload_id: &UploadId, number: u16) -> Option<&StoredPart> {
        self.parts.get(upload_id)?.get(&number)
    }

    /// Every staged part of `upload_id`, in part-number order, if any.
    pub(crate) fn parts_of(&self, upload_id: &UploadId) -> Option<&BTreeMap<u16, StoredPart>> {
        self.parts.get(upload_id)
    }

    /// Whether nothing is staged and nothing is being received.
    pub(crate) fn is_idle(&self) -> bool {
        self.parts.is_empty() && self.staged_bytes == 0 && self.reserved_bytes == 0
    }

    /// Bytes currently staged.
    #[cfg(test)]
    fn staged_bytes(&self) -> u64 {
        self.staged_bytes
    }

    /// Bytes currently charged to in-flight receptions.
    #[cfg(test)]
    fn reserved_bytes(&self) -> u64 {
        self.reserved_bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A typed upload id. The ledger keys parts by identity, so a test builds
    /// one rather than passing a bare string.
    fn upload(id: &str) -> UploadId {
        UploadId::from_raw(id)
    }

    /// A part of `size` bytes. The ledger accounts by `size`, so the tests do
    /// not allocate bodies to exercise the quota.
    fn part(number: u16, size: u64) -> StoredPart {
        StoredPart {
            number,
            size,
            data: Bytes::new(),
            md5: [0u8; 16],
            checksum: None,
        }
    }

    #[test]
    fn receiving_bytes_is_charged_until_it_is_staged() {
        let mut ledger = StagingLedger::new(100);
        ledger.reserve(60, 0).expect("60 fits in 100");
        assert_eq!(ledger.reserved_bytes(), 60);
        assert_eq!(ledger.staged_bytes(), 0);

        // A second reception past the quota is refused while the first is still
        // in flight, even though neither has been staged yet.
        assert!(ledger.reserve(50, 0).is_err());

        ledger.release(60);
        assert_eq!(ledger.reserved_bytes(), 0);
        ledger.reserve(50, 0).expect("the space was returned");
    }

    #[test]
    fn staging_charges_the_quota_and_replacement_charges_only_the_difference() {
        let mut ledger = StagingLedger::new(100);
        ledger.stage(&upload("u"), part(1, 60)).expect("60 fits");
        assert_eq!(ledger.staged_bytes(), 60);

        // Replacing releases the old part in the same step, so only the
        // difference is charged.
        ledger
            .stage(&upload("u"), part(1, 90))
            .expect("the difference fits");
        assert_eq!(ledger.staged_bytes(), 90);

        assert!(
            ledger.stage(&upload("u"), part(2, 11)).is_err(),
            "a new part is still bounded by the quota"
        );
    }

    #[test]
    fn a_reception_credits_the_part_it_replaces() {
        let mut ledger = StagingLedger::new(100);
        ledger.stage(&upload("u"), part(1, 80)).expect("80 fits");
        // Only 20 bytes are free, but a 60-byte replacement still fits: the part
        // it replaces is released in the same step.
        ledger
            .reserve(60, 80)
            .expect("the replaced bytes are credited");
    }

    #[test]
    fn taking_an_upload_returns_its_bytes_and_empties_the_ledger() {
        let mut ledger = StagingLedger::new(100);
        ledger.stage(&upload("u"), part(1, 30)).expect("fits");
        ledger.stage(&upload("u"), part(2, 40)).expect("fits");

        assert_eq!(ledger.take(&upload("u")), 70);
        assert_eq!(ledger.staged_bytes(), 0);
        assert_eq!(
            ledger.take(&upload("u")),
            0,
            "taking twice frees nothing more"
        );
        assert!(ledger.is_idle());
    }

    #[test]
    fn the_part_count_is_bounded_but_replacement_is_always_allowed() {
        let mut ledger = StagingLedger::new(u64::MAX);
        for number in 1..=MAX_PARTS_PER_UPLOAD {
            ledger
                .stage(&upload("u"), part(number, 0))
                .expect("under the bound");
        }
        assert!(
            ledger
                .stage(&upload("u"), part(MAX_PARTS_PER_UPLOAD + 1, 0))
                .is_err(),
            "a new part past the bound is refused"
        );
        ledger
            .stage(&upload("u"), part(1, 0))
            .expect("replacing an existing part stays allowed at the bound");
    }

    #[test]
    fn the_table_is_readable_by_upload_and_part_number() {
        let mut ledger = StagingLedger::new(10);
        ledger.stage(&upload("u"), part(3, 4)).expect("fits");

        assert_eq!(ledger.part(&upload("u"), 3).map(|part| part.size), Some(4));
        assert!(ledger.part(&upload("u"), 1).is_none());
        assert_eq!(ledger.parts_of(&upload("u")).map(BTreeMap::len), Some(1));
        assert!(ledger.parts_of(&upload("other")).is_none());
    }
}
