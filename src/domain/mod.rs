//! Domain model for the `allins3` object service.
//!
//! Everything in this module is independent of HTTP, S3 XML, signature
//! verification and any concrete storage product. The types here are the
//! vocabulary shared by the protocol layer, the object service and every
//! driver implementation.
//!
//! See `docs/implementation-spec.md` §3.

mod checksum;
mod context;
mod error;
mod ids;
mod key;
mod list;
mod metadata;
mod range;
mod write;

pub use checksum::{Checksum, ChecksumAlgorithm};
pub use context::{DriverContext, RequestId, TraceContext};
pub use error::{CommitState, DriverError, DriverErrorKind, RetryHint};
pub use ids::{BucketId, DriverInstanceId, IdError, NamespaceId};
pub use key::{KeyError, ObjectKey};
pub use list::{ListCursor, ListPage, ListRequest, ListRequestError, MAX_LIST_PREFIX_LEN};
pub use metadata::{
    ContentProperties, EntityTag, MAX_METADATA_BYTES, MAX_METADATA_COUNT,
    MAX_USER_METADATA_KEY_LEN, MetadataError, ObjectMetadata, ObjectRevision, UserMetadata,
};
pub use range::{RangeResolution, RangeSpec, RangeSpecError, ResolvedRange};
pub use write::{
    BeginWrite, CommitAttributes, DeleteRequest, DeleteResult, PartNumber, PartNumberError,
    Preconditions, WriteId,
};
