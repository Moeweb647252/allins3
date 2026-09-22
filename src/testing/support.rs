//! Helpers shared by the test-support drivers.

use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use futures_core::Stream;

use crate::domain::{
    BeginWrite, CommitAttributes, ContentProperties, DriverContext, DriverError, EntityTag,
    ObjectKey, ObjectMetadata, Preconditions, UserMetadata, WriteId,
};
use crate::driver::{NamespaceHandle, ObjectByteStream};

/// A single-chunk byte stream, used by in-memory test drivers.
struct OneShot {
    data: Option<Bytes>,
}

impl Stream for OneShot {
    type Item = Result<Bytes, DriverError>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(self.data.take().map(Ok))
    }
}

/// Wraps a byte buffer as a driver byte stream.
pub fn one_shot_stream(bytes: Bytes) -> ObjectByteStream {
    Box::pin(OneShot { data: Some(bytes) })
}

/// A generated body: `total` bytes delivered in `chunk`-sized pieces, without
/// ever materialising the whole object in the test process.
struct Generated {
    total: u64,
    chunk: usize,
    produced: u64,
}

impl Stream for Generated {
    type Item = Result<Bytes, DriverError>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.produced >= self.total {
            return Poll::Ready(None);
        }
        let remaining = self.total - self.produced;
        let size = (self.chunk as u64).min(remaining) as usize;
        self.produced += size as u64;
        Poll::Ready(Some(Ok(Bytes::from(vec![b'x'; size]))))
    }
}

/// A body stream of `total` bytes delivered in `chunk`-sized pieces.
///
/// The bytes are generated on demand, so a multi-gigabyte body costs a chunk of
/// memory rather than an object's worth. Used to check that the write pipeline's
/// memory does not follow the object size (spec §8.3).
pub fn generated_stream(total: u64, chunk: usize) -> ObjectByteStream {
    Box::pin(Generated {
        total,
        chunk,
        produced: 0,
    })
}

/// A deterministic, non-cryptographic placeholder entity tag.
///
/// The unified MD5-based ETag policy (spec §5.4) belongs to the service layer
/// and lands in P2. P1 contract tests only need a stable, content-derived tag
/// that changes when content changes.
pub fn placeholder_etag(content: &[u8]) -> EntityTag {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for b in content {
        hash ^= u64::from(*b);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    EntityTag::new(format!("{hash:016x}")).expect("hash is a valid tag")
}

/// A fresh write id for a test operation.
pub fn new_write_id() -> WriteId {
    WriteId::generate()
}

/// Starts, fills and commits a staged write for `data`.
///
/// This is the canonical test path for creating an object through the driver
/// interface, and exercises the same call sequence the service will use.
pub async fn put_object(
    handle: &NamespaceHandle,
    ctx: &DriverContext,
    key: ObjectKey,
    data: &[u8],
    preconditions: Preconditions,
) -> Result<ObjectMetadata, DriverError> {
    put_object_with_metadata(
        handle,
        ctx,
        key,
        data,
        preconditions,
        ContentProperties::default(),
        UserMetadata::new(),
    )
    .await
}

/// Like [`put_object`], but also sets content properties and user metadata.
pub async fn put_object_with_metadata(
    handle: &NamespaceHandle,
    ctx: &DriverContext,
    key: ObjectKey,
    data: &[u8],
    preconditions: Preconditions,
    content: ContentProperties,
    user_metadata: UserMetadata,
) -> Result<ObjectMetadata, DriverError> {
    let write = handle
        .write
        .as_ref()
        .ok_or_else(|| DriverError::not_supported("namespace is not writable"))?;

    let mut staged = write
        .begin_write(
            ctx,
            BeginWrite {
                key,
                expected_length: Some(data.len() as u64),
                preconditions,
                write_id: new_write_id(),
                content,
                user_metadata,
                checksum_algorithm: None,
            },
        )
        .await?;

    if !data.is_empty() {
        staged
            .write_chunk(ctx, Bytes::copy_from_slice(data))
            .await?;
    }

    let etag = placeholder_etag(data);
    staged
        .commit(
            ctx,
            CommitAttributes {
                length: data.len() as u64,
                etag: Some(etag),
                checksums: Vec::new(),
            },
        )
        .await
}

/// Collects a whole read stream into memory. Test-only; never a production
/// interface.
pub async fn read_to_end(mut stream: ObjectByteStream) -> Result<Vec<u8>, DriverError> {
    let mut out = Vec::new();
    while let Some(chunk) = std::future::poll_fn(|cx| stream.as_mut().poll_next(cx)).await {
        out.extend_from_slice(&chunk?);
    }
    Ok(out)
}
