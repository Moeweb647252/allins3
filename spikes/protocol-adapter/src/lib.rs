//! P0 protocol-adapter spike.
//!
//! This crate compiles against the real `s3s` API to record, as executable
//! evidence, exactly which integration points the assessment in
//! `docs/p0-protocol-adapter-assessment.md` depends on:
//!
//! 1. A custom credential provider (`S3Auth`) can be supplied.
//! 2. Authorization can be supplied (`S3Access`).
//! 3. SigV2 is disabled and key-byte normalisation is off by default.
//! 4. Raw request parts (method/URI/headers/body) are reachable.
//! 5. Streaming trailers are reachable through a handle.
//!
//! What this crate does **not** prove (and must not be read as proving): that
//! real AWS SDK / CLI requests round-trip, or that trailers are validated before
//! commit. Those require a live client and are listed as open items in the
//! assessment.

use std::sync::Arc;

use async_trait::async_trait;
use s3s::access::S3Access;
use s3s::auth::{S3Auth, SecretKey};
use s3s::config::{S3Config, StaticConfigProvider};
use s3s::dto::{
    GetObjectInput, GetObjectOutput, HeadObjectInput, HeadObjectOutput, PutObjectInput,
    PutObjectOutput,
};
use s3s::service::S3ServiceBuilder;
use s3s::{S3, S3Request, S3Response, S3Result, s3_error};

/// A credential provider that resolves an access key to a secret key.
struct SpikeAuth;

#[async_trait]
impl S3Auth for SpikeAuth {
    async fn get_secret_key(&self, access_key: &str) -> S3Result<SecretKey> {
        match access_key {
            "EXAMPLEACCESSKEY" => Ok(SecretKey::from("EXAMPLE_SECRET")),
            _ => Err(s3_error!(InvalidAccessKeyId)),
        }
    }
}

/// Access control. The default `check` denies anonymous requests.
struct SpikeAccess;

#[async_trait]
impl S3Access for SpikeAccess {}

#[derive(Clone)]
struct SpikeS3;

#[async_trait]
impl S3 for SpikeS3 {
    async fn get_object(
        &self,
        _req: S3Request<GetObjectInput>,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        Err(s3_error!(NotImplemented, "spike"))
    }

    async fn head_object(
        &self,
        _req: S3Request<HeadObjectInput>,
    ) -> S3Result<S3Response<HeadObjectOutput>> {
        Err(s3_error!(NotImplemented, "spike"))
    }

    async fn put_object(
        &self,
        _req: S3Request<PutObjectInput>,
    ) -> S3Result<S3Response<PutObjectOutput>> {
        Err(s3_error!(NotImplemented, "spike"))
    }
}

/// Records the security-relevant defaults we intend to rely on.
///
/// Returns the defaults so callers can assert on them.
pub fn secured_config() -> S3Config {
    let mut config = S3Config::default();
    // We want SigV4 only; SigV2 must stay off.
    config.enable_sig_v2 = false;
    // Key bytes must not be normalised (`a//b` != `a/b`).
    config.normalize_forward_slash_path = false;

    assert!(
        !S3Config::default().enable_sig_v2,
        "s3s must default SigV2 off"
    );
    assert!(
        !S3Config::default().normalize_forward_slash_path,
        "s3s must default key normalisation off"
    );
    assert_eq!(
        S3Config::default().presigned_url_max_expires_secs,
        604_800,
        "presigned URL cap should match the spec"
    );
    config
}

/// Builds a service with our auth and access layers wired in.
pub fn build_service() -> impl Send + Sync {
    let mut builder = S3ServiceBuilder::new(SpikeS3);
    let provider = StaticConfigProvider::new(Arc::new(secured_config()));
    builder.set_config(Arc::new(provider));
    builder.set_auth(SpikeAuth);
    builder.set_access(SpikeAccess);
    builder.build()
}

/// Reads the raw HTTP parts a signature verifier and router must see.
pub fn raw_parts<B>(req: &s3s::HttpRequest<B>) -> (&hyper::Method, &hyper::Uri, &hyper::HeaderMap) {
    (req.method(), req.uri(), req.headers())
}

/// Records that streaming trailers are reachable after the body is consumed.
pub fn trailer_len(headers: Option<s3s::TrailingHeaders>) -> Option<usize> {
    let headers = headers?;
    headers.read(|h| h.len())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn secured_defaults_hold() {
        let _ = secured_config();
        let _builder = build_service();
    }
}
