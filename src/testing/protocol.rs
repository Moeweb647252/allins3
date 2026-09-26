//! An in-process S3 protocol harness.
//!
//! Fuzzing and property-testing the request surface needs the *real* dispatch —
//! routing, signature verification, XML deserialization, error encoding — not a
//! reimplementation of it. This harness builds the same `S3Service` the binary
//! serves (same handler, credentials, access policy and `s3s` flags as
//! `server::run`) and calls it with raw requests, so a property test or a fuzz
//! target drives exactly the code path a socket would, without a listener.
//!
//! Requests are signed here with `s3s-sigv4` — the same crate the service
//! verifies them with — so a test can send a valid request, then damage exactly
//! one part of it and assert the response the client would see.

use std::sync::Arc;

use bytes::Bytes;
use hyper::http::{Method, Request, StatusCode, Uri};
use s3s::HttpRequest;

use crate::auth::{Credential, CredentialStore};
use crate::driver::{DriverFactory, Profile, Secret};
use crate::integrity::{compute_checksum, hex};
use crate::protocol::s3::{S3Body as Body, S3Service};
use crate::service::ObjectService;

use super::fixture::{self, BucketSpec};
use super::reference::ReferenceDriverFactory;

/// The access key the harness signs with.
pub const ACCESS_KEY: &str = "HARNESACCESSKEY";
/// The secret the harness signs with.
pub const SECRET_KEY: &str = "harness-secret";
/// The region the harness signs for.
pub const REGION: &str = "us-east-1";
/// The bucket every harness test may use.
pub const BUCKET: &str = "harness-a";

/// The host the harness addresses, which it also signs.
const HOST: &str = "127.0.0.1:9000";

/// A response as a client would see it.
#[derive(Debug, Clone)]
pub struct ProtocolResponse {
    /// The HTTP status.
    pub status: StatusCode,
    /// The response headers.
    pub headers: hyper::HeaderMap,
    /// The body bytes (an S3 error document for a failure).
    pub body: Bytes,
}

impl ProtocolResponse {
    /// Whether the response is a server fault, which a fuzzed request must
    /// never provoke.
    pub fn is_server_error(&self) -> bool {
        self.status.is_server_error()
    }

    /// One response header as text, if present.
    pub fn header(&self, name: &str) -> Option<String> {
        self.headers
            .get(name)
            .and_then(|value| value.to_str().ok())
            .map(str::to_string)
    }

    /// Whether the body carries `code` as its S3 error code.
    pub fn has_code(&self, code: &str) -> bool {
        let needle = format!("<Code>{code}</Code>");
        std::str::from_utf8(&self.body).is_ok_and(|text| text.contains(&needle))
    }

    /// The body as text, for diagnostics.
    pub fn text(&self) -> String {
        String::from_utf8_lossy(&self.body).to_string()
    }
}

/// The real S3 service, callable in process.
pub struct ProtocolHarness {
    service: S3Service,
    objects: ObjectService,
}

impl ProtocolHarness {
    /// Builds the service over the in-memory reference driver.
    pub async fn new() -> Self {
        Self::build(Arc::new(ReferenceDriverFactory::new("reference"))).await
    }

    /// Builds the same service over a reference driver that declares native
    /// multipart.
    ///
    /// The native route is otherwise never reached through real dispatch, so
    /// signature verification, XML encoding and the adapter's flags for
    /// multipart would be proven only on the coordinator. This harness closes
    /// that gap without a listener.
    pub async fn new_native_multipart() -> Self {
        Self::build(Arc::new(ReferenceDriverFactory::native_multipart(
            "reference",
        )))
        .await
    }

    async fn build(factory: Arc<dyn DriverFactory>) -> Self {
        let objects = fixture::service(
            vec![("inst-1", factory)],
            vec![BucketSpec {
                bucket: BUCKET,
                driver: "inst-1",
                namespace: "ns-harness",
                profile: Profile::CoreWrite,
            }],
            fixture::default_limits(),
            fixture::full_access(),
        )
        .await;

        let credentials = Arc::new(
            CredentialStore::new(vec![Credential {
                access_key: ACCESS_KEY.to_string(),
                secret: Secret::new(SECRET_KEY),
                principal: "tester".to_string(),
            }])
            .expect("one credential"),
        );
        let policy = Arc::new(fixture::full_access());
        // The same wiring the server uses, over the same limits: the adapter
        // cannot diverge from the service it fronts (see `crate::compose`).
        let service = crate::compose::s3_service(
            objects.clone(),
            credentials,
            policy,
            &fixture::default_limits(),
        );
        Self { service, objects }
    }

    /// The object service behind the protocol, for arranging state that is not
    /// the subject under test.
    pub fn objects(&self) -> &ObjectService {
        &self.objects
    }

    /// Signs `method target` the way an SDK would, with `body` and any extra
    /// headers included in the signature.
    pub fn signed(
        &self,
        method: &str,
        target: &str,
        body: impl AsRef<[u8]>,
        extra: &[(&str, &str)],
    ) -> HttpRequest {
        let body = Bytes::copy_from_slice(body.as_ref());
        let (amz, amz_text) = amz_date();
        let payload_hash = hex(&compute_checksum(
            crate::domain::ChecksumAlgorithm::Sha256,
            &body,
        ));

        let mut headers: Vec<(String, String)> = vec![
            ("content-length".to_string(), body.len().to_string()),
            ("host".to_string(), HOST.to_string()),
            ("x-amz-content-sha256".to_string(), payload_hash.clone()),
            ("x-amz-date".to_string(), amz_text),
        ];
        for (name, value) in extra {
            headers.push((name.to_ascii_lowercase(), (*value).to_string()));
        }
        headers.sort();

        let pairs: Vec<(&str, &str)> = headers
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str()))
            .collect();
        let query = query_pairs(target);
        let canonical = s3s_sigv4::create_canonical_request(
            method,
            Uri::try_from(target).expect("valid target").path(),
            &query,
            &pairs,
            s3s_sigv4::Payload::SingleChunk(&payload_hash),
        );
        let string_to_sign = s3s_sigv4::create_string_to_sign(&canonical, &amz, REGION, "s3");
        let signing_key = s3s_sigv4::derive_signing_key(SECRET_KEY, &amz, REGION, "s3");
        let signature = hex(&s3s_sigv4::calculate_signature_with_key(
            &string_to_sign,
            &signing_key,
        ));
        let signed_names = headers
            .iter()
            .map(|(name, _)| name.as_str())
            .collect::<Vec<_>>()
            .join(";");
        let authorization = format!(
            "AWS4-HMAC-SHA256 Credential={ACCESS_KEY}/{}/{REGION}/s3/aws4_request, \
             SignedHeaders={signed_names}, Signature={signature}",
            amz.fmt_date()
        );

        let mut builder = Request::builder()
            .method(Method::from_bytes(method.as_bytes()).expect("valid method"))
            .uri(target);
        for (name, value) in &headers {
            builder = builder.header(name.as_str(), value.as_str());
        }
        builder = builder.header("authorization", authorization);
        builder.body(Body::from(body)).expect("valid request")
    }

    /// A request with no signature at all.
    pub fn unsigned(&self, method: &str, target: &str, body: impl AsRef<[u8]>) -> HttpRequest {
        let body = Bytes::copy_from_slice(body.as_ref());
        Request::builder()
            .method(Method::from_bytes(method.as_bytes()).expect("valid method"))
            .uri(target)
            .header("host", HOST)
            .header("content-length", body.len().to_string())
            .body(Body::from(body))
            .expect("valid request")
    }

    /// Sends a prepared request and reads the whole response.
    pub async fn send(&self, request: HttpRequest) -> ProtocolResponse {
        match self.service.call(request).await {
            Ok(response) => {
                let (parts, body) = response.into_parts();
                let bytes = http_body_util::BodyExt::collect(body)
                    .await
                    .map(|collected| collected.to_bytes())
                    .unwrap_or_default();
                ProtocolResponse {
                    status: parts.status,
                    headers: parts.headers,
                    body: bytes,
                }
            }
            // Mirrors `server::internal_error`: a transport-level failure is not
            // an S3 error document.
            Err(_) => ProtocolResponse {
                status: StatusCode::INTERNAL_SERVER_ERROR,
                headers: hyper::HeaderMap::new(),
                body: Bytes::from_static(b"internal error"),
            },
        }
    }

    /// Signs and sends in one step.
    pub async fn call(
        &self,
        method: &str,
        target: &str,
        body: impl AsRef<[u8]>,
        extra: &[(&str, &str)],
    ) -> ProtocolResponse {
        let request = self.signed(method, target, body, extra);
        self.send(request).await
    }
}

/// An empty request body, for requests that carry none.
pub fn empty_body() -> Body {
    Body::empty()
}

/// The current instant in `AmzDate` form, which the service checks for skew.
fn amz_date() -> (s3s_sigv4::AmzDate, String) {
    let now = time::OffsetDateTime::now_utc();
    let text = format!(
        "{:04}{:02}{:02}T{:02}{:02}{:02}Z",
        now.year(),
        u8::from(now.month()),
        now.day(),
        now.hour(),
        now.minute(),
        now.second()
    );
    (
        s3s_sigv4::AmzDate::parse(&text).expect("formatted date parses"),
        text,
    )
}

/// The decoded query pairs of a target, which the canonical request encodes.
fn query_pairs(target: &str) -> Vec<(String, String)> {
    let Some((_, query)) = target.split_once('?') else {
        return Vec::new();
    };
    query
        .split('&')
        .filter(|pair| !pair.is_empty())
        .map(|pair| match pair.split_once('=') {
            Some((name, value)) => (decode(name), decode(value)),
            None => (decode(pair), String::new()),
        })
        .collect()
}

/// Percent-decodes one query component.
fn decode(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'%' if index + 2 < bytes.len() => {
                let hex = std::str::from_utf8(&bytes[index + 1..index + 3]).unwrap_or("00");
                match u8::from_str_radix(hex, 16) {
                    Ok(byte) => {
                        out.push(byte);
                        index += 3;
                    }
                    Err(_) => {
                        out.push(bytes[index]);
                        index += 1;
                    }
                }
            }
            byte => {
                out.push(byte);
                index += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).to_string()
}
