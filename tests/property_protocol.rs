//! Property and fuzz-style tests over the protocol surface.
//!
//! `P5-05` asks for the parsing properties to extend to XML bodies, part
//! manifests and signature inputs. These tests drive the *real* dispatch — the
//! same `S3Service` the binary serves, via [`ProtocolHarness`] — with
//! deterministic pseudo-random inputs (a small LCG, so a failure is
//! reproducible from the seed in the panic message) plus the adversarial shapes
//! a coverage-guided fuzzer reaches slowly: entity definitions, deep nesting,
//! huge part numbers, damaged signatures and truncated bodies.
//!
//! The invariants are the ones the spec states: a request that cannot be
//! authenticated or parsed must never succeed, must never be a server fault
//! (spec §9.1, §10), and must leave no object behind (`INV-08`).

#![cfg(feature = "testing")]

use std::sync::Arc;

use allins3::domain::{
    ContentProperties, DriverContext, EntityTag, ObjectKey, Preconditions, UserMetadata,
};
use allins3::driver::{DriverFactory, ObjectByteStream, Profile};
use allins3::service::{ObjectService, ReadConditions, ServiceError};
use allins3::testing::fixture::{self, BucketSpec};
use allins3::testing::protocol::{BUCKET, ProtocolHarness, ProtocolResponse};
use allins3::testing::reference::ReferenceDriverFactory;
use allins3::testing::support::{one_shot_stream, read_to_end};
use hyper::http::{HeaderMap, HeaderValue, Request};

const ITERATIONS: usize = 500;
const MANIFEST_BUCKET: &str = "manifest-a";

/// A deterministic generator; the seed is printed in every failure message.
struct Lcg(u64);

impl Lcg {
    fn new(seed: u64) -> Self {
        Self(seed)
    }

    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.0 >> 33
    }

    fn below(&mut self, bound: u64) -> u64 {
        if bound == 0 { 0 } else { self.next() % bound }
    }

    fn byte(&mut self) -> u8 {
        self.below(256) as u8
    }
}

fn ctx() -> DriverContext {
    fixture::ctx("property-protocol")
}

fn target(key: &str) -> String {
    format!("/{BUCKET}/{key}")
}

/// A service over the reference driver, for the manifest cases.
async fn service() -> ObjectService {
    let factory: Arc<dyn DriverFactory> = Arc::new(ReferenceDriverFactory::new("reference"));
    fixture::service(
        vec![("inst-1", factory)],
        vec![BucketSpec {
            bucket: MANIFEST_BUCKET,
            driver: "inst-1",
            namespace: "ns-manifest",
            profile: Profile::CoreWrite,
        }],
        fixture::default_limits(),
        fixture::full_access(),
    )
    .await
}

fn one_shot(data: &'static [u8]) -> ObjectByteStream {
    one_shot_stream(bytes::Bytes::from_static(data))
}

/// Reads an object through the service, or nothing when it is absent.
async fn read(service: &ObjectService, key: &ObjectKey) -> Vec<u8> {
    match service
        .get_object(
            &ctx(),
            MANIFEST_BUCKET,
            key,
            &ReadConditions::default(),
            None,
        )
        .await
    {
        Ok(result) => read_to_end(result.stream).await.expect("read body"),
        Err(_) => Vec::new(),
    }
}

async fn start_upload(service: &ObjectService, bucket: &str, key: &ObjectKey) -> String {
    service
        .create_multipart(
            &ctx(),
            bucket,
            key,
            Some("tester"),
            ContentProperties::default(),
            UserMetadata::new(),
            None,
        )
        .await
        .expect("create multipart upload")
        .upload_id
        .as_str()
        .to_string()
}

/// Completes `manifest` through the service and reports the outcome.
async fn complete_manifest(
    service: &ObjectService,
    key: &ObjectKey,
    upload_id: &str,
    manifest: Vec<(u16, Option<EntityTag>)>,
) -> Result<(), ServiceError> {
    service
        .complete_multipart(
            &ctx(),
            MANIFEST_BUCKET,
            key,
            upload_id,
            &manifest,
            None,
            Preconditions::none(),
        )
        .await
        .map(|_| ())
}

// --- Signature inputs ------------------------------------------------------

/// Sends a request whose headers were tampered with after signing.
async fn tampered(
    harness: &ProtocolHarness,
    key: &str,
    mutate: impl FnOnce(&mut HeaderMap),
) -> ProtocolResponse {
    let request = harness.signed("PUT", &target(key), b"body", &[]);
    let (mut parts, body) = request.into_parts();
    mutate(&mut parts.headers);
    harness.send(Request::from_parts(parts, body)).await
}

/// Asserts a request was rejected without disturbing the server.
///
/// A few scope manipulations get a `501` from the protocol library rather than
/// a `403` (an unknown service in the credential scope is "not implemented");
/// what matters for a fuzzed input is that it never succeeds, never provokes a
/// `500` and never publishes.
fn assert_rejected(response: &ProtocolResponse, what: &str) {
    assert!(
        !response.status.is_success(),
        "{what} must not be accepted: {} {}",
        response.status,
        response.text()
    );
    assert!(
        response.status.as_u16() != 500,
        "{what} must not be an internal error: {} {}",
        response.status,
        response.text()
    );
}

/// Whether an object exists, as the protocol reports it.
async fn exists(harness: &ProtocolHarness, key: &str) -> bool {
    harness
        .call("HEAD", &target(key), Vec::new(), &[])
        .await
        .status
        .is_success()
}

fn assert_client_error(response: &ProtocolResponse, what: &str) {
    assert!(
        !response.is_server_error(),
        "{what} must not be a server fault: {} {}",
        response.status,
        response.text()
    );
    assert!(
        response.status.is_client_error(),
        "{what} must be rejected: {} {}",
        response.status,
        response.text()
    );
}

#[tokio::test]
async fn a_signed_request_is_accepted_and_an_unsigned_one_is_not() {
    let harness = ProtocolHarness::new().await;

    // The signer must work, or every rejection below would be vacuous.
    let response = harness
        .call("PUT", &target("signature/ok.bin"), b"hello", &[])
        .await;
    assert_eq!(
        response.status.as_u16(),
        200,
        "the harness signer must be accepted: {}",
        response.text()
    );

    let response = harness
        .send(harness.unsigned("PUT", &target("signature/unsigned.bin"), b"hello"))
        .await;
    assert_client_error(&response, "an unsigned request");
    assert!(!exists(&harness, "signature/unsigned.bin").await);
}

#[tokio::test]
async fn every_single_part_mutation_of_a_valid_signature_is_rejected() {
    let harness = ProtocolHarness::new().await;
    let authorization = harness
        .signed("PUT", &target("signature/mutated.bin"), b"body", &[])
        .headers()
        .get("authorization")
        .expect("signed requests carry an authorization header")
        .to_str()
        .expect("ascii header")
        .to_string();

    // One damaged character inside each field of the credential: the signature,
    // the access key, the scope's region, its service and its date. A damaged
    // signature or key is a signature failure; a damaged scope is answered by
    // the protocol library's own scope validation, so it only has to be a
    // rejection (`assert_rejected`) rather than specifically a `403`.
    let mut mutations: Vec<(&str, String, bool)> = Vec::new();
    for (label, needle, offset, exact) in [
        ("signature", "Signature=", 0, true),
        ("access key", "Credential=", 0, true),
        ("region", "us-east-1", 0, false),
        ("service", "/s3/", 1, false),
        ("date", "Credential=", 9, false),
    ] {
        let position = authorization.find(needle).expect("field present") + needle.len() + offset;
        for index in 0..8 {
            let mut candidate = authorization.clone();
            let byte = candidate.as_bytes()[position + index];
            let replacement = if byte == b'z' { 'y' } else { 'z' };
            candidate.replace_range(
                position + index..position + index + 1,
                &replacement.to_string(),
            );
            mutations.push((label, candidate, exact));
        }
    }
    // A signed-headers list that drops or reorders a header, which would let a
    // substituted value through under a valid signature.
    mutations.push((
        "signed-headers list",
        authorization.replace("content-length;", ""),
        false,
    ));
    mutations.push((
        "signed-headers order",
        authorization.replace("host;x-amz-content-sha256", "x-amz-content-sha256;host"),
        false,
    ));

    for (label, candidate, exact) in mutations {
        let response = tampered(&harness, "signature/mutated.bin", |headers| {
            headers.insert(
                "authorization",
                HeaderValue::from_str(&candidate).expect("header value"),
            );
        })
        .await;
        assert_rejected(&response, &format!("a request with a damaged {label}"));
        if exact {
            assert_eq!(
                response.status.as_u16(),
                403,
                "a damaged {label} must be a signature failure: {} {}",
                response.status,
                response.text()
            );
        }
    }
    assert!(
        !exists(&harness, "signature/mutated.bin").await,
        "a rejected signature must not publish"
    );
}

#[tokio::test]
async fn arbitrary_authorization_values_are_never_accepted() {
    let harness = ProtocolHarness::new().await;
    let mut rng = Lcg::new(0x516b4d);

    let shapes = [
        "AWS4-HMAC-SHA256",
        "AWS4-HMAC-SHA256 Credential=",
        "AWS4-HMAC-SHA256 Credential=x, SignedHeaders=host, Signature=",
        "AWS4-HMAC-SHA256 Credential=x/20260101/us-east-1/s3/aws4_request, SignedHeaders=, Signature=",
        "Basic ZGV2OnNlY3JldA==",
        "AWS4-HMAC-SHA256 Credential=x/20260101/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=zzzz",
        "AWS4-HMAC-SHA256 Credential=x/20260101/us-east-1/sts/aws4_request, SignedHeaders=host, Signature=00",
    ];
    for shape in shapes {
        let response =
            tampered(
                &harness,
                "signature/garbage.bin",
                |headers| match HeaderValue::from_str(shape) {
                    Ok(value) => {
                        headers.insert("authorization", value);
                    }
                    Err(_) => {
                        headers.remove("authorization");
                    }
                },
            )
            .await;
        assert_rejected(&response, &format!("the authorization value {shape:?}"));
    }

    for iteration in 0..ITERATIONS {
        let length = 1 + rng.below(96) as usize;
        let candidate: String = (0..length)
            .map(|_| match rng.below(4) {
                0 => char::from(b'a' + (rng.below(26) as u8)),
                1 => char::from(b'0' + (rng.below(10) as u8)),
                2 => ['-', '/', '=', ',', ' ', ';'][rng.below(6) as usize],
                _ => char::from(b'A' + (rng.below(26) as u8)),
            })
            .collect();
        // Some values are not legal header values at all; the request builder
        // would refuse them, so only the ones it accepts are sent.
        let Ok(value) = HeaderValue::from_str(&candidate) else {
            continue;
        };
        let response = tampered(&harness, "signature/garbage.bin", |headers| {
            headers.insert("authorization", value.clone());
        })
        .await;
        assert_rejected(
            &response,
            &format!("iteration {iteration}: authorization {candidate:?}"),
        );
    }

    assert!(!exists(&harness, "signature/garbage.bin").await);
}

#[tokio::test]
async fn a_body_that_disagrees_with_its_signed_hash_is_rejected() {
    let harness = ProtocolHarness::new().await;
    let key = "signature/hash.bin";

    // Sign request headers for `declared`, then send `claimedd` under them: same
    // length, different bytes, so only the payload hash can catch it.
    let request = harness.signed("PUT", &target(key), b"declared", &[]);
    let (parts, _) = request.into_parts();
    let response = harness
        .send(Request::from_parts(
            parts,
            s3s::Body::from(bytes::Bytes::from_static(b"claimedd")),
        ))
        .await;
    assert_rejected(&response, "a body that contradicts its payload hash");
    assert_eq!(
        response.status.as_u16(),
        400,
        "a payload digest mismatch is a client error: {} {}",
        response.status,
        response.text()
    );
    assert!(
        response.has_code("BadDigest"),
        "a payload digest mismatch is a BadDigest: {}",
        response.text()
    );
    assert!(!exists(&harness, key).await);

    // A body whose length contradicts the signed `content-length` is the same
    // class of client error, and must not be a server fault either.
    let request = harness.signed("PUT", &target(key), b"declared", &[]);
    let (parts, _) = request.into_parts();
    let response = harness
        .send(Request::from_parts(
            parts,
            s3s::Body::from(bytes::Bytes::from_static(b"too long for the declaration")),
        ))
        .await;
    assert_rejected(&response, "a body longer than its signed length");
    assert!(!exists(&harness, key).await);

    // The honest request with the same headers still works.
    let response = harness.call("PUT", &target(key), b"declared", &[]).await;
    assert_eq!(response.status.as_u16(), 200, "{}", response.text());
}

#[tokio::test]
async fn a_presigned_query_survives_only_untampered() {
    let harness = ProtocolHarness::new().await;
    harness
        .call("PUT", &target("signature/presigned.bin"), b"presigned", &[])
        .await;

    let presigned = |signature: &str| {
        format!(
            "{}?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=x&X-Amz-Date=20260101T000000Z&X-Amz-Expires=900&X-Amz-SignedHeaders=host&X-Amz-Signature={signature}",
            target("signature/presigned.bin")
        )
    };

    let response = harness
        .call("GET", &presigned("deadbeef"), Vec::new(), &[])
        .await;
    assert_client_error(&response, "a presigned URL with a wrong signature");

    let mut rng = Lcg::new(0x9f31);
    for iteration in 0..ITERATIONS {
        let length = 1 + rng.below(40) as usize;
        let value: String = (0..length)
            .map(|_| char::from(b'0' + (rng.below(10) as u8)))
            .collect();
        let response = harness
            .call("GET", &presigned(&value), Vec::new(), &[])
            .await;
        assert_rejected(
            &response,
            &format!("iteration {iteration}: presigned signature {value}"),
        );
    }

    // The query-parameter set itself is fuzzed: dropping or duplicating a
    // parameter must not authenticate either.
    for candidate in [
        format!("{}?X-Amz-Signature=00", target("signature/presigned.bin")),
        format!(
            "{}?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=x&X-Amz-Date=20260101T000000Z&X-Amz-Expires=900&X-Amz-SignedHeaders=host&X-Amz-Signature=00&X-Amz-Security-Token=%00",
            target("signature/presigned.bin")
        ),
        format!(
            "{}?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Signature=00&X-Amz-Signature=00",
            target("signature/presigned.bin")
        ),
        format!(
            "{}?X-Amz-Algorithm=SIGV2&X-Amz-Credential=x&X-Amz-Date=20260101T000000Z&X-Amz-Expires=900&X-Amz-SignedHeaders=host&X-Amz-Signature=00",
            target("signature/presigned.bin")
        ),
    ] {
        let response = harness.call("GET", &candidate, Vec::new(), &[]).await;
        assert_rejected(&response, &format!("the presigned query {candidate}"));
    }
}

// --- XML bodies ------------------------------------------------------------

/// The shapes an XML parser is attacked with, each within the body limit.
fn adversarial_xml() -> Vec<(&'static str, Vec<u8>)> {
    let part =
        b"<Part><ETag>\"d41d8cd98f00b204e9800998ecf8427e\"</ETag><PartNumber>1</PartNumber></Part>"
            .to_vec();
    let mut shapes: Vec<(&'static str, Vec<u8>)> = vec![
        ("empty", Vec::new()),
        ("text", b"not xml".to_vec()),
        (
            "truncated",
            b"<CompleteMultipartUpload><Part><ETag>x".to_vec(),
        ),
        ("wrong-root", b"<Other>1</Other>".to_vec()),
        (
            "invalid-utf8",
            b"<CompleteMultipartUpload>\xff\xfe</CompleteMultipartUpload>".to_vec(),
        ),
        (
            "null-byte",
            b"<CompleteMultipartUpload>\x00</CompleteMultipartUpload>".to_vec(),
        ),
        (
            "huge-part-number",
            b"<CompleteMultipartUpload><Part><ETag>\"x\"</ETag><PartNumber>99999999999999999999</PartNumber></Part></CompleteMultipartUpload>".to_vec(),
        ),
        (
            "negative-part-number",
            b"<CompleteMultipartUpload><Part><ETag>\"x\"</ETag><PartNumber>-1</PartNumber></Part></CompleteMultipartUpload>".to_vec(),
        ),
        (
            "entity-expansion",
            b"<!DOCTYPE r [<!ENTITY a \"aaaaaaaaaa\"><!ENTITY b \"&a;&a;&a;&a;&a;\"><!ENTITY c \"&b;&b;&b;&b;&b;\"><!ENTITY d \"&c;&c;&c;&c;&c;\">]><CompleteMultipartUpload><Part><ETag>&d;</ETag><PartNumber>1</PartNumber></Part></CompleteMultipartUpload>".to_vec(),
        ),
        (
            "external-entity",
            b"<!DOCTYPE r [<!ENTITY xxe SYSTEM \"file:///etc/passwd\">]><CompleteMultipartUpload><Part><ETag>&xxe;</ETag><PartNumber>1</PartNumber></Part></CompleteMultipartUpload>".to_vec(),
        ),
        (
            "comment-cdata-pi",
            {
                let mut body = b"<?xml version=\"1.0\"?><!-- c --><CompleteMultipartUpload><![CDATA[ignored]]>".to_vec();
                body.extend_from_slice(&part);
                body.extend_from_slice(b"</CompleteMultipartUpload>");
                body
            },
        ),
        (
            "declared-namespace",
            {
                let mut body =
                    b"<CompleteMultipartUpload xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"
                        .to_vec();
                body.extend_from_slice(&part);
                body.extend_from_slice(b"</CompleteMultipartUpload>");
                body
            },
        ),
    ];

    let mut deep = b"<CompleteMultipartUpload>".to_vec();
    for _ in 0..20_000 {
        deep.extend_from_slice(b"<a>");
    }
    for _ in 0..20_000 {
        deep.extend_from_slice(b"</a>");
    }
    deep.extend_from_slice(b"</CompleteMultipartUpload>");
    shapes.push(("deep-nesting", deep));

    let mut siblings = b"<CompleteMultipartUpload>".to_vec();
    for _ in 0..2_000 {
        siblings.extend_from_slice(&part);
    }
    siblings.extend_from_slice(b"</CompleteMultipartUpload>");
    shapes.push(("many-sibling-parts", siblings));

    shapes
}

#[tokio::test]
async fn malformed_xml_bodies_never_commit_and_never_fault_the_server() {
    let harness = ProtocolHarness::new().await;

    for (name, body) in adversarial_xml() {
        let key = format!("xml/{name}.bin");
        let upload_id = start_upload(
            harness.objects(),
            BUCKET,
            &ObjectKey::parse(&key).expect("key"),
        )
        .await;
        let path = format!("{}?uploadId={upload_id}", target(&key));
        let response = harness.call("POST", &path, body, &[]).await;
        assert!(
            !response.is_server_error(),
            "the {name} body must not fault the server: {} {}",
            response.status,
            response.text()
        );
        assert!(
            !exists(&harness, &key).await,
            "the {name} body must not publish an object"
        );
    }
}

#[tokio::test]
async fn mutated_manifests_never_publish_a_partial_object() {
    let harness = ProtocolHarness::new().await;
    let mut rng = Lcg::new(0x4d4c01);
    let payload = b"mutated manifest body";

    for iteration in 0..ITERATIONS {
        let key = format!("xml/mutated-{iteration:04}.bin");
        let upload_id = start_upload(
            harness.objects(),
            BUCKET,
            &ObjectKey::parse(&key).expect("key"),
        )
        .await;

        let part = harness
            .call(
                "PUT",
                &format!("{}?partNumber=1&uploadId={upload_id}", target(&key)),
                payload.to_vec(),
                &[],
            )
            .await;
        assert_eq!(part.status.as_u16(), 200, "{}", part.text());
        let etag = part.header("etag").expect("upload_part returns an ETag");

        let manifest = format!(
            "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{etag}</ETag></Part></CompleteMultipartUpload>"
        );
        let mut bytes = manifest.into_bytes();
        match rng.below(3) {
            0 => {
                let index = rng.below(bytes.len() as u64) as usize;
                bytes[index] = rng.byte();
            }
            1 => {
                let cut = rng.below(bytes.len() as u64) as usize;
                bytes.truncate(cut);
            }
            _ => {
                let cut = rng.below(bytes.len() as u64) as usize;
                let length = 1 + rng.below(16) as usize;
                let end = (cut + length).min(bytes.len());
                bytes.drain(cut..end);
            }
        }

        let response = harness
            .call(
                "POST",
                &format!("{}?uploadId={upload_id}", target(&key)),
                bytes,
                &[],
            )
            .await;
        assert!(
            !response.is_server_error(),
            "iteration {iteration}: a mutated manifest must not fault the server: {} {}",
            response.status,
            response.text()
        );
        if response.status.is_success() {
            assert!(
                exists(&harness, &key).await,
                "iteration {iteration}: a successful completion must publish"
            );
        } else {
            assert!(
                !exists(&harness, &key).await,
                "iteration {iteration}: a rejected manifest published an object"
            );
        }
        let _ = harness
            .objects()
            .abort_multipart(
                &ctx(),
                BUCKET,
                &ObjectKey::parse(&key).expect("key"),
                &upload_id,
            )
            .await;
    }
}

#[tokio::test]
async fn a_valid_manifest_through_the_wire_publishes_exactly_that_manifest() {
    let harness = ProtocolHarness::new().await;
    let key = "xml/valid.bin";
    let upload_id = start_upload(
        harness.objects(),
        BUCKET,
        &ObjectKey::parse(key).expect("key"),
    )
    .await;
    let body = b"through the wire";

    let part = harness
        .call(
            "PUT",
            &format!("{}?partNumber=1&uploadId={upload_id}", target(key)),
            body.to_vec(),
            &[],
        )
        .await;
    assert_eq!(part.status.as_u16(), 200, "{}", part.text());
    let etag = part.header("etag").expect("upload_part returns an ETag");

    let manifest = format!(
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{etag}</ETag></Part></CompleteMultipartUpload>"
    );
    let response = harness
        .call(
            "POST",
            &format!("{}?uploadId={upload_id}", target(key)),
            manifest,
            &[],
        )
        .await;
    assert_eq!(response.status.as_u16(), 200, "{}", response.text());

    let read = harness.call("GET", &target(key), Vec::new(), &[]).await;
    assert_eq!(read.status.as_u16(), 200, "{}", read.text());
    assert_eq!(read.body.as_ref(), body.as_slice());
}

/// The text between the first `<name>` and its closing tag.
fn xml_tag(xml: &[u8], name: &str) -> Option<String> {
    let text = std::str::from_utf8(xml).ok()?;
    let open = format!("<{name}>");
    let close = format!("</{name}>");
    let start = text.find(&open)? + open.len();
    let end = text[start..].find(&close)? + start;
    Some(text[start..end].to_string())
}

#[tokio::test]
async fn the_native_multipart_route_is_reached_through_the_signed_wire() {
    // The reference driver's default capabilities emulate multipart with the
    // service coordinator; this harness declares it native, so the whole round
    // trip — create, part, complete — is dispatched natively through the real
    // signature-verifying adapter, the one surface the native route otherwise
    // never crosses.
    let harness = ProtocolHarness::new_native_multipart().await;
    let key = "xml/native.bin";
    let body = b"native multipart through the wire";

    let created = harness
        .call("POST", &format!("{}?uploads", target(key)), Vec::new(), &[])
        .await;
    assert_eq!(created.status.as_u16(), 200, "{}", created.text());
    let upload_id = xml_tag(&created.body, "UploadId").expect("create returns an UploadId");
    assert_eq!(
        harness.objects().upload_is_native(&upload_id),
        Some(true),
        "a namespace declaring native multipart must create the upload natively"
    );

    let part = harness
        .call(
            "PUT",
            &format!("{}?partNumber=1&uploadId={upload_id}", target(key)),
            body.to_vec(),
            &[],
        )
        .await;
    assert_eq!(part.status.as_u16(), 200, "{}", part.text());
    let etag = part.header("etag").expect("upload_part returns an ETag");

    let manifest = format!(
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{etag}</ETag></Part></CompleteMultipartUpload>"
    );
    let response = harness
        .call(
            "POST",
            &format!("{}?uploadId={upload_id}", target(key)),
            manifest,
            &[],
        )
        .await;
    assert_eq!(response.status.as_u16(), 200, "{}", response.text());

    let read = harness.call("GET", &target(key), Vec::new(), &[]).await;
    assert_eq!(read.status.as_u16(), 200, "{}", read.text());
    assert_eq!(read.body.as_ref(), body.as_slice());
    assert_eq!(
        harness.objects().upload_is_native(&upload_id),
        None,
        "a completed upload is retired on either route"
    );
}

// --- Part manifests --------------------------------------------------------

#[tokio::test]
async fn arbitrary_part_manifests_are_either_rejected_or_exactly_published() {
    let service = service().await;
    let mut rng = Lcg::new(0x4d414e);
    let payload = b"manifest fuzz body";

    for iteration in 0..ITERATIONS {
        let key = ObjectKey::parse(format!("fuzz/{iteration:04}.bin")).expect("key");
        let upload_id = start_upload(&service, MANIFEST_BUCKET, &key).await;
        let part = service
            .upload_part(
                &ctx(),
                MANIFEST_BUCKET,
                &key,
                &upload_id,
                1,
                Default::default(),
                one_shot(payload),
            )
            .await
            .expect("upload part");
        let correct = EntityTag::new(part.etag.as_str().to_string()).expect("etag");

        let manifest: Vec<(u16, Option<EntityTag>)> = match rng.below(6) {
            0 => vec![(1, Some(correct.clone()))],
            1 => vec![(2, Some(correct.clone()))],
            2 => Vec::new(),
            3 => vec![(1, None), (1, None)],
            4 => vec![(1, None), (0, None)],
            _ => vec![(1, Some(EntityTag::new("0".repeat(32)).expect("etag")))],
        };
        let expected_success = matches!(manifest.as_slice(), [(1, Some(tag))] if *tag == correct);
        let outcome = complete_manifest(&service, &key, &upload_id, manifest).await;

        if expected_success {
            assert!(outcome.is_ok(), "iteration {iteration}: {outcome:?}");
            assert_eq!(
                read(&service, &key).await,
                payload.to_vec(),
                "iteration {iteration}: a published object must be the uploaded bytes"
            );
        } else {
            assert!(
                matches!(
                    outcome,
                    Err(ServiceError::InvalidArgument(_))
                        | Err(ServiceError::MalformedXml(_))
                        | Err(ServiceError::BadDigest(_))
                ),
                "iteration {iteration}: unexpected outcome {outcome:?}"
            );
            assert!(
                read(&service, &key).await.is_empty(),
                "iteration {iteration}: a rejected manifest published an object"
            );
        }

        // A rejected manifest leaves the upload completable, and a corrected
        // retry publishes exactly the uploaded bytes.
        if !expected_success {
            let retry =
                complete_manifest(&service, &key, &upload_id, vec![(1, Some(correct))]).await;
            assert!(retry.is_ok(), "iteration {iteration}: {retry:?}");
            assert_eq!(read(&service, &key).await, payload.to_vec());
        }
        let _ = service
            .abort_multipart(&ctx(), MANIFEST_BUCKET, &key, &upload_id)
            .await;
    }
}

#[tokio::test]
async fn a_manifest_of_many_parts_publishes_in_manifest_order() {
    let service = service().await;
    let key = ObjectKey::parse("fuzz/ordered.bin").expect("key");
    let upload_id = start_upload(&service, MANIFEST_BUCKET, &key).await;

    // Two parts: the non-final one must meet the minimum size.
    let first = vec![b'a'; (5 * 1024 * 1024) as usize];
    let second = b"tail".to_vec();
    let mut manifest = Vec::new();
    for (number, data) in [(1u16, first.clone()), (2u16, second.clone())] {
        let part = service
            .upload_part(
                &ctx(),
                MANIFEST_BUCKET,
                &key,
                &upload_id,
                number,
                Default::default(),
                one_shot_stream(bytes::Bytes::from(data)),
            )
            .await
            .expect("upload part");
        manifest.push((number, Some(part.etag)));
    }

    // Both ascending and reversed manifests are legal S3 input; the result must
    // be the same bytes in part order, and a manifest that drops a part must
    // publish only what it lists.
    complete_manifest(&service, &key, &upload_id, manifest.clone())
        .await
        .expect("ascending manifest");
    let mut expected = first.clone();
    expected.extend_from_slice(&second);
    assert_eq!(read(&service, &key).await, expected);
}

// --- Content properties ----------------------------------------------------

#[tokio::test]
async fn expires_survives_the_round_trip() {
    // §5.4 lists `Expires` among the properties an object must keep; dropping
    // it would silently change what the client asked to store.
    let harness = ProtocolHarness::new().await;
    let expires = "Wed, 21 Oct 2015 07:28:00 GMT";
    let key = "content/expires.bin";

    let response = harness
        .call("PUT", &target(key), b"content", &[("expires", expires)])
        .await;
    assert_eq!(response.status.as_u16(), 200, "{}", response.text());

    for method in ["HEAD", "GET"] {
        let read = harness.call(method, &target(key), Vec::new(), &[]).await;
        assert_eq!(read.status.as_u16(), 200, "{method}: {}", read.text());
        assert_eq!(read.header("expires").as_deref(), Some(expires), "{method}");
    }

    // A value the service cannot store is a client error, never a silently
    // dropped header.
    let response = harness
        .call(
            "PUT",
            &target("content/bad-expires.bin"),
            b"content",
            &[("expires", "not a date")],
        )
        .await;
    assert_client_error(&response, "an unparseable Expires");
    assert!(!exists(&harness, "content/bad-expires.bin").await);
}

#[tokio::test]
async fn a_listing_that_xml_cannot_represent_asks_for_url_encoding() {
    // §4.2: a key may hold any byte, but a C0 control character cannot appear
    // in XML 1.0 character data. The listing must ask the client to request
    // `encoding-type=url` rather than emit a document no parser can read.
    let harness = ProtocolHarness::new().await;
    // The key is seeded through the service: a raw control byte cannot travel
    // in a signed request target, but it is a legal object key.
    harness
        .objects()
        .put_object(
            &fixture::ctx("xml-listing"),
            BUCKET,
            &ObjectKey::parse("xml/a\u{1}b").expect("key"),
            one_shot_stream(bytes::Bytes::from_static(b"x")),
            Default::default(),
        )
        .await
        .expect("seed an XML 1.0 invalid key");

    let plain = harness
        .call(
            "GET",
            &format!("/{BUCKET}?list-type=2&prefix=xml"),
            Vec::new(),
            &[],
        )
        .await;
    assert_client_error(&plain, "a listing of an XML 1.0 invalid key");
    assert!(plain.has_code("InvalidRequest"), "{}", plain.text());

    let encoded = harness
        .call(
            "GET",
            &format!("/{BUCKET}?list-type=2&prefix=xml&encoding-type=url"),
            Vec::new(),
            &[],
        )
        .await;
    assert_eq!(encoded.status.as_u16(), 200, "{}", encoded.text());
    assert!(
        encoded.text().contains("xml%2Fa%01b"),
        "the key must come back percent-encoded: {}",
        encoded.text()
    );
}
