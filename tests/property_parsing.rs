//! Property tests over the parsing and routing surfaces.
//!
//! These are deterministic: a small LCG generates the inputs, so a failure is
//! reproducible from the seed printed in the panic message. They cover the
//! boundaries the spec calls out — range resolution, key encoding round trips,
//! digest encoding and continuation-token scoping.

#![cfg(feature = "testing")]

use std::sync::Arc;

use allins3::domain::{ObjectKey, RangeResolution, RangeSpec};
use allins3::driver::{DriverFactory, Profile};
use allins3::integrity::{base64, hex};
use allins3::service::{ReadConditions, ServiceError};
use allins3::testing::fixture::{self, BucketSpec};
use allins3::testing::reference::ReferenceDriverFactory;
use allins3::testing::support::one_shot_stream;

const BUCKET: &str = "bucket-a";
const OTHER: &str = "bucket-b";
const DRIVER: &str = "inst-1";
const ITERATIONS: usize = 500;

/// Decodes the hexadecimal the crate emits (test-local, to avoid a public
/// decode helper that only tests would use).
fn decode_hex(text: &str) -> Vec<u8> {
    text.as_bytes()
        .chunks(2)
        .map(|pair| {
            let high = (pair[0] as char).to_digit(16).expect("hex digit");
            let low = (pair[1] as char).to_digit(16).expect("hex digit");
            ((high << 4) | low) as u8
        })
        .collect()
}

/// A deterministic generator; the seed is printed in every failure message.
struct Lcg(u64);

impl Lcg {
    fn new(seed: u64) -> Self {
        Self(seed)
    }

    /// The next pseudo-random value.
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.0 >> 33
    }

    /// A value in `0..bound`.
    fn below(&mut self, bound: u64) -> u64 {
        if bound == 0 { 0 } else { self.next() % bound }
    }
}

fn ctx() -> allins3::domain::DriverContext {
    fixture::ctx("property")
}

async fn service() -> allins3::service::ObjectService {
    let factory: Arc<dyn DriverFactory> = Arc::new(ReferenceDriverFactory::new("reference"));
    fixture::service(
        vec![(DRIVER, factory)],
        vec![
            BucketSpec {
                bucket: BUCKET,
                driver: DRIVER,
                namespace: "ns-a",
                profile: Profile::CoreWrite,
            },
            BucketSpec {
                bucket: OTHER,
                driver: DRIVER,
                namespace: "ns-b",
                profile: Profile::CoreWrite,
            },
        ],
        fixture::default_limits(),
        fixture::full_access(),
    )
    .await
}

#[test]
fn resolved_ranges_are_always_inside_the_object() {
    let mut rng = Lcg::new(0x5eed);
    for iteration in 0..ITERATIONS {
        let total = 1 + rng.below(4096);
        let start = rng.below(total);
        let end = start + rng.below(total - start);

        let spec = RangeSpec::Closed { start, end };
        match spec.resolve(total) {
            RangeResolution::Full => {
                assert_eq!(start, 0, "seed a full range unless it starts at 0");
                assert_eq!(
                    end,
                    total - 1,
                    "seed a full range unless it ends at the end"
                );
            }
            RangeResolution::Partial(range) => {
                assert!(range.start() <= range.end_inclusive());
                assert!(
                    range.end_inclusive() < total,
                    "iteration {iteration}: range {range:?} escapes an object of {total} bytes"
                );
                assert_eq!(range.len(), range.end_inclusive() - range.start() + 1);
                assert_eq!(range.total_len(), total);
            }
            RangeResolution::Unsatisfiable => {
                panic!("iteration {iteration}: start {start} < total {total} must be satisfiable");
            }
        }
    }
}

#[test]
fn a_start_past_the_end_is_never_satisfiable() {
    let mut rng = Lcg::new(0x1a2b3c);
    for _ in 0..ITERATIONS {
        let total = 1 + rng.below(1024);
        let start = total + rng.below(1024);
        assert_eq!(
            RangeSpec::Open { start }.resolve(total),
            RangeResolution::Unsatisfiable
        );
    }
    // An empty object has no satisfiable range at all.
    assert_eq!(
        RangeSpec::Closed { start: 0, end: 0 }.resolve(0),
        RangeResolution::Unsatisfiable
    );
}

#[test]
fn object_keys_round_trip_exactly() {
    let mut rng = Lcg::new(0xc0ffee);
    let alphabet: Vec<char> = "ab/\\+ .-_~\u{00e9}\u{4e2d}!*'()".chars().collect();
    for iteration in 0..ITERATIONS {
        let len = 1 + rng.below(40) as usize;
        let candidate: String = (0..len)
            .map(|_| alphabet[rng.below(alphabet.len() as u64) as usize])
            .collect();

        match ObjectKey::parse(candidate.clone()) {
            Ok(key) => assert_eq!(
                key.as_str(),
                candidate,
                "iteration {iteration}: key bytes changed"
            ),
            Err(_) => {
                // Only genuinely invalid keys may be rejected: over-long keys,
                // empty keys and control characters.
                let too_long = candidate.len() > 1024;
                let has_control = candidate.chars().any(char::is_control);
                assert!(
                    too_long || has_control || candidate.is_empty(),
                    "iteration {iteration}: `{candidate}` was rejected but looks valid"
                );
            }
        }
    }
}

#[test]
fn control_characters_round_trip_without_normalisation() {
    // S3 keys are an opaque byte space: control characters are legal and must
    // survive unchanged, not be stripped or rejected (spec §4.2).
    for code in 0..ITERATIONS as u32 {
        let mut candidate = String::from("prefix");
        candidate.push(char::from_u32(code % 0x20).expect("control code point"));
        candidate.push_str("suffix");
        let key = ObjectKey::parse(candidate.clone()).expect("keys keep their bytes");
        assert_eq!(key.as_str(), candidate);
        assert_eq!(key.as_bytes(), candidate.as_bytes());
    }

    // The only rejections are an empty key and one over the byte limit.
    assert!(ObjectKey::parse("").is_err());
    assert!(ObjectKey::parse("a".repeat(1024)).is_ok());
    assert!(ObjectKey::parse("a".repeat(1025)).is_err());
    // A trailing slash is significant and is not merged away.
    assert_ne!(
        ObjectKey::parse("a/").expect("key"),
        ObjectKey::parse("a").expect("key")
    );
    assert_ne!(
        ObjectKey::parse("a//b").expect("key"),
        ObjectKey::parse("a/b").expect("key")
    );
}

#[test]
fn digest_encodings_round_trip() {
    let mut rng = Lcg::new(0xd1e5);
    for iteration in 0..ITERATIONS {
        let len = rng.below(64) as usize;
        let bytes: Vec<u8> = (0..len).map(|_| rng.below(256) as u8).collect();

        let text = hex(&bytes);
        assert_eq!(text.len(), bytes.len() * 2);
        assert_eq!(
            decode_hex(&text),
            bytes,
            "iteration {iteration}: hex did not round trip"
        );

        let encoded = base64(&bytes);
        use base64::Engine as _;
        assert_eq!(
            base64::engine::general_purpose::STANDARD
                .decode(encoded.as_bytes())
                .expect("base64 decodes"),
            bytes,
            "iteration {iteration}: base64 did not round trip"
        );
    }
}

#[tokio::test]
async fn continuation_tokens_are_bound_to_their_scope() {
    let service = service().await;
    for index in 0..6 {
        service
            .put_object(
                &ctx(),
                BUCKET,
                &ObjectKey::parse(format!("scope/{index:02}.txt")).expect("key"),
                one_shot_stream(bytes::Bytes::from_static(b"x")),
                Default::default(),
            )
            .await
            .expect("seed object");
    }

    let first = service
        .list_objects_v2(
            &ctx(),
            BUCKET,
            Some("tester"),
            Some("scope/".to_string()),
            None,
            None,
            None,
            None,
            Some(2),
        )
        .await
        .expect("first page");
    assert!(first.is_truncated);
    let token = first.next_token.clone().expect("a token for the next page");

    // The same page is reachable again with its own token.
    let second = service
        .list_objects_v2(
            &ctx(),
            BUCKET,
            Some("tester"),
            Some("scope/".to_string()),
            None,
            None,
            Some(token.clone()),
            None,
            Some(2),
        )
        .await
        .expect("second page");
    assert_eq!(second.objects.len(), 2);
    assert!(
        second
            .objects
            .iter()
            .all(|o| o.key.as_str() != "scope/00.txt"),
        "the second page repeated the first page"
    );

    // A token must not be replayable against another bucket...
    let wrong_bucket = service
        .list_objects_v2(
            &ctx(),
            OTHER,
            Some("tester"),
            Some("scope/".to_string()),
            None,
            None,
            Some(token.clone()),
            None,
            Some(2),
        )
        .await
        .expect_err("a token must be bound to its bucket");
    assert!(matches!(wrong_bucket, ServiceError::InvalidArgument(_)));

    // ...nor against a different prefix or delimiter...
    for (prefix, delimiter) in [(Some("other/".to_string()), None), (None, Some('/'))] {
        let mismatched = service
            .list_objects_v2(
                &ctx(),
                BUCKET,
                Some("tester"),
                prefix,
                delimiter,
                None,
                Some(token.clone()),
                None,
                Some(2),
            )
            .await
            .expect_err("a token must be bound to its listing scope");
        assert!(matches!(mismatched, ServiceError::InvalidArgument(_)));
    }

    // ...nor may a forged token be accepted.
    let forged = service
        .list_objects_v2(
            &ctx(),
            BUCKET,
            Some("tester"),
            Some("scope/".to_string()),
            None,
            None,
            Some("forged-token".to_string()),
            None,
            Some(2),
        )
        .await
        .expect_err("a forged token must be rejected");
    assert!(matches!(forged, ServiceError::InvalidArgument(_)));

    // A full listing never repeats a key.
    let mut seen = Vec::new();
    let mut cursor = None;
    loop {
        let page = service
            .list_objects_v2(
                &ctx(),
                BUCKET,
                Some("tester"),
                Some("scope/".to_string()),
                None,
                None,
                cursor.clone(),
                None,
                Some(2),
            )
            .await
            .expect("page");
        seen.extend(page.objects.iter().map(|o| o.key.as_str().to_string()));
        match page.next_token {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }
    let mut unique = seen.clone();
    unique.sort();
    unique.dedup();
    assert_eq!(seen.len(), unique.len(), "pagination repeated a key");
    assert_eq!(unique.len(), 6, "pagination lost keys: {seen:?}");

    // Reading a key the caller cannot see must not be possible either.
    assert!(
        service
            .get_object(
                &ctx(),
                OTHER,
                &ObjectKey::parse("scope/00.txt").expect("key"),
                &ReadConditions::default(),
                None
            )
            .await
            .is_err()
    );
}
