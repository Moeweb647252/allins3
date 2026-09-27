//! The 139 request signature.
//!
//! Every call to the drive API carries an `mcloud-sign` header of the form
//! `"<timestamp>,<random>,<sign>"`, where `<sign>` is derived from the request
//! body according to the algorithm the web client uses (reverse-engineered and
//! verified against captured traffic; see `docs/yun139-api.md`):
//!
//! ```text
//! s    = sort_chars(encodeURIComponent(JSON.stringify(body)))     // "" when no body
//! r    = md5_hex(base64(s))
//! l    = md5_hex(timestamp + ":" + random)
//! sign = md5_hex(r + l).to_uppercase()
//! ```
//!
//! The character sort makes the signature independent of JSON key order, so a
//! serde map and the browser's object literal canonicalise identically.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use md5::{Digest as _, Md5};

/// The length of the random nonce (`getRandomSring(16)`).
const RANDOM_LEN: usize = 16;
/// The alphabet the nonce is drawn from, matching the client byte for byte.
const RANDOM_CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

/// Computes the `mcloud-sign` value for a request.
///
/// `timestamp` is the `YYYY-MM-DD HH:mm:ss` form produced by
/// [`super::stamp::now_shanghai`], and `random` is a [`random_token`].
pub(crate) fn signature(body: Option<&serde_json::Value>, timestamp: &str, random: &str) -> String {
    let canonical = match body {
        // A missing or JSON `null` body signs the empty string, exactly as the
        // client's falsy check on `data` does.
        None => String::new(),
        Some(value) if value.is_null() => String::new(),
        Some(value) => {
            let json = serde_json::to_string(value).unwrap_or_default();
            let mut chars: Vec<char> = encode_uri_component(&json).chars().collect();
            chars.sort_unstable();
            chars.into_iter().collect()
        }
    };

    let body_digest = md5_hex(&crate::integrity::base64(canonical.as_bytes()));
    let key_digest = md5_hex(&format!("{timestamp}:{random}"));
    md5_hex(&format!("{body_digest}{key_digest}")).to_uppercase()
}

/// A fresh 16-character nonce from the client's alphabet.
pub(crate) fn random_token() -> String {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    let mut state = nanos
        ^ (SEQ.fetch_add(1, Ordering::Relaxed)).wrapping_mul(0x9E37_79B9_7F4A_7C15)
        ^ u64::from(std::process::id());

    let mut out = String::with_capacity(RANDOM_LEN);
    for _ in 0..RANDOM_LEN {
        state = splitmix64(state);
        out.push(RANDOM_CHARSET[(state % RANDOM_CHARSET.len() as u64) as usize] as char);
    }
    out
}

/// JavaScript `encodeURIComponent`: percent-encodes every UTF-8 byte except the
/// unreserved set, using uppercase hex.
fn encode_uri_component(value: &str) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(value.len());
    for &byte in value.as_bytes() {
        let unreserved = byte.is_ascii_alphanumeric()
            || matches!(
                byte,
                b'-' | b'_' | b'.' | b'!' | b'~' | b'*' | b'\'' | b'(' | b')'
            );
        if unreserved {
            out.push(byte as char);
        } else {
            let _ = write!(out, "%{byte:02X}");
        }
    }
    out
}

/// Lowercase hex MD5 of a UTF-8 string.
fn md5_hex(value: &str) -> String {
    let mut hasher = Md5::new();
    hasher.update(value.as_bytes());
    crate::integrity::hex(&hasher.finalize())
}

/// SplitMix64, used only to spread the nonce bits.
fn splitmix64(seed: u64) -> u64 {
    let mut z = seed.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // The vectors below were captured from the live web client and independently
    // reproduced outside the browser. They pin the whole algorithm.

    #[test]
    fn a_bodyless_request_matches_the_captured_vector() {
        assert_eq!(
            signature(None, "2026-09-27 19:40:57", "0oW3SlEX1rYl0ln8"),
            "F263E15C4344FDE450CD254FE98486E6"
        );
    }

    #[test]
    fn a_signed_body_matches_the_captured_vector() {
        let body = json!({
            "subDTO": { "controlSwitchType": 100, "userDomainId": "1039903047986028223" }
        });
        assert_eq!(
            signature(Some(&body), "2026-09-27 19:40:58", "JB7Ouq5f3bmVfpXh"),
            "4204E163468D3A3E7F844AFC2837D564"
        );
    }

    #[test]
    fn a_null_body_signs_like_no_body() {
        assert_eq!(
            signature(
                Some(&serde_json::Value::Null),
                "2026-09-27 19:40:57",
                "0oW3SlEX1rYl0ln8"
            ),
            "F263E15C4344FDE450CD254FE98486E6"
        );
    }

    #[test]
    fn key_order_does_not_change_the_signature() {
        // The character sort is what makes this true, and it is what lets a
        // serde map (sorted keys) match a browser object literal.
        let a = json!({ "alpha": 1, "beta": 2 });
        let b = json!({ "beta": 2, "alpha": 1 });
        assert_eq!(
            signature(Some(&a), "2026-01-01 00:00:00", "ABCDEFGHIJKLMNOP"),
            signature(Some(&b), "2026-01-01 00:00:00", "ABCDEFGHIJKLMNOP")
        );
    }

    #[test]
    fn encode_uri_component_matches_javascript() {
        assert_eq!(encode_uri_component("a b"), "a%20b");
        assert_eq!(encode_uri_component("a/b:c"), "a%2Fb%3Ac");
        // The unreserved set is left intact.
        assert_eq!(encode_uri_component("A-z_0.9!~*'()"), "A-z_0.9!~*'()");
        // Non-ASCII is percent-encoded as UTF-8 bytes.
        assert_eq!(encode_uri_component("é"), "%C3%A9");
    }

    #[test]
    fn random_tokens_are_well_formed_and_distinct() {
        let a = random_token();
        let b = random_token();
        assert_eq!(a.len(), RANDOM_LEN);
        assert!(a.bytes().all(|c| RANDOM_CHARSET.contains(&c)));
        assert_ne!(a, b);
    }
}
